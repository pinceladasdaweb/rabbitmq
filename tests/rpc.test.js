import assert from 'node:assert/strict'
import Rpc from '../src/messaging/rpc.js'
import { test, describe } from 'node:test'
import { EventEmitter } from 'node:events'
import Publisher from '../src/messaging/publisher.js'
import MessageCodec from '../src/messaging/message-codec.js'
import CircuitBreaker from '../src/resilience/circuit-breaker.js'
import ConsumerManager from '../src/consumers/consumer-manager.js'

const silentLogger = { info () {}, warn () {}, error () {}, debug () {} }

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const waitFor = async (predicate, timeoutMs = 2000, label = 'condition') => {
  const start = Date.now()

  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return

    await sleep(5)
  }

  throw new Error(`Timeout waiting for: ${label}`)
}

class FakeChannel extends EventEmitter {
  constructor () {
    super()
    this.consumers = []
    this.published = []
    this.acked = []
    this.nacked = []
    this.consumeSequence = 0
  }

  async consume (queue, callback, options) {
    this.consumers.push({ queue, callback, options })

    return { consumerTag: `tag-${++this.consumeSequence}` }
  }

  publish (exchange, routingKey, content, options, confirmCallback) {
    this.published.push({ exchange, routingKey, content, options })

    if (confirmCallback) setImmediate(() => confirmCallback(null))

    return true
  }

  async prefetch () {}

  ack (msg) {
    this.acked.push(msg)
  }

  nack (msg, allUpTo, requeue) {
    this.nacked.push({ msg, requeue })
  }
}

const createHarness = () => {
  const codec = new MessageCodec({ logger: silentLogger })
  const replyChannel = new FakeChannel()
  const poolChannel = new FakeChannel()
  const consumerChannel = new FakeChannel()

  const channelPool = {
    getDedicatedChannel: async (id) => (id === 'rpc-reply' ? replyChannel : consumerChannel)
  }

  const context = {
    logger: silentLogger,
    codec,
    circuitBreaker: new CircuitBreaker(),
    rateLimiter: undefined,
    maxPriority: 10,
    prefetchCount: 10,
    getExchange: () => ({ name: 'rpc-exchange', type: 'direct' }),
    getChannel: async () => poolChannel,
    getChannelPool: () => channelPool,
    emit: () => {}
  }

  const publisher = new Publisher(context)
  const consumers = new ConsumerManager(context)
  const rpc = new Rpc(context, { publisher, consumers })

  return { rpc, codec, replyChannel, poolChannel, consumerChannel }
}

const deliverReply = async (harness, correlationId, payload, extraHeaders = {}) => {
  const { content, compressed } = await harness.codec.encode(payload)

  await harness.replyChannel.consumers[0].callback({
    content,
    fields: {},
    properties: { correlationId, headers: { 'x-compressed': compressed, ...extraHeaders } }
  })
}

describe('Rpc request()', () => {
  test('resolves with the correlated reply and wires direct reply-to properties', async () => {
    const harness = createHarness()

    const requestPromise = harness.rpc.request('users.get', { id: 42 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    const request = harness.replyChannel.published[0]

    assert.equal(request.exchange, 'rpc-exchange')
    assert.equal(request.routingKey, 'users.get')
    assert.equal(request.options.replyTo, 'amq.rabbitmq.reply-to')
    assert.equal(request.options.persistent, false)
    assert.equal(request.options.expiration, '2000')
    assert.ok(request.options.correlationId, 'request must carry a correlationId')
    assert.equal(harness.replyChannel.consumers[0].queue, 'amq.rabbitmq.reply-to')
    assert.equal(harness.replyChannel.consumers[0].options.noAck, true)

    await deliverReply(harness, request.options.correlationId, { id: 42, name: 'Pedro' })

    assert.deepEqual(await requestPromise, { id: 42, name: 'Pedro' })
  })

  test('rejects with RPC_TIMEOUT when no reply arrives in time', async () => {
    const harness = createHarness()

    await assert.rejects(
      () => harness.rpc.request('users.get', { id: 1 }, { timeout: 50 }),
      (error) => error.code === 'RPC_TIMEOUT'
    )
  })

  test('a reply with an unknown correlationId is discarded without settling others', async () => {
    const harness = createHarness()

    const requestPromise = harness.rpc.request('users.get', { id: 7 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    await deliverReply(harness, 'bogus-correlation-id', { hijacked: true })
    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { ok: true })

    assert.deepEqual(await requestPromise, { ok: true })
  })

  test('rejects with RPC_RESPONDER_ERROR when the reply carries the error envelope', async () => {
    const harness = createHarness()

    const requestPromise = harness.rpc.request('users.get', { id: 9 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { message: 'boom' }, { 'x-rpc-error': true })

    await assert.rejects(() => requestPromise, (error) =>
      error.code === 'RPC_RESPONDER_ERROR' && error.message === 'boom'
    )
  })

  test('rejects in-flight requests with RPC_CONNECTION_LOST when the reply channel closes', async () => {
    const harness = createHarness()

    const requestPromise = harness.rpc.request('users.get', { id: 3 }, { timeout: 10000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    harness.replyChannel.emit('close')

    await assert.rejects(() => requestPromise, (error) => error.code === 'RPC_CONNECTION_LOST')
  })

  test('recreates the reply consumer lazily after a connection loss', async () => {
    const harness = createHarness()

    const first = harness.rpc.request('users.get', { id: 1 }, { timeout: 10000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'first request published')

    harness.rpc.handleConnectionLoss()

    await assert.rejects(() => first, (error) => error.code === 'RPC_CONNECTION_LOST')

    const second = harness.rpc.request('users.get', { id: 2 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 2, 2000, 'second request published')

    assert.equal(harness.replyChannel.consumers.length, 2, 'a fresh reply consumer must be created')

    await deliverReply(harness, harness.replyChannel.published[1].options.correlationId, { ok: 2 })

    assert.deepEqual(await second, { ok: 2 })
  })

  test('concurrent first requests share a single reply consumer', async () => {
    const harness = createHarness()

    const requests = [
      harness.rpc.request('users.get', { id: 1 }, { timeout: 2000 }),
      harness.rpc.request('users.get', { id: 2 }, { timeout: 2000 })
    ]

    await waitFor(() => harness.replyChannel.published.length === 2, 2000, 'both requests published')

    assert.equal(harness.replyChannel.consumers.length, 1, 'the pseudo-queue must be consumed exactly once')

    for (const { options } of harness.replyChannel.published) {
      await deliverReply(harness, options.correlationId, { echo: true })
    }

    assert.deepEqual(await Promise.all(requests), [{ echo: true }, { echo: true }])
  })

  test('validates the timeout option', async () => {
    const harness = createHarness()

    await assert.rejects(() => harness.rpc.request('users.get', {}, { timeout: 0 }), /positive number/)
    await assert.rejects(() => harness.rpc.request('users.get', {}, { timeout: -5 }), /positive number/)
    await assert.rejects(() => harness.rpc.request('users.get', {}, { timeout: 'soon' }), /positive number/)
  })
})

describe('Rpc respond()', () => {
  const deliverRequest = async (harness, payload, properties = {}) => {
    const { content, compressed } = await harness.codec.encode(payload)

    await harness.consumerChannel.consumers[0].callback({
      content,
      fields: { deliveryTag: 1 },
      properties: { headers: { 'x-compressed': compressed }, ...properties }
    })
  }

  test('publishes the handler result back to replyTo with the correlationId', async () => {
    const harness = createHarness()

    await harness.rpc.respond('rpc-queue', async (content) => ({ doubled: content.value * 2 }))

    await deliverRequest(harness, { value: 21 }, { replyTo: 'amq.rabbitmq.reply-to.abc', correlationId: 'corr-1' })

    await waitFor(() => harness.poolChannel.published.length === 1, 2000, 'reply published')

    const reply = harness.poolChannel.published[0]

    assert.equal(reply.exchange, '', 'replies go through the default exchange')
    assert.equal(reply.routingKey, 'amq.rabbitmq.reply-to.abc')
    assert.equal(reply.options.correlationId, 'corr-1')
    assert.deepEqual(await harness.codec.decode(reply.content, false), { doubled: 42 })
    assert.equal(harness.consumerChannel.acked.length, 1, 'the request must be acked')
  })

  test('a handler returning undefined still settles the requester (null reply)', async () => {
    const harness = createHarness()

    await harness.rpc.respond('rpc-queue', async () => {})

    await deliverRequest(harness, { fireAndForget: true }, { replyTo: 'amq.rabbitmq.reply-to.abc', correlationId: 'corr-2' })

    await waitFor(() => harness.poolChannel.published.length === 1, 2000, 'reply published')

    assert.equal(await harness.codec.decode(harness.poolChannel.published[0].content, false), null)
  })

  test('handler crash with replyOnError publishes the error envelope and acks', async () => {
    const harness = createHarness()

    await harness.rpc.respond('rpc-queue', async () => {
      throw new Error('boom')
    }, { replyOnError: true })

    await deliverRequest(harness, { value: 1 }, { replyTo: 'amq.rabbitmq.reply-to.abc', correlationId: 'corr-3' })

    await waitFor(() => harness.poolChannel.published.length === 1, 2000, 'error reply published')

    const reply = harness.poolChannel.published[0]

    assert.equal(reply.options.headers['x-rpc-error'], true)
    assert.deepEqual(await harness.codec.decode(reply.content, false), { message: 'boom' })
    assert.equal(harness.consumerChannel.acked.length, 1)
    assert.equal(harness.consumerChannel.nacked.length, 0)
  })

  test('handler crash without replyOnError nacks to the DLQ and publishes nothing', async () => {
    const harness = createHarness()

    await harness.rpc.respond('rpc-queue', async () => {
      throw new Error('boom')
    })

    await deliverRequest(harness, { value: 1 }, { replyTo: 'amq.rabbitmq.reply-to.abc', correlationId: 'corr-4' })

    await waitFor(() => harness.consumerChannel.nacked.length === 1, 2000, 'request nacked')

    assert.equal(harness.consumerChannel.nacked[0].requeue, false, 'poison policy: no hot requeue loops')
    assert.equal(harness.poolChannel.published.length, 0)
    assert.equal(harness.consumerChannel.acked.length, 0)
  })

  test('a message without replyTo is processed without publishing a reply', async () => {
    const harness = createHarness()

    const handled = []

    await harness.rpc.respond('rpc-queue', async (content) => {
      handled.push(content)

      return { ignored: true }
    })

    await deliverRequest(harness, { plain: true }, {})

    await waitFor(() => handled.length === 1, 2000, 'message handled')
    await sleep(20)

    assert.equal(harness.poolChannel.published.length, 0)
    assert.equal(harness.consumerChannel.acked.length, 1)
  })

  test('rejects a non-function handler', async () => {
    const harness = createHarness()

    await assert.rejects(() => harness.rpc.respond('rpc-queue', 'not-a-function'), /Handler must be a function/)
  })
})
