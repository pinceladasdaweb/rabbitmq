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

// Node 22's test runner cancels a test whose only pending work is an unref'd
// timer ('Promise resolution is still pending but the event loop has already
// resolved') — and the RPC timeout timer is deliberately unref'd. A ref'd
// interval holds the loop open while a purely timeout-driven assertion runs.
const withLiveEventLoop = async (run) => {
  const keepAlive = setInterval(() => {}, 50)

  try {
    return await run()
  } finally {
    clearInterval(keepAlive)
  }
}

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
    // Failure knobs: confirmErrors is a FIFO of errors to feed to confirm
    // callbacks; neverConfirm simulates a broker that never answers;
    // confirmDelayMs delays the confirm to model a slow broker.
    this.confirmErrors = []
    this.neverConfirm = false
    this.confirmDelayMs = 0
  }

  async consume (queue, callback, options) {
    this.consumers.push({ queue, callback, options })

    return { consumerTag: `tag-${++this.consumeSequence}` }
  }

  publish (exchange, routingKey, content, options, confirmCallback) {
    this.published.push({ exchange, routingKey, content, options })

    if (confirmCallback && !this.neverConfirm) {
      const error = this.confirmErrors.shift() ?? null

      if (this.confirmDelayMs > 0) {
        setTimeout(() => confirmCallback(error), this.confirmDelayMs)
      } else {
        setImmediate(() => confirmCallback(error))
      }
    }

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

const createHarness = ({ codecOptions = {}, logger = silentLogger } = {}) => {
  const codec = new MessageCodec({ logger, ...codecOptions })
  const replyChannel = new FakeChannel()
  const poolChannel = new FakeChannel()
  const consumerChannel = new FakeChannel()

  const channelPool = {
    getDedicatedChannel: async (id) => (id === 'rpc-reply' ? replyChannel : consumerChannel)
  }

  const context = {
    logger,
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

  await harness.replyChannel.consumers.at(-1).callback({
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
    assert.equal(request.options.mandatory, true, 'requests publish mandatory to fail fast on unroutable keys')
    assert.ok(request.options.headers['x-rpc-deadline'] > Date.now(), 'request must carry the staleness deadline')
    assert.ok(request.options.correlationId, 'request must carry a correlationId')
    assert.equal(harness.replyChannel.consumers[0].queue, 'amq.rabbitmq.reply-to')
    assert.equal(harness.replyChannel.consumers[0].options.noAck, true)

    await deliverReply(harness, request.options.correlationId, { id: 42, name: 'Pedro' })

    assert.deepEqual(await requestPromise, { id: 42, name: 'Pedro' })
  })

  test('rejects with RPC_TIMEOUT when no reply arrives in time', async () => {
    const harness = createHarness()

    await withLiveEventLoop(() => assert.rejects(
      () => harness.rpc.request('users.get', { id: 1 }, { timeout: 50 }),
      (error) => error.code === 'RPC_TIMEOUT'
    ))
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

  test('rejects with the broker error when the publish confirm fails (single attempt by default)', async () => {
    const harness = createHarness()

    harness.replyChannel.confirmErrors.push(new Error('channel closed by server'))

    await assert.rejects(
      () => harness.rpc.request('users.get', { id: 1 }, { timeout: 2000 }),
      /not confirmed by the broker/
    )

    assert.equal(harness.replyChannel.published.length, 1, 'default is a single publish attempt')
    assert.equal(harness.rpc.pendingRequests.size, 0, 'failed request must not leak a pending entry')
  })

  test('honors maxRetries: failed confirms are retried and the reply still resolves', async () => {
    const harness = createHarness()

    harness.replyChannel.confirmErrors.push(new Error('nacked 1'), new Error('nacked 2'))

    const requestPromise = harness.rpc.request('users.get', { id: 5 }, { timeout: 5000, maxRetries: 3, retryDelay: 5 })

    await waitFor(() => harness.replyChannel.published.length === 3, 3000, 'three publish attempts')

    const correlationIds = new Set(harness.replyChannel.published.map(p => p.options.correlationId))

    assert.equal(correlationIds.size, 1, 'every attempt must reuse the same correlationId')

    await deliverReply(harness, harness.replyChannel.published.at(-1).options.correlationId, { ok: true })

    assert.deepEqual(await requestPromise, { ok: true })
  })

  test('rejects with RPC_TIMEOUT even when the broker never answers the publish confirm', async () => {
    // Regression: the publish confirm used to be awaited before returning the
    // response promise — a confirm the broker never answered hung request()
    // forever and turned the internal timeout into an unhandled rejection.
    const harness = createHarness()

    harness.replyChannel.neverConfirm = true

    await withLiveEventLoop(() => assert.rejects(
      () => harness.rpc.request('users.get', { id: 1 }, { timeout: 50 }),
      (error) => error.code === 'RPC_TIMEOUT'
    ))
  })

  test('a publish failure arriving after the request settled is logged, not thrown', async () => {
    const warnings = []
    const harness = createHarness({
      logger: { ...silentLogger, warn: (message) => warnings.push(message) }
    })

    // The confirm fails only AFTER the timeout has already settled the request.
    harness.replyChannel.confirmErrors.push(new Error('late nack'))
    harness.replyChannel.confirmDelayMs = 100

    await assert.rejects(
      () => harness.rpc.request('users.get', { id: 1 }, { timeout: 30 }),
      (error) => error.code === 'RPC_TIMEOUT'
    )

    await waitFor(() => warnings.some(w => /after the request already settled/.test(w)), 2000, 'late failure logged')
  })

  test('rejects with RPC_UNROUTABLE when the broker returns the request (nothing bound)', async () => {
    const harness = createHarness()

    const requestPromise = harness.rpc.request('users.get', { id: 1 }, { timeout: 5000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    harness.replyChannel.emit('return', {
      fields: { routingKey: 'users.get' },
      properties: { correlationId: harness.replyChannel.published[0].options.correlationId }
    })

    await assert.rejects(() => requestPromise, (error) =>
      error.code === 'RPC_UNROUTABLE' && /could not be routed/.test(error.message)
    )
  })

  test('rejects when the reply cannot be decoded instead of waiting for the timeout', async () => {
    const harness = createHarness()

    const requestPromise = harness.rpc.request('users.get', { id: 1 }, { timeout: 5000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    // Marked compressed but the content is not gzip: decode must fail.
    await harness.replyChannel.consumers.at(-1).callback({
      content: Buffer.from('not-gzip'),
      fields: {},
      properties: {
        correlationId: harness.replyChannel.published[0].options.correlationId,
        headers: { 'x-compressed': true }
      }
    })

    await assert.rejects(() => requestPromise, (error) => /incorrect header check/.test(error.message))
  })

  test('a compressed reply round-trips transparently', async () => {
    const harness = createHarness({ codecOptions: { useCompression: true, compressionThreshold: 10 } })

    const bigPayload = { blob: 'x'.repeat(500) }
    const requestPromise = harness.rpc.request('users.get', { id: 1 }, { timeout: 5000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')
    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, bigPayload)

    assert.deepEqual(await requestPromise, bigPayload)
  })

  test('a reply-consumer setup that straddles a connection loss is fenced out', async () => {
    const harness = createHarness()

    let releaseConsume
    const consumeGate = new Promise(resolve => { releaseConsume = resolve })
    const originalConsume = harness.replyChannel.consume.bind(harness.replyChannel)

    harness.replyChannel.consume = async (...args) => {
      await consumeGate

      return originalConsume(...args)
    }

    const first = harness.rpc.request('users.get', { id: 1 }, { timeout: 2000 })

    // Let the request reach the in-flight consume, then declare the world dead.
    await sleep(10)
    harness.rpc.handleConnectionLoss()
    releaseConsume()

    await assert.rejects(() => first, (error) => error.code === 'RPC_CONNECTION_LOST')
    assert.equal(harness.rpc.replyChannel, null, 'the fenced channel must not be installed')

    // The lazy rebuild must produce a working consumer afterwards.
    harness.replyChannel.consume = originalConsume

    const second = harness.rpc.request('users.get', { id: 2 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'second request published')
    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { ok: true })

    assert.deepEqual(await second, { ok: true })
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

  test('drops a stale request (past deadline) without running the handler', async () => {
    const harness = createHarness()

    const handled = []

    await harness.rpc.respond('rpc-queue', async (content) => {
      handled.push(content)

      return { late: true }
    })

    await deliverRequest(harness, { value: 1 }, {
      replyTo: 'amq.rabbitmq.reply-to.abc',
      correlationId: 'corr-stale',
      headers: { 'x-compressed': false, 'x-rpc-deadline': Date.now() - 1000 }
    })

    await sleep(20)

    assert.equal(handled.length, 0, 'handler must not run for a stale request')
    assert.equal(harness.poolChannel.published.length, 0, 'no reply for a stale request')
    assert.equal(harness.consumerChannel.acked.length, 1, 'stale request is acked away')
    assert.equal(harness.consumerChannel.nacked.length, 0)
  })

  test('a reply-publish failure after a successful handler falls back to the error envelope and acks', async () => {
    const harness = createHarness()

    // First publish (the result reply) fails its confirm; the envelope succeeds.
    harness.poolChannel.confirmErrors.push(new Error('reply confirm nacked'))

    await harness.rpc.respond('rpc-queue', async () => ({ done: true }))

    await deliverRequest(harness, { value: 1 }, { replyTo: 'amq.rabbitmq.reply-to.abc', correlationId: 'corr-5' })

    await waitFor(() => harness.poolChannel.published.length === 2, 2000, 'result reply + error envelope published')

    const envelope = harness.poolChannel.published[1]

    assert.equal(envelope.options.headers['x-rpc-error'], true)
    assert.match((await harness.codec.decode(envelope.content, false)).message, /Failed to publish RPC reply/)
    assert.equal(harness.consumerChannel.acked.length, 1, 'processed request must be acked, not dead-lettered')
    assert.equal(harness.consumerChannel.nacked.length, 0)
  })

  test('a processed request is still acked when both the reply and the envelope fail', async () => {
    const harness = createHarness()

    harness.poolChannel.confirmErrors.push(new Error('reply nacked'), new Error('envelope nacked'))

    await harness.rpc.respond('rpc-queue', async () => ({ done: true }))

    await deliverRequest(harness, { value: 1 }, { replyTo: 'amq.rabbitmq.reply-to.abc', correlationId: 'corr-6' })

    await waitFor(() => harness.consumerChannel.acked.length === 1, 2000, 'request acked')

    assert.equal(harness.consumerChannel.nacked.length, 0, 'a processed request must never be dead-lettered')
  })

  test('a large handler result is compressed and flagged on the reply', async () => {
    const harness = createHarness({ codecOptions: { useCompression: true, compressionThreshold: 10 } })

    const bigResult = { blob: 'y'.repeat(500) }

    await harness.rpc.respond('rpc-queue', async () => bigResult)

    await deliverRequest(harness, { value: 1 }, { replyTo: 'amq.rabbitmq.reply-to.abc', correlationId: 'corr-7' })

    await waitFor(() => harness.poolChannel.published.length === 1, 2000, 'reply published')

    const reply = harness.poolChannel.published[0]

    assert.equal(reply.options.headers['x-compressed'], true, 'reply above the threshold must be compressed')
    assert.deepEqual(await harness.codec.decode(reply.content, true), bigResult)
  })
})
