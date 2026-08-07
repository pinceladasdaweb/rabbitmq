import assert from 'node:assert/strict'
import Rpc from '../src/messaging/rpc.js'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { test, describe, before, after } from 'node:test'
import { EventEmitter } from 'node:events'
import Publisher from '../src/messaging/publisher.js'
import MessageCodec from '../src/messaging/message-codec.js'
import CircuitBreaker from '../src/resilience/circuit-breaker.js'
import ConsumerManager from '../src/consumers/consumer-manager.js'
import { ManualClock, recordingLogger, withLiveEventLoop } from './helpers.js'

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

const createHarness = ({ codecOptions = {}, logger = silentLogger, context: extraContext = {} } = {}) => {
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
    emit: () => {},
    ...extraContext
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
  test('reuses one reply consumer across requests instead of consuming again', async () => {
    // The reply channel is set up lazily on the first request. Consuming the
    // direct reply-to pseudo-queue a second time on the same channel is a
    // protocol error, so the cached-channel short circuit is load-bearing.
    const harness = createHarness()

    const first = harness.rpc.request('users.get', { id: 1 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'first request published')

    const second = harness.rpc.request('users.get', { id: 2 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 2, 2000, 'second request published')

    assert.equal(harness.replyChannel.consumers.length, 1, 'consume was issued exactly once')

    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { n: 1 })
    await deliverReply(harness, harness.replyChannel.published[1].options.correlationId, { n: 2 })

    assert.deepEqual(await first, { n: 1 })
    assert.deepEqual(await second, { n: 2 })
  })

  test('uses the default timeout when the caller does not set one', async () => {
    const harness = createHarness()

    const pending = harness.rpc.request('users.get', { id: 1 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    // 30s default, expressed as the per-message TTL.
    assert.equal(harness.replyChannel.published[0].options.expiration, '30000')

    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { ok: true })

    assert.deepEqual(await pending, { ok: true })
  })

  test('a reply with no correlationId is discarded, settling nothing', async () => {
    const harness = createHarness()

    const pending = harness.rpc.request('users.get', { id: 1 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    await deliverReply(harness, undefined, { stray: true })

    const correlationId = harness.replyChannel.published[0].options.correlationId

    await deliverReply(harness, correlationId, { ok: true })

    assert.deepEqual(await pending, { ok: true }, 'the uncorrelated reply never settled the request')
  })

  test('an error envelope with no message still rejects with a usable reason', async () => {
    const harness = createHarness()

    const pending = harness.rpc.request('users.get', { id: 1 }, { timeout: 2000 })
      .then(() => null, (error) => error)

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    await deliverReply(
      harness,
      harness.replyChannel.published[0].options.correlationId,
      {},
      { 'x-rpc-error': true }
    )

    const error = await pending

    assert.equal(error.code, 'RPC_RESPONDER_ERROR')
    assert.match(error.message, /RPC responder failed/)
  })

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
    // NaN is a number, finite it is not: only the isFinite arm catches it.
    await assert.rejects(() => harness.rpc.request('users.get', {}, { timeout: NaN }), /positive number/)
    await assert.rejects(() => harness.rpc.request('users.get', {}, { timeout: Infinity }), /positive number/)
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

    await assert.rejects(() => first, (error) =>
      error.code === 'RPC_CONNECTION_LOST' && /connection lost during reply consumer setup/.test(error.message)
    )
    assert.equal(harness.rpc.replyChannel, null, 'the fenced channel must not be installed')
    // The fenced setup installed its listeners before discovering the epoch
    // had moved: abandoning the channel without detaching them would leave
    // them live and let the next rebuild stack another pair on top.
    assert.equal(harness.replyChannel.listenerCount('return'), 0, 'a fenced setup must leave no return listener behind')

    // The lazy rebuild must produce a working consumer afterwards.
    harness.replyChannel.consume = originalConsume

    const second = harness.rpc.request('users.get', { id: 2 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'second request published')

    assert.equal(harness.replyChannel.listenerCount('return'), 1, 'exactly one listener set after the rebuild')

    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { ok: true })

    assert.deepEqual(await second, { ok: true })
  })
})

describe('Rpc reply channel lifecycle', () => {
  test('rejects when there is no channel pool (not connected)', async () => {
    const harness = createHarness()

    harness.rpc.getChannelPool = () => null

    await assert.rejects(
      () => harness.rpc.request('users.get', { id: 1 }, { timeout: 2000 }),
      /Not connected to RabbitMQ/
    )
  })

  test('a broker cancel of the reply consumer rejects in-flight requests and forces a rebuild', async () => {
    const harness = createHarness()

    const requestPromise = harness.rpc.request('users.get', { id: 1 }, { timeout: 10000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    // The broker delivers a null message to signal basic.cancel.
    await harness.replyChannel.consumers.at(-1).callback(null)

    await assert.rejects(() => requestPromise, (error) =>
      error.code === 'RPC_CONNECTION_LOST' && /cancelled by the broker/.test(error.message)
    )

    // The next request rebuilds the consumer and works end to end.
    const second = harness.rpc.request('users.get', { id: 2 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 2, 2000, 'second request published')
    assert.equal(harness.replyChannel.consumers.length, 2, 'a fresh reply consumer must be created')

    await deliverReply(harness, harness.replyChannel.published[1].options.correlationId, { ok: true })

    assert.deepEqual(await second, { ok: true })
  })

  test('a cancel arriving after the connection was already lost is ignored', async () => {
    // handleConnectionLoss detaches the 'return'/'close' listeners but cannot
    // unregister the consume callback, so a basic.cancel can still arrive for
    // a channel the RPC layer has already let go. Without the identity check
    // it would invalidate whatever channel is current by then.
    const harness = createHarness()

    const pending = harness.rpc.request('users.get', { id: 1 }, { timeout: 10000 })
      .then(() => null, (error) => error)

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    harness.rpc.handleConnectionLoss()

    const first = await pending

    assert.equal(first.code, 'RPC_CONNECTION_LOST')

    // The late cancel for the channel that is no longer the reply channel.
    await harness.replyChannel.consumers.at(-1).callback(null)

    // A fresh request still works: nothing was torn down a second time.
    const second = harness.rpc.request('users.get', { id: 2 }, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 2, 2000, 'second request published')
    await deliverReply(harness, harness.replyChannel.published[1].options.correlationId, { ok: true })

    assert.deepEqual(await second, { ok: true })
  })

  test('repeated broker cancels do not accumulate listeners on the cached channel', async () => {
    // A basic.cancel leaves the channel OPEN, and the pool caches dedicated
    // channels by id — so every rebuild lands on the same channel object. If
    // the previous listeners are not detached first, each cancel adds another
    // 'return'/'close' pair and they pile up for the lifetime of the process.
    const harness = createHarness()

    // Wrapped in an object so awaiting this helper cannot adopt (and therefore
    // wait on) the request promise, which only settles once we cancel it.
    const issueRequest = async () => {
      const publishedCount = harness.replyChannel.published.length
      const pending = harness.rpc.request('users.get', { id: 1 }, { timeout: 5000 })

      pending.catch(() => {})

      await waitFor(() => harness.replyChannel.published.length > publishedCount, 2000, 'request published')

      return { pending }
    }

    const { pending: first } = await issueRequest()

    await harness.replyChannel.consumers.at(-1).callback(null)
    await assert.rejects(() => first, (error) => error.code === 'RPC_CONNECTION_LOST')

    const baseline = {
      return: harness.replyChannel.listenerCount('return'),
      close: harness.replyChannel.listenerCount('close')
    }

    // Three more cancel/rebuild cycles must not change the listener counts.
    for (let cycle = 0; cycle < 3; cycle++) {
      const { pending } = await issueRequest()

      await harness.replyChannel.consumers.at(-1).callback(null)
      await assert.rejects(() => pending, (error) => error.code === 'RPC_CONNECTION_LOST')

      assert.equal(harness.replyChannel.listenerCount('return'), baseline.return, `cycle ${cycle}: no extra return listener`)
      assert.equal(harness.replyChannel.listenerCount('close'), baseline.close, `cycle ${cycle}: no extra close listener`)
    }

    // A connection loss is the other rebuild path and must detach too.
    for (let cycle = 0; cycle < 3; cycle++) {
      const { pending } = await issueRequest()

      harness.rpc.handleConnectionLoss()
      await assert.rejects(() => pending, (error) => error.code === 'RPC_CONNECTION_LOST')

      assert.equal(harness.replyChannel.listenerCount('return'), baseline.return, `loss cycle ${cycle}: no extra return listener`)
      assert.equal(harness.replyChannel.listenerCount('close'), baseline.close, `loss cycle ${cycle}: no extra close listener`)
    }

    // And the reply route still works after all that churn.
    const { pending: final } = await issueRequest()

    await deliverReply(harness, harness.replyChannel.published.at(-1).options.correlationId, { ok: true })

    assert.deepEqual(await final, { ok: true })
  })

  test('a basic.return for an unknown correlationId settles nothing', async () => {
    const harness = createHarness()

    const requestPromise = harness.rpc.request('users.get', { id: 1 }, { timeout: 5000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'request published')

    harness.replyChannel.emit('return', {
      fields: { routingKey: 'someone.else' },
      properties: { correlationId: 'not-our-request' }
    })
    // A malformed return (no properties at all) must not throw either.
    harness.replyChannel.emit('return', { fields: {} })

    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { ok: true })

    assert.deepEqual(await requestPromise, { ok: true }, 'the real reply must still settle the request')
  })

  test('an in-flight RPC timer does not keep the process alive', async (t) => {
    // The only way to observe the unref: a child process that issues a request
    // with a 60s timeout must still exit immediately.
    const probe = fileURLToPath(new URL('./fixtures/rpc-unref-probe.mjs', import.meta.url))
    const child = spawn(process.execPath, [probe], { stdio: ['ignore', 'pipe', 'pipe'] })

    // Reaps the child on every exit path, including an assertion failure.
    t.after(() => {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
    })

    const { code, stdout } = await new Promise((resolve, reject) => {
      let out = ''
      let killTimer = null

      // Every settle path clears the timer, or a ref'd 10s timer would hold
      // the test runner's event loop open long after this test finished.
      const settle = (fn) => (value) => {
        clearTimeout(killTimer)
        fn(value)
      }

      const succeed = settle(() => resolve({ code: child.exitCode, stdout: out }))
      const fail = settle(reject)

      killTimer = setTimeout(() => {
        child.kill('SIGKILL')
        fail(new Error('the probe did not exit: the RPC timeout timer is keeping the process alive'))
      }, 10000)

      child.stdout.on('data', (chunk) => { out += chunk })
      // Draining stderr prevents a deadlock if the probe ever writes a lot.
      child.stderr.resume()
      child.on('error', fail)
      // 'close' (not 'exit') guarantees stdout has been drained.
      child.on('close', succeed)
    })

    assert.equal(code, 0)
    assert.match(stdout, /EXITED_WITHOUT_WAITING_FOR_TIMEOUT/)
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

  test('a responder that throws a non-Error still produces an envelope (issue #18)', async () => {
    // The envelope used to read error.message directly: `throw null` crashed
    // the catch, so instead of the promised RPC_RESPONDER_ERROR the requester
    // got silence until its timeout — and the request went to the DLQ.
    const harness = createHarness()

    await harness.rpc.respond('rpc-queue', async () => {
      throw null // eslint-disable-line no-throw-literal
    }, { replyOnError: true })

    await deliverRequest(harness, { value: 1 }, { replyTo: 'amq.rabbitmq.reply-to.abc', correlationId: 'corr-null' })

    await waitFor(() => harness.poolChannel.published.length === 1, 2000, 'error reply published')

    const reply = harness.poolChannel.published[0]

    assert.equal(reply.options.headers['x-rpc-error'], true)
    assert.deepEqual(await harness.codec.decode(reply.content, false), { message: 'null' })
    assert.equal(harness.consumerChannel.acked.length, 1, 'the request is acked, not dead-lettered')
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

describe('Rpc survivor round', () => {
  // Node 22's test runner cancels a test whose only pending work is an
  // unref'd timer — and every in-flight RPC request holds exactly that
  // (its timeout timer is deliberately unref'd). One ref'd interval keeps
  // the event loop alive for the whole suite; same mechanics as
  // withLiveEventLoop, hoisted to describe scope.
  let keepAlive

  before(() => { keepAlive = setInterval(() => {}, 50) })
  after(() => clearInterval(keepAlive))

  test('a publish failure that settles the request does not also warn about a late failure', async () => {
    const logger = recordingLogger()
    const harness = createHarness({ logger })

    harness.replyChannel.confirmErrors.push(new Error('nacked'))

    await assert.rejects(() => harness.rpc.request('users.get', {}, { timeout: 2000 }), /not confirmed/)

    assert.equal(
      logger.records.warn.some(line => line.includes('already settled')),
      false,
      'the failure settled the pending — warning as well would be double-reporting'
    )
  })

  test('connection-loss rejections carry the reasons callers read', async () => {
    const harness = createHarness()

    const pending = harness.rpc.request('users.get', {}, { timeout: 2000 })

    await waitFor(() => harness.rpc.pendingRequests.size === 1, 2000, 'request pending')

    harness.rpc.handleConnectionLoss()

    await assert.rejects(() => pending, (error) => {
      assert.equal(error.code, 'RPC_CONNECTION_LOST')
      assert.match(error.message, /connection to RabbitMQ lost/, 'the default reason names the cause')

      return true
    })
  })

  test('a close of the CURRENT reply channel rejects with its own reason', async () => {
    const harness = createHarness()

    const pending = harness.rpc.request('users.get', {}, { timeout: 2000 })

    await waitFor(() => harness.rpc.pendingRequests.size === 1, 2000, 'request pending')

    harness.replyChannel.emit('close')

    await assert.rejects(() => pending, /reply channel closed/)
  })

  test('a stale close event from a replaced reply channel leaves new pendings alone', async () => {
    // A pool that hands out a FRESH channel per call, as a real reconnection
    // does — the default harness reuses one instance, which would make the
    // stale channel and its replacement indistinguishable.
    const replyChannels = []
    const channelPool = {
      getDedicatedChannel: async () => {
        const channel = new FakeChannel()

        replyChannels.push(channel)

        return channel
      }
    }
    const harness = createHarness({ context: { getChannelPool: () => channelPool } })

    const first = harness.rpc.request('users.get', { id: 1 }, { timeout: 2000 })

    await waitFor(() => replyChannels.length === 1 && replyChannels[0].published.length === 1, 2000, 'first published')

    // The connection turns over: pendings die with it, the channel is retired.
    harness.rpc.handleConnectionLoss('turnover')
    await assert.rejects(() => first, /turnover/)

    const survivor = harness.rpc.request('users.get', { id: 2 }, { timeout: 2000 })

    await waitFor(() => replyChannels.length === 2 && replyChannels[1].published.length === 1, 2000, 'second pending on a fresh channel')

    // The old channel's own close handler was detached at turnover; the
    // path that CAN still fire for it is the broker cancelling its consumer
    // (a null delivery). That late cancel must not touch the new route.
    await replyChannels[0].consumers[0].callback(null)
    replyChannels[0].emit('close')
    await sleep(10)

    assert.equal(harness.rpc.pendingRequests.size, 1, 'the new pending survived the stale close')

    const { content, compressed } = await harness.codec.encode({ ok: true })

    await replyChannels[1].consumers.at(-1).callback({
      content,
      fields: {},
      properties: {
        correlationId: replyChannels[1].published[0].options.correlationId,
        headers: { 'x-compressed': compressed }
      }
    })

    assert.deepEqual(await survivor, { ok: true })
  })

  test('malformed basic.return events are tolerated and unroutable fields degrade gracefully', async () => {
    const harness = createHarness()

    const pending = harness.rpc.request('users.get', {}, { timeout: 2000 })

    await waitFor(() => harness.rpc.pendingRequests.size === 1, 2000, 'request pending')

    // Emitters outside amqplib's contract must not crash the reply route.
    harness.replyChannel.emit('return', {})
    harness.replyChannel.emit('return', undefined)

    assert.equal(harness.rpc.pendingRequests.size, 1, 'garbage returns settled nothing')

    // A return that DOES match a pending but carries no fields still rejects
    // as unroutable instead of crashing on fields.routingKey.
    const { correlationId } = harness.replyChannel.published[0].options

    harness.replyChannel.emit('return', { properties: { correlationId } })

    await assert.rejects(() => pending, (error) => error.code === 'RPC_UNROUTABLE')
  })

  test('an error envelope without a message still rejects with the fallback', async () => {
    const harness = createHarness()

    const pending = harness.rpc.request('users.get', {}, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'published')

    // A foreign responder can flag x-rpc-error with a body that decodes to
    // null; the requester still deserves a coded rejection, not a timeout.
    await harness.replyChannel.consumers.at(-1).callback({
      content: Buffer.from('null'),
      fields: {},
      properties: {
        correlationId: harness.replyChannel.published[0].options.correlationId,
        headers: { 'x-compressed': false, 'x-rpc-error': true }
      }
    })

    await assert.rejects(() => pending, (error) => {
      assert.equal(error.code, 'RPC_RESPONDER_ERROR')
      assert.match(error.message, /RPC responder failed/)

      return true
    })
  })

  test('timeouts name the route and the budget in their message', async () => {
    const harness = createHarness()

    await assert.rejects(
      () => harness.rpc.request('users.get', {}, { timeout: 20 }),
      (error) => {
        assert.equal(error.code, 'RPC_TIMEOUT')
        assert.match(error.message, /users\.get.*timed out after 20ms/)

        return true
      }
    )
  })

  test('requests are rate-limited under the rpc: namespace with their explicit cost', async () => {
    const calls = []
    const rateLimiter = {
      checkRateLimit: async (key, cost) => {
        calls.push({ key, cost })

        return true
      },
      getStatus: () => ({})
    }

    const harness = createHarness({ context: { rateLimiter } })

    const first = harness.rpc.request('users.get', {}, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'published')
    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { ok: 1 })
    await first

    const second = harness.rpc.request('users.get', {}, { timeout: 2000, rateLimitCost: 3 })

    await waitFor(() => harness.replyChannel.published.length === 2, 2000, 'published again')
    await deliverReply(harness, harness.replyChannel.published[1].options.correlationId, { ok: 2 })
    await second

    assert.deepEqual(calls, [
      { key: 'rpc:users.get', cost: 1 },
      { key: 'rpc:users.get', cost: 3 }
    ])
  })

  test('requests go out transient: a queue restart must not replay stale RPCs', async () => {
    const harness = createHarness()

    const pending = harness.rpc.request('users.get', {}, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'published')

    assert.equal(harness.replyChannel.published[0].options.persistent, false)

    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { ok: true })
    await pending
  })

  test('tolerates a clock whose timeout handles have no unref', async () => {
    const bare = {
      now: () => Date.now(),
      setInterval: (fn, ms) => setInterval(fn, ms),
      clearInterval: (handle) => clearInterval(handle),
      setTimeout: (fn, ms) => {
        const id = setTimeout(fn, ms)

        return { id, close: () => clearTimeout(id) }
      },
      clearTimeout: (handle) => { if (handle) clearTimeout(handle.id) },
      sleep: (ms) => new Promise(resolve => setTimeout(resolve, ms))
    }

    const harness = createHarness({ context: { clock: bare } })

    const pending = harness.rpc.request('users.get', {}, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'published without crashing on unref')
    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { ok: true })
    assert.deepEqual(await pending, { ok: true })
  })

  test('a debug-less logger survives discarded replies and stale-request drops', async () => {
    const logger = { info: () => {}, warn: () => {}, error: () => {} }
    const harness = createHarness({ logger })

    const pending = harness.rpc.request('users.get', {}, { timeout: 2000 })

    await waitFor(() => harness.replyChannel.published.length === 1, 2000, 'published')

    // Unknown correlationId: the discard path logs at debug level.
    await deliverReply(harness, 'no-such-correlation', { ok: false })

    await deliverReply(harness, harness.replyChannel.published[0].options.correlationId, { ok: true })
    assert.deepEqual(await pending, { ok: true })
  })
})

describe('Rpc respond() staleness', () => {
  // Same Node 22 keep-alive as the suite above.
  let keepAlive

  before(() => { keepAlive = setInterval(() => {}, 50) })
  after(() => clearInterval(keepAlive))

  const deliverRequest = async (harness, payload, headers = {}, properties = {}) => {
    const { content, compressed } = await harness.codec.encode(payload)

    await harness.consumerChannel.consumers.at(-1).callback({
      content,
      fields: { consumerTag: 'tag-1', deliveryTag: 1 },
      properties: {
        replyTo: 'amq.rabbitmq.reply-to',
        correlationId: 'req-1',
        headers: { 'x-compressed': compressed, ...headers },
        ...properties
      }
    })
  }

  test('a request whose deadline is exactly now is still served', async (t) => {
    // Staleness is strictly past-deadline: the boundary instant belongs to
    // the requester (its own timeout fires only after this moment).
    const clock = new ManualClock(5000)
    const harness = createHarness({ context: { clock } })
    const served = []

    await harness.rpc.respond('rpc.queue', async (content) => {
      served.push(content)

      return { ok: true }
    })

    await deliverRequest(harness, { n: 1 }, { 'x-rpc-deadline': 5000 })

    assert.deepEqual(served, [{ n: 1 }], 'deadline == now is not stale')
  })

  test('a live deadline is served and an expired one is dropped with an ack', async () => {
    const clock = new ManualClock(5000)
    // Debug-less on purpose: the drop path logs at debug level through
    // optional chaining, and this logger is the reason the ?. exists.
    const logger = { info: () => {}, warn: () => {}, error: () => {} }
    const harness = createHarness({ logger, context: { clock } })
    const served = []

    await harness.rpc.respond('rpc.queue', async (content) => {
      served.push(content)

      return { ok: true }
    })

    await deliverRequest(harness, { fresh: true }, { 'x-rpc-deadline': 9000 })
    await deliverRequest(harness, { stale: true }, { 'x-rpc-deadline': 4000 })

    assert.deepEqual(served, [{ fresh: true }], 'only the live request reached the handler')
    assert.equal(harness.consumerChannel.nacked.length, 0, 'a stale drop is an ack, never a nack — a crash in the drop path would dead-letter it')
    assert.equal(harness.consumerChannel.acked.length, 2, 'both deliveries were settled')
  })

  test('failing to publish even the error envelope is reported and still acks the request', async () => {
    const logger = recordingLogger()
    const harness = createHarness({ logger })

    await harness.rpc.respond('rpc.queue', async () => ({ ok: true }))

    harness.poolChannel.confirmErrors.push(new Error('reply gone'), new Error('envelope gone'))

    await deliverRequest(harness, { n: 1 }, { 'x-rpc-deadline': Date.now() + 60000 })

    assert.ok(
      logger.records.error.some(line => line.includes('Failed to publish RPC error envelope')),
      'the double failure is the one case the operator can only learn from the log'
    )
    assert.equal(harness.consumerChannel.nacked.length, 0, 'the request is still acked — a DLQ replay would re-run committed side effects')
  })

  test('replies are transient, and a failed reply falls back to an error envelope', async () => {
    const harness = createHarness()

    await harness.rpc.respond('rpc.queue', async () => ({ big: 'result' }))

    // First reply confirm fails; the responder must fall back to an error
    // envelope so the requester rejects fast instead of burning its timeout.
    harness.poolChannel.confirmErrors.push(new Error('reply route gone'))

    await deliverRequest(harness, { n: 1 }, { 'x-rpc-deadline': Date.now() + 60000 })

    assert.equal(harness.poolChannel.published.length, 2, 'the failed reply was followed by the envelope')

    const [reply, envelope] = harness.poolChannel.published

    assert.equal(reply.options.persistent, false, 'replies are transient — the requester is ephemeral by definition')
    assert.equal(envelope.options.headers['x-rpc-error'], true)
    assert.match(JSON.parse(envelope.content.toString()).message, /Failed to publish RPC reply/)
  })

  test('a request without headers is processed normally', async () => {
    const harness = createHarness()
    const served = []

    await harness.rpc.respond('rpc.queue', async (content) => {
      served.push(content)

      return { ok: true }
    })

    const { content } = await harness.codec.encode({ bare: true })

    await harness.consumerChannel.consumers.at(-1).callback({
      content,
      fields: { consumerTag: 'tag-1', deliveryTag: 1 },
      properties: { replyTo: 'amq.rabbitmq.reply-to', correlationId: 'req-2' }
    })

    assert.deepEqual(served, [{ bare: true }], 'no headers means no deadline, not a crash')
  })
})
