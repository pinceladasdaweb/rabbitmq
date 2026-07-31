import assert from 'node:assert/strict'
import RabbitMQ from '../src/index.js'
import { test, describe } from 'node:test'
import { installDialer } from './fake-amqp.js'
import { createDialer, recordingLogger, silentLogger, waitFor } from './helpers.js'

// The dialer is installed onto amqplib itself (see fake-amqp.js): the facade is
// constructed exactly as a user would construct it.
const createRabbit = (t, dialer, options = {}) => {
  installDialer(t, dialer)

  return new RabbitMQ({
    username: 'admin',
    password: 'admin',
    endpoints: ['node-a:5672'],
    connectionName: 'lifecycle-test',
    reconnectInterval: 10,
    maxReconnectInterval: 20,
    channelPoolSize: 2,
    exchange: { name: 'lifecycle-exchange', type: 'direct' },
    logger: silentLogger,
    ...options
  })
}

const deliverTo = (consumer, payload, properties = {}) => consumer.callback({
  content: Buffer.from(JSON.stringify(payload)),
  fields: { consumerTag: consumer.consumerTag, deliveryTag: 1 },
  properties: { headers: { 'x-compressed': false }, ...properties }
})

describe('RabbitMQ lifecycle (fake dialer)', () => {
  test('connect builds the channel pool, asserts the exchange and emits connected', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    const events = []

    rabbit.on('connected', () => events.push('connected'))

    const connection = await rabbit.connect()

    assert.ok(connection)
    assert.deepEqual(events, ['connected'])
    assert.equal(dialer.connections[0].channels.length, 2, 'one channel per channelPoolSize')
    assert.ok(dialer.connections[0].channels.some(c => c.assertedExchanges.some(e => e.name === 'lifecycle-exchange')))
    assert.equal(rabbit.getClusterStatus().connectionState, 'connected')
  })

  test('concurrent connect() calls share one attempt and build a single channel pool', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    const [first, second] = await Promise.all([rabbit.connect(), rabbit.connect()])

    assert.equal(first, second)
    assert.equal(dialer.dials, 1, 'a second dial would leak an AMQP connection')
    assert.equal(dialer.connections[0].channels.length, 2, 'a second pool would leak channelPoolSize channels')
  })

  test('publish and subscribe work end to end over the fake broker', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const received = []

    await rabbit.subscribe('orders', async (content) => {
      received.push(content)
    })

    await rabbit.publish('orders-route', { id: 7 })
    await rabbit.publishAsync('orders-route', { id: 8 })

    const published = dialer.connections[0].publishedOn()

    assert.equal(published.length, 2)
    assert.equal(published[0].exchange, 'lifecycle-exchange')

    await deliverTo(dialer.connections[0].consumersOn()[0], { inbound: true })

    assert.deepEqual(received, [{ inbound: true }])
  })

  test('an unexpected connection close recovers pool and consumers, then publishes again', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()
    await rabbit.subscribe('orders', async () => {})

    const events = []

    rabbit.on('disconnected', () => events.push('disconnected'))
    rabbit.on('reconnected', () => events.push('reconnected'))

    dialer.connections[0].emit('close')

    await waitFor(() => events.includes('reconnected'), 3000, 'facade reconnection')

    assert.deepEqual(events, ['disconnected', 'reconnected'])
    assert.equal(dialer.connections.length, 2, 'a fresh AMQP connection must be dialed')
    assert.equal(dialer.connections[1].consumersOn().length, 1, 'consumer recreated on the new connection')

    await rabbit.publish('orders-route', { after: 'reconnect' })

    assert.equal(dialer.connections[1].publishedOn().length, 1, 'publishing resumes on the new pool')
  })

  test('the circuit breaker is reset after a successful reconnection', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer, { circuitBreaker: { failureThreshold: 2 } })

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    // Trip the breaker with failed confirms against the current connection.
    for (const channel of dialer.connections[0].channels) {
      channel.confirmErrors.push(new Error('broker down'), new Error('broker down'))
    }

    await assert.rejects(() => rabbit.publish('orders-route', { n: 1 }, { maxRetries: 1 }))
    await assert.rejects(() => rabbit.publish('orders-route', { n: 2 }, { maxRetries: 1 }))

    assert.equal(rabbit.getCircuitBreakerState().state, 'OPEN', 'breaker must be open before recovery')

    const reconnected = new Promise(resolve => rabbit.once('reconnected', resolve))

    dialer.connections[0].emit('close')
    await reconnected

    // Failures accumulated against the dead connection say nothing about the
    // new one: publishing must work immediately after recovery.
    assert.equal(rabbit.getCircuitBreakerState().state, 'CLOSED')

    await rabbit.publish('orders-route', { n: 3 })

    assert.equal(dialer.connections[1].publishedOn().length, 1)
  })

  test('emits reconnectError when post-reconnection setup fails', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const reconnectError = new Promise(resolve => rabbit.once('reconnectError', resolve))
    let reconnectedFired = false

    rabbit.once('reconnected', () => { reconnectedFired = true })

    dialer.onConnection = (connection) => {
      connection.channelError = new Error('channels refused')
    }

    dialer.connections[0].emit('close')

    const error = await reconnectError

    assert.match(error.message, /channels refused/)
    assert.equal(reconnectedFired, false, 'a failed restore must not report success')
  })

  test('connect({ waitForConnection }) resolves when a later reconnection succeeds', async (t) => {
    const dialer = createDialer([new Error('down'), new Error('still down'), 'ok'])
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    const connection = await rabbit.connect({ waitForConnection: true })

    assert.ok(connection, 'must resolve with the connection produced by the background retry')
    assert.equal(rabbit.getClusterStatus().connectionState, 'connected')
  })

  test('connect({ waitForConnection, timeout }) rejects when nothing connects in time', async (t) => {
    const dialer = createDialer([new Error('permanently down')])
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await assert.rejects(
      () => rabbit.connect({ waitForConnection: true, timeout: 100 }),
      /Timed out after 100ms/
    )
  })

  test('connect({ waitForConnection }) rejects when reconnection gives up', async (t) => {
    const dialer = createDialer([new Error('permanently down')])
    const rabbit = createRabbit(t, dialer, { maxReconnectAttempts: 1 })

    t.after(() => rabbit.disconnect())

    await assert.rejects(
      () => rabbit.connect({ waitForConnection: true }),
      /all reconnection attempts failed/
    )
  })

  test('disconnect closes the pool, emits disconnected and getChannel fails afterwards', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    await rabbit.connect()

    const poolChannels = [...dialer.connections[0].channels]
    const events = []

    rabbit.on('disconnected', () => events.push('disconnected'))

    await rabbit.disconnect()

    assert.deepEqual(events, ['disconnected'])
    assert.equal(rabbit.getClusterStatus().connectionState, 'disconnected')
    assert.ok(poolChannels.every(channel => channel.closed), 'every pool channel must be closed')
    assert.equal(dialer.connections[0].closed, true, 'the AMQP connection must be closed')
    await assert.rejects(() => rabbit.getChannel(), /Not connected/)
  })
})

describe('RabbitMQ graceful shutdown', () => {
  // These tests touch the real process signal handlers, so they install one
  // handler for a signal the runner does not use and always restore the
  // listener set afterwards. They must NEVER install SIGINT/SIGTERM handlers
  // with exitProcess enabled: a CI cancellation would then be swallowed by
  // process.exit(0) and an aborted run would be reported as passing.
  const SAFE_SIGNAL = 'SIGPIPE'

  const withSignalCleanup = (t) => {
    const original = process.listeners(SAFE_SIGNAL)

    t.after(() => {
      process.removeAllListeners(SAFE_SIGNAL)

      for (const listener of original) process.on(SAFE_SIGNAL, listener)
    })

    return original.length
  }

  test('installs one handler per signal and disconnects exactly once when it fires', async (t) => {
    const baseline = withSignalCleanup(t)
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    await rabbit.connect()

    let disconnectCalls = 0
    const realDisconnect = rabbit.disconnect.bind(rabbit)

    rabbit.disconnect = async () => {
      disconnectCalls++

      return realDisconnect()
    }

    rabbit.enableGracefulShutdown({ signals: [SAFE_SIGNAL], exitProcess: false })

    assert.equal(process.listenerCount(SAFE_SIGNAL), baseline + 1, 'one handler installed')

    rabbit.enableGracefulShutdown({ signals: [SAFE_SIGNAL], exitProcess: false })

    assert.equal(process.listenerCount(SAFE_SIGNAL), baseline + 1, 'a second call must not install another handler')

    process.emit(SAFE_SIGNAL)

    await waitFor(() => rabbit.getClusterStatus().connectionState === 'disconnected', 3000, 'graceful shutdown')

    assert.equal(disconnectCalls, 1, 'the signal must trigger exactly one disconnect')
  })

  test('logs the failure instead of throwing when shutdown cannot disconnect', async (t) => {
    const baseline = withSignalCleanup(t)
    const logger = recordingLogger()
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer, { logger })

    await rabbit.connect()

    rabbit.disconnect = async () => {
      throw new Error('teardown exploded')
    }

    rabbit.enableGracefulShutdown({ signals: [SAFE_SIGNAL], exitProcess: false })
    assert.equal(process.listenerCount(SAFE_SIGNAL), baseline + 1)

    process.emit(SAFE_SIGNAL)

    await waitFor(
      () => logger.records.error.some(message => /teardown exploded/.test(message)),
      3000,
      'shutdown failure logged'
    )
  })

  test('setupGracefulShutdown warns and delegates to enableGracefulShutdown', async (t) => {
    const logger = recordingLogger()
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer, { logger })

    // The delegation target is replaced so the alias cannot install real
    // SIGINT/SIGTERM handlers in the test runner process — with the default
    // exitProcess: true, a CI cancellation would exit 0 and mask the failure.
    const calls = []

    rabbit.enableGracefulShutdown = (...args) => calls.push(args)

    rabbit.setupGracefulShutdown()

    assert.equal(calls.length, 1, 'the alias must delegate, not merely warn')
    assert.ok(logger.records.warn.some(message => /deprecated/.test(message)))
    assert.equal(process.listenerCount('SIGINT'), 0, 'no real signal handler may leak into the runner')
    assert.equal(process.listenerCount('SIGTERM'), 0)
  })
})

describe('RabbitMQ cache (fake dialer)', () => {
  test('publishWithCache publishes on miss, dedups on hit and repopulates after invalidation', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer, { useCache: true, cacheTTL: 60 })

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    let calls = 0
    const generator = () => ({ generated: ++calls })

    const first = await rabbit.publishWithCache('orders-route', generator)
    const second = await rabbit.publishWithCache('orders-route', generator)

    assert.equal(calls, 1, 'generator runs only on cache miss')
    assert.deepEqual(first, second)
    assert.deepEqual(await rabbit.getFromCache('orders-route'), first)

    const published = dialer.connections[0].publishedOn()

    assert.equal(published.length, 1, 'cache hit must not publish again')
    assert.equal(published[0].options.headers['x-cached'], true)

    rabbit.invalidateCache('orders-route')
    await rabbit.publishWithCache('orders-route', generator)

    assert.equal(calls, 2, 'invalidation forces regeneration')
    assert.equal(dialer.connections[0].publishedOn().length, 2)

    rabbit.clearCache()
    assert.equal(await rabbit.getFromCache('orders-route'), undefined)
  })

  test('publishWithCache accepts a plain value instead of a generator', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer, { useCache: true })

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const result = await rabbit.publishWithCache('orders-route', { static: true })

    assert.deepEqual(result, { static: true })
    assert.equal(dialer.connections[0].publishedOn().length, 1)
  })
})

describe('RabbitMQ configuration setters (fake dialer)', () => {
  test('setSerializer changes how outgoing messages are encoded', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    rabbit.setSerializer((message) => `wrapped:${JSON.stringify(message)}`)
    await rabbit.publish('orders-route', { id: 1 })

    assert.equal(dialer.connections[0].publishedOn()[0].content.toString(), 'wrapped:{"id":1}')
  })

  test('setDeserializer changes how incoming messages are decoded', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    rabbit.setDeserializer((raw) => ({ unwrapped: raw.replace(/^wrapped:/, '') }))

    const received = []

    await rabbit.subscribe('orders', async (content) => received.push(content))

    const consumer = dialer.connections[0].consumersOn()[0]

    await consumer.callback({
      content: Buffer.from('wrapped:payload'),
      fields: { consumerTag: consumer.consumerTag },
      properties: { headers: { 'x-compressed': false } }
    })

    assert.deepEqual(received, [{ unwrapped: 'payload' }], 'the custom deserializer must be used')
  })

  test('setCompression toggles gzip and setCompressionThreshold sets the cutoff', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const bigPayload = { blob: 'x'.repeat(500) }

    // Disabled by default: nothing is compressed regardless of size.
    await rabbit.publish('orders-route', bigPayload)
    assert.equal(dialer.connections[0].publishedOn().at(-1).options.headers['x-compressed'], false)

    rabbit.setCompression(true)
    rabbit.setCompressionThreshold(100)
    await rabbit.publish('orders-route', bigPayload)

    const compressed = dialer.connections[0].publishedOn().at(-1)

    assert.equal(compressed.options.headers['x-compressed'], true, 'payload above the threshold must be compressed')
    assert.ok(compressed.content.length < 500)

    // Raising the threshold above the payload size disables it again.
    rabbit.setCompressionThreshold(5000)
    await rabbit.publish('orders-route', bigPayload)

    assert.equal(dialer.connections[0].publishedOn().at(-1).options.headers['x-compressed'], false)
  })
})

describe('RabbitMQ rate limiting and DLQ processing (fake dialer)', () => {
  test('rate limiter blocks publishes, emits rateLimited and honors reset/block', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer, {
      rateLimiter: { strategy: 'fixed-window', maxRequests: 1, windowMs: 60000 }
    })

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const limited = []

    rabbit.on('rateLimited', (payload) => limited.push(payload))

    await rabbit.publish('orders-route', { n: 1 })

    const publishedBeforeLimit = dialer.connections[0].publishedOn().length

    await assert.rejects(
      () => rabbit.publish('orders-route', { n: 2 }),
      (error) => error.code === 'RATE_LIMIT_EXCEEDED'
    )

    assert.equal(dialer.connections[0].publishedOn().length, publishedBeforeLimit, 'a limited publish must not reach the broker')
    assert.equal(limited.length, 1)
    assert.equal(limited[0].key, 'orders-route')
    assert.equal(rabbit.getRateLimitStatus('orders-route').isBlocked, false)

    rabbit.resetRateLimit('orders-route')
    await rabbit.publish('orders-route', { n: 3 })

    assert.equal(dialer.connections[0].publishedOn().length, publishedBeforeLimit + 1, 'reset must let publishes through')

    rabbit.blockRateLimit('orders-route', 60000)
    assert.equal(rabbit.getRateLimitStatus('orders-route').isBlocked, true)
  })

  test('processDeadLetterQueue acks messages whose processor crashed and keeps consuming', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const processed = []

    await rabbit.processDeadLetterQueue('orders', async (content) => {
      processed.push(content)

      if (content.explode) throw new Error('processor crashed')
    })

    const consumer = dialer.connections[0].consumersOn()[0]

    assert.equal(consumer.queue, 'orders_dlq')

    await deliverTo(consumer, { dead: 1 })
    await deliverTo(consumer, { dead: 2, explode: true })
    // The consumer must still be alive for messages arriving after the crash.
    await deliverTo(consumer, { dead: 3 })

    assert.deepEqual(processed.map(p => p.dead), [1, 2, 3])

    const consumerChannel = dialer.connections[0].channels.find(c => c.consumers.length > 0)

    // The swallowed error is the point: without it the subscribe pipeline
    // would nack the message and dead-letter an already-inspected message.
    assert.equal(consumerChannel.acked.length, 3, 'every DLQ message must be acked, including the crashing one')
    assert.equal(consumerChannel.nacked.length, 0)
  })
})
