import assert from 'node:assert/strict'
import RabbitMQ from '../src/index.js'
import { test, describe } from 'node:test'
import { EventEmitter } from 'node:events'

const silentLogger = { info () {}, warn () {}, error () {}, debug () {} }

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const waitFor = async (predicate, timeoutMs = 3000, label = 'condition') => {
  const start = Date.now()

  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return

    await sleep(10)
  }

  throw new Error(`Timeout waiting for: ${label}`)
}

class FakeChannel extends EventEmitter {
  constructor () {
    super()
    this.published = []
    this.consumers = []
    this.acked = []
    this.assertedExchanges = []
    this.consumeSequence = 0
  }

  async assertExchange (name, type, options) {
    this.assertedExchanges.push({ name, type, options })
  }

  async assertQueue () {}
  async bindQueue () {}
  async deleteExchange () {}
  async prefetch () {}

  async consume (queue, callback, options) {
    this.consumers.push({ queue, callback, options })

    return { consumerTag: `tag-${++this.consumeSequence}` }
  }

  publish (exchange, routingKey, content, options, confirmCallback) {
    this.published.push({ exchange, routingKey, content, options })

    if (confirmCallback) setImmediate(() => confirmCallback(null))

    return true
  }

  ack (msg) {
    this.acked.push(msg)
  }

  nack () {}
  async cancel () {}
  async close () {}
}

class FakeAmqpConnection extends EventEmitter {
  constructor () {
    super()
    this.channels = []
    this.channelError = null
  }

  async createConfirmChannel () {
    if (this.channelError) throw this.channelError

    const channel = new FakeChannel()

    this.channels.push(channel)

    return channel
  }

  async close () {
    this.closed = true
  }

  publishedOn () {
    return this.channels.flatMap(channel => channel.published)
  }

  consumersOn () {
    return this.channels.flatMap(channel => channel.consumers)
  }
}

const createDialer = (plan = ['ok']) => {
  const dialer = {
    dials: 0,
    connections: [],
    // The connection instance captures the connect function at construction,
    // so tests customize freshly dialed connections through this hook instead
    // of reassigning dialer.connect (which would never be seen).
    onConnection: null,
    connect: async () => {
      dialer.dials++

      const outcome = plan.length > 1 ? plan.shift() : plan[0]

      if (outcome !== 'ok') throw outcome

      const connection = new FakeAmqpConnection()

      dialer.connections.push(connection)
      dialer.onConnection?.(connection)

      return connection
    }
  }

  return dialer
}

const createRabbit = (dialer, options = {}) => new RabbitMQ({
  username: 'admin',
  password: 'admin',
  endpoints: ['node-a:5672'],
  connectionName: 'lifecycle-test',
  reconnectInterval: 10,
  maxReconnectInterval: 20,
  channelPoolSize: 2,
  exchange: { name: 'lifecycle-exchange', type: 'direct' },
  logger: silentLogger,
  amqpConnect: dialer.connect,
  ...options
})

describe('RabbitMQ lifecycle (fake dialer)', () => {
  test('connect builds the channel pool, asserts the exchange and emits connected', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer)

    t.after(() => rabbit.disconnect())

    const events = []

    rabbit.on('connected', () => events.push('connected'))

    const connection = await rabbit.connect()

    assert.ok(connection)
    assert.deepEqual(events, ['connected'])
    // channelPoolSize pool channels, exchange asserted on one of them.
    assert.equal(dialer.connections[0].channels.length, 2)
    assert.ok(dialer.connections[0].channels.some(c => c.assertedExchanges.some(e => e.name === 'lifecycle-exchange')))
    assert.equal(rabbit.getClusterStatus().connectionState, 'connected')
  })

  test('publish and subscribe work end to end over the fake broker', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const received = []

    await rabbit.subscribe('orders', async (content, message) => {
      received.push(content)
    })

    await rabbit.publish('orders-route', { id: 7 })
    await rabbit.publishAsync('orders-route', { id: 8 })

    const published = dialer.connections[0].publishedOn()

    assert.equal(published.length, 2)
    assert.equal(published[0].exchange, 'lifecycle-exchange')

    // Deliver a message back through the consumer channel.
    const consumer = dialer.connections[0].consumersOn()[0]

    await consumer.callback({
      content: Buffer.from(JSON.stringify({ inbound: true })),
      fields: { consumerTag: 'tag-1' },
      properties: { headers: { 'x-compressed': false } }
    })

    assert.deepEqual(received, [{ inbound: true }])
  })

  test('an unexpected connection close recovers pool and consumers, then publishes again', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer)

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

  test('emits reconnectError when post-reconnection setup fails', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const reconnectError = new Promise(resolve => rabbit.once('reconnectError', resolve))

    // The next connection dials fine but cannot open channels: the pool
    // rebuild fails and the facade must surface it instead of emitting
    // 'reconnected'.
    dialer.onConnection = (connection) => {
      connection.channelError = new Error('channels refused')
    }

    dialer.connections[0].emit('close')

    const error = await reconnectError

    assert.match(error.message, /channels refused/)
  })

  test('connect({ waitForConnection }) resolves when a later reconnection succeeds', async (t) => {
    const dialer = createDialer([new Error('down'), new Error('still down'), 'ok'])
    const rabbit = createRabbit(dialer)

    t.after(() => rabbit.disconnect())

    const connection = await rabbit.connect({ waitForConnection: true })

    assert.ok(connection, 'must resolve with the connection produced by the background retry')
    assert.equal(rabbit.getClusterStatus().connectionState, 'connected')
  })

  test('connect({ waitForConnection, timeout }) rejects when nothing connects in time', async (t) => {
    const dialer = createDialer([new Error('permanently down')])
    const rabbit = createRabbit(dialer)

    t.after(() => rabbit.disconnect())

    await assert.rejects(
      () => rabbit.connect({ waitForConnection: true, timeout: 100 }),
      /Timed out after 100ms/
    )
  })

  test('connect({ waitForConnection }) rejects when reconnection gives up', async (t) => {
    const dialer = createDialer([new Error('permanently down')])
    const rabbit = createRabbit(dialer, { maxReconnectAttempts: 1 })

    t.after(() => rabbit.disconnect())

    await assert.rejects(
      () => rabbit.connect({ waitForConnection: true }),
      /all reconnection attempts failed/
    )
  })

  test('disconnect closes the pool, emits disconnected and getChannel fails afterwards', async () => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer)

    await rabbit.connect()

    const events = []

    rabbit.on('disconnected', () => events.push('disconnected'))

    await rabbit.disconnect()

    assert.deepEqual(events, ['disconnected'])
    assert.equal(rabbit.getClusterStatus().connectionState, 'disconnected')
    await assert.rejects(() => rabbit.getChannel(), /Not connected/)
  })

  test('enableGracefulShutdown disconnects on the configured signal exactly once', async () => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer)

    await rabbit.connect()

    rabbit.enableGracefulShutdown({ signals: ['SIGPIPE'], exitProcess: false })
    // Second call must be a no-op (no duplicate handlers).
    rabbit.enableGracefulShutdown({ signals: ['SIGPIPE'], exitProcess: false })

    process.emit('SIGPIPE')

    await waitFor(() => rabbit.getClusterStatus().connectionState === 'disconnected', 3000, 'graceful shutdown')
  })

  test('setupGracefulShutdown is a deprecated alias that warns', async () => {
    const warnings = []
    const dialer = createDialer()
    const rabbit = createRabbit(dialer, {
      logger: { ...silentLogger, warn: (message) => warnings.push(message) }
    })

    rabbit.setupGracefulShutdown()

    assert.ok(warnings.some(w => /deprecated/.test(w)))
  })
})

describe('RabbitMQ cache (fake dialer)', () => {
  test('publishWithCache publishes on miss, dedups on hit and repopulates after invalidation', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer, { useCache: true, cacheTTL: 60 })

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

    rabbit.clearCache()
    assert.equal(await rabbit.getFromCache('orders-route'), undefined)
  })

  test('publishWithCache accepts a plain value instead of a generator', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer, { useCache: true })

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const result = await rabbit.publishWithCache('orders-route', { static: true })

    assert.deepEqual(result, { static: true })
  })
})

describe('RabbitMQ configuration and rate limiting (fake dialer)', () => {
  test('setSerializer/setDeserializer/setCompression actually reshape the wire format', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    rabbit.setCompression(false)
    rabbit.setCompressionThreshold(5000)
    rabbit.setSerializer((message) => `wrapped:${JSON.stringify(message)}`)
    rabbit.setDeserializer((raw) => JSON.parse(raw.replace(/^wrapped:/, '')))

    await rabbit.publish('orders-route', { id: 1 })

    const [published] = dialer.connections[0].publishedOn()

    assert.match(published.content.toString(), /^wrapped:/)
  })

  test('rate limiter delegates work and the rateLimited event fires on rejection', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer, {
      rateLimiter: { strategy: 'fixed-window', maxRequests: 1, windowMs: 60000 }
    })

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const limited = []

    rabbit.on('rateLimited', (payload) => limited.push(payload))

    await rabbit.publish('orders-route', { n: 1 })

    await assert.rejects(
      () => rabbit.publish('orders-route', { n: 2 }),
      (error) => error.code === 'RATE_LIMIT_EXCEEDED'
    )

    assert.equal(limited.length, 1)
    assert.equal(limited[0].key, 'orders-route')

    const status = rabbit.getRateLimitStatus('orders-route')

    assert.equal(status.isBlocked, false)

    rabbit.resetRateLimit('orders-route')
    await rabbit.publish('orders-route', { n: 3 })

    rabbit.blockRateLimit('orders-route', 60000)
    assert.equal(rabbit.getRateLimitStatus('orders-route').isBlocked, true)
  })

  test('processDeadLetterQueue consumes the DLQ and swallows processor errors', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const processed = []

    await rabbit.processDeadLetterQueue('orders', async (content) => {
      processed.push(content)

      if (content.explode) throw new Error('processor crashed')
    })

    const consumer = dialer.connections[0].consumersOn()[0]

    assert.equal(consumer.queue, 'orders_dlq')

    const deliver = (payload) => consumer.callback({
      content: Buffer.from(JSON.stringify(payload)),
      fields: { consumerTag: 'tag-1' },
      properties: { headers: { 'x-compressed': false } }
    })

    await deliver({ dead: 1 })
    await deliver({ dead: 2, explode: true })

    assert.equal(processed.length, 2, 'a crashing processor must not stop the DLQ consumer')
  })
})
