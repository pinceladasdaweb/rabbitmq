import assert from 'node:assert/strict'
import RabbitMQ from '../src/index.js'
import { fileURLToPath } from 'node:url'
import { test, describe } from 'node:test'
import { installDialer } from './fake-amqp.js'
import { createDialer, recordingLogger, silentLogger, sleep, waitFor } from './helpers.js'

const ECHO_WORKER = fileURLToPath(new URL('./fixtures/echo-worker.mjs', import.meta.url))

// The facade is mostly thin delegation, but "thin" is exactly where a wrong
// collaborator or a dropped argument hides: every test here asserts the
// observable effect on the fake broker, never that a method merely exists.
const createRabbit = (t, dialer, options = {}) => {
  installDialer(t, dialer)

  return new RabbitMQ({
    username: 'admin',
    password: 'admin',
    endpoints: ['node-a:5672'],
    connectionName: 'facade-test',
    reconnectInterval: 10,
    maxReconnectInterval: 20,
    channelPoolSize: 2,
    exchange: { name: 'facade-exchange', type: 'direct' },
    logger: silentLogger,
    ...options
  })
}

const connected = async (t, options = {}) => {
  const dialer = createDialer()
  const rabbit = createRabbit(t, dialer, options)

  t.after(() => rabbit.disconnect())

  await rabbit.connect()

  return { rabbit, dialer, connection: dialer.connections[0] }
}

// Consumers get a dedicated channel, so the newest channel on the connection
// is the one the subscription just claimed.
const lastConsumer = (connection) => {
  const channels = connection.channels.filter(channel => channel.consumers.length > 0)

  return channels.at(-1).consumers.at(-1)
}

const deliverTo = (consumer, payload, properties = {}) => consumer.callback({
  content: Buffer.from(JSON.stringify(payload)),
  fields: { consumerTag: consumer.consumerTag, deliveryTag: 1 },
  properties: { headers: { 'x-compressed': false }, ...properties }
})

describe('RabbitMQ facade consumption delegation', () => {
  test('subscribe reaches the broker with the configured prefetch', async (t) => {
    const { rabbit, connection } = await connected(t, { prefetchCount: 7 })

    await rabbit.subscribe('orders', async () => {})

    const consumer = lastConsumer(connection)

    assert.equal(consumer.queue, 'orders')
    assert.deepEqual(connection.channels.find(c => c.consumers.length).prefetches, [7])
  })

  test('subscribeSequential uses its own prefetch default, not subscribe()\'s', async (t) => {
    // Distinguishes the two delegations: wiring subscribeSequential to
    // subscribe would produce the pool's prefetch instead of 1.
    const { rabbit, connection } = await connected(t, { prefetchCount: 7 })

    await rabbit.subscribeSequential('steps', async () => {})

    const channel = connection.channels.find(c => c.consumers.length)

    assert.deepEqual(channel.prefetches, [1])
  })

  test('subscribeWithOptimizedPrefetch starts at its initial prefetch', async (t) => {
    const { rabbit, connection } = await connected(t)

    await rabbit.subscribeWithOptimizedPrefetch('heavy', async () => {}, { initialPrefetch: 3 })

    const channel = connection.channels.find(c => c.consumers.length)

    assert.deepEqual(channel.prefetches, [3])
  })

  test('subscribeParallel runs the payload through a worker and acks', async (t) => {
    const { rabbit, connection } = await connected(t)

    const consumer = await rabbit.subscribeParallel('jobs', ECHO_WORKER, { workerCount: 1 })

    t.after(() => rabbit.unsubscribe(consumer.consumerTag))

    const channel = connection.channels.find(c => c.consumers.length)

    await deliverTo(channel.consumers.at(-1), { job: 'resize' })
    await waitFor(() => channel.acked.length === 1, 5000, 'worker processed the message')
  })

  test('unsubscribe cancels the consumer on the broker', async (t) => {
    const { rabbit, connection } = await connected(t)

    const consumer = await rabbit.subscribe('orders', async () => {})
    const channel = connection.channels.find(c => c.consumers.length)

    assert.equal(await rabbit.unsubscribe(consumer.consumerTag), true)
    assert.deepEqual(channel.cancelled, [consumer.consumerTag])
  })

  test('acknowledgeMessage and negativeAcknowledgeMessage settle on the delivering channel', async (t) => {
    const { rabbit, connection } = await connected(t)
    const received = []

    await rabbit.subscribe('orders', async (content, message) => {
      received.push(message)
    }, { noAck: true })

    const channel = connection.channels.find(c => c.consumers.length)

    await deliverTo(channel.consumers.at(-1), { id: 1 })
    await deliverTo(channel.consumers.at(-1), { id: 2 })

    await rabbit.acknowledgeMessage(received[0])
    await rabbit.negativeAcknowledgeMessage(received[1], { requeue: true })

    assert.equal(channel.acked.length, 1)
    assert.deepEqual(channel.nacked.map(n => n.requeue), [true], 'the requeue option is forwarded')
  })
})

describe('RabbitMQ facade topology delegation', () => {
  test('setupDeadLetterExchange asserts the DLX', async (t) => {
    const { rabbit, connection } = await connected(t, { deadLetterExchange: 'facade-dlx' })

    await rabbit.setupDeadLetterExchange()

    assert.ok(
      connection.channels.some(c => c.assertedExchanges.some(e => e.name === 'facade-dlx')),
      'the dead letter exchange was declared'
    )
  })

  test('createQueue declares the queue and its DLQ', async (t) => {
    const { rabbit, connection } = await connected(t)

    await rabbit.createQueue('orders')

    const declared = connection.channels.flatMap(c => c.assertedQueues).map(q => q.name)

    assert.ok(declared.includes('orders'))
    assert.ok(declared.includes('orders_dlq'), 'the companion DLQ is declared too')
  })

  test('moveToDeadLetter publishes the message to the DLX', async (t) => {
    const { rabbit, connection } = await connected(t)

    await rabbit.createQueue('orders')

    const message = {
      content: Buffer.from(JSON.stringify({ id: 1 })),
      fields: { routingKey: 'orders' },
      properties: { headers: {} }
    }

    await rabbit.moveToDeadLetter(message, 'unprocessable')

    const published = connection.channels.flatMap(c => c.published)

    assert.ok(published.some(p => p.routingKey === 'orders_dlq'), 'routed to the DLQ binding')
  })

  test('processDeadLetterQueue consumes the companion DLQ', async (t) => {
    const { rabbit, connection } = await connected(t)
    const seen = []

    await rabbit.processDeadLetterQueue('orders', async (content) => {
      seen.push(content)
    })

    const consumer = lastConsumer(connection)

    assert.equal(consumer.queue, 'orders_dlq', 'derives the DLQ name from the original queue')

    await deliverTo(consumer, { id: 1 })
    await waitFor(() => seen.length === 1, 3000, 'dead letter processed')
  })

  test('processDeadLetterQueue logs a failing processor and still acks', async (t) => {
    // A dead letter that cannot be processed has nowhere further to go:
    // rethrowing would nack it back into the same DLQ forever.
    const logger = recordingLogger()
    const { rabbit, connection } = await connected(t, { logger })

    await rabbit.processDeadLetterQueue('orders', async () => {
      throw new Error('processor exploded')
    })

    const channel = connection.channels.find(c => c.consumers.length)

    await deliverTo(channel.consumers.at(-1), { id: 1 })
    await waitFor(() => channel.acked.length === 1, 3000, 'dead letter acked despite the failure')

    assert.equal(channel.nacked.length, 0, 'never nacked back into the DLQ')
    assert.ok(logger.records.error.some(line => line.includes('processor exploded')))
  })

  test('setupDelayExchange, setupDelayPlugin and isDelayPluginEnabled reach the broker', async (t) => {
    const { rabbit, connection } = await connected(t, { delayExchange: 'facade-delayed' })

    await rabbit.setupDelayExchange()

    assert.ok(
      connection.channels.some(c => c.assertedExchanges.some(e => e.name === 'facade-delayed' && e.type === 'x-delayed-message')),
      'declares the delayed-message exchange'
    )

    assert.equal(await rabbit.isDelayPluginEnabled(), true, 'the fake broker accepts the probe')
    await rabbit.setupDelayPlugin()
  })
})

describe('RabbitMQ facade publishing delegation', () => {
  test('publishAsync and publishAsyncBatch reach the exchange', async (t) => {
    const { rabbit, connection } = await connected(t)

    await rabbit.publishAsync('route-a', { n: 1 })
    await rabbit.publishAsyncBatch('route-b', [{ n: 2 }, { n: 3 }])

    await waitFor(
      () => connection.channels.flatMap(c => c.published).length === 3,
      3000,
      'all three messages published'
    )

    const published = connection.channels.flatMap(c => c.published)

    assert.deepEqual(published.map(p => p.routingKey).sort(), ['route-a', 'route-b', 'route-b'])
  })

  test('publishDelayed forwards the delay as the x-delay header', async (t) => {
    const { rabbit, connection } = await connected(t, { delayExchange: 'facade-delayed' })

    await rabbit.setupDelayExchange()
    await rabbit.publishDelayed('route-later', { n: 1 }, 5000)

    const published = connection.channels.flatMap(c => c.published).find(p => p.routingKey === 'route-later')

    assert.ok(published, 'the delayed message was published')
    assert.equal(published.options.headers['x-delay'], 5000)
  })
})

describe('RabbitMQ facade RPC delegation', () => {
  test('respond subscribes to the request queue and replies to replyTo', async (t) => {
    const { rabbit, connection } = await connected(t)

    await rabbit.respond('rpc-users', async (content) => ({ echoed: content.id }))

    const consumer = lastConsumer(connection)

    assert.equal(consumer.queue, 'rpc-users')

    await deliverTo(consumer, { id: 42 }, { replyTo: 'amq.rabbitmq.reply-to', correlationId: 'corr-1' })

    await waitFor(
      () => connection.channels.flatMap(c => c.published).some(p => p.routingKey === 'amq.rabbitmq.reply-to'),
      3000,
      'reply published back to the requester'
    )

    const reply = connection.channels.flatMap(c => c.published).find(p => p.routingKey === 'amq.rabbitmq.reply-to')

    assert.equal(reply.options.correlationId, 'corr-1')
    assert.deepEqual(JSON.parse(reply.content.toString()), { echoed: 42 })
  })

  test('request rejects with RPC_TIMEOUT when no reply arrives', async (t) => {
    const { rabbit } = await connected(t)

    const error = await rabbit.request('nowhere', { ping: 1 }, { timeout: 60 })
      .then(() => null, (err) => err)

    assert.equal(error.code, 'RPC_TIMEOUT')
  })
})

describe('RabbitMQ facade configuration', () => {
  test('setExchange replaces the exchange used by publishes', async (t) => {
    const { rabbit, connection } = await connected(t)

    rabbit.setExchange('other-exchange', 'topic')

    await rabbit.publish('route', { n: 1 })

    const published = connection.channels.flatMap(c => c.published)

    assert.deepEqual(published.map(p => p.exchange), ['other-exchange'])
  })

  test('setCompression toggles compression for subsequent publishes', async (t) => {
    const { rabbit, connection } = await connected(t, { compressionThreshold: 10 })
    const big = { text: 'x'.repeat(500) }

    rabbit.setCompression(false)
    await rabbit.publish('route', big)

    rabbit.setCompression(true)
    await rabbit.publish('route', big)

    const [plain, compressed] = connection.channels.flatMap(c => c.published)

    assert.notEqual(plain.options.headers['x-compressed'], true)
    assert.equal(compressed.options.headers['x-compressed'], true)
  })

  test('setCompressionThreshold changes what gets compressed', async (t) => {
    const { rabbit, connection } = await connected(t, { useCompression: true })
    const payload = { text: 'x'.repeat(200) }

    rabbit.setCompressionThreshold(100000)
    await rabbit.publish('route', payload)

    rabbit.setCompressionThreshold(10)
    await rabbit.publish('route', payload)

    const [under, over] = connection.channels.flatMap(c => c.published)

    assert.notEqual(under.options.headers['x-compressed'], true, 'below the threshold: sent as-is')
    assert.equal(over.options.headers['x-compressed'], true)

    assert.throws(() => rabbit.setCompressionThreshold(-1), /non-negative number/)
    assert.throws(() => rabbit.setCompressionThreshold('big'), /non-negative number/)
  })

  test('setSerializer and setDeserializer replace the wire format end to end', async (t) => {
    const { rabbit, connection } = await connected(t)

    rabbit.setSerializer((value) => `serialized:${value.n}`)
    rabbit.setDeserializer((raw) => ({ roundTripped: raw.replace('serialized:', '') }))

    await rabbit.publish('route', { n: 7 })

    const published = connection.channels.flatMap(c => c.published).at(-1)

    assert.equal(published.content.toString(), 'serialized:7')

    const received = []

    await rabbit.subscribe('orders', async (content) => { received.push(content) })

    const consumer = lastConsumer(connection)

    await consumer.callback({
      content: Buffer.from('serialized:7'),
      fields: { consumerTag: consumer.consumerTag, deliveryTag: 1 },
      properties: { headers: { 'x-compressed': false } }
    })

    assert.deepEqual(received, [{ roundTripped: '7' }])

    assert.throws(() => rabbit.setSerializer('nope'), /must be a function/)
    assert.throws(() => rabbit.setDeserializer('nope'), /must be a function/)
  })

  test('getCircuitBreakerState reports the breaker state', async (t) => {
    const { rabbit } = await connected(t)

    assert.equal(rabbit.getCircuitBreakerState().state, 'CLOSED')
    assert.equal(rabbit.getCircuitBreakerState().failureCount, 0)
  })
})

describe('RabbitMQ facade rate limiter', () => {
  const rateLimited = (overrides = {}) => ({
    rateLimiter: { enabled: true, maxRequests: 1, interval: 60000, strategy: 'fixed-window', ...overrides }
  })

  test('getRateLimitStatus, resetRateLimit and blockRateLimit act on the limiter', async (t) => {
    // maxRequests: 1 per window, so each method has a distinct observable
    // effect on whether the very next publish is allowed through.
    const { rabbit } = await connected(t, rateLimited())

    await rabbit.publish('route', { n: 1 })

    assert.ok(rabbit.getRateLimitStatus('route'), 'status is reported for a used key')
    await assert.rejects(() => rabbit.publish('route', { n: 2 }, { maxRetries: 1 }), /Rate limit exceeded/)

    rabbit.resetRateLimit('route')
    await rabbit.publish('route', { n: 3 }, { maxRetries: 1 })

    rabbit.blockRateLimit('route', 60000)
    await assert.rejects(() => rabbit.publish('route', { n: 4 }, { maxRetries: 1 }), /Rate limit exceeded/)
  })

  test('rateLimited and rateBlocked are re-emitted on the facade', async (t) => {
    const { rabbit } = await connected(t, rateLimited())
    const events = []

    rabbit.on('rateLimited', (payload) => events.push({ event: 'rateLimited', ...payload }))
    rabbit.on('rateBlocked', (payload) => events.push({ event: 'rateBlocked', ...payload }))

    await rabbit.publish('route', { n: 1 })
    await rabbit.publish('route', { n: 2 }, { maxRetries: 1 }).catch(() => {})

    rabbit.blockRateLimit('route', 50)
    await rabbit.publish('route', { n: 3 }, { maxRetries: 1 }).catch(() => {})

    assert.ok(events.some(e => e.event === 'rateLimited' && e.key === 'route'), 'rateLimited forwarded with its key')
    assert.ok(events.some(e => e.event === 'rateBlocked' && e.key === 'route'), 'rateBlocked forwarded with its key')
  })

  test('every rate limiter method throws when the limiter is disabled', async (t) => {
    const { rabbit } = await connected(t)

    assert.throws(() => rabbit.getRateLimitStatus('route'), /Rate limiter is not enabled/)
    assert.throws(() => rabbit.resetRateLimit('route'), /Rate limiter is not enabled/)
    assert.throws(() => rabbit.blockRateLimit('route', 10), /Rate limiter is not enabled/)
  })
})

describe('RabbitMQ facade cache', () => {
  test('getFromCache, invalidateCache and clearCache operate on published entries', async (t) => {
    const { rabbit } = await connected(t, { useCache: true })

    await rabbit.publishWithCache('route', { n: 1 }, { cacheTTL: 60 })

    assert.ok(await rabbit.getFromCache('route'), 'the publish populated the cache')

    rabbit.invalidateCache('route')
    assert.equal(await rabbit.getFromCache('route'), undefined)

    await rabbit.publishWithCache('route', { n: 2 }, { cacheTTL: 60 })
    assert.ok(await rabbit.getFromCache('route'))

    rabbit.clearCache()
    assert.equal(await rabbit.getFromCache('route'), undefined)
  })

  test('every cache method throws when the cache is disabled', async (t) => {
    const { rabbit } = await connected(t)

    await assert.rejects(() => rabbit.getFromCache('route'), /Cache is not enabled/)
    assert.throws(() => rabbit.invalidateCache('route'), /Cache is not enabled/)
    assert.throws(() => rabbit.clearCache(), /Cache is not enabled/)
  })
})

describe('RabbitMQ facade connection edge cases', () => {
  test('connect returns null when every endpoint fails and waitForConnection is off', async (t) => {
    const dialer = createDialer([new Error('broker down')])
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    assert.equal(await rabbit.connect(), null, 'the caller is told, not left waiting')
  })

  test('waitForConnection rejects when the reconnection restores state badly', async (t) => {
    // 'reconnected' never fires when post-connection setup throws, so without
    // the reconnectError listener the waiter would hang until its timeout.
    const dialer = createDialer([new Error('broker down'), 'ok'])
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    dialer.onConnection = (connection) => {
      connection.channelError = new Error('channel refused')
    }

    await assert.rejects(
      () => rabbit.connect({ waitForConnection: true, timeout: 3000 }),
      /failed to restore state: channel refused/
    )
  })

  test('getChannel rejects once the pool is gone', async (t) => {
    const { rabbit } = await connected(t)

    await rabbit.disconnect()

    await assert.rejects(() => rabbit.getChannel(), /Not connected/)
  })

  test('getClusterStatus describes the live connection', async (t) => {
    const { rabbit } = await connected(t)

    assert.equal(rabbit.getClusterStatus().connectionState, 'connected')
  })

  test('consumer events raised inside ConsumerManager surface on the facade', async (t) => {
    // The collaborators emit through a context.emit bridge rather than being
    // EventEmitters themselves. Without that bridge these documented events
    // would fire into the void.
    const { rabbit, connection } = await connected(t)
    const events = []

    rabbit.on('consumerCancelled', (payload) => events.push(payload))

    await rabbit.subscribe('orders', async () => {})

    const consumer = lastConsumer(connection)

    // amqplib delivers a broker-side cancellation as a null message.
    await consumer.callback(null)

    await waitFor(() => events.length === 1, 3000, 'consumerCancelled reached the facade')

    assert.equal(events[0].queueName, 'orders')
    assert.equal(events[0].consumerTag, consumer.consumerTag)
  })

  test('a socket that dies during setup yields null, never a half-built facade', async (t) => {
    // The pool is built right after the dial returns. A connection lost in
    // that window must not produce a facade that reports success while having
    // no pool: connect() reports the failure and the reconnection loop takes
    // over. This is also why #setupChannelPool's null-connection guard is
    // never reached in practice — connect() already returned null by then.
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    rabbit.once('connected', () => {
      dialer.connections.at(-1).emit('close')
    })

    assert.equal(await rabbit.connect(), null)
    await assert.rejects(() => rabbit.getChannel(), /Not connected/, 'no pool was left behind')
  })
})

describe('RabbitMQ facade shutdown', () => {
  test('enableGracefulShutdown is idempotent and disconnects on the signal', async (t) => {
    const { rabbit } = await connected(t)
    const installed = []

    // Captured instead of installed for real: a genuine SIGTERM handler would
    // let a cancelled CI run exit 0 and report an aborted suite as green.
    const originalOn = process.on.bind(process)

    process.on = (event, handler) => {
      if (event === 'SIGINT' || event === 'SIGTERM') {
        installed.push({ event, handler })

        return process
      }

      return originalOn(event, handler)
    }

    t.after(() => { process.on = originalOn })

    rabbit.enableGracefulShutdown({ exitProcess: false })
    rabbit.enableGracefulShutdown({ exitProcess: false })

    assert.deepEqual(installed.map(entry => entry.event), ['SIGINT', 'SIGTERM'], 'installed exactly once')

    await installed[0].handler()
    await sleep(20)

    assert.equal(rabbit.getClusterStatus().connectionState, 'disconnected')
  })
})
