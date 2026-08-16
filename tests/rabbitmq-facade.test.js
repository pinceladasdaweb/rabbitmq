import assert from 'node:assert/strict'
import RabbitMQ from '../src/index.js'
import { fileURLToPath } from 'node:url'
import { test, describe } from 'node:test'
import { installDialer } from './fake-amqp.js'
import { createDialer, recordingLogger, silentLogger, sleep, waitFor, withLiveEventLoop } from './helpers.js'

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

describe('RabbitMQ facade constructor defaults', () => {
  // None of these had an assertion: every fallback was free to be anything,
  // including nothing. They are observable at the wire — the connection string
  // the dialer receives, the client properties, the exchanges declared.
  const bare = (t, options = {}) => {
    const dialer = createDialer()

    installDialer(t, dialer)

    const rabbit = new RabbitMQ({
      username: 'admin',
      password: 'admin',
      endpoints: ['node-a:5672'],
      channelPoolSize: 1,
      logger: silentLogger,
      ...options
    })

    t.after(() => rabbit.disconnect())

    return { rabbit, dialer }
  }

  test('names the connection default_connection when none is given', async (t) => {
    const { rabbit, dialer } = bare(t)

    await rabbit.connect()

    assert.equal(dialer.socketOptions.clientProperties.connection_name, 'default_connection')
  })

  test('falls back to the environment for credentials and endpoint', async (t) => {
    const previous = {
      user: process.env.RABBITMQ_USER,
      pass: process.env.RABBITMQ_PASS,
      endpoint: process.env.RABBITMQ_ENDPOINT
    }

    process.env.RABBITMQ_USER = 'env-user'
    process.env.RABBITMQ_PASS = 'env-pass'
    process.env.RABBITMQ_ENDPOINT = 'env-host:5672'

    t.after(() => {
      for (const [key, value] of [['RABBITMQ_USER', previous.user], ['RABBITMQ_PASS', previous.pass], ['RABBITMQ_ENDPOINT', previous.endpoint]]) {
        if (value === undefined) delete process.env[key]
        else process.env[key] = value
      }
    })

    const dialer = createDialer()

    installDialer(t, dialer)

    const rabbit = new RabbitMQ({ channelPoolSize: 1, logger: silentLogger })

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    assert.equal(dialer.urls[0], 'amqp://env-user:env-pass@env-host:5672')
  })

  test('explicit options win over the environment', async (t) => {
    const previous = process.env.RABBITMQ_USER

    process.env.RABBITMQ_USER = 'env-user'
    t.after(() => {
      if (previous === undefined) delete process.env.RABBITMQ_USER
      else process.env.RABBITMQ_USER = previous
    })

    const { rabbit, dialer } = bare(t)

    await rabbit.connect()

    assert.match(dialer.urls[0], /^amqp:\/\/admin:admin@node-a:5672$/)
  })

  test('accepts a single endpoint as well as a list', async (t) => {
    const dialer = createDialer()

    installDialer(t, dialer)

    const rabbit = new RabbitMQ({
      username: 'admin',
      password: 'admin',
      endpoint: 'solo-host:5672',
      channelPoolSize: 1,
      logger: silentLogger
    })

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    assert.equal(dialer.urls[0], 'amqp://admin:admin@solo-host:5672')
  })

  test('defaults the dead letter exchange to dlx', async (t) => {
    const { rabbit, dialer } = bare(t)

    await rabbit.connect()
    await rabbit.setupDeadLetterExchange()

    const declared = dialer.connections[0].channels.flatMap(c => c.assertedExchanges)

    assert.ok(declared.some(e => e.name === 'dlx'), 'the DLQ routing depends on this name')
  })

  test('defaults the delay exchange to delayed', async (t) => {
    const { rabbit, dialer } = bare(t)

    await rabbit.connect()
    await rabbit.setupDelayExchange()

    const declared = dialer.connections[0].channels.flatMap(c => c.assertedExchanges)

    assert.ok(declared.some(e => e.name === 'delayed' && e.type === 'x-delayed-message'))
  })

  test('defaults maxPriority to 10', async (t) => {
    const { rabbit } = bare(t, { exchange: { name: 'prio-exchange', type: 'direct' } })

    await rabbit.connect()

    await rabbit.publish('route', { n: 1 }, { priority: 10 })
    await assert.rejects(() => rabbit.publish('route', { n: 1 }, { priority: 11 }), /priority/i)
  })

  test('defaults the exchange type to direct when only a name is given', async (t) => {
    const { rabbit, dialer } = bare(t, { exchange: { name: 'typeless-exchange' } })

    await rabbit.connect()

    const declared = dialer.connections[0].channels.flatMap(c => c.assertedExchanges)

    assert.ok(declared.some(e => e.name === 'typeless-exchange' && e.type === 'direct'))
  })

  test('resolves a queue name from a consumer tag for dead lettering', async (t) => {
    // The facade wires this lookup into Topology; without it moveToDeadLetter
    // falls back to the routing key and can pick the wrong DLQ.
    const { rabbit, dialer } = bare(t, { exchange: { name: 'dl-exchange', type: 'direct' } })

    await rabbit.connect()
    await rabbit.createQueue('orders')

    const consumer = await rabbit.subscribe('orders', async () => {})

    await rabbit.moveToDeadLetter({
      content: Buffer.from('{}'),
      fields: { consumerTag: consumer.consumerTag, routingKey: 'some-other-route' },
      properties: { headers: {} }
    })

    const published = dialer.connections[0].channels.flatMap(c => c.published)

    assert.ok(
      published.some(p => p.routingKey === 'orders_dlq'),
      'resolved through the consumer tag, not the routing key'
    )
  })
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

  test('processDeadLetterQueue reports a failing processor and settles under the policy', async (t) => {
    // Default 'none': the message is discarded, which is what a nack without
    // requeue means on a DLQ that has no dead letter exchange of its own.
    const logger = recordingLogger()
    const { rabbit, connection } = await connected(t, { logger })

    await rabbit.processDeadLetterQueue('orders', async () => {
      throw new Error('processor exploded')
    })

    const channel = connection.channels.find(c => c.consumers.length)

    await deliverTo(channel.consumers.at(-1), { id: 1 })
    await waitFor(() => channel.nacked.length === 1, 3000, 'dead letter settled')

    assert.equal(channel.nacked[0].requeue, false)
    assert.equal(channel.acked.length, 0, 'a failed processor must not report success')
    assert.ok(logger.records.error.some(line => line.includes('processor exploded')))
  })

  test('processDeadLetterQueue settles a processor that throws a non-Error (issue #18)', async (t) => {
    // The wrapper logs before rethrowing; reading .message on a null throw
    // crashed the log line and turned a handled failure into an unhandled one.
    const logger = recordingLogger()
    const { rabbit, connection } = await connected(t, { logger })

    await rabbit.processDeadLetterQueue('orders', async () => {
      throw null // eslint-disable-line no-throw-literal
    })

    const channel = connection.channels.find(c => c.consumers.length)

    await deliverTo(channel.consumers.at(-1), { id: 1 })
    await waitFor(() => channel.nacked.length === 1, 3000, 'dead letter settled despite the null throw')

    // The full line, not a substring: the un-normalised form crashes while
    // building this exact message, and the TypeError it raises also contains
    // the word "null" — a looser assertion is satisfied by the bug itself.
    assert.ok(
      logger.records.error.some(line => line.includes('Error processing dead letter message: null')),
      'the DLQ log line is produced, not derailed by the thrown shape'
    )
  })

  test('processDeadLetterQueue honors retryPolicy instead of silently ignoring it', async (t) => {
    // Regression: the wrapper used to swallow every processor error, so the
    // callback never threw and the option documented for this method did
    // nothing at all.
    const { rabbit, connection } = await connected(t)

    await rabbit.processDeadLetterQueue('orders', async () => {
      throw new Error('processor exploded')
    }, { retryPolicy: 'once' })

    const channel = connection.channels.find(c => c.consumers.length)

    await deliverTo(channel.consumers.at(-1), { id: 1 })
    await waitFor(() => channel.nacked.length === 1, 3000, 'dead letter settled')

    assert.equal(channel.nacked[0].requeue, true, 'the first delivery is retried')
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

    const error = await withLiveEventLoop(() =>
      rabbit.request('nowhere', { ping: 1 }, { timeout: 60 }).then(() => null, (err) => err)
    )

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

  test('waitForConnection with no timeout waits indefinitely and still resolves', async (t) => {
    // Omitting the timeout is the documented way to say "block until the
    // broker is back". No timer is armed at all, so the cleanup path runs with
    // nothing to clear — a branch the timed variant never reaches.
    const dialer = createDialer([new Error('broker down'), 'ok'])
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    const connection = await rabbit.connect({ waitForConnection: true })

    assert.ok(connection, 'the reconnection satisfied the waiter')
    assert.equal(rabbit.getClusterStatus().connectionState, 'connected')
  })

  test('waitForConnection treats timeout: 0 as no timeout', async (t) => {
    // The guard is `timeout > 0`, so zero means "no deadline" rather than
    // "expire immediately" — arming a 0ms timer would reject the caller before
    // the first reconnection attempt even runs.
    const dialer = createDialer([new Error('broker down'), 'ok'])
    const rabbit = createRabbit(t, dialer, { reconnectInterval: 60, maxReconnectInterval: 60 })

    t.after(() => rabbit.disconnect())

    const connection = await rabbit.connect({ waitForConnection: true, timeout: 0 })

    assert.ok(connection, 'zero did not cut the wait short')
    assert.equal(rabbit.getClusterStatus().connectionState, 'connected')
  })

  test('waitForConnection gives up when the reconnection does', async (t) => {
    const dialer = createDialer([new Error('broker down')])
    const rabbit = createRabbit(t, dialer, { maxReconnectAttempts: 1 })

    t.after(() => rabbit.disconnect())

    await assert.rejects(
      () => rabbit.connect({ waitForConnection: true }),
      /all reconnection attempts failed/
    )
  })

  test('disconnect disposes the rate limiter', async (t) => {
    // The limiter owns a sweep interval and per-key state. Leaving it running
    // after disconnect keeps a timer alive for a client nobody is using.
    const { rabbit } = await connected(t, {
      rateLimiter: { enabled: true, maxRequests: 1, interval: 60000, strategy: 'fixed-window' }
    })

    await rabbit.publish('route', { n: 1 })
    await assert.rejects(() => rabbit.publish('route', { n: 2 }, { maxRetries: 1 }), /Rate limit exceeded/)

    await rabbit.disconnect()

    assert.equal(
      rabbit.getRateLimitStatus('route').remainingTokens,
      1,
      'disposing cleared the per-key state'
    )
  })

  test('a throwing disconnected listener does not abort the shutdown', async (t) => {
    // emit('disconnected') runs inside disconnect()'s try, so a listener that
    // throws lands in the catch. Without it, one bad application listener
    // would turn a graceful shutdown into a rejected promise.
    const logger = recordingLogger()
    const { rabbit } = await connected(t, { logger })

    rabbit.on('disconnected', () => { throw new Error('listener blew up') })

    await assert.doesNotReject(() => rabbit.disconnect())

    assert.ok(
      logger.records.warn.some(line => line.includes('listener blew up')),
      'the failure is reported, not swallowed silently'
    )
    assert.equal(rabbit.getClusterStatus().connectionState, 'disconnected', 'the connection still went down')
  })

  test('uses the built-in logger when none is provided', async (t) => {
    // options.logger has a default. Losing it makes every log call throw on a
    // client constructed the simplest possible way. The console is mocked so
    // the default logger's output lands in an assertion instead of leaking
    // into the test runner's stdout.
    const infoMock = t.mock.method(console, 'log', () => {})

    const dialer = createDialer()

    installDialer(t, dialer)

    const rabbit = new RabbitMQ({
      username: 'admin',
      password: 'admin',
      endpoints: ['node-a:5672'],
      channelPoolSize: 1
    })

    t.after(() => rabbit.disconnect())
    t.after(() => infoMock.mock.restore())

    await assert.doesNotReject(() => rabbit.connect())
    assert.equal(rabbit.getClusterStatus().connectionState, 'connected')
    assert.ok(infoMock.mock.callCount() > 0, 'the default logger actually logged the connection')
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

  test('circuit breaker state changes surface on the facade', async (t) => {
    // Another context.emit bridge — this one documented as
    // circuitBreakerStateChanged, and the only way an application learns
    // publishing has been cut off.
    const { rabbit, connection } = await connected(t, {
      circuitBreaker: { failureThreshold: 2, timeout: 60000 }
    })

    const states = []

    rabbit.on('circuitBreakerStateChanged', (state) => states.push(state))

    connection.channels.forEach((channel) => {
      channel.confirmErrors.push(new Error('broker refused'), new Error('broker refused'))
    })

    for (let i = 0; i < 2; i++) {
      await rabbit.publish('route', { n: i }, { maxRetries: 1 }).catch(() => {})
    }

    await waitFor(() => states.includes('OPEN'), 3000, 'the breaker opening reached the facade')
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
  // Both process hooks are captured rather than installed: a genuine SIGTERM
  // handler would let a cancelled CI run exit 0 and report an aborted suite as
  // green, and a genuine process.exit would end the test runner.
  const captureProcessHooks = (t) => {
    const installed = []
    const exits = []
    const originalOn = process.on.bind(process)
    const originalExit = process.exit.bind(process)

    // process.once() routes through process.on() with a wrapper, so this
    // intercepts both.
    process.on = (event, handler) => {
      if (event === 'SIGINT' || event === 'SIGTERM') {
        installed.push({ event, handler })

        return process
      }

      return originalOn(event, handler)
    }

    process.exit = (code) => { exits.push(code) }

    t.after(() => {
      process.on = originalOn
      process.exit = originalExit
    })

    return { installed, exits }
  }

  test('exits the process on a clean shutdown by default', async (t) => {
    // The default is exitProcess: true — a signal handler that disconnects and
    // then lets the process linger defeats the point of installing one.
    const { rabbit } = await connected(t)
    const { installed, exits } = captureProcessHooks(t)

    rabbit.enableGracefulShutdown()

    await installed[0].handler()
    await waitFor(() => exits.length === 1, 3000, 'the process was asked to exit')

    assert.deepEqual(exits, [0], 'a clean shutdown exits 0')
    assert.equal(rabbit.getClusterStatus().connectionState, 'disconnected')
  })

  test('honors a custom signal list', async (t) => {
    const { rabbit } = await connected(t)
    const { installed } = captureProcessHooks(t)

    rabbit.enableGracefulShutdown({ signals: ['SIGTERM'], exitProcess: false })

    assert.deepEqual(installed.map(entry => entry.event), ['SIGTERM'], 'only what was asked for')
  })

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

describe('RabbitMQ facade survivor round', () => {
  test('a second reconnection cycle recreates consumers again (the restore slot is released)', async (t) => {
    // #restoreState parks its promise in a slot released by a finally; if
    // the release is lost, every reconnection AFTER the first reuses the
    // completed restore and never recreates anything — consumers silently
    // stop draining on the second drop.
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()
    await rabbit.subscribe('orders', async () => {})

    for (let cycle = 1; cycle <= 2; cycle++) {
      const reconnected = new Promise(resolve => rabbit.once('reconnected', resolve))

      dialer.connections.at(-1).emit('close')
      await reconnected
    }

    assert.equal(dialer.connections.length, 3, 'two drops, two fresh dials')
    assert.equal(
      dialer.connections.at(-1).consumersOn().length,
      1,
      'the consumer lives on the newest connection after the SECOND recovery'
    )
  })

  test('a manual connect() after a FAILED restore runs a fresh restore, not the cached rejection', async (t) => {
    // #handleDisconnection clears the restore slot between cycles, so the
    // finally-release only matters here: the restore failed (reconnectError,
    // no pool built) and no further disconnection came along. The manual
    // retry must get a fresh attempt — a leaked slot would hand every future
    // connect() the same rejected promise forever.
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()
    await rabbit.subscribe('orders', async () => {})

    // The next connection refuses channels: the reconnect's restore fails
    // before any pool exists.
    dialer.onConnection = (connection) => {
      connection.channelError = new Error('channels refused')
    }

    const restoreFailed = new Promise(resolve => rabbit.once('reconnectError', resolve))

    dialer.connections[0].emit('close')
    await restoreFailed

    // The broker heals; the operator retries by hand.
    dialer.onConnection = null
    dialer.connections.at(-1).channelError = null

    await rabbit.connect()

    assert.equal(
      dialer.connections.at(-1).consumersOn().length,
      1,
      'the retry rebuilt the pool and recreated the consumer'
    )

    await rabbit.publish('orders-route', { n: 1 })
  })

  test('the tag subscribe() returned still cancels the consumer after a reconnection', async (t) => {
    // The broker issues a fresh tag on recreation and the caller has no way to
    // learn it — the old behavior retired the original tag, so unsubscribe
    // answered false and the consumer (plus its worker pool) kept running.
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    const consumer = await rabbit.subscribe('orders', async () => {})
    const originalTag = consumer.consumerTag

    const reconnected = new Promise(resolve => rabbit.once('reconnected', resolve))

    dialer.connections[0].emit('close')
    await reconnected

    const liveConsumer = dialer.connections.at(-1).consumersOn()[0]

    assert.notEqual(liveConsumer.consumerTag, originalTag, 'the broker really did issue a new tag')
    assert.equal(await rabbit.unsubscribe(originalTag), true, 'the caller-held tag must still work')
    assert.equal(dialer.connections.at(-1).channels.some(channel => channel.cancelled.length > 0), true, 'the cancel reached the broker')
  })

  test('a restore that fails AFTER the pool was built is retried in full', async (t) => {
    // The pool is only half the restore. Leaving it installed after
    // ensureExchange/recreateAll failed satisfied #doConnect's gate, so the
    // retry resolved as a success onto an instance with no exchange and no
    // consumers.
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()
    await rabbit.subscribe('orders', async () => {})

    let exchangeSick = true

    dialer.onConnection = (connection) => {
      const realCreate = connection.createConfirmChannel.bind(connection)

      connection.createConfirmChannel = async () => {
        const channel = await realCreate()

        if (exchangeSick) channel.assertExchangeError = new Error('exchange under maintenance')

        return channel
      }
    }

    const restoreFailed = new Promise(resolve => rabbit.once('reconnectError', resolve))

    dialer.connections[0].emit('close')
    await restoreFailed

    exchangeSick = false

    for (const connection of dialer.connections) {
      for (const channel of connection.channels) channel.assertExchangeError = null
    }

    await rabbit.connect()
    await sleep(30)

    const live = dialer.connections.at(-1)

    assert.equal(live.consumersOn().length, 1, 'the retry recreated the consumer')
    assert.ok(live.channels.some(channel => channel.assertedExchanges.length > 0), 'and asserted the exchange')

    await rabbit.publish('orders-route', { n: 1 })
  })

  test('waitForConnection leaves no listeners or timer behind once it resolves', async (t) => {
    const { ManualClock } = await import('./helpers.js')
    const clock = new ManualClock()
    const dialer = createDialer([new Error('down'), 'ok'])
    const rabbit = createRabbit(t, dialer, { clock })

    t.after(() => rabbit.disconnect())

    const connection = await rabbit.connect({ waitForConnection: true, timeout: 60000 })

    assert.ok(connection, 'the reconnect cycle eventually satisfied the waiter')
    assert.equal(rabbit.listenerCount('reconnected'), 0, 'the waiter unhooked itself')
    assert.equal(rabbit.listenerCount('reconnectFailed'), 0)
    assert.equal(rabbit.listenerCount('reconnectError'), 0)
    assert.equal(clock.timeouts.size, 0, 'the guard timer was cleared, not left to fire')
  })

  test('disconnecting rejects in-flight RPCs naming the client as the cause', async (t) => {
    const { rabbit, connection } = await connected(t)

    const pending = rabbit.request('users.get', {}, { timeout: 60000 })

    await waitFor(() => connection.publishedOn().length === 1, 2000, 'request in flight')

    await rabbit.disconnect()

    await assert.rejects(() => pending, (error) => {
      assert.equal(error.code, 'RPC_CONNECTION_LOST')
      assert.match(error.message, /client disconnected/, 'the reason tells the caller who hung up')

      return true
    })
  })

  test('disconnect on a never-connected client still announces disconnected', async (t) => {
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    const events = []

    rabbit.on('disconnected', () => events.push('disconnected'))

    await rabbit.disconnect()

    assert.deepEqual(events, ['disconnected'], 'no pool to close is not an error — the shutdown completed')
  })

  test('graceful shutdown exits 0 on success and 1 when the teardown fails', async (t) => {
    const exits = []
    const exitMock = t.mock.method(process, 'exit', (code) => { exits.push(code) })

    t.after(() => exitMock.mock.restore())

    const happy = await connected(t)

    // SIGWINCH, not SIGUSR2: the test runner's child process registers
    // SIGUSR2 as its diagnostic-report trigger, so emitting it wrote a
    // report.*.json to the repo root on every suite run.
    happy.rabbit.enableGracefulShutdown({ signals: ['SIGWINCH'], exitProcess: true })
    process.emit('SIGWINCH')
    await waitFor(() => exits.length === 1, 2000, 'shutdown handler ran')

    assert.deepEqual(exits, [0], 'a clean teardown exits 0')

    const sad = await connected(t)

    sad.rabbit.disconnect = async () => { throw new Error('teardown wedged') }
    sad.rabbit.enableGracefulShutdown({ signals: ['SIGPIPE'], exitProcess: true })
    process.emit('SIGPIPE')
    await waitFor(() => exits.length === 2, 2000, 'failing handler ran')

    // Give the harness cleanup its real disconnect back.
    delete sad.rabbit.disconnect

    assert.deepEqual(exits, [0, 1], 'a failed teardown exits 1 so the supervisor restarts the process')
  })

  test('setExchange accepts every AMQP exchange type and defaults to direct', async (t) => {
    const { rabbit } = await connected(t)

    for (const type of ['direct', 'topic', 'fanout', 'headers']) {
      rabbit.setExchange(`x-${type}`, type)
    }

    assert.throws(() => rabbit.setExchange('x', 'x-delayed-message'), /Invalid exchange type/)
    assert.throws(() => rabbit.setExchange('   '), /non-empty string/)
    assert.throws(() => rabbit.setExchange(42), /non-empty string/)
  })

  test('the delay exchange inherits the type setExchange defaulted', async (t) => {
    const { rabbit, connection } = await connected(t)

    rabbit.setExchange('defaulted')
    await rabbit.setupDelayExchange()

    const assertion = connection.channels
      .flatMap(channel => channel.assertedExchanges)
      .find(exchange => exchange.type === 'x-delayed-message')

    assert.equal(assertion.options.arguments['x-delayed-type'], 'direct', 'the defaulted type flowed through')
  })

  test('setupDelayPlugin actually probes the broker', async (t) => {
    const { rabbit, connection } = await connected(t)

    await rabbit.setupDelayPlugin()

    const probes = connection.channels.flatMap(channel => channel.assertedExchanges)

    assert.ok(probes.some(exchange => exchange.name === 'test.delay'), 'no probe means the check proved nothing')
  })

  test('setCompressionThreshold accepts zero and rejects negatives', async (t) => {
    const { rabbit } = await connected(t)

    rabbit.setCompressionThreshold(0)
    assert.throws(() => rabbit.setCompressionThreshold(-1), /non-negative|Must be/i)
  })
})

describe('RabbitMQ facade cache accounting', () => {
  const cached = (t, extra = {}) => connected(t, {
    useCache: true,
    rateLimiter: { strategy: 'fixed-window', maxRequests: 1, windowMs: 60000 },
    ...extra
  })

  test('publishWithCache works as a plain publish when the cache is disabled', async (t) => {
    const { rabbit, connection } = await connected(t)

    await rabbit.publishWithCache('route', { n: 1 })

    assert.equal(connection.publishedOn().length, 1, 'no cache means every call publishes')
    await assert.rejects(() => rabbit.getFromCache('route'), /not enabled/)
  })

  test('cache keys are scoped by exchange and routing key, never shared', async (t) => {
    const { rabbit } = await cached(t)

    await rabbit.publishWithCache('route-a', { n: 1 }, { cacheTTL: 60 })

    assert.equal(await rabbit.getFromCache('route-b'), undefined, 'a different routing key must miss')
    assert.ok(await rabbit.getFromCache('route-a'))
  })

  test('cached publishes charge the cached: rate-limit namespace', async (t) => {
    const { rabbit } = await cached(t)

    await rabbit.publishWithCache('route', { n: 1 }, { cacheTTL: 60 })

    assert.equal(
      rabbit.getRateLimitStatus('cached:route').remainingTokens,
      0,
      'the token came out of the cached: bucket, not the plain routing key'
    )
    assert.equal(rabbit.getRateLimitStatus('route').remainingTokens, 1, 'the plain bucket is untouched')
  })

  test('the cached TTL is the explicit option, falling back to the configured default', async (t) => {
    const logger = recordingLogger()
    const explicit = await cached(t, { logger })

    await explicit.rabbit.publishWithCache('route', { n: 1 }, { cacheTTL: 5 })
    assert.ok(logger.records.info.some(line => line.includes('TTL: 5s')), 'the explicit TTL wins')

    const defaultedLogger = recordingLogger()
    const defaulted = await cached(t, { logger: defaultedLogger })

    await defaulted.rabbit.publishWithCache('route', { n: 1 })
    assert.ok(defaultedLogger.records.info.some(line => line.includes('TTL: 60s')), 'the default stdTTL is 60s')
  })
})

describe('RabbitMQ facade connect/disconnect fencing', () => {
  test('disconnect() aborts a waitForConnection instead of hanging it forever', async (t) => {
    // The wait lives on reconnection cycles that disconnect() ends, so nothing
    // would ever settle it — and since #connectPromise is only released in its
    // finally, EVERY later connect() would get the same dead promise.
    const dialer = createDialer([new Error('down'), 'ok'])
    const rabbit = createRabbit(t, dialer, { reconnectInterval: 60000, maxReconnectInterval: 60000 })

    t.after(() => rabbit.disconnect())

    // The handler is attached up front: a rejection sitting unhandled for even
    // one tick takes the whole test process down with it.
    const waiting = rabbit.connect({ waitForConnection: true }).then(() => null, (error) => error)

    await sleep(20)
    await rabbit.disconnect()

    const error = await waiting

    assert.match(error.message, /Connection wait aborted/)

    // The funnel must be usable again: a fresh connect() gets a fresh attempt
    // instead of the dead promise the aborted wait left behind.
    const connection = await rabbit.connect()

    assert.ok(connection, 'the instance recovered instead of staying poisoned')
  })
})

describe('RabbitMQ facade teardown failures', () => {
  test('a failed restore whose pool refuses to close still fails cleanly', async (t) => {
    // Tearing the partial pool down is best-effort: a close that rejects must
    // not replace the real restore error with a teardown one.
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()

    dialer.onConnection = (connection) => {
      const realCreate = connection.createConfirmChannel.bind(connection)

      connection.createConfirmChannel = async () => {
        const channel = await realCreate()

        channel.assertExchangeError = new Error('exchange under maintenance')
        channel.close = async () => { throw new Error('channel wedged') }

        return channel
      }
    }

    const failed = new Promise(resolve => rabbit.once('reconnectError', resolve))

    dialer.connections[0].emit('close')

    const restoreError = await failed

    assert.match(restoreError.message, /exchange under maintenance/, 'the real cause survives the failed teardown')
  })

  test('a disconnection whose stale pool refuses to close still recovers', async (t) => {
    // The stale pool is closed best-effort on the way out; a rejection there
    // must not derail the recovery that follows.
    const dialer = createDialer()
    const rabbit = createRabbit(t, dialer)

    t.after(() => rabbit.disconnect())

    await rabbit.connect()
    await rabbit.subscribe('orders', async () => {})

    for (const channel of dialer.connections[0].channels) {
      channel.close = async () => { throw new Error('channel wedged') }
    }

    const reconnected = new Promise(resolve => rabbit.once('reconnected', resolve))

    dialer.connections[0].emit('close')
    await reconnected

    assert.equal(dialer.connections.at(-1).consumersOn().length, 1, 'recovery completed despite the wedged teardown')
  })
})
