import assert from 'node:assert/strict'
import { fileURLToPath } from 'node:url'
import RabbitMQ from '../../src/index.js'
import { test, describe, before, after } from 'node:test'

const ECHO_WORKER = fileURLToPath(new URL('../fixtures/echo-worker.mjs', import.meta.url))

// Integration tests against a real RabbitMQ broker.
//
// They require a running broker (docker compose up -d) and are enabled via
// environment variable so the local `npm test` does not break without docker:
//
//   RABBITMQ_INTEGRATION=1 npm test
//
// The forced connection drop uses the management API (port 15672), available
// in the rabbitmq:*-management image used by docker-compose and by CI.

const RUN_INTEGRATION = process.env.RABBITMQ_INTEGRATION === '1'

const HOST = process.env.RABBITMQ_HOST || 'localhost'
const AMQP_PORT = process.env.RABBITMQ_AMQP_PORT || '5672'
const ADMIN_PORT = process.env.RABBITMQ_ADMIN_PORT || '15672'
const USERNAME = process.env.RABBITMQ_USER || 'admin'
const PASSWORD = process.env.RABBITMQ_PASS || 'admin'

const EXCHANGE = 'integration-exchange'
const QUEUE = 'integration-queue'
const ROUTING_KEY = 'integration-route'

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const waitFor = async (predicate, timeoutMs = 15000, label = 'condition') => {
  const start = Date.now()

  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return

    await sleep(100)
  }

  throw new Error(`Timeout waiting for: ${label}`)
}

const managementRequest = async (path, options = {}) => {
  const auth = Buffer.from(`${USERNAME}:${PASSWORD}`).toString('base64')
  const response = await fetch(`http://${HOST}:${ADMIN_PORT}/api${path}`, {
    ...options,
    headers: { Authorization: `Basic ${auth}`, ...options.headers }
  })

  if (!response.ok && response.status !== 404) {
    throw new Error(`Management API ${path} failed: ${response.status}`)
  }

  return response.status === 204 ? null : response.json()
}

// Closes every AMQP connection from the broker side, simulating a real outage.
// The management API lists connections with a lag: when waitForName is given,
// wait until THAT connection is visible, or a freshly opened connection could
// survive the sweep and the test would wait forever for a drop that never came.
const forceCloseAllConnections = async (waitForName) => {
  await waitFor(async () => {
    const connections = await managementRequest('/connections')

    if (!Array.isArray(connections) || connections.length === 0) return false
    if (!waitForName) return true

    return connections.some(connection => connection.client_properties?.connection_name === waitForName)
  }, 30000, `connection ${waitForName || ''} visible in management API`)

  const connections = await managementRequest('/connections')

  for (const connection of connections) {
    await managementRequest(`/connections/${encodeURIComponent(connection.name)}`, { method: 'DELETE' })
  }
}

const silentLogger = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} }

describe('RabbitMQ integration', { skip: !RUN_INTEGRATION && 'set RABBITMQ_INTEGRATION=1 (requires a running broker)' }, () => {
  let rabbitMQ
  const received = []

  before(async () => {
    rabbitMQ = new RabbitMQ({
      username: USERNAME,
      password: PASSWORD,
      endpoints: [`${HOST}:${AMQP_PORT}`],
      connectionName: 'integration-test',
      exchange: { name: EXCHANGE, type: 'direct' },
      useCompression: true,
      compressionThreshold: 100,
      reconnectInterval: 500,
      maxReconnectInterval: 2000,
      logger: silentLogger
    })

    const connection = await rabbitMQ.connect()
    assert.ok(connection, 'connect() must return a connection')

    const channel = await rabbitMQ.getChannel()

    await rabbitMQ.setupDeadLetterExchange()
    await rabbitMQ.createQueue(QUEUE)
    await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY)
    await channel.purgeQueue(QUEUE)
    await channel.purgeQueue(`${QUEUE}_dlq`)

    await rabbitMQ.subscribe(QUEUE, async (content) => {
      if (content && content.fail) throw new Error('forced failure')

      received.push(content)
    })
  })

  after(async () => {
    await rabbitMQ.disconnect()
  })

  test('publishes and consumes across all publish variants', async () => {
    const bigString = 'x'.repeat(500)

    await rabbitMQ.publish(ROUTING_KEY, { hello: 'world' })
    await rabbitMQ.publish(ROUTING_KEY, bigString)
    await rabbitMQ.publishBatch(ROUTING_KEY, [{ n: 1 }, { n: 2 }, { n: 3 }])
    await rabbitMQ.publishAsync(ROUTING_KEY, { async: true })
    await rabbitMQ.publish(ROUTING_KEY, 42)

    await waitFor(() => received.length >= 7, 15000, '7 messages consumed')

    assert.ok(received.some(message => message?.hello === 'world'), 'plain object roundtrip')
    assert.ok(received.includes(bigString), 'compressed message roundtrip (gzip)')
    assert.ok(received.includes(42), 'numeric message roundtrip')
  })

  test('failed messages are dead-lettered to the DLQ', async () => {
    await rabbitMQ.publish(ROUTING_KEY, { fail: true })

    const channel = await rabbitMQ.getChannel()

    await waitFor(async () => {
      const dlq = await channel.checkQueue(`${QUEUE}_dlq`)

      return dlq.messageCount >= 1
    }, 15000, 'message dead-lettered')
  })

  test('manual acknowledgment inside subscribe does not double-ack', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-manual', { durable: false, autoDelete: true })
    await channel.bindQueue('integration-manual', EXCHANGE, 'integration-manual-route')

    let manuallyAcked = false

    await rabbitMQ.subscribe('integration-manual', async (content, message) => {
      await rabbitMQ.acknowledgeMessage(message)
      manuallyAcked = true
    })

    await rabbitMQ.publish('integration-manual-route', { manual: true })
    await waitFor(() => manuallyAcked, 10000, 'manual ack executed')
    await sleep(500)

    const queue = await channel.checkQueue('integration-manual')

    assert.equal(queue.messageCount, 0, 'message must be acked exactly once')
  })

  test('recovers channel pool and consumers after a forced connection drop', async () => {
    const reconnected = new Promise(resolve => rabbitMQ.once('reconnected', resolve))

    await forceCloseAllConnections()

    await Promise.race([
      reconnected,
      sleep(30000).then(() => { throw new Error('timeout waiting for reconnection') })
    ])

    const countBefore = received.length

    await rabbitMQ.publish(ROUTING_KEY, { after: 'reconnect' })

    await waitFor(() => received.length > countBefore, 15000, 'recreated consumer receives messages')

    assert.equal(rabbitMQ.getClusterStatus().connectionState, 'connected')
  })

  test('subscribeSequential honors depends-on ordering', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-seq', { durable: false, autoDelete: true })
    await channel.bindQueue('integration-seq', EXCHANGE, 'integration-seq-route')

    const order = []

    await rabbitMQ.subscribeSequential('integration-seq', async (content) => {
      await sleep(50)
      order.push(content.step)
    }, { prefetchCount: 5 })

    await rabbitMQ.publish('integration-seq-route', { step: 1 }, { messageId: 'seq-1' })
    await rabbitMQ.publish('integration-seq-route', { step: 2 }, { messageId: 'seq-2', headers: { 'depends-on': 'seq-1' } })
    await rabbitMQ.publish('integration-seq-route', { step: 3 }, { messageId: 'seq-3', headers: { 'depends-on': 'seq-2' } })

    await waitFor(() => order.length === 3, 15000, 'sequential messages processed')

    assert.deepEqual(order, [1, 2, 3])
  })

  test('unsubscribe cancels the consumer and stops delivery', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-unsub', { durable: false, autoDelete: false })
    await channel.bindQueue('integration-unsub', EXCHANGE, 'integration-unsub-route')

    const consumed = []
    const consumer = await rabbitMQ.subscribe('integration-unsub', async (content) => {
      consumed.push(content)
    })

    await rabbitMQ.publish('integration-unsub-route', { n: 1 })
    await waitFor(() => consumed.length === 1, 10000, 'first message consumed')

    const removed = await rabbitMQ.unsubscribe(consumer.consumerTag)
    assert.equal(removed, true)

    await rabbitMQ.publish('integration-unsub-route', { n: 2 })
    await sleep(800)

    assert.equal(consumed.length, 1, 'no delivery after unsubscribe')

    const queue = await channel.checkQueue('integration-unsub')
    assert.equal(queue.messageCount, 1, 'second message stays in the queue')

    await channel.deleteQueue('integration-unsub')
  })

  test('subscribeParallel processes messages through worker threads', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-parallel', { durable: false, autoDelete: false })
    await channel.bindQueue('integration-parallel', EXCHANGE, 'integration-parallel-route')

    const consumer = await rabbitMQ.subscribeParallel('integration-parallel', ECHO_WORKER, {
      workerCount: 2,
      prefetch: 2
    })

    await rabbitMQ.publishBatch('integration-parallel-route', [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }])

    await waitFor(async () => {
      const queue = await channel.checkQueue('integration-parallel')

      return queue.messageCount === 0
    }, 15000, 'all messages processed by workers')

    await rabbitMQ.unsubscribe(consumer.consumerTag)

    const channelAfter = await rabbitMQ.getChannel()
    await channelAfter.deleteQueue('integration-parallel')
  })

  test('subscribeWithOptimizedPrefetch consumes messages normally', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-prefetch', { durable: false, autoDelete: false })
    await channel.bindQueue('integration-prefetch', EXCHANGE, 'integration-prefetch-route')

    const consumed = []
    const consumer = await rabbitMQ.subscribeWithOptimizedPrefetch('integration-prefetch', async (content) => {
      consumed.push(content)
    }, { initialPrefetch: 2 })

    await rabbitMQ.publishBatch('integration-prefetch-route', [{ n: 1 }, { n: 2 }, { n: 3 }])
    await waitFor(() => consumed.length === 3, 10000, 'optimized prefetch consumer receives messages')

    await rabbitMQ.unsubscribe(consumer.consumerTag)

    const channelAfter = await rabbitMQ.getChannel()
    await channelAfter.deleteQueue('integration-prefetch')
  })

  test('publishWithCache publishes on miss and dedups on hit', async () => {
    const cached = new RabbitMQ({
      username: USERNAME,
      password: PASSWORD,
      endpoints: [`${HOST}:${AMQP_PORT}`],
      connectionName: 'integration-cache-test',
      exchange: { name: EXCHANGE, type: 'direct' },
      useCache: true,
      cacheTTL: 60,
      logger: silentLogger
    })

    try {
      await cached.connect()

      const channel = await cached.getChannel()

      await channel.assertQueue('integration-cache', { durable: false, autoDelete: false })
      await channel.bindQueue('integration-cache', EXCHANGE, 'integration-cache-route')

      const consumed = []
      await cached.subscribe('integration-cache', async (content) => {
        consumed.push(content)
      })

      let generatorCalls = 0
      const generator = () => {
        generatorCalls++

        return { generated: generatorCalls }
      }

      const first = await cached.publishWithCache('integration-cache-route', generator)
      const second = await cached.publishWithCache('integration-cache-route', generator)

      assert.equal(generatorCalls, 1, 'generator runs only on cache miss')
      assert.deepEqual(first, second, 'cache hit returns the original message')
      assert.deepEqual(await cached.getFromCache('integration-cache-route'), first)

      await waitFor(() => consumed.length === 1, 10000, 'cached publish delivered once')
      await sleep(500)
      assert.equal(consumed.length, 1, 'cache hit must not publish again')

      cached.invalidateCache('integration-cache-route')
      await cached.publishWithCache('integration-cache-route', generator)

      assert.equal(generatorCalls, 2, 'invalidation forces a new publish')
      await waitFor(() => consumed.length === 2, 10000, 'second publish delivered')

      const channelAfter = await cached.getChannel()
      await channelAfter.deleteQueue('integration-cache')
    } finally {
      await cached.disconnect()
    }
  })

  test('moveToDeadLetter republishes into the DLQ and processDeadLetterQueue consumes it', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.purgeQueue(`${QUEUE}_dlq`)

    const fakeMessage = {
      content: Buffer.from(JSON.stringify({ manual: 'dead-letter' })),
      fields: { routingKey: QUEUE, exchange: EXCHANGE },
      properties: { headers: {} }
    }

    await rabbitMQ.moveToDeadLetter(fakeMessage, 'integration test move')

    await waitFor(async () => {
      const dlq = await channel.checkQueue(`${QUEUE}_dlq`)

      return dlq.messageCount === 1
    }, 10000, 'message landed in the DLQ')

    const deadLettered = []
    await rabbitMQ.processDeadLetterQueue(QUEUE, async (message) => {
      deadLettered.push(message)
    })

    await waitFor(() => deadLettered.length === 1, 10000, 'DLQ processor consumed the message')

    assert.deepEqual(deadLettered[0], { manual: 'dead-letter' })
  })

  test('emits consumerCancelled and consumerLost when the broker cancels a consumer', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-cancel', { durable: false, autoDelete: false })

    const cancelled = new Promise(resolve => rabbitMQ.once('consumerCancelled', resolve))
    const lost = new Promise(resolve => rabbitMQ.once('consumerLost', resolve))

    await rabbitMQ.subscribe('integration-cancel', async () => {})

    const deleteChannel = await rabbitMQ.getChannel()
    await deleteChannel.deleteQueue('integration-cancel')

    const cancelledEvent = await Promise.race([
      cancelled,
      sleep(10000).then(() => { throw new Error('timeout waiting for consumerCancelled') })
    ])

    assert.equal(cancelledEvent.queueName, 'integration-cancel')

    // The queue no longer exists: recovery attempts run out and the
    // consumer is removed.
    const lostEvent = await Promise.race([
      lost,
      sleep(15000).then(() => { throw new Error('timeout waiting for consumerLost') })
    ])

    assert.equal(lostEvent.queueName, 'integration-cancel')
  })

  test('publishDelayed delivers after the delay when the plugin is available', async (t) => {
    const pluginEnabled = await rabbitMQ.isDelayPluginEnabled()

    if (!pluginEnabled) {
      t.skip('rabbitmq_delayed_message_exchange plugin is not installed on the broker')

      return
    }

    await rabbitMQ.setupDelayExchange()

    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-delayed', { durable: false, autoDelete: false })
    await channel.bindQueue('integration-delayed', 'delayed', 'integration-delayed-route')

    const consumed = []
    await rabbitMQ.subscribe('integration-delayed', async (content) => {
      consumed.push({ content, at: Date.now() })
    })

    const publishedAt = Date.now()
    await rabbitMQ.publishDelayed('integration-delayed-route', { delayed: true }, 500)

    await waitFor(() => consumed.length === 1, 10000, 'delayed message delivered')

    assert.ok(consumed[0].at - publishedAt >= 400, 'message must arrive only after the delay')

    const channelAfter = await rabbitMQ.getChannel()
    await channelAfter.deleteQueue('integration-delayed')
  })

  test('circuit breaker reports a healthy state after the full run', () => {
    const state = rabbitMQ.getCircuitBreakerState()

    assert.equal(state.state, 'CLOSED')
  })

  test('moveToDeadLetter rejects when the dead letter routing has no binding', async () => {
    // Regression: unroutable dead-letter publishes used to be confirmed and
    // silently dropped; mandatory publishing must surface them as errors.
    const fakeMessage = {
      content: Buffer.from(JSON.stringify({ lost: 'never again' })),
      fields: { routingKey: 'integration-nonexistent-queue', exchange: EXCHANGE },
      properties: { headers: {} }
    }

    await assert.rejects(() => rabbitMQ.moveToDeadLetter(fakeMessage, 'unroutable test'), /no binding/)
  })

  test('RPC request/response happy path over direct reply-to', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-rpc', { durable: false, autoDelete: false })
    await channel.bindQueue('integration-rpc', EXCHANGE, 'integration-rpc-route')

    const consumer = await rabbitMQ.respond('integration-rpc', async (content) => {
      return { doubled: content.value * 2 }
    })

    const response = await rabbitMQ.request('integration-rpc-route', { value: 21 }, { timeout: 10000 })

    assert.deepEqual(response, { doubled: 42 })

    // Concurrent requests must each get their own correlated reply.
    const responses = await Promise.all([
      rabbitMQ.request('integration-rpc-route', { value: 1 }, { timeout: 10000 }),
      rabbitMQ.request('integration-rpc-route', { value: 2 }, { timeout: 10000 }),
      rabbitMQ.request('integration-rpc-route', { value: 3 }, { timeout: 10000 })
    ])

    assert.deepEqual(responses, [{ doubled: 2 }, { doubled: 4 }, { doubled: 6 }])

    await rabbitMQ.unsubscribe(consumer.consumerTag)

    const channelAfter = await rabbitMQ.getChannel()
    await channelAfter.deleteQueue('integration-rpc')
  })

  test('RPC request rejects with RPC_TIMEOUT when nobody responds', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-rpc-void', { durable: false, autoDelete: false })
    await channel.bindQueue('integration-rpc-void', EXCHANGE, 'integration-rpc-void-route')

    await assert.rejects(
      () => rabbitMQ.request('integration-rpc-void-route', { ping: true }, { timeout: 1000 }),
      (error) => error.code === 'RPC_TIMEOUT'
    )

    const channelAfter = await rabbitMQ.getChannel()
    await channelAfter.deleteQueue('integration-rpc-void')
  })

  test('RPC responder crash surfaces as RPC_RESPONDER_ERROR with replyOnError', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-rpc-crash', { durable: false, autoDelete: false })
    await channel.bindQueue('integration-rpc-crash', EXCHANGE, 'integration-rpc-crash-route')

    const consumer = await rabbitMQ.respond('integration-rpc-crash', async () => {
      throw new Error('responder exploded')
    }, { replyOnError: true })

    await assert.rejects(
      () => rabbitMQ.request('integration-rpc-crash-route', { boom: true }, { timeout: 10000 }),
      (error) => error.code === 'RPC_RESPONDER_ERROR' && /responder exploded/.test(error.message)
    )

    await rabbitMQ.unsubscribe(consumer.consumerTag)

    const channelAfter = await rabbitMQ.getChannel()
    await channelAfter.deleteQueue('integration-rpc-crash')
  })

  test('RPC responder crash without replyOnError dead-letters the request', async () => {
    // The queue comes from createQueue() so it is already wired to the DLX:
    // the poison-message policy (nack, no requeue) must apply to RPC too.
    await rabbitMQ.createQueue('integration-rpc-poison')

    const channel = await rabbitMQ.getChannel()

    await channel.bindQueue('integration-rpc-poison', EXCHANGE, 'integration-rpc-poison-route')
    await channel.purgeQueue('integration-rpc-poison_dlq')

    const consumer = await rabbitMQ.respond('integration-rpc-poison', async () => {
      throw new Error('unrecoverable')
    })

    await assert.rejects(
      () => rabbitMQ.request('integration-rpc-poison-route', { poison: true }, { timeout: 2000 }),
      (error) => error.code === 'RPC_TIMEOUT'
    )

    await waitFor(async () => {
      const dlq = await channel.checkQueue('integration-rpc-poison_dlq')

      return dlq.messageCount >= 1
    }, 10000, 'poison RPC request dead-lettered')

    await rabbitMQ.unsubscribe(consumer.consumerTag)

    const channelAfter = await rabbitMQ.getChannel()
    await channelAfter.deleteQueue('integration-rpc-poison')
    await channelAfter.deleteQueue('integration-rpc-poison_dlq')
  })

  test('in-flight RPC requests reject with RPC_CONNECTION_LOST on a forced connection drop', async () => {
    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('integration-rpc-slow', { durable: false, autoDelete: false })
    await channel.bindQueue('integration-rpc-slow', EXCHANGE, 'integration-rpc-slow-route')

    // Slow enough that the connection is killed while the request is in
    // flight, waiting for its reply.
    const consumer = await rabbitMQ.respond('integration-rpc-slow', async () => {
      await sleep(8000)

      return { late: true }
    })

    const reconnected = new Promise(resolve => rabbitMQ.once('reconnected', resolve))

    // The outcome handler is attached BEFORE the drop: the rejection fires
    // while the test is still orchestrating the kill, and an unobserved
    // rejection would fail the test as unhandled.
    const outcome = rabbitMQ.request('integration-rpc-slow-route', { ping: 1 }, { timeout: 30000 })
      .then(() => { throw new Error('request must not resolve across a connection drop') }, (error) => error)

    // Give the request time to be published and picked up before the kill.
    await sleep(500)
    await forceCloseAllConnections('integration-test')

    const error = await outcome

    assert.equal(error.code, 'RPC_CONNECTION_LOST')

    await Promise.race([
      reconnected,
      sleep(30000).then(() => { throw new Error('timeout waiting for reconnection after RPC drop') })
    ])

    await rabbitMQ.unsubscribe(consumer.consumerTag)

    // The reply consumer is recreated lazily: a fresh request after the
    // reconnection must work end to end again.
    const channelAfter = await rabbitMQ.getChannel()

    await channelAfter.assertQueue('integration-rpc-after', { durable: false, autoDelete: false })
    await channelAfter.bindQueue('integration-rpc-after', EXCHANGE, 'integration-rpc-after-route')

    const echoConsumer = await rabbitMQ.respond('integration-rpc-after', async (content) => content)

    const response = await rabbitMQ.request('integration-rpc-after-route', { recovered: true }, { timeout: 10000 })

    assert.deepEqual(response, { recovered: true })

    await rabbitMQ.unsubscribe(echoConsumer.consumerTag)

    const cleanupChannel = await rabbitMQ.getChannel()
    await cleanupChannel.deleteQueue('integration-rpc-slow')
    await cleanupChannel.deleteQueue('integration-rpc-after')
  })

  test('resumes automatic reconnection after an explicit disconnect() + connect() cycle', async () => {
    // Regression: disconnect() used to permanently disable reconnection for
    // the instance because #isShuttingDown was never reset by connect().
    const cycled = new RabbitMQ({
      username: USERNAME,
      password: PASSWORD,
      endpoints: [`${HOST}:${AMQP_PORT}`],
      connectionName: 'integration-cycle-test',
      reconnectInterval: 500,
      maxReconnectInterval: 2000,
      logger: silentLogger
    })

    try {
      await cycled.connect()
      await cycled.disconnect()

      const connection = await cycled.connect()
      assert.ok(connection, 'connect() after disconnect() must return a connection')

      const reconnected = new Promise(resolve => cycled.once('reconnected', resolve))

      await forceCloseAllConnections('integration-cycle-test')

      await Promise.race([
        reconnected,
        sleep(30000).then(() => { throw new Error('timeout waiting for reconnection after disconnect+connect cycle') })
      ])

      assert.equal(cycled.getClusterStatus().connectionState, 'connected')
    } finally {
      await cycled.disconnect()
    }
  })
})
