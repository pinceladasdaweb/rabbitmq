import assert from 'node:assert/strict'
import { fileURLToPath } from 'node:url'
import { test, describe } from 'node:test'
import MessageCodec from '../src/messaging/message-codec.js'
import ConsumerManager from '../src/consumers/consumer-manager.js'
import { FakeChannel, recordingLogger, silentLogger, sleep, waitFor } from './helpers.js'

const ECHO_WORKER = fileURLToPath(new URL('./fixtures/echo-worker.mjs', import.meta.url))
const FLAKY_WORKER = fileURLToPath(new URL('./fixtures/flaky-worker.mjs', import.meta.url))

const createManager = (overrides = {}) => {
  const codec = new MessageCodec({ logger: silentLogger })
  const channel = new FakeChannel()
  const events = []

  const channelPool = {
    getDedicatedChannel: async () => channel
  }

  const context = {
    logger: silentLogger,
    codec,
    prefetchCount: 10,
    // Shrinks the broker-cancel recovery backoff so these tests assert on a
    // deterministic signal instead of racing a 1s production timer.
    consumerRecoveryInterval: 20,
    getChannelPool: () => channelPool,
    getChannel: async () => channel,
    emit: (event, payload) => events.push({ event, payload }),
    ...overrides
  }

  return { manager: new ConsumerManager(context), channel, codec, events }
}

const deliver = async (harness, payload, properties = {}) => {
  const { content, compressed } = await harness.codec.encode(payload)
  const consumer = harness.channel.consumers.at(-1)
  const msg = {
    content,
    fields: { consumerTag: consumer.consumerTag, deliveryTag: 1 },
    properties: { headers: { 'x-compressed': compressed }, ...properties }
  }

  await consumer.callback(msg)

  return msg
}

describe('ConsumerManager subscribe', () => {
  test('validates queue name and callback', async () => {
    const { manager } = createManager()

    await assert.rejects(() => manager.subscribe('', async () => {}), /non-empty string/)
    await assert.rejects(() => manager.subscribe('queue', 'not-a-function'), /must be a function/)
  })

  test('rejects when there is no channel pool (not connected)', async () => {
    const { manager } = createManager({ getChannelPool: () => null })

    await assert.rejects(() => manager.subscribe('queue', async () => {}), /Not connected to RabbitMQ/)
  })

  test('decodes messages, invokes the callback and acks exactly once', async () => {
    const harness = createManager()
    const received = []

    const consumer = await harness.manager.subscribe('orders', async (content, msg) => {
      received.push(content)
    })

    assert.ok(consumer.consumerTag)
    assert.deepEqual(harness.channel.prefetches, [10], 'default prefetch applied')

    await deliver(harness, { id: 7 })

    assert.deepEqual(received, [{ id: 7 }])
    assert.equal(harness.channel.acked.length, 1)
    assert.equal(harness.channel.nacked.length, 0)
  })

  test('honors a custom prefetchCount and skips prefetch/ack for noAck consumers', async () => {
    const custom = createManager()

    await custom.manager.subscribe('orders', async () => {}, { prefetchCount: 3 })
    assert.deepEqual(custom.channel.prefetches, [3])

    const noAck = createManager()

    await noAck.manager.subscribe('orders', async () => {}, { noAck: true })
    await deliver(noAck, { id: 1 })

    assert.deepEqual(noAck.channel.prefetches, [], 'noAck consumers get no prefetch')
    assert.equal(noAck.channel.acked.length, 0)
  })

  test('a crashing callback nacks without requeue (poison policy)', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {
      throw new Error('handler exploded')
    })

    await deliver(harness, { id: 1 })

    assert.equal(harness.channel.acked.length, 0)
    assert.deepEqual(harness.channel.nacked.map(n => n.requeue), [false])
  })

  test('an undecodable message is nacked without invoking the callback', async () => {
    const harness = createManager()
    let called = false

    await harness.manager.subscribe('orders', async () => { called = true })

    const consumer = harness.channel.consumers.at(-1)

    await consumer.callback({
      content: Buffer.from('not-gzip'),
      fields: { consumerTag: consumer.consumerTag },
      properties: { headers: { 'x-compressed': true } }
    })

    assert.equal(called, false)
    assert.deepEqual(harness.channel.nacked.map(n => n.requeue), [false])
  })

  test('a failing initial setup rejects and leaves no consumer registered', async () => {
    const harness = createManager()

    harness.channel.consumeError = new Error('queue does not exist')

    await assert.rejects(() => harness.manager.subscribe('ghost', async () => {}), /queue does not exist/)
    assert.equal(harness.manager.activeConsumers.size, 0)
    assert.equal(harness.manager.consumersByTag.size, 0)
  })
})

describe('ConsumerManager manual ack/nack', () => {
  test('ackMessage and nackMessage settle exactly once', async () => {
    const harness = createManager()
    let delivered

    await harness.manager.subscribe('orders', async (content, msg) => {
      delivered = msg
      await harness.manager.ackMessage(msg)
      await harness.manager.ackMessage(msg)
    })

    await deliver(harness, { id: 1 })

    assert.equal(harness.channel.acked.length, 1, 'second ack must be a no-op')
    assert.equal(delivered.__ackSettled, true)
  })

  test('nackMessage honors the requeue option and settles once', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async (content, msg) => {
      await harness.manager.nackMessage(msg, { requeue: true })
      await harness.manager.nackMessage(msg)
    })

    await deliver(harness, { id: 1 })

    assert.deepEqual(harness.channel.nacked.map(n => n.requeue), [true])
    assert.equal(harness.channel.acked.length, 0, 'wrapper must not ack a settled message')
  })

  test('a nack that the broker rejects is logged and leaves the message unsettled', async () => {
    // settleAck only marks the message settled on success: if the channel
    // rejected the nack the delivery genuinely was not settled, so a later
    // explicit ack/nack must still be attempted rather than silently skipped.
    const logger = recordingLogger()
    const harness = createManager({ logger })

    await harness.manager.subscribe('orders', async () => {
      throw new Error('handler exploded')
    })

    harness.channel.nack = () => {
      throw new Error('channel gone')
    }

    const msg = await deliver(harness, { id: 1 })

    assert.ok(
      logger.records.error.some(message => /Failed to nack message: channel gone/.test(message)),
      'the failure must be reported'
    )
    assert.equal(msg.__ackSettled, false, 'a failed nack must not claim the message was settled')
  })

  test('manual ack/nack settle on the channel that delivered the message', async () => {
    // Delivery tags are scoped to their channel, so settlement must go back to
    // the delivering channel — never to an arbitrary pool channel, which the
    // broker would answer with PRECONDITION_FAILED and close.
    const harness = createManager()
    const delivering = new FakeChannel()
    const message = { fields: { deliveryTag: 7 }, properties: {} }

    harness.manager.attachAckControls(message, delivering)
    await harness.manager.ackMessage(message)

    assert.equal(delivering.acked.length, 1, 'the delivering channel settles the message')
    assert.equal(harness.channel.acked.length, 0, 'a pool channel must not be used')
    assert.equal(message.__ackSettled, true)
  })

  test('nack failures are rethrown and leave the message unsettled', async () => {
    const harness = createManager()
    const message = {
      __channel: { nack () { throw new Error('channel gone') } },
      fields: {},
      properties: {}
    }

    await assert.rejects(() => harness.manager.nackMessage(message), /channel gone/)
    assert.equal(
      message.__ackSettled,
      undefined,
      'a failed nack must not claim settlement, or a later ack/nack would short-circuit'
    )
  })

  test('ack failures are rethrown and leave the message unsettled', async () => {
    const harness = createManager()
    const message = {
      __channel: { ack () { throw new Error('channel gone') } },
      fields: {},
      properties: {}
    }

    await assert.rejects(() => harness.manager.ackMessage(message), /channel gone/)
    assert.equal(
      message.__ackSettled,
      undefined,
      'a failed ack must not claim settlement, or a later nack would short-circuit'
    )
  })
})

describe('ConsumerManager lifecycle', () => {
  test('findQueueNameByTag resolves known tags and returns null otherwise', async () => {
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})

    assert.equal(harness.manager.findQueueNameByTag(consumer.consumerTag), 'orders')
    assert.equal(harness.manager.findQueueNameByTag('unknown-tag'), null)
  })

  test('unsubscribe cancels the consumer and drops all tracking', async () => {
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})
    const removed = await harness.manager.unsubscribe(consumer.consumerTag)

    assert.equal(removed, true)
    assert.deepEqual(harness.channel.cancelled, [consumer.consumerTag])
    assert.equal(harness.manager.activeConsumers.size, 0)
    assert.equal(harness.manager.findQueueNameByTag(consumer.consumerTag), null)

    assert.equal(await harness.manager.unsubscribe('unknown-tag'), false)
  })

  test('recreateAll re-runs every setup and re-tracks the new consumer tags', async () => {
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})

    await harness.manager.recreateAll()

    assert.equal(harness.channel.consumers.length, 2, 'consume must be issued again')

    const newTag = harness.channel.consumers.at(-1).consumerTag

    assert.notEqual(newTag, consumer.consumerTag)
    assert.equal(harness.manager.findQueueNameByTag(newTag), 'orders')
    assert.equal(harness.manager.findQueueNameByTag(consumer.consumerTag), null, 'stale tag dropped')
  })

  test('recreateAll survives a consumer that fails to recreate', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {})

    harness.channel.consumeError = new Error('still down')

    await harness.manager.recreateAll()

    assert.equal(harness.manager.activeConsumers.size, 1, 'failed consumer stays registered for the next sweep')
  })

  test('disposeAll drops every consumer and stops broker-cancel recovery', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {})
    await harness.manager.subscribe('invoices', async () => {})

    const consumersBefore = harness.channel.consumers.length

    await harness.manager.disposeAll()

    assert.equal(harness.manager.activeConsumers.size, 0)
    assert.equal(harness.manager.consumersByTag.size, 0)

    // A cancel notification arriving after disposal must not resurrect the
    // consumer — disconnect() disposes and then closes the pool.
    await harness.channel.consumers[0].callback(null)
    await sleep(120)

    assert.equal(harness.channel.consumers.length, consumersBefore, 'no consumer may be recreated after disposal')
    assert.equal(harness.events.filter(e => e.event === 'consumerRecovered').length, 0)
  })

  test('a broker cancel (null message) emits consumerCancelled and recovers the consumer', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {})

    // Broker-initiated cancel is delivered as a null message.
    await harness.channel.consumers[0].callback(null)

    assert.ok(harness.events.some(e => e.event === 'consumerCancelled'), 'cancel must be announced')

    // Recovery backs off 1s before the first attempt.
    await waitFor(() => harness.events.some(e => e.event === 'consumerRecovered'), 3000, 'consumer recovery')

    assert.equal(harness.channel.consumers.length, 2, 'consumer recreated after broker cancel')

    const newTag = harness.channel.consumers.at(-1).consumerTag

    assert.equal(harness.manager.findQueueNameByTag(newTag), 'orders')
  })

  test('broker-cancel recovery yields to a concurrent recreation (epoch guard)', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {})

    await harness.channel.consumers[0].callback(null)

    // recreateAll (e.g. a reconnection) wins the race while the cancel
    // handler is still backing off.
    await harness.manager.recreateAll()

    assert.equal(harness.channel.consumers.length, 2)

    // Long enough for all three recovery attempts (20+40+60ms) to have run.
    await sleep(200)

    assert.equal(harness.channel.consumers.length, 2, 'the cancel handler must not create a duplicate consumer')
  })

  test('a consumer that cannot be recovered is dropped and emits consumerLost', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {})

    // The queue is gone for good: every recovery attempt fails.
    harness.channel.consumeError = new Error('NOT_FOUND - no queue "orders"')

    await harness.channel.consumers[0].callback(null)

    await waitFor(
      () => harness.events.some(e => e.event === 'consumerLost'),
      3000,
      'consumerLost emitted after exhausting recovery attempts'
    )

    const lost = harness.events.find(e => e.event === 'consumerLost')

    assert.equal(lost.payload.queueName, 'orders')
    assert.equal(harness.manager.activeConsumers.size, 0, 'an unrecoverable consumer must not stay registered')
    assert.equal(harness.manager.consumersByTag.size, 0)
  })
})

describe('ConsumerManager subscribeSequential wiring', () => {
  test('processes and acks messages through the sequential processor', async () => {
    const harness = createManager()
    const processed = []

    await harness.manager.subscribeSequential('orders', async (content) => {
      processed.push(content)
    })

    assert.deepEqual(harness.channel.prefetches, [1], 'sequential defaults to prefetch 1')

    await deliver(harness, { step: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })

    await waitFor(() => harness.channel.acked.length === 1, 3000, 'sequential message acked')
    assert.deepEqual(processed, [{ step: 1 }])
  })

  test('a failing sequential callback nacks the message', async () => {
    const harness = createManager()

    await harness.manager.subscribeSequential('orders', async () => {
      throw new Error('boom')
    })

    await deliver(harness, { step: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })

    await waitFor(() => harness.channel.nacked.length === 1, 3000, 'sequential message nacked')

    assert.equal(harness.channel.nacked[0].requeue, true, 'a first delivery may be retried')
    assert.equal(harness.channel.acked.length, 0)
  })

  test('a failing sequential redelivery is dead-lettered, not requeued forever', async () => {
    const harness = createManager()

    await harness.manager.subscribeSequential('orders', async () => {
      throw new Error('boom')
    })

    const consumer = harness.channel.consumers.at(-1)
    const { content } = await harness.codec.encode({ step: 1 })

    await consumer.callback({
      content,
      fields: { consumerTag: consumer.consumerTag, deliveryTag: 1, redelivered: true },
      properties: { messageId: 'm1', headers: { 'x-compressed': false } }
    })

    await waitFor(() => harness.channel.nacked.length === 1, 3000, 'sequential redelivery nacked')

    assert.equal(harness.channel.nacked[0].requeue, false, 'poison policy: redeliveries go to the DLQ')
  })
})

describe('ConsumerManager subscribeWithOptimizedPrefetch', () => {
  test('raises the prefetch when processing is consistently fast', async () => {
    const harness = createManager()

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 2,
      optimizationInterval: 20,
      increaseFactor: 2
    })

    assert.deepEqual(harness.channel.prefetches, [2])

    await deliver(harness, { n: 1 })
    await sleep(30)
    await deliver(harness, { n: 2 })

    await waitFor(() => harness.channel.prefetches.includes(4), 3000, 'prefetch raised')
  })

  test('re-applies the optimized prefetch after a reconnection resets the channel', async () => {
    // A recreated channel starts back at the initial prefetch, so the
    // optimizer must push the value it believes in again. Without this the
    // consumer silently runs at initialPrefetch forever after any reconnect.
    const harness = createManager()

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 2,
      optimizationInterval: 20,
      increaseFactor: 2
    })

    await deliver(harness, { n: 1 })
    await sleep(30)
    await deliver(harness, { n: 2 })
    await waitFor(() => harness.channel.prefetches.includes(4), 3000, 'prefetch raised to 4')

    // Simulate the reconnection path: recreateAll bumps the consumer's epoch,
    // and the recreated channel is back at the initial prefetch.
    harness.channel.prefetches.length = 0
    await harness.manager.recreateAll()

    assert.deepEqual(harness.channel.prefetches, [2], 'the recreated consumer starts at the initial prefetch')

    // The next delivery must detect the epoch change and restore the value.
    await deliver(harness, { n: 3 })

    await waitFor(
      () => harness.channel.prefetches.includes(4),
      3000,
      'optimized prefetch re-applied after the epoch change'
    )
  })
})

describe('ConsumerManager subscribeParallel', () => {
  test('routes messages through worker threads and acks successes', async (t) => {
    const harness = createManager()

    // A failed assertion must still terminate the worker pool, or the
    // surviving threads keep the test process pending.
    t.after(() => harness.manager.disposeAll())

    const consumer = await harness.manager.subscribeParallel('orders', ECHO_WORKER, { workerCount: 1, prefetch: 2 })

    await deliver(harness, { job: 'resize' })

    await waitFor(() => harness.channel.acked.length === 1, 5000, 'worker success acked')

    await harness.manager.unsubscribe(consumer.consumerTag)
  })

  test('nacks when the worker reports a failure', async (t) => {
    const harness = createManager()

    t.after(() => harness.manager.disposeAll())

    await harness.manager.subscribeParallel('orders', FLAKY_WORKER, { workerCount: 1 })

    // subscribeParallel hands the worker { content }, so the failure trigger
    // must live inside the message content itself.
    await deliver(harness, { shouldFail: true })

    await waitFor(() => harness.channel.nacked.length === 1, 5000, 'worker failure nacked')
    assert.equal(harness.channel.nacked[0].requeue, false)
  })
})
