import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { fileURLToPath } from 'node:url'
import { test, describe } from 'node:test'
import MessageCodec from '../src/messaging/message-codec.js'
import ConsumerManager from '../src/consumers/consumer-manager.js'
import { FakeChannel, ManualClock, recordingLogger, silentLogger, sleep, waitFor, withLiveEventLoop } from './helpers.js'

const ECHO_WORKER = fileURLToPath(new URL('./fixtures/echo-worker.mjs', import.meta.url))
const FLAKY_WORKER = fileURLToPath(new URL('./fixtures/flaky-worker.mjs', import.meta.url))

const createManager = (overrides = {}) => {
  const codec = new MessageCodec({ logger: silentLogger })
  const channel = new FakeChannel()
  const events = []

  const channelPool = {
    released: [],
    getDedicatedChannel: async () => channel,
    releaseDedicatedChannel: async (id) => { channelPool.released.push(id) }
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

  return { manager: new ConsumerManager(context), channel, channelPool, codec, events }
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

  test('the ack controls are hidden from anything that walks the message', async () => {
    // __channel points at the amqplib channel, which is cyclic. If either
    // property were enumerable, JSON.stringify on a delivered message — the
    // most ordinary thing a handler can do — would throw, and a spread would
    // silently carry a live channel around.
    const harness = createManager()
    let delivered = null
    let settledDuringHandler = null

    await harness.manager.subscribe('orders', async (content, msg) => {
      delivered = msg
      // Read inside the handler: the pipeline acks as soon as it returns.
      settledDuringHandler = msg.__ackSettled
    })

    await deliver(harness, { id: 1 })

    assert.ok(delivered.__channel, 'the delivering channel is still reachable')
    assert.equal(settledDuringHandler, false, 'the message starts out unsettled')

    assert.deepEqual(
      Object.keys(delivered).filter(key => key.startsWith('__')),
      [],
      'neither control shows up as an own enumerable key'
    )

    assert.doesNotThrow(() => JSON.stringify(delivered), 'a handler can still serialize the message')
    assert.equal(JSON.stringify(delivered).includes('__channel'), false)
  })

  test('settlement never uses allUpTo: one message at a time', async () => {
    // allUpTo: true would settle every unacknowledged message up to this
    // delivery tag — silently acknowledging work that was never done.
    const harness = createManager()

    await harness.manager.subscribe('orders', async (content, msg) => {
      if (content.bad) await harness.manager.nackMessage(msg)
    })

    await deliver(harness, { bad: true })

    assert.deepEqual(harness.channel.nacked.map(n => n.allUpTo), [false])
  })

  test('nackMessage defaults to no requeue', async () => {
    // The default matters more than the explicit case: a handler that calls
    // nackMessage(msg) on a poison message and gets a requeue hot-loops.
    const harness = createManager()

    await harness.manager.subscribe('orders', async (content, msg) => {
      await harness.manager.nackMessage(msg)
    })

    await deliver(harness, { id: 1 })

    assert.deepEqual(harness.channel.nacked.map(n => n.requeue), [false])
  })

  test('settleAck defaults to no requeue and latches after the first settlement', async () => {
    // settleAck keeps its own copy of the exactly-once guard, separate from
    // ackMessage/nackMessage. Without it the pipeline can ack a message on the
    // success path and then nack the same delivery tag from the outer catch.
    const harness = createManager()
    const channel = harness.channel
    const msg = { fields: {}, properties: {} }

    harness.manager.attachAckControls(msg, channel)
    harness.manager.settleAck(msg, channel, 'nack')
    harness.manager.settleAck(msg, channel, 'nack')
    harness.manager.settleAck(msg, channel, 'ack')

    assert.deepEqual(channel.nacked.map(n => ({ requeue: n.requeue, allUpTo: n.allUpTo })), [{ requeue: false, allUpTo: false }])
    assert.equal(channel.acked.length, 0, 'a settled message cannot be acked afterwards')
  })

  test('a settled message is never settled twice, whichever path settles it', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async (content, msg) => {
      await harness.manager.ackMessage(msg)
      await harness.manager.ackMessage(msg)
      await harness.manager.nackMessage(msg)
    })

    await deliver(harness, { id: 1 })

    assert.equal(harness.channel.acked.length, 1, 'the ack flag really latches')
    assert.equal(harness.channel.nacked.length, 0)
  })

  test('rejects a queue name that is only whitespace', async () => {
    const harness = createManager()

    await assert.rejects(() => harness.manager.subscribe('   ', async () => {}), /non-empty string/)
    await assert.rejects(() => harness.manager.subscribeSequential('\t\n', async () => {}), /non-empty string/)
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

  test('settling a message with no delivering channel fails loudly', async () => {
    // Every message this library hands to a callback carries its channel. One
    // that does not cannot be settled correctly, so guessing a pool channel
    // (which the broker would reject and close) is worse than an error.
    const harness = createManager()
    const orphan = { fields: { deliveryTag: 9 }, properties: {} }

    await assert.rejects(() => harness.manager.ackMessage(orphan), /channel is unknown/)
    await assert.rejects(() => harness.manager.nackMessage(orphan), /channel is unknown/)

    assert.equal(harness.channel.acked.length, 0, 'no pool channel may be touched')
    assert.equal(harness.channel.nacked.length, 0)
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

  test('recreateAll re-runs every setup and keeps the original tag valid', async () => {
    // The caller only ever holds the tag subscribe() returned. Retiring it on
    // recreation made unsubscribe(originalTag) answer false after the first
    // reconnection, with no way to learn the new tag — so every tag the
    // consumer has held stays a valid handle for its whole life.
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})

    await harness.manager.recreateAll()

    assert.equal(harness.channel.consumers.length, 2, 'consume must be issued again')

    const newTag = harness.channel.consumers.at(-1).consumerTag

    assert.notEqual(newTag, consumer.consumerTag, 'the broker issued a fresh tag')
    assert.equal(harness.manager.findQueueNameByTag(newTag), 'orders')
    assert.equal(harness.manager.findQueueNameByTag(consumer.consumerTag), 'orders', 'the original tag still resolves')
  })

  test('unsubscribe with the original tag still works after a recreation', async () => {
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})

    await harness.manager.recreateAll()

    assert.equal(await harness.manager.unsubscribe(consumer.consumerTag), true, 'the caller-held tag must still cancel')
    assert.equal(harness.manager.activeConsumers.size, 0)
    assert.equal(harness.manager.consumersByTag.size, 0, 'every alias went with the consumer')
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
    const logger = recordingLogger()
    const harness = createManager({ logger })

    await harness.manager.subscribe('orders', async () => {})

    // The queue is gone for good: every recovery attempt fails.
    harness.channel.consumeError = new Error('NOT_FOUND - no queue "orders"')

    await harness.channel.consumers[0].callback(null)

    await waitFor(
      () => harness.events.some(e => e.event === 'consumerLost'),
      3000,
      'consumerLost emitted after exhausting recovery attempts'
    )

    // The queue name is the whole point of these lines: an application with a
    // dozen consumers needs to know which one stopped draining, and how many
    // attempts were spent before giving up.
    assert.ok(logger.records.warn.some(line => line.includes('orders') && line.includes('cancelled by the broker')))
    assert.ok(logger.records.warn.some(line => line.includes('orders') && /attempt \d+\/3/.test(line)))
    assert.ok(logger.records.error.some(line => line.includes('orders') && line.includes('could not be recovered')))

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

describe('ConsumerManager retryPolicy', () => {
  const deliverFailing = async (harness, { redelivered = false, retryable } = {}) => {
    const consumer = harness.channel.consumers.at(-1)
    const { content } = await harness.codec.encode({ step: 1 })

    await consumer.callback({
      content,
      fields: { consumerTag: consumer.consumerTag, deliveryTag: 1, redelivered },
      properties: { messageId: 'm1', headers: { 'x-compressed': false } }
    })

    await waitFor(() => harness.channel.nacked.length === 1, 3000, 'message nacked')

    return harness.channel.nacked[0].requeue
  }

  const failing = (retryable) => async () => {
    const error = new Error('boom')

    if (retryable !== undefined) error.retryable = retryable

    throw error
  }

  test("subscribe defaults to 'none': a failure goes straight to the DLQ", async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', failing())

    assert.equal(await deliverFailing(harness), false)
  })

  test("subscribe with 'once' retries a first delivery", async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', failing(), { retryPolicy: 'once' })

    assert.equal(await deliverFailing(harness), true)
  })

  test("subscribe with 'once' dead-letters a redelivery", async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', failing(), { retryPolicy: 'once' })

    assert.equal(await deliverFailing(harness, { redelivered: true }), false)
  })

  test("subscribe with 'once' honors error.retryable === false", async () => {
    // The opt-out used to exist only in the sequential path; a handler that
    // knows the failure is permanent can now skip the retry in both.
    const harness = createManager()

    await harness.manager.subscribe('orders', failing(false), { retryPolicy: 'once' })

    assert.equal(await deliverFailing(harness), false)
  })

  test("error.retryable === true cannot force a retry under 'none'", async () => {
    // The subscription's policy is a ceiling: a handler must not be able to
    // requeue into a subscription that opted out of retries.
    const harness = createManager()

    await harness.manager.subscribe('orders', failing(true))

    assert.equal(await deliverFailing(harness), false)
  })

  test("subscribeSequential defaults to 'once'", async () => {
    const harness = createManager()

    await harness.manager.subscribeSequential('orders', failing())

    assert.equal(await deliverFailing(harness), true)
  })

  test("subscribeSequential with 'none' never requeues", async () => {
    const harness = createManager()

    await harness.manager.subscribeSequential('orders', failing(), { retryPolicy: 'none' })

    assert.equal(await deliverFailing(harness), false)
  })

  test('an undecodable message follows the policy too', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {}, { retryPolicy: 'once' })

    const consumer = harness.channel.consumers.at(-1)

    await consumer.callback({
      content: Buffer.from('not-gzip'),
      fields: { consumerTag: consumer.consumerTag, deliveryTag: 1, redelivered: false },
      properties: { headers: { 'x-compressed': true } }
    })

    await waitFor(() => harness.channel.nacked.length === 1, 3000, 'undecodable message nacked')

    assert.equal(harness.channel.nacked[0].requeue, true)
  })

  // The wrapper methods build their own options object before delegating to
  // subscribe. Asserting the pass-through rather than reasoning about it: a
  // future destructure that swallows retryPolicy would silently disable the
  // caller's policy.
  test('subscribeWithOptimizedPrefetch forwards the policy', async () => {
    const harness = createManager()

    await harness.manager.subscribeWithOptimizedPrefetch('orders', failing(), { retryPolicy: 'once' })

    assert.equal(await deliverFailing(harness), true)
  })

  test('subscribeParallel forwards the policy', async (t) => {
    const harness = createManager()

    t.after(() => harness.manager.disposeAll())

    await harness.manager.subscribeParallel('orders', FLAKY_WORKER, { workerCount: 1, retryPolicy: 'once' })

    await deliver(harness, { shouldFail: true })
    await waitFor(() => harness.channel.nacked.length === 1, 5000, 'worker failure nacked')

    assert.equal(harness.channel.nacked[0].requeue, true)
  })

  test('noAck: true means the library never settles anything', async () => {
    // With noAck the broker considers the message delivered the moment it
    // leaves the queue. Sending an ack or nack afterwards is a protocol error
    // on an unknown delivery tag, and the broker closes the channel for it.
    const harness = createManager()

    await harness.manager.subscribe('orders', async (content) => {
      if (content.explode) throw new Error('boom')
    }, { noAck: true })

    await deliver(harness, { id: 1 })
    // The failure path settles separately from the success path, so it needs
    // its own delivery: a handler that never throws leaves the catch untested.
    await deliver(harness, { id: 2, explode: true })

    assert.equal(harness.channel.acked.length, 0)
    assert.equal(harness.channel.nacked.length, 0)
    assert.deepEqual(harness.channel.prefetches, [], 'prefetch is meaningless without acknowledgement')
  })

  test('noAck: true also silences the sequential path, success and failure alike', async () => {
    const harness = createManager()
    const seen = []

    await harness.manager.subscribeSequential('orders', async (content) => {
      seen.push(content)

      if (content.explode) throw new Error('boom')
    }, { noAck: true })

    await deliver(harness, { id: 1 }, { messageId: 'm1' })
    await deliver(harness, { id: 2, explode: true }, { messageId: 'm2' })

    await waitFor(() => seen.length === 2, 3000, 'both messages processed')

    assert.equal(harness.channel.acked.length, 0)
    assert.equal(harness.channel.nacked.length, 0)
  })

  test('a handler that throws a non-Error still settles the message (issue #18)', async () => {
    // JavaScript allows `throw null`. The catch used to read error.message
    // before settling, so the TypeError it raised skipped the nack entirely:
    // the delivery sat unacknowledged until the channel died, with no
    // redelivery in the meantime. One case per throwable shape.
    for (const thrown of [null, undefined, 'just a string']) {
      const harness = createManager()

      await harness.manager.subscribe('orders', async () => {
        throw thrown // eslint-disable-line no-throw-literal
      })

      await deliver(harness, { id: 1 })

      assert.equal(harness.channel.nacked.length, 1, `thrown ${String(thrown)}: the message must be settled`)
      assert.equal(harness.channel.acked.length, 0)
    }
  })

  test("a null throw under 'once' still gets its retry (issue #18)", async () => {
    // Pins the optional chaining in #shouldRequeue: `error?.retryable` runs
    // against the raw thrown value, and with `throw null` the un-guarded form
    // crashes inside the catch — no settlement at all, let alone a retry.
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {
      throw null // eslint-disable-line no-throw-literal
    }, { retryPolicy: 'once' })

    assert.equal(await deliverFailing(harness), true, 'a first delivery is still retried')
  })

  test('a message delivered without a fields object does not break the retry policy', async () => {
    // The policy reads message.fields.redelivered. Anything that hands the
    // pipeline a message without `fields` — a hand-rolled republish, a shim,
    // a future amqplib — would throw inside the catch, turning a handled
    // failure into an unhandled rejection that settles nothing.
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {
      throw new Error('boom')
    }, { retryPolicy: 'once' })

    const consumer = harness.channel.consumers.at(-1)
    const { content } = await harness.codec.encode({ id: 1 })

    await consumer.callback({
      content,
      properties: { headers: { 'x-compressed': false } }
    })

    await waitFor(() => harness.channel.nacked.length === 1, 3000, 'settled despite the missing fields')

    assert.equal(harness.channel.nacked[0].requeue, true, 'treated as a first delivery')
  })

  test('recovers a broker-cancelled consumer on the last allowed attempt', async () => {
    // Three attempts, not two: the recovery budget is what carries a consumer
    // through a queue being recreated during a deploy.
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {})

    const recovered = []

    harness.events.length = 0
    harness.channel.consumeError = new Error('queue still gone')

    // Fails on attempts 1 and 2, succeeds on 3.
    let failures = 2

    const realConsume = harness.channel.consume.bind(harness.channel)

    harness.channel.consume = async (...args) => {
      if (failures-- > 0) throw new Error('queue still gone')

      return realConsume(...args)
    }

    harness.channel.consumeError = null

    await harness.channel.consumers.at(-1).callback(null)

    await waitFor(
      () => harness.events.some(e => e.event === 'consumerRecovered'),
      5000,
      'consumer recovered on the third attempt'
    )

    recovered.push(...harness.events.filter(e => e.event === 'consumerRecovered'))

    assert.equal(recovered.length, 1)
    assert.equal(recovered[0].payload.queueName, 'orders', 'the event says which queue came back')
    assert.ok(recovered[0].payload.consumerTag, 'and under which tag')
  })

  test('subscribeSequential defaults its stale timeout to 30 seconds', async () => {
    const harness = createManager()

    const consumer = await harness.manager.subscribeSequential('orders', async () => {})
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)

    assert.equal(harness.manager.activeConsumers.get(consumerId).sequentialProcessor.staleTimeout, 30000)

    await harness.manager.disposeAll()
  })

  test('the subscription log names the queue and the prefetch it settled on', async () => {
    // Operators read these to confirm a deploy actually attached its consumers.
    const logger = recordingLogger()
    const harness = createManager({ logger })

    await harness.manager.subscribe('orders', async () => {}, { prefetchCount: 7 })
    await harness.manager.subscribeSequential('steps', async () => {})

    assert.ok(logger.records.info.some(line => line.includes('orders') && line.includes('7')))
    assert.ok(logger.records.info.some(line => line.includes('steps') && line.includes('sequential')))

    await harness.manager.disposeAll()
  })

  test('unsubscribing a sequential consumer disposes its processor', async () => {
    // The processor owns a cleanup interval and two maps. Dropping the consumer
    // without disposing it leaks all three for the life of the process.
    const harness = createManager()

    const consumer = await harness.manager.subscribeSequential('orders', async () => {})
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)
    const processor = harness.manager.activeConsumers.get(consumerId).sequentialProcessor

    assert.ok(processor, 'the sequential consumer built a processor')

    await harness.manager.unsubscribe(consumer.consumerTag)

    assert.equal(processor.cleanupInterval._destroyed ?? false, true, 'its cleanup timer was cleared')
  })

  test('recreating a sequential consumer disposes the processor tied to the old channel', async () => {
    // On reconnect the setup runs again. Keeping the previous processor alive
    // leaves a second cleanup timer settling messages on a dead channel.
    const harness = createManager()

    const consumer = await harness.manager.subscribeSequential('orders', async () => {})
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)
    const first = harness.manager.activeConsumers.get(consumerId).sequentialProcessor

    await harness.manager.recreateAll()

    const second = harness.manager.activeConsumers.get(consumerId).sequentialProcessor

    assert.notEqual(second, first, 'a fresh processor was built')
    assert.equal(first.cleanupInterval._destroyed ?? false, true, 'the previous one was disposed')

    await harness.manager.disposeAll()
  })

  test('unsubscribe still tears down when the broker refuses the cancel', async () => {
    const logger = recordingLogger()
    const harness = createManager({ logger })

    const consumer = await harness.manager.subscribe('orders', async () => {})

    harness.channel.cancel = async () => { throw new Error('unknown consumer tag') }

    assert.equal(await harness.manager.unsubscribe(consumer.consumerTag), true, 'the consumer is dropped anyway')
    assert.ok(logger.records.warn.some(line => line.includes('unknown consumer tag')))
    assert.equal(harness.manager.findConsumerIdByTag(consumer.consumerTag), null)
  })

  test('an invalid policy is rejected at subscribe time, not silently ignored', async () => {
    const harness = createManager()

    await assert.rejects(
      () => harness.manager.subscribe('orders', async () => {}, { retryPolicy: 'twice' }),
      /Invalid retryPolicy 'twice'/
    )
    await assert.rejects(
      () => harness.manager.subscribeSequential('orders', async () => {}, { retryPolicy: 'always' }),
      /Invalid retryPolicy 'always'/
    )
  })

  test('retryPolicy is not forwarded to the broker as a consume argument', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {}, { retryPolicy: 'once', priority: 5 })

    const consumer = harness.channel.consumers.at(-1)

    assert.equal(consumer.options.retryPolicy, undefined)
    assert.equal(consumer.options.priority, 5, 'genuine consume options still pass through')
  })
})

describe('ConsumerManager subscribeWithOptimizedPrefetch', () => {
  // The optimizer measures processing pace through the injected clock, so
  // "slow processing" is a callback that advances the clock — these tests
  // used to sleep 520ms of real time to cross the saturation threshold.
  test('raises the prefetch when processing is consistently fast', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock })

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 2,
      optimizationInterval: 20,
      increaseFactor: 2
    })

    assert.deepEqual(harness.channel.prefetches, [2])

    await deliver(harness, { n: 1 })
    clock.advance(21)
    await deliver(harness, { n: 2 })

    await waitFor(() => harness.channel.prefetches.includes(4), 3000, 'prefetch raised')
  })

  test('lowers the prefetch when processing is consistently slow', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock })

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {
      // Above the 500ms threshold the optimizer treats the consumer as
      // saturated and backs the prefetch off.
      clock.advance(520)
    }, {
      initialPrefetch: 8,
      optimizationInterval: 20,
      decreaseFactor: 0.5,
      minPrefetch: 1
    })

    assert.deepEqual(harness.channel.prefetches, [8])

    await deliver(harness, { n: 1 })
    await deliver(harness, { n: 2 })

    await waitFor(() => harness.channel.prefetches.includes(4), 5000, 'prefetch lowered')
  })

  test('leaves the prefetch alone when the measured pace warrants no change', async () => {
    // Between the two thresholds (100ms fast, 500ms slow) the optimizer must
    // decide on nothing and skip the channel round-trip entirely.
    const clock = new ManualClock()
    const harness = createManager({ clock })

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {
      clock.advance(200)
    }, {
      initialPrefetch: 4,
      optimizationInterval: 20
    })

    await deliver(harness, { n: 1 })
    await deliver(harness, { n: 2 })

    assert.deepEqual(harness.channel.prefetches, [4], 'no adjustment was applied')
  })

  test('the optimizer stops quietly while the consumer has no channel', async () => {
    // Two distinct states must both short-circuit: the consumer entry being
    // gone (unsubscribed) and the entry existing with no channel yet — which
    // is how a consumer looks mid-recreation, since registerConsumer starts
    // it at channel: null. Only the second exercises the `?.channel` guard;
    // reaching the optimizer in that state would throw on undefined.prefetch.
    const logger = recordingLogger()
    const harness = createManager({ logger })

    const consumer = await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 2,
      optimizationInterval: 20,
      increaseFactor: 2
    })

    const channel = harness.channel
    const wrapped = channel.consumers.at(-1).callback
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)

    harness.manager.activeConsumers.get(consumerId).channel = null

    const before = channel.prefetches.length

    await wrapped({
      content: (await harness.codec.encode({ n: 1 })).content,
      fields: { consumerTag: consumer.consumerTag, deliveryTag: 1 },
      properties: { headers: { 'x-compressed': false } }
    })

    await sleep(30)

    await wrapped({
      content: (await harness.codec.encode({ n: 2 })).content,
      fields: { consumerTag: consumer.consumerTag, deliveryTag: 2 },
      properties: { headers: { 'x-compressed': false } }
    })

    assert.equal(channel.prefetches.length, before, 'no prefetch call while the channel is missing')
    assert.equal(channel.acked.length, 2, 'the messages were still processed and acked')

    // The real discriminator: without the guard the optimizer reaches
    // applyPrefetch and dereferences the missing channel. That failure is
    // swallowed by applyPrefetch's own catch, so silence in the warn log is
    // the only evidence that the guard actually short-circuited.
    assert.deepEqual(logger.records.warn, [], 'the guard short-circuits instead of failing inside applyPrefetch')
  })

  test('a failed prefetch adjustment is logged and never fails the message', async () => {
    // The optimization runs after the callback already succeeded: letting it
    // throw would nack an already-processed message to the DLQ.
    const logger = recordingLogger()
    const harness = createManager({ logger })

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 2,
      optimizationInterval: 20,
      increaseFactor: 2
    })

    harness.channel.prefetch = async () => { throw new Error('channel is closing') }

    await deliver(harness, { n: 1 })
    await sleep(30)
    await deliver(harness, { n: 2 })

    await waitFor(
      () => logger.records.warn.some(line => line.includes('channel is closing')),
      3000,
      'prefetch failure logged'
    )

    assert.equal(harness.channel.nacked.length, 0, 'the processed messages were not dead-lettered')
    assert.equal(harness.channel.acked.length, 2)
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

  test('terminates the worker pool when the subscription itself fails', async () => {
    // Otherwise a failed subscribe leaks live worker threads and the process
    // never exits.
    const harness = createManager()

    harness.channel.consumeError = new Error('queue does not exist')

    await assert.rejects(
      () => harness.manager.subscribeParallel('orders', ECHO_WORKER, { workerCount: 1 }),
      /queue does not exist/
    )

    assert.equal(harness.manager.workerPools.size, 0, 'no pool was left registered')
  })
})

describe('ConsumerManager survivor round', () => {
  test('subscribe validates the queue name and the callback', async () => {
    const harness = createManager()

    await assert.rejects(() => harness.manager.subscribe('', async () => {}), /Queue name must be a non-empty string/)
    await assert.rejects(() => harness.manager.subscribe('   ', async () => {}), /Queue name must be a non-empty string/)
    // A non-string must hit the SAME friendly error, not a TypeError from
    // .trim() — the typeof arm exists exactly for this input.
    await assert.rejects(() => harness.manager.subscribe(42, async () => {}), /Queue name must be a non-empty string/)
    await assert.rejects(() => harness.manager.subscribe('orders', 'not-a-function'), /Callback must be a function/)
  })

  test('ack controls can be re-attached to a delivery that crosses consumption paths', async () => {
    // configurable: true is what allows a second attachAckControls on the
    // same message object with a DIFFERENT channel and a fresh settlement
    // state (a delivery re-entering through another consumption path).
    // Redefining an identical descriptor never throws, so the test must
    // actually change both values.
    const harness = createManager()
    const secondChannel = new FakeChannel()
    const msg = { properties: {}, fields: {} }

    harness.manager.attachAckControls(msg, harness.channel)
    harness.manager.settleAck(msg, harness.channel, 'ack')

    assert.equal(msg.__ackSettled, true)

    harness.manager.attachAckControls(msg, secondChannel)

    assert.equal(msg.__channel, secondChannel, 'the new delivering channel took over')
    assert.equal(msg.__ackSettled, false, 'the re-attached delivery starts unsettled again')
  })

  test('a consumer that fails to recreate is reported, not silently dropped', async () => {
    const logger = recordingLogger()
    const harness = createManager({ logger })

    await harness.manager.subscribe('orders', async () => {})

    harness.channel.consumeError = new Error('queue rebuilding')

    await harness.manager.recreateAll()

    assert.ok(
      logger.records.error.some(line => line.includes('Failed to recreate consumer')),
      'the operator can only learn about a half-recovered consumer from this line'
    )
  })

  test('ack bookkeeping properties stay non-enumerable on the delivered message', async () => {
    // __channel and __ackSettled ride on the raw message; if they became
    // enumerable they would leak into every JSON.stringify/log of it.
    const harness = createManager()
    let delivered

    await harness.manager.subscribe('orders', async (content, message) => {
      delivered = message
    })

    await deliver(harness, { n: 1 })

    assert.equal(Object.keys(delivered).some(key => key.startsWith('__')), false, 'bookkeeping must not enumerate')
    assert.equal(delivered.__channel, harness.channel, 'but it is still reachable for settlement')
  })

  test('the sequential processor is disposed when its channel closes', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock })

    await harness.manager.subscribeSequential('orders', async () => {})

    assert.equal(clock.intervals.size, 1, 'the processor sweep is running')

    harness.channel.emit('close')

    assert.equal(clock.intervals.size, 0, 'the close disposed the processor — its sweep would otherwise leak')
  })

  test('broker-cancel recovery backs off by attempt through the injected clock', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock, consumerRecoveryInterval: 100 })

    await harness.manager.subscribe('orders', async () => {})

    const consumer = harness.channel.consumers.at(-1)

    // Recreation always fails: the recovery loop must walk its full budget.
    harness.channel.consume = async () => { throw new Error('still down') }

    await consumer.callback(null)

    // The recovery loop runs detached from the delivery callback.
    await waitFor(() => clock.sleeps.length === 3, 2000, 'recovery walked its full budget')

    assert.deepEqual(clock.sleeps, [100, 200, 300], 'three attempts, linear backoff on the base interval')
  })

  test('an unsubscribe mid-recovery stops the recreation while the entry is still visible', async () => {
    // unsubscribe marks cancelled=true and only drops the entry after its
    // awaits; a recovery attempt landing inside that window must observe the
    // flag and stand down instead of recreating a consumer being removed.
    const clock = new ManualClock()
    const harness = createManager({ clock, consumerRecoveryInterval: 100 })

    const consumer = await harness.manager.subscribe('orders', async () => {})
    const channelConsumer = harness.channel.consumers.at(-1)
    const consumesBefore = harness.channel.consumers.length

    let releaseCancel
    const cancelGate = new Promise(resolve => { releaseCancel = resolve })

    harness.channel.cancel = async () => { await cancelGate }

    const recovery = channelConsumer.callback(null)
    const unsubscribing = harness.manager.unsubscribe(consumer.consumerTag)

    await new Promise(resolve => setImmediate(resolve))

    // The recovery sweep fires while unsubscribe is parked on channel.cancel:
    // the entry still exists, cancelled is already true.
    clock.advance(100)
    await new Promise(resolve => setImmediate(resolve))

    releaseCancel()
    await unsubscribing
    await recovery

    assert.equal(harness.channel.consumers.length, consumesBefore, 'no recreation for a consumer being unsubscribed')
    assert.equal(harness.manager.activeConsumers.size, 0)
  })

  test('unsubscribe tolerates a consumer whose channel is mid-recreation', async () => {
    const harness = createManager()
    const consumer = await harness.manager.subscribe('orders', async () => {})
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)

    harness.manager.activeConsumers.get(consumerId).channel = null

    assert.equal(await harness.manager.unsubscribe(consumer.consumerTag), true, 'no crash cancelling without a channel')
    assert.equal(harness.manager.activeConsumers.size, 0)
  })
})

describe('ConsumerManager prefetch optimizer edges', () => {
  test('the optimizer survives the consumer entry disappearing entirely', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock })

    const consumer = await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 2,
      optimizationInterval: 20
    })

    const wrapped = harness.channel.consumers.at(-1).callback

    await harness.manager.unsubscribe(consumer.consumerTag)

    const ackedBefore = harness.channel.acked.length

    // A late delivery races the unsubscribe: the optimizer's lookup misses.
    await wrapped({
      content: (await harness.codec.encode({ n: 1 })).content,
      fields: { consumerTag: consumer.consumerTag, deliveryTag: 9 },
      properties: { headers: { 'x-compressed': false } }
    })

    // The discriminator: a crash in the optimizer would land in the message
    // pipeline's catch and settle this delivery as a NACK.
    assert.equal(harness.channel.acked.length, ackedBefore + 1, 'the late delivery still acked')
    assert.equal(harness.channel.nacked.length, 0)
  })

  test('an epoch change without an optimized value applies nothing', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock })

    const consumer = await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 4,
      optimizationInterval: 1000
    })

    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)
    const applied = harness.channel.prefetches.length

    // A recreation bumps the epoch; the prefetch never left its initial
    // value, so there is nothing to re-apply.
    harness.manager.activeConsumers.get(consumerId).epoch++

    await deliver(harness, { n: 1 })

    assert.equal(harness.channel.prefetches.length, applied, 'initial value: nothing to restore')
  })

  test('an optimized value is re-applied exactly once per epoch change', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock })

    const consumer = await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 2,
      optimizationInterval: 20,
      increaseFactor: 2
    })

    await deliver(harness, { n: 1 })
    clock.advance(21)
    await deliver(harness, { n: 2 })
    await waitFor(() => harness.channel.prefetches.includes(4), 2000, 'raised')

    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)

    harness.manager.activeConsumers.get(consumerId).epoch++

    const before = harness.channel.prefetches.length

    await deliver(harness, { n: 3 })

    assert.equal(harness.channel.prefetches.length, before + 1, 'the recreated channel got the optimized value back')
    assert.equal(harness.channel.prefetches.at(-1), 4)

    await deliver(harness, { n: 4 })

    assert.equal(harness.channel.prefetches.length, before + 1, 'no re-application while the epoch is stable')
  })

  test('average boundaries decide nothing at exactly 100ms and 500ms', async () => {
    for (const [avg, initial] of [[100, 4], [500, 4]]) {
      const clock = new ManualClock()
      const harness = createManager({ clock })

      await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {
        clock.advance(avg)
      }, {
        initialPrefetch: initial,
        optimizationInterval: 20
      })

      await deliver(harness, { n: 1 })
      await deliver(harness, { n: 2 })

      assert.deepEqual(harness.channel.prefetches, [initial], `avg exactly ${avg}ms is the no-man's land — no adjustment`)
    }
  })

  test('an elapsed window equal to the interval is enough to optimize', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock })

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 2,
      optimizationInterval: 100,
      increaseFactor: 2
    })

    await deliver(harness, { n: 1 })

    // Exactly the interval, not a tick more.
    clock.advance(100)
    await deliver(harness, { n: 2 })

    assert.ok(harness.channel.prefetches.includes(4), 'elapsed == interval optimizes; only strictly-less waits')
  })

  test('the average is a mean over the whole window, not a sum', async () => {
    // Three 60ms samples accumulate in ONE optimization window (the interval
    // only elapses on the third): mean 60 -> raise. A sum-flavoured formula
    // (180*3, or plain 180) lands at or past the slow threshold and either
    // stalls or LOWERS instead. Single-sample windows cannot tell these
    // apart — sum/1 equals sum*1 — which is why the window must hold three.
    const clock = new ManualClock()
    const harness = createManager({ clock })

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {
      clock.advance(60)
    }, {
      initialPrefetch: 2,
      optimizationInterval: 1000,
      increaseFactor: 2
    })

    await deliver(harness, { n: 1 })
    await deliver(harness, { n: 2 })
    clock.advance(900)
    await deliver(harness, { n: 3 })

    assert.deepEqual(harness.channel.prefetches, [2, 4], 'three fast samples mean fast')
  })

  test('the sample window resets to a clean slate after every decision', async () => {
    // A polluted reset (anything but an empty array) poisons every later
    // average — one junk entry turns all future sums into NaN and the
    // optimizer silently stops adapting forever.
    const clock = new ManualClock()
    const harness = createManager({ clock })

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {}, {
      initialPrefetch: 2,
      optimizationInterval: 20,
      increaseFactor: 2
    })

    await deliver(harness, { n: 1 })
    clock.advance(21)
    await deliver(harness, { n: 2 })
    await waitFor(() => harness.channel.prefetches.includes(4), 2000, 'first raise')

    clock.advance(21)
    await deliver(harness, { n: 3 })

    await waitFor(() => harness.channel.prefetches.includes(8), 2000, 'the second cycle still adapts')
  })
})

describe('ConsumerManager subscribeParallel (fake workers)', () => {
  class FakeParallelWorker extends EventEmitter {
    constructor (file, options, replyWith) {
      super()
      this.file = file
      this.options = options
      this.replyWith = replyWith
      this.terminated = false
    }

    postMessage (payload) {
      setImmediate(() => this.emit('message', this.replyWith(payload)))
    }

    async terminate () {
      this.terminated = true
      this.emit('exit', 0)
    }
  }

  const createParallelHarness = (replyWith) => {
    const spawned = []
    // The spawn seam is construction-time wiring, so it enters through the
    // manager's context — the per-subscription options belong to the caller.
    const createWorker = (file, options) => {
      const worker = new FakeParallelWorker(file, options, replyWith)

      spawned.push(worker)

      return worker
    }

    const harness = createManager({ createWorker })

    return { ...harness, spawned }
  }

  test('spawns the requested workers with the queue name in their workerData', async () => {
    const harness = createParallelHarness(() => ({ success: true }))

    await harness.manager.subscribeParallel('orders', 'processor.js', {
      workerCount: 2,
      prefetch: 10
    })

    assert.equal(harness.spawned.length, 2)
    assert.equal(harness.spawned[0].file, 'processor.js')
    assert.deepEqual(harness.spawned[0].options.workerData, { queueName: 'orders', workerId: 0 })
    assert.deepEqual(harness.channel.prefetches, [20], 'prefetch scales by the pool size')
  })

  test('a worker failure nacks the message with the worker-provided reason', async () => {
    const logger = recordingLogger()
    const harness = createParallelHarness(() => ({ success: false, error: 'schema mismatch' }))

    harness.manager.logger = logger

    await harness.manager.subscribeParallel('orders', 'processor.js', {
      workerCount: 1
    })

    await deliver(harness, { n: 1 })

    assert.equal(harness.channel.nacked.length, 1, 'a failed worker result settles as a nack')
    assert.ok(logger.records.error.some(line => line.includes('schema mismatch')), 'the worker reason surfaces')
  })

  test('a worker replying nothing usable falls back to a generic failure', async () => {
    const logger = recordingLogger()
    const harness = createParallelHarness(() => null)

    harness.manager.logger = logger

    await harness.manager.subscribeParallel('orders', 'processor.js', {
      workerCount: 1
    })

    await deliver(harness, { n: 1 })

    assert.equal(harness.channel.nacked.length, 1)
    assert.ok(logger.records.error.some(line => line.includes('Worker processing failed')), 'the fallback reason names the culprit')
  })

  test('a failing subscribe rejects AND terminates the freshly spawned pool', async () => {
    const harness = createParallelHarness(() => ({ success: true }))

    harness.channel.consume = async () => { throw new Error('queue gone') }

    await assert.rejects(
      () => harness.manager.subscribeParallel('orders', 'processor.js', { workerCount: 1 }),
      /queue gone/
    )

    assert.equal(harness.spawned[0].terminated, true, 'no orphan threads on a failed subscribe')
  })

  test('unsubscribe terminates the parallel pool; disposeAll disposes everything', async () => {
    const clock = new ManualClock()
    const spawned = []
    const createWorker = (file, options) => {
      const worker = new FakeParallelWorker(file, options, () => ({ success: true }))

      spawned.push(worker)

      return worker
    }
    const base = createManager({ clock, createWorker })

    const consumer = await base.manager.subscribeParallel('orders', 'processor.js', { workerCount: 1 })

    await base.manager.unsubscribe(consumer.consumerTag)

    assert.equal(spawned[0].terminated, true, 'unsubscribe shut the pool down')

    await base.manager.subscribeParallel('orders', 'processor.js', { workerCount: 1 })
    await base.manager.subscribeSequential('billing', async () => {})

    assert.equal(clock.intervals.size, 1, 'one sequential sweep running')

    await base.manager.disposeAll()

    assert.equal(spawned[1].terminated, true, 'disposeAll shut the second pool down')
    assert.equal(clock.intervals.size, 0, 'disposeAll disposed the sequential processor too')
    assert.equal(base.manager.activeConsumers.size, 0)
  })
})

describe('ConsumerManager channel-level loss', () => {
  test('a dedicated channel closing on a live connection recovers the consumer', async () => {
    // amqplib delivers the null message only on a broker basic.cancel, never
    // on a channel close — so a channel-level exception (PRECONDITION_FAILED,
    // ACCESS_REFUSED) used to kill the consumer in total silence while the
    // connection stayed healthy.
    const clock = new ManualClock()
    const logger = recordingLogger()
    const harness = createManager({ clock, logger, consumerRecoveryInterval: 10 })

    await harness.manager.subscribe('orders', async () => {})

    const consumesBefore = harness.channel.consumers.length

    harness.channel.emit('close')
    clock.advance(10)

    await waitFor(() => harness.channel.consumers.length > consumesBefore, 2000, 'consumer recreated')

    assert.ok(
      logger.records.warn.some(line => line.includes('closed unexpectedly')),
      'the loss is reported with a reason naming the channel'
    )
    assert.equal(harness.manager.activeConsumers.size, 1, 'the consumer survived')
  })

  test('the pool tearing down does NOT trigger per-channel recovery', async () => {
    // disconnect() and connection loss close every channel; that path belongs
    // to recreateAll. Racing it here would duplicate consumers or burn the
    // retry budget against a broker that is not there.
    const clock = new ManualClock()
    const harness = createManager({ clock, consumerRecoveryInterval: 10 })

    await harness.manager.subscribe('orders', async () => {})

    const consumesBefore = harness.channel.consumers.length

    harness.channelPool.closed = true
    harness.channel.emit('close')
    clock.advance(100)
    await sleep(20)

    assert.equal(harness.channel.consumers.length, consumesBefore, 'no recreation while the pool is closing')
  })

  test('recoveries that reuse the channel do not stack close listeners', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {})

    const baseline = harness.channel.listenerCount('close')

    await harness.manager.recreateAll()
    await harness.manager.recreateAll()

    assert.equal(harness.channel.listenerCount('close'), baseline, 'one watcher per channel, not per setup')
  })

  test('a sequential consumer disposes its processor when the channel dies', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock })

    await harness.manager.subscribeSequential('orders', async () => {})

    assert.equal(clock.intervals.size, 1, 'the processor sweep is running')

    harness.channelPool.closed = true
    harness.channel.emit('close')

    assert.equal(clock.intervals.size, 0, 'the dead channel took its processor with it')
  })
})

describe('ConsumerManager resource ownership', () => {
  test('unsubscribe releases the consumer dedicated channel', async () => {
    // Without the release, every subscribe/unsubscribe cycle leaked one
    // channel until the connection hit channel_max.
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)

    await harness.manager.unsubscribe(consumer.consumerTag)

    assert.deepEqual(harness.channelPool.released, [consumerId], 'the channel went back to the pool')
  })

  test('unsubscribe cancels the tag the BROKER knows, not the caller alias', async () => {
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})

    await harness.manager.recreateAll()

    const currentTag = harness.channel.consumers.at(-1).consumerTag

    await harness.manager.unsubscribe(consumer.consumerTag)

    assert.deepEqual(harness.channel.cancelled, [currentTag], 'cancelling the stale alias would leave it consuming')
  })

  test('giving up on a lost consumer terminates its worker pool', async () => {
    const clock = new ManualClock()
    let terminated = false
    const createWorker = () => {
      const worker = new EventEmitter()

      worker.postMessage = () => {}
      worker.terminate = async () => { terminated = true; worker.emit('exit', 0) }

      return worker
    }
    const harness = createManager({ clock, consumerRecoveryInterval: 10, createWorker })

    const consumer = await harness.manager.subscribeParallel('orders', 'processor.js', { workerCount: 1 })

    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)

    // Every recreation attempt fails, so recovery walks its budget and drops
    // the consumer — which used to leave the worker threads running forever.
    harness.channel.consume = async () => { throw new Error('queue gone') }

    await harness.manager.handleConsumerLoss(consumerId, 'queue deleted')

    assert.equal(harness.manager.activeConsumers.size, 0, 'the consumer was dropped')
    assert.equal(terminated, true, 'and its threads went with it')
    assert.equal(harness.manager.workerPools.size, 0)
  })
})

describe('ConsumerManager retry budget', () => {
  const deliverWithCount = async (harness, deliveryCount, redelivered) => {
    const { content, compressed } = await harness.codec.encode({ n: 1 })
    const consumer = harness.channel.consumers.at(-1)

    await consumer.callback({
      content,
      fields: { consumerTag: consumer.consumerTag, deliveryTag: 1, redelivered },
      properties: {
        headers: {
          'x-compressed': compressed,
          ...(deliveryCount === undefined ? {} : { 'x-delivery-count': deliveryCount })
        }
      }
    })
  }

  test('{ attempts: N } spends a real budget from the quorum-queue delivery count', async () => {
    // The redelivered flag is set by ANY requeue, including a connection drop,
    // so it cannot express "three tries". x-delivery-count counts actual
    // deliveries and is immune to that.
    //
    // Verified against a real broker: the header is ABSENT on the first
    // delivery and starts at 1 on the redelivery — undefined here is delivery
    // number one, not a missing feature.
    for (const [deliveryCount, expected] of [[undefined, true], [1, true], [2, false], [7, false]]) {
      const harness = createManager()

      await harness.manager.subscribe('orders', async () => { throw new Error('boom') }, {
        retryPolicy: { attempts: 3 }
      })

      await deliverWithCount(harness, deliveryCount, deliveryCount !== undefined)

      assert.equal(
        harness.channel.nacked.at(-1).requeue,
        expected,
        `delivery number ${(deliveryCount ?? 0) + 1} of a 3-attempt budget`
      )
    }
  })

  test('{ attempts: 1 } never requeues, matching none', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => { throw new Error('boom') }, {
      retryPolicy: { attempts: 1 }
    })

    // First delivery of a quorum queue: no header yet, and the budget of one
    // must still mean no retry.
    await deliverWithCount(harness, undefined, false)

    assert.equal(harness.channel.nacked.at(-1).requeue, false)
  })

  test('on a classic queue the budget degrades to the one-shot ceiling', async () => {
    // A REDELIVERY with no counter is the giveaway: quorum queues always send
    // one from the second delivery on. Honouring a budget the broker cannot
    // track would hot-loop the message forever.
    for (const [redelivered, expected] of [[false, true], [true, false]]) {
      const harness = createManager()

      await harness.manager.subscribe('orders', async () => { throw new Error('boom') }, {
        retryPolicy: { attempts: 5 }
      })

      await deliverWithCount(harness, undefined, redelivered)

      assert.equal(harness.channel.nacked.at(-1).requeue, expected)
    }
  })

  test('a budget still respects error.retryable === false', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {
      const error = new Error('malformed payload')
      error.retryable = false

      throw error
    }, { retryPolicy: { attempts: 5 } })

    await deliverWithCount(harness, undefined, false)

    assert.equal(harness.channel.nacked.at(-1).requeue, false, 'the handler opted out of the retry')
  })

  test('a malformed budget is rejected at subscribe time', async () => {
    const harness = createManager()

    for (const policy of [{ attempts: 0 }, { attempts: -1 }, { attempts: 1.5 }, { attempts: 'three' }, {}]) {
      await assert.rejects(
        () => harness.manager.subscribe('orders', async () => {}, { retryPolicy: policy }),
        /Invalid retryPolicy/
      )
    }
  })
})

describe('ConsumerManager recovery guards', () => {
  test('recovering an unknown consumer id is a no-op', async () => {
    // handleConsumerLoss is reachable from two paths that each pre-check
    // something different; the guard covers whichever one did not.
    const harness = createManager()

    await harness.manager.handleConsumerLoss('consumer-never-registered-1', 'queue deleted')

    assert.equal(harness.manager.activeConsumers.size, 0)
  })

  test('recovering a consumer already being unsubscribed stands down', async () => {
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)
    const consumesBefore = harness.channel.consumers.length

    harness.manager.activeConsumers.get(consumerId).cancelled = true

    await harness.manager.handleConsumerLoss(consumerId, 'channel closed')

    assert.equal(harness.channel.consumers.length, consumesBefore, 'no recreation for a consumer on its way out')
  })
})

describe('ConsumerManager guards on partial inputs', () => {
  test('a delivery without properties or fields is still settled', async () => {
    // amqplib hands over what the broker sent; a message missing either shape
    // must not crash the retry decision on its way to being nacked.
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => { throw new Error('boom') }, {
      retryPolicy: { attempts: 3 }
    })

    const consumer = harness.channel.consumers.at(-1)
    const { content } = await harness.codec.encode({ n: 1 })

    await consumer.callback({ content, fields: {}, properties: {} })

    assert.equal(harness.channel.nacked.length, 1, 'the bare delivery was settled')
    assert.equal(harness.channel.nacked[0].requeue, true, 'and the budget still applied to it')
  })

  test('a malformed retry policy names what it received', async () => {
    // The message is the only thing the caller has to work with at subscribe
    // time; "[object Object]" would send them hunting.
    const harness = createManager()

    await assert.rejects(
      () => harness.manager.subscribe('orders', async () => {}, { retryPolicy: { attempts: 0 } }),
      /Invalid retryPolicy '\{"attempts":0\}'/
    )

    await assert.rejects(
      () => harness.manager.subscribe('orders', async () => {}, { retryPolicy: 'twice' }),
      /Invalid retryPolicy 'twice'/
    )
  })

  test('a close from a channel the consumer already replaced triggers no recovery', async () => {
    // The watcher fires per channel; after a recreation moved the consumer to
    // a new one, the old channel's close is stale news.
    const clock = new ManualClock()
    const harness = createManager({ clock, consumerRecoveryInterval: 10 })

    await harness.manager.subscribe('orders', async () => {})

    const staleChannel = harness.channel
    const consumerId = harness.manager.findConsumerIdByTag(harness.channel.consumers.at(-1).consumerTag)

    // Move the consumer onto a different channel, as a recreation would.
    harness.manager.activeConsumers.get(consumerId).channel = new FakeChannel()

    const consumesBefore = staleChannel.consumers.length

    staleChannel.emit('close')
    clock.advance(100)
    await sleep(20)

    assert.equal(staleChannel.consumers.length, consumesBefore, 'the stale channel recreated nothing')
  })

  test('unsubscribing while disconnected does not crash on the missing pool', async () => {
    // The pool is gone during recovery; releasing a channel back to it has to
    // tolerate that rather than throwing over a consumer being removed.
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})
    let pool = harness.channelPool

    harness.manager.getChannelPool = () => pool
    pool = null

    assert.equal(await harness.manager.unsubscribe(consumer.consumerTag), true)
    assert.equal(harness.manager.activeConsumers.size, 0, 'the consumer was still removed')
  })
})

describe('ConsumerManager per-message events', () => {
  const consumerEvents = (harness) =>
    harness.events.filter(({ event }) => event === 'messageProcessed' || event === 'messageFailed')

  test('a processed message reports queue, identity and measured duration', async () => {
    // A non-zero epoch: with the clock starting at 0, `now - startedAt` and
    // `now + startedAt` agree and the subtraction would be untestable.
    const clock = new ManualClock(1000)
    const harness = createManager({ clock })

    await harness.manager.subscribe('orders', async () => {
      clock.jump(25)
    })

    await deliver(harness, { id: 7 }, { messageId: 'm1', headers: { 'x-compressed': false } })

    const tag = harness.channel.consumers.at(-1).consumerTag

    assert.deepEqual(consumerEvents(harness), [{
      event: 'messageProcessed',
      payload: { queue: 'orders', messageId: 'm1', consumerTag: tag, durationMs: 25 }
    }])
  })

  test('a failed message reports the error and the real requeue decision', async () => {
    const clock = new ManualClock(1000)
    const harness = createManager({ clock })
    const boom = new Error('handler exploded')

    await harness.manager.subscribe('orders', async () => {
      clock.jump(40)
      throw boom
    })

    await deliver(harness, { id: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })

    const [failed] = consumerEvents(harness)

    assert.equal(failed.event, 'messageFailed')
    assert.equal(failed.payload.queue, 'orders')
    assert.equal(failed.payload.durationMs, 40)
    assert.equal(failed.payload.error, boom)
    assert.equal(failed.payload.requeued, false, "the default 'none' policy dead-letters")
    assert.equal(failed.payload.requeued, harness.channel.nacked[0].requeue, 'the event mirrors the nack')
  })

  test("under 'once' the event reports the retry the broker was asked for", async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {
      throw new Error('boom')
    }, { retryPolicy: 'once' })

    await deliver(harness, { id: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })

    const [failed] = consumerEvents(harness)

    assert.equal(failed.payload.requeued, true)
    assert.equal(harness.channel.nacked[0].requeue, true)
  })

  test('noAck failures always report requeued: false — nothing was settled', async () => {
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {
      throw new Error('boom')
    }, { noAck: true, retryPolicy: 'once' })

    await deliver(harness, { id: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })

    const [failed] = consumerEvents(harness)

    assert.equal(failed.payload.requeued, false, "even under 'once': a noAck delivery is already gone")
    assert.equal(harness.channel.nacked.length, 0)
  })

  test('an undecodable message emits messageFailed without running the callback', async () => {
    const harness = createManager()
    let called = false

    await harness.manager.subscribe('orders', async () => { called = true })

    const consumer = harness.channel.consumers.at(-1)

    await consumer.callback({
      content: Buffer.from('not-gzip'),
      fields: { consumerTag: consumer.consumerTag },
      properties: { messageId: 'm1', headers: { 'x-compressed': true } }
    })

    const [failed] = consumerEvents(harness)

    assert.equal(called, false)
    assert.equal(failed.event, 'messageFailed')
    assert.equal(failed.payload.messageId, 'm1')
    assert.equal(failed.payload.requeued, false)
  })

  test('a delivery with no fields object is still reported as processed', async () => {
    // The event payload reads fields for the consumer tag AFTER the ack: a
    // crash there would flip a successfully processed message into a
    // messageFailed report.
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {})

    const { content, compressed } = await harness.codec.encode({ id: 1 })

    await harness.channel.consumers.at(-1).callback({
      content,
      properties: { messageId: 'm1', headers: { 'x-compressed': compressed } }
    })

    const [processed] = consumerEvents(harness)

    assert.equal(processed.event, 'messageProcessed')
    assert.equal(processed.payload.consumerTag, undefined)
    assert.equal(harness.channel.acked.length, 1)
  })

  test('a delivery with no properties object still fails safely and is nacked', async () => {
    // Reading the compression header throws before the callback ever runs;
    // the failure path must tolerate the missing properties or the delivery
    // would hang unsettled with no event at all.
    const harness = createManager()

    await harness.manager.subscribe('orders', async () => {})

    await harness.channel.consumers.at(-1).callback({ content: Buffer.from('x') })

    const [failed] = consumerEvents(harness)

    assert.equal(failed.event, 'messageFailed')
    assert.equal(failed.payload.messageId, undefined)
    assert.equal(failed.payload.requeued, false)
    assert.deepEqual(harness.channel.nacked.map(n => n.requeue), [false])
  })

  test('a sequential success reports the duration measured by the processor', async () => {
    const clock = new ManualClock(1000)
    const harness = createManager({ clock })

    await harness.manager.subscribeSequential('orders', async () => {
      clock.jump(60)
    })

    await deliver(harness, { step: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })
    await waitFor(() => harness.channel.acked.length === 1, 3000, 'sequential message acked')

    const tag = harness.channel.consumers.at(-1).consumerTag

    assert.deepEqual(consumerEvents(harness), [{
      event: 'messageProcessed',
      payload: { queue: 'orders', messageId: 'm1', consumerTag: tag, durationMs: 60 }
    }])
  })

  test('a sequential failure reports duration, error and the requeue decision', async () => {
    const clock = new ManualClock(1000)
    const harness = createManager({ clock })
    const boom = new Error('boom')

    await harness.manager.subscribeSequential('orders', async () => {
      clock.jump(15)
      throw boom
    })

    await deliver(harness, { step: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })
    await waitFor(() => harness.channel.nacked.length === 1, 3000, 'sequential message nacked')

    const [failed] = consumerEvents(harness)

    assert.equal(failed.event, 'messageFailed')
    assert.equal(failed.payload.durationMs, 15)
    assert.equal(failed.payload.error, boom)
    assert.equal(failed.payload.requeued, true, "sequential defaults to 'once': first delivery retries")
  })

  test('a parked message reports nothing until its dependency releases it', async () => {
    const gate = Promise.withResolvers()
    const harness = createManager()

    await harness.manager.subscribeSequential('orders', async (content) => {
      if (content.step === 1) await gate.promise
    })

    // The child only parks while its dependency is visibly in flight, so the
    // parent goes first and holds the processing slot at the gate.
    const parent = deliver(harness, { step: 1 }, { messageId: 'parent', headers: { 'x-compressed': false } })

    await deliver(harness, { step: 2 }, {
      messageId: 'child',
      headers: { 'x-compressed': false, 'depends-on': 'parent' }
    })

    assert.deepEqual(consumerEvents(harness), [], 'nothing settled, nothing reported')

    gate.resolve()
    await parent
    await waitFor(() => harness.channel.acked.length === 2, 3000, 'parent and child acked')

    assert.deepEqual(
      consumerEvents(harness).map(({ event, payload }) => [event, payload.messageId]),
      [['messageProcessed', 'parent'], ['messageProcessed', 'child']]
    )
  })

  test('a duplicate delivery of a parked message is acked but never reported twice', async () => {
    const gate = Promise.withResolvers()
    const harness = createManager()

    await harness.manager.subscribeSequential('orders', async (content) => {
      if (content.step === 1) await gate.promise
    })

    const parent = deliver(harness, { step: 1 }, { messageId: 'parent', headers: { 'x-compressed': false } })

    await deliver(harness, { step: 2 }, {
      messageId: 'child',
      headers: { 'x-compressed': false, 'depends-on': 'parent' }
    })
    await deliver(harness, { step: 2 }, {
      messageId: 'child',
      headers: { 'x-compressed': false, 'depends-on': 'parent' }
    })

    assert.equal(harness.channel.acked.length, 1, 'the duplicate itself was acked')
    assert.deepEqual(consumerEvents(harness), [], 'an acked duplicate is not a processed message')

    gate.resolve()
    await parent
    await waitFor(() => harness.channel.acked.length === 3, 3000, 'parent, duplicate and child acked')

    assert.deepEqual(
      consumerEvents(harness).map(({ event, payload }) => [event, payload.messageId]),
      [['messageProcessed', 'parent'], ['messageProcessed', 'child']],
      'the child completes once, whatever the broker redelivered'
    )
  })

  test('a dependency that never resolves reports a failure with no duration', async () => {
    const clock = new ManualClock()
    const harness = createManager({ clock })

    await harness.manager.subscribeSequential('orders', async (content) => {
      if (content.step === 1) await new Promise(() => {})
    }, { staleTimeout: 100 })

    const parent = deliver(harness, { step: 1 }, { messageId: 'parent', headers: { 'x-compressed': false } })

    await deliver(harness, { step: 2 }, {
      messageId: 'child',
      headers: { 'x-compressed': false, 'depends-on': 'parent' }
    })

    clock.advance(201)
    await waitFor(() => harness.channel.nacked.length === 1, 3000, 'expired child nacked')

    const [failed] = consumerEvents(harness)

    assert.equal(failed.event, 'messageFailed')
    assert.equal(failed.payload.messageId, 'child')
    assert.equal(failed.payload.durationMs, undefined, 'it never ran, so there is no duration')
    assert.equal(failed.payload.requeued, true, "'once' gives the dependency a second chance to arrive")

    parent.catch(() => {})
  })

  test('a worker failure on the parallel path reports messageFailed', async (t) => {
    const harness = createManager()

    await harness.manager.subscribeParallel('orders', FLAKY_WORKER, { workerCount: 1 })
    t.after(() => harness.manager.disposeAll())

    await deliver(harness, { shouldFail: true }, { messageId: 'm1', headers: { 'x-compressed': false } })
    await waitFor(() => harness.channel.nacked.length === 1, 5000, 'worker failure nacked')

    const [failed] = consumerEvents(harness)

    assert.equal(failed.event, 'messageFailed')
    assert.equal(failed.payload.queue, 'orders')
    assert.equal(failed.payload.requeued, false)
  })
})

describe('ConsumerManager recovery ownership fence', () => {
  test('recovery yields when the channel pool changed while it was backing off', async () => {
    // The window recreateAll cannot see: a recovery wake landing after the
    // new pool is installed but before recreateAll reaches this consumer.
    // Without the pool-identity fence the recovery would consume the queue a
    // second time — the epoch check alone cannot catch a recreation that has
    // not happened yet.
    const harness = createManager()
    const cancelled = []
    const recovered = []

    harness.manager.emit = (event) => {
      if (event === 'consumerCancelled') cancelled.push(event)
      if (event === 'consumerRecovered') recovered.push(event)
    }

    await harness.manager.subscribe('orders', async () => {})

    const consumesBefore = harness.channel.consumers.length

    // The dedicated channel dies while the pool is still the one that lost
    // it; the watcher hands the loss to the recovery loop.
    harness.channel.emit('close')
    await waitFor(() => cancelled.length === 1, 3000, 'recovery started')

    // A reconnection installs a NEW pool while the recovery sleeps.
    const freshChannel = new (harness.channel.constructor)()

    harness.manager.getChannelPool = () => ({
      getDedicatedChannel: async () => freshChannel,
      releaseDedicatedChannel: async () => {}
    })

    // Let every backoff attempt elapse (20ms base -> 20+40+60).
    await sleep(200)

    assert.deepEqual(recovered, [], 'recovery must not act on a pool it does not own')
    assert.equal(harness.channel.consumers.length, consumesBefore, 'no consume on the dead pool')
    assert.equal(freshChannel.consumers.length, 0, 'no duplicate consume on the new pool — that setup belongs to recreateAll')
    assert.equal(harness.manager.activeConsumers.size, 1, 'the consumer stays registered for recreateAll to restore')
  })
})

describe('ConsumerManager event reporting never interferes with settlement', () => {
  test('a throwing messageFailed listener cannot leave the delivery unsettled', async () => {
    const logger = recordingLogger()
    const harness = createManager({
      logger,
      emit: (event) => {
        if (event === 'messageFailed') throw new Error('listener boom')
      }
    })

    await harness.manager.subscribe('orders', async () => {
      throw new Error('handler exploded')
    }, { retryPolicy: 'once' })

    await deliver(harness, { id: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })

    assert.deepEqual(
      harness.channel.nacked.map(n => n.requeue),
      [true],
      'the nack happened before the listener could interfere'
    )
    assert.ok(
      logger.records.error.some(line => line.includes("'messageFailed' listener threw") && line.includes('listener boom')),
      'the listener crash is reported as the listener bug it is'
    )
  })

  test('a throwing messageProcessed listener cannot turn a success into a failure', async () => {
    const logger = recordingLogger()
    const emitted = []
    const harness = createManager({
      logger,
      emit: (event) => {
        emitted.push(event)

        if (event === 'messageProcessed') throw new Error('metrics down')
      }
    })

    await harness.manager.subscribe('orders', async () => {})
    await deliver(harness, { id: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })

    assert.equal(harness.channel.acked.length, 1, 'the ack stands')
    assert.equal(harness.channel.nacked.length, 0)
    assert.ok(!emitted.includes('messageFailed'), 'no spurious failure report for an acked message')
    assert.ok(logger.records.error.some(line => line.includes("'messageProcessed' listener threw")))
  })

  test('the sequential path contains a throwing listener the same way', async () => {
    const logger = recordingLogger()
    const harness = createManager({
      logger,
      emit: (event) => {
        if (event === 'messageProcessed') throw new Error('listener boom')
      }
    })

    await harness.manager.subscribeSequential('orders', async () => {})

    await deliver(harness, { step: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })
    await waitFor(() => harness.channel.acked.length === 1, 3000, 'sequential message acked')

    assert.ok(logger.records.error.some(line => line.includes("'messageProcessed' listener threw")))
  })

  test('no listener, no payload: events are skipped entirely when nobody subscribed', async () => {
    // Arg-sensitive on purpose: the guard must ask about THIS event's
    // listeners, not about listeners in general.
    const harness = createManager({
      listenerCount: (event) => (event === 'messageProcessed' || event === 'messageFailed') ? 0 : 1
    })

    await harness.manager.subscribe('orders', async (content) => {
      if (content.explode) throw new Error('boom')
    })

    await deliver(harness, { id: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })
    await deliver(harness, { explode: true }, { messageId: 'm2', headers: { 'x-compressed': false } })

    assert.deepEqual(harness.events, [], 'emit was never called, success and failure alike')
    assert.equal(harness.channel.acked.length, 1, 'the ack is unaffected')
    assert.equal(harness.channel.nacked.length, 1, 'the nack is unaffected')
  })
})

describe('ConsumerManager unsubscribe drains in-flight handlers', () => {
  test('the dedicated channel outlives a handler that is still running', async () => {
    const gate = Promise.withResolvers()
    const logger = recordingLogger()
    const harness = createManager({ logger })
    let delivered

    await harness.manager.subscribe('orders', async (content, msg) => {
      delivered = msg
      await gate.promise
    })

    const tag = harness.channel.consumers.at(-1).consumerTag
    const inFlight = deliver(harness, { id: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })

    await waitFor(() => delivered, 3000, 'the handler is inside the pipeline')

    const unsubscribing = harness.manager.unsubscribe(tag)

    // The cancel reached the broker, but the channel must stay open while the
    // handler runs — closing it now would kill the ack and force a
    // redelivery of work that succeeds.
    await sleep(30)
    assert.deepEqual(harness.channelPool.released, [], 'the channel is still open')
    assert.equal(harness.channel.acked.length, 0, 'the handler has not finished yet')

    gate.resolve()
    await inFlight
    assert.equal(await unsubscribing, true)

    assert.equal(harness.channel.acked.length, 1, 'the late ack landed on the still-open channel')
    assert.equal(delivered.__ackSettled, true)
    assert.equal(harness.channelPool.released.length, 1, 'the channel closed only after the drain')
    assert.ok(
      !logger.records.warn.some(line => line.includes('in flight')),
      'a drain that completed is not reported as a forced close'
    )
  })

  test('one finished handler does not end the drain while another still runs', async () => {
    const gates = [Promise.withResolvers(), Promise.withResolvers()]
    const harness = createManager()
    let started = 0

    await harness.manager.subscribe('orders', async (content) => {
      started++
      await gates[content.slot].promise
    })

    const tag = harness.channel.consumers.at(-1).consumerTag
    const first = deliver(harness, { slot: 0 }, { headers: { 'x-compressed': false } })
    const second = deliver(harness, { slot: 1 }, { headers: { 'x-compressed': false } })

    await waitFor(() => started === 2, 3000, 'both handlers are inside the pipeline')

    const unsubscribing = harness.manager.unsubscribe(tag)

    gates[0].resolve()
    await first
    await sleep(20)

    assert.deepEqual(harness.channelPool.released, [], 'one completion must not release the channel under the other handler')

    gates[1].resolve()
    await second
    await unsubscribing

    assert.equal(harness.channel.acked.length, 2, 'both acks landed before the close')
    assert.equal(harness.channelPool.released.length, 1)
  })

  test('a wedged handler cannot hang unsubscribe forever: the grace period bounds the drain', async () => {
    const logger = recordingLogger()
    const harness = createManager({ logger, consumerDrainTimeout: 25 })
    let entered = false

    await harness.manager.subscribe('orders', async () => {
      entered = true
      await new Promise(() => {})
    })

    const tag = harness.channel.consumers.at(-1).consumerTag

    deliver(harness, { id: 1 }, { messageId: 'm1', headers: { 'x-compressed': false } })
    await waitFor(() => entered, 3000, 'the wedged handler is inside the pipeline')

    // The grace period rides an unref'd clock.sleep; Node 22 cancels a test
    // whose only pending work is an unref'd timer.
    const result = await withLiveEventLoop(() => harness.manager.unsubscribe(tag))

    assert.equal(result, true, 'unsubscribe returns after the grace period')
    assert.equal(harness.channelPool.released.length, 1, 'the channel was closed anyway')
    assert.ok(
      logger.records.warn.some(line => line.includes('in flight after 25ms')),
      'the forced close is reported, not silent'
    )
  })
})

describe('ConsumerManager tag bookkeeping across consumers', () => {
  test('dropping one consumer leaves the other one reachable by its tag', async () => {
    // #dropConsumer sweeps consumersByTag for the dropped consumer's tags —
    // sweeping indiscriminately would orphan every OTHER consumer's tag too,
    // making them impossible to unsubscribe.
    const harness = createManager()

    const orders = await harness.manager.subscribe('orders', async () => {})
    const billing = await harness.manager.subscribe('billing', async () => {})

    assert.equal(await harness.manager.unsubscribe(orders.consumerTag), true)

    assert.equal(
      harness.manager.findQueueNameByTag(billing.consumerTag),
      'billing',
      'the surviving consumer still answers to its tag'
    )
    assert.equal(await harness.manager.unsubscribe(billing.consumerTag), true, 'and can still be unsubscribed')
  })
})

describe('ConsumerManager listener containment', () => {
  test('a throwing lifecycle listener neither aborts recovery nor escapes the detached path', async () => {
    // consumerCancelled/consumerRecovered/consumerLost used to be raw emits on
    // paths nothing awaits (a channel 'close' handler, amqplib's null
    // delivery), so an application listener that threw did not merely skip a
    // notification: it abandoned the recovery midway AND surfaced as an
    // unhandled rejection that took the process down.
    const logger = recordingLogger()
    const events = []
    const harness = createManager({
      logger,
      emit: (event, payload) => {
        events.push({ event, payload })

        if (event === 'consumerCancelled') throw new Error('listener exploded')
      }
    })

    await harness.manager.subscribe('orders', async () => {})

    await harness.channel.consumers[0].callback(null)

    await waitFor(() => events.some(e => e.event === 'consumerRecovered'), 3000, 'recovery outlives the listener')

    assert.equal(harness.channel.consumers.length, 2, 'the consumer was recreated despite the crash')
    assert.ok(
      logger.records.error.some(line => line.includes("'consumerCancelled' listener threw")),
      'the listener bug is reported as such, not as a consumer failure'
    )
  })

  test('a throwing consumerLost listener still leaves the consumer fully dropped', async () => {
    const logger = recordingLogger()
    const events = []
    const harness = createManager({
      logger,
      emit: (event, payload) => {
        events.push({ event, payload })

        if (event === 'consumerLost') throw new Error('listener exploded')
      }
    })

    await harness.manager.subscribe('orders', async () => {})

    harness.channel.consumeError = new Error('NOT_FOUND - no queue "orders"')

    await harness.channel.consumers[0].callback(null)

    await waitFor(() => events.some(e => e.event === 'consumerLost'), 3000, 'consumerLost')

    assert.equal(harness.manager.activeConsumers.size, 0, 'the drop completed before the notification')
    assert.ok(logger.records.error.some(line => line.includes("'consumerLost' listener threw")))
  })
})

describe('ConsumerManager cancellation during channel setup', () => {
  test('a consumer cancelled while its channel opens never reaches channel.consume', async () => {
    // Opening a channel is a broker round trip, and every RECREATION runs the
    // setup closure while the caller already holds an unsubscribable tag.
    // Without a re-check after the await, the consume below issued a live
    // broker consumer on a channel nobody tracked: every delivery then threw
    // on the missing consumerInfo and was never settled, so the queue filled
    // with unacked messages until the connection dropped.
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)
    const consumersAfterSubscribe = harness.channel.consumers.length

    // Hold the next handout open: this is the window.
    const gate = Promise.withResolvers()

    harness.channelPool.getDedicatedChannel = async () => {
      await gate.promise

      return harness.channel
    }

    const recreation = harness.manager.recreateAll()

    await harness.manager.unsubscribe(consumer.consumerTag)

    gate.resolve()
    await recreation

    assert.equal(
      harness.channel.consumers.length,
      consumersAfterSubscribe,
      'no consume may be issued for a consumer that is already gone'
    )
    assert.deepEqual(
      harness.channelPool.released,
      [consumerId, consumerId],
      'the channel reopened after the drop goes back too, or it leaks'
    )
  })
})

describe('ConsumerManager dedicated channel ownership', () => {
  test('giving up on a lost consumer releases its dedicated channel', async () => {
    // The release used to live in unsubscribe() alone, so the other two ways a
    // consumer goes away each leaked one channel toward channel_max.
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)

    harness.channel.consumeError = new Error('NOT_FOUND - no queue "orders"')

    await harness.channel.consumers[0].callback(null)

    await waitFor(() => harness.events.some(e => e.event === 'consumerLost'), 3000, 'consumerLost')

    assert.deepEqual(harness.channelPool.released, [consumerId], 'a consumer that is gone must not keep a channel')
  })

  test('a subscribe that fails during setup releases the channel it opened', async () => {
    const harness = createManager()

    harness.channel.consumeError = new Error('ACCESS_REFUSED - refused')

    await assert.rejects(() => harness.manager.subscribe('orders', async () => {}), /ACCESS_REFUSED/)

    assert.equal(harness.channelPool.released.length, 1, 'a failed subscribe must not leak its channel')
  })
})

describe('ConsumerManager detached recovery failures', () => {
  test('a recovery that fails on a detached path is reported, not thrown into the void', async () => {
    // handleConsumerLoss is kicked off from places that cannot await it, so an
    // unexpected rejection inside it — here the clock seam the backoff sleeps
    // on — used to become an unhandled rejection and take the process down
    // over a consumer that was already lost.
    const logger = recordingLogger()
    const clock = {
      now: () => 1000,
      sleep: async () => { throw new Error('clock seam exploded') }
    }
    const harness = createManager({ logger, clock })

    await harness.manager.subscribe('orders', async () => {})

    // The detached entry point: amqplib delivers null on a broker cancel.
    await harness.channel.consumers[0].callback(null)

    await waitFor(
      () => logger.records.error.some(line => line.includes('Consumer recovery failed unexpectedly')),
      3000,
      'the detached failure was reported'
    )
  })
})

describe('ConsumerManager channel ownership during a cancelled setup', () => {
  test('an unsubscribe still in flight keeps the channel the guard must not release', async () => {
    // Two ways to be cancelled, two owners. A consumer already DROPPED left
    // nobody to close the channel the setup just reopened, so the guard closes
    // it; a consumer merely FLAGGED cancelled has an unsubscribe parked on
    // that very channel, waiting to cancel and drain on it — pulling it out
    // from under that call would kill the acks it is draining for.
    const harness = createManager()

    const consumer = await harness.manager.subscribe('orders', async () => {})
    const consumerId = harness.manager.findConsumerIdByTag(consumer.consumerTag)

    const channelReady = Promise.withResolvers()
    const cancelReached = Promise.withResolvers()
    const cancelRelease = Promise.withResolvers()

    harness.channelPool.getDedicatedChannel = async () => {
      await channelReady.promise

      return harness.channel
    }

    harness.channel.cancel = async () => {
      cancelReached.resolve()

      await cancelRelease.promise
    }

    const recreation = harness.manager.recreateAll()
    const unsubscribing = harness.manager.unsubscribe(consumer.consumerTag)

    // unsubscribe has flagged the consumer and is parked on the broker's
    // cancel: still registered, so the guard takes the other branch.
    await cancelReached.promise

    channelReady.resolve()
    await recreation

    assert.deepEqual(harness.channelPool.released, [], 'the channel unsubscribe is still using must survive the guard')

    cancelRelease.resolve()
    await unsubscribing

    assert.deepEqual(harness.channelPool.released, [consumerId], 'and unsubscribe releases it exactly once')
  })

  test('the guard tolerates the pool vanishing while the channel was opening', async () => {
    // Reaching the guard means a channel WAS handed out, but the connection can
    // drop before the guard runs — the release has to survive a pool that is
    // already gone, and still report the cancellation rather than a TypeError.
    const logger = recordingLogger()
    let livePool = null
    const harness = createManager({ logger, getChannelPool: () => livePool })

    livePool = harness.channelPool

    const consumer = await harness.manager.subscribe('orders', async () => {})

    const channelReady = Promise.withResolvers()

    harness.channelPool.getDedicatedChannel = async () => {
      await channelReady.promise

      return harness.channel
    }

    const recreation = harness.manager.recreateAll()

    await harness.manager.unsubscribe(consumer.consumerTag)

    // The connection drops while the recreation is still waiting for a channel.
    livePool = null

    channelReady.resolve()
    await recreation

    assert.ok(
      logger.records.error.some(line => line.includes('orders') && line.includes('cancelled before its channel was ready')),
      'the operator is told which consumer was abandoned and why'
    )
  })
})

describe('ConsumerManager recovery give-up ownership', () => {
  test('a last attempt that fails because the pool changed leaves the consumer to recreateAll', async () => {
    // The fences ran before every attempt but not before the final drop: a
    // connection dropping under the LAST attempt made handleConsumerLoss remove
    // a consumer the reconnection's recreateAll was about to restore — the
    // queue went silent for good behind a healthy reconnect.
    let livePool = null
    const harness = createManager({ getChannelPool: () => livePool })

    livePool = harness.channelPool

    await harness.manager.subscribe('orders', async () => {})

    let attempts = 0

    harness.channel.consume = async () => {
      // The connection turns over under the third and final attempt.
      if (++attempts === 3) livePool = { getDedicatedChannel: async () => harness.channel, releaseDedicatedChannel: async () => {} }

      throw new Error('NOT_FOUND - no queue "orders"')
    }

    await harness.channel.consumers[0].callback(null)

    // Long enough for all three attempts (20+40+60ms) and the give-up decision.
    await sleep(250)

    assert.equal(attempts, 3, 'the budget was spent')
    assert.equal(harness.manager.activeConsumers.size, 1, 'the consumer stays registered for recreateAll to restore')
    assert.ok(!harness.events.some(e => e.event === 'consumerLost'), 'and is not reported lost')
  })

  test('giving up on a lost consumer waits for in-flight handlers before closing their channel', async () => {
    // The drain used to belong to unsubscribe alone: recovery giving up closed
    // the channel under handlers still running, their acks died with it, and
    // the broker redelivered work that had actually completed.
    const harness = createManager({ consumerRecoveryInterval: 10 })
    const handlerGate = Promise.withResolvers()
    const entered = Promise.withResolvers()

    await harness.manager.subscribe('orders', async () => {
      entered.resolve()

      await handlerGate.promise
    })

    const consumerId = harness.manager.findConsumerIdByTag(harness.channel.consumers[0].consumerTag)
    const delivery = deliver(harness, { n: 1 })

    await entered.promise

    harness.channel.consumeError = new Error('NOT_FOUND - no queue "orders"')

    await harness.channel.consumers[0].callback(null)

    await waitFor(
      () => harness.manager.activeConsumers.get(consumerId)?.drainWaiters.length === 1,
      3000,
      'the drop parks on the running handler'
    )

    assert.deepEqual(harness.channelPool.released, [], 'the channel must outlive the handler running on it')

    handlerGate.resolve()
    await delivery

    await waitFor(() => harness.channelPool.released.length === 1, 3000, 'released once the handler finished')

    assert.equal(harness.channel.acked.length, 1, 'and its ack landed on a live channel')
    assert.ok(harness.events.some(e => e.event === 'consumerLost'))
  })
})

describe('ConsumerManager cancellation while consume is in flight', () => {
  test('an unsubscribe that completes during the consume leaves no stale tag behind', async () => {
    // The first fence runs before prefetch/consume — two more round trips. An
    // unsubscribe finishing #dropConsumer inside them had already swept the
    // tags and released the channel; tracking the new tag then registered it
    // for a consumer that no longer existed (findQueueNameByTag threw on
    // undefined) while the broker kept delivering to an orphaned callback.
    const harness = createManager()
    const consumer = await harness.manager.subscribe('orders', async () => {})

    const gate = Promise.withResolvers()
    const consumeEntered = Promise.withResolvers()
    const realConsume = harness.channel.consume.bind(harness.channel)

    harness.channel.consume = async (...args) => {
      consumeEntered.resolve()

      await gate.promise

      return realConsume(...args)
    }

    const recreation = harness.manager.recreateAll()

    // The recreation must be PAST the pre-consume fence and parked inside the
    // broker round trip before unsubscribe flags the consumer — unsubscribe
    // sets `cancelled` synchronously, so calling it any earlier trips the first
    // fence instead and never reaches the race this test is about.
    await consumeEntered.promise

    await harness.manager.unsubscribe(consumer.consumerTag)

    gate.resolve()
    await recreation

    const newTag = harness.channel.consumers.at(-1).consumerTag

    assert.equal(harness.manager.findConsumerIdByTag(newTag), null, 'no tag registered for a consumer that is gone')
    assert.ok(harness.channel.cancelled.includes(newTag), 'the consume issued for it was cancelled at the broker')
    assert.equal(harness.manager.consumersByTag.size, 0)
  })

  test('subscribing while disconnected fails with code NOT_CONNECTED', async () => {
    const { manager } = createManager({ getChannelPool: () => null })

    await assert.rejects(() => manager.subscribe('queue', async () => {}), { code: 'NOT_CONNECTED' })
  })
})

describe('ConsumerManager recovery fences during the backoff', () => {
  test('an unsubscribe during the backoff stops the recovery cold', async () => {
    // The consumer is still registered while unsubscribe is parked on the
    // broker's cancel, so presence alone cannot tell; the `cancelled` flag is
    // what the fence must read. Recreating here would issue a consume for a
    // consumer being torn down.
    const harness = createManager({ consumerRecoveryInterval: 30 })
    const consumer = await harness.manager.subscribe('orders', async () => {})
    const cancelRelease = Promise.withResolvers()

    harness.channel.cancel = async () => { await cancelRelease.promise }

    // Recovery starts backing off (first attempt at 30ms).
    await harness.channel.consumers[0].callback(null)

    const unsubscribing = harness.manager.unsubscribe(consumer.consumerTag)

    // Well past every attempt (30+60+90ms), with the consumer still present.
    await sleep(250)

    assert.equal(harness.channel.consumers.length, 1, 'no consume was issued for a consumer being torn down')
    assert.ok(!harness.events.some(e => e.event === 'consumerRecovered'))

    cancelRelease.resolve()
    await unsubscribing

    // The setup closure's own fence would have refused the consume anyway; what
    // the LOOP's fence prevents is the give-up path running for a consumer that
    // unsubscribe owns — dropping it a second time and announcing it lost.
    assert.ok(!harness.events.some(e => e.event === 'consumerLost'), 'an unsubscribe is not a loss')
    assert.equal(harness.channelPool.released.length, 1, 'released exactly once, by unsubscribe')
  })
})

describe('ConsumerManager post-consume fence, cancelled but still present', () => {
  test('an unsubscribe parked on the broker cancel still gets the in-flight consume cancelled', async () => {
    // Between the two fences the consumer can be flagged without being dropped
    // yet (unsubscribe is awaiting the OLD tag's cancel). The consume that just
    // landed for it must be cancelled at the broker too — nobody else will,
    // and the tag sweep later only removes what was tracked.
    const logger = recordingLogger()
    const harness = createManager({ logger })
    const consumer = await harness.manager.subscribe('orders', async () => {})

    const consumeGate = Promise.withResolvers()
    const consumeEntered = Promise.withResolvers()
    const cancelReached = Promise.withResolvers()
    const cancelRelease = Promise.withResolvers()
    const realConsume = harness.channel.consume.bind(harness.channel)
    const realCancel = harness.channel.cancel.bind(harness.channel)

    harness.channel.consume = async (...args) => {
      consumeEntered.resolve()

      await consumeGate.promise

      return realConsume(...args)
    }
    harness.channel.cancel = async (tag) => {
      await realCancel(tag)

      if (tag === consumer.consumerTag) {
        cancelReached.resolve()

        await cancelRelease.promise
      }
    }

    const recreation = harness.manager.recreateAll()

    await consumeEntered.promise

    const unsubscribing = harness.manager.unsubscribe(consumer.consumerTag)

    await cancelReached.promise

    consumeGate.resolve()
    await recreation

    const newTag = harness.channel.consumers.at(-1).consumerTag

    assert.ok(harness.channel.cancelled.includes(newTag), 'the just-issued consume was cancelled at the broker')
    assert.equal(harness.manager.findConsumerIdByTag(newTag), null, 'and never tracked')
    assert.ok(
      logger.records.error.some(line => line.includes('orders') && line.includes('cancelled while its consume was in flight')),
      'the operator is told which consumer and why'
    )

    cancelRelease.resolve()
    await unsubscribing
  })
})
