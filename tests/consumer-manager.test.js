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

  test('lowers the prefetch when processing is consistently slow', async () => {
    const harness = createManager()

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {
      // Above the 500ms threshold the optimizer treats the consumer as
      // saturated and backs the prefetch off.
      await sleep(520)
    }, {
      initialPrefetch: 8,
      optimizationInterval: 20,
      decreaseFactor: 0.5,
      minPrefetch: 1
    })

    assert.deepEqual(harness.channel.prefetches, [8])

    await deliver(harness, { n: 1 })
    await sleep(30)
    await deliver(harness, { n: 2 })

    await waitFor(() => harness.channel.prefetches.includes(4), 5000, 'prefetch lowered')
  })

  test('leaves the prefetch alone when the measured pace warrants no change', async () => {
    // Between the two thresholds (100ms fast, 500ms slow) the optimizer must
    // decide on nothing and skip the channel round-trip entirely.
    const harness = createManager()

    await harness.manager.subscribeWithOptimizedPrefetch('orders', async () => {
      await sleep(200)
    }, {
      initialPrefetch: 4,
      optimizationInterval: 20
    })

    await deliver(harness, { n: 1 })
    await sleep(30)
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
