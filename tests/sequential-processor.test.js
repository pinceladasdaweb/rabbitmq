import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import SequentialProcessor from '../src/consumers/sequential-processor.js'

const silentLogger = { info: () => {}, warn: () => {}, error: () => {} }

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const makeMessage = (messageId, { dependsOn, redelivered = false } = {}) => ({
  properties: {
    messageId,
    headers: dependsOn ? { 'depends-on': dependsOn } : {}
  },
  fields: { redelivered }
})

const createProcessor = (t, overrides = {}) => {
  const acked = []
  const nacked = []

  const processor = new SequentialProcessor({
    callback: overrides.callback || (async () => {}),
    logger: silentLogger,
    staleTimeout: overrides.staleTimeout || 30000,
    onSuccess: (message) => acked.push(message.properties.messageId),
    onFailure: (message, error, requeue) => nacked.push({ messageId: message.properties.messageId, requeue }),
    ...overrides.options
  })

  t.after(() => processor.dispose())

  return { processor, acked, nacked }
}

describe('SequentialProcessor', () => {
  test('processes independent messages immediately and acks them', async (t) => {
    const { processor, acked } = createProcessor(t)

    await processor.handle({ n: 1 }, makeMessage('m1'))
    await processor.handle({ n: 2 }, makeMessage('m2'))

    assert.deepEqual(acked, ['m1', 'm2'])
  })

  test('parks a message while its dependency is processing and releases it in order', async (t) => {
    const order = []
    let releaseFirst

    const firstBlocked = new Promise(resolve => { releaseFirst = resolve })

    const { processor, acked } = createProcessor(t, {
      callback: async (content) => {
        if (content.step === 1) await firstBlocked

        order.push(content.step)
      }
    })

    const first = processor.handle({ step: 1 }, makeMessage('s1'))

    await sleep(20)
    await processor.handle({ step: 2 }, makeMessage('s2', { dependsOn: 's1' }))

    assert.deepEqual(order, [], 'nothing processed while dependency is blocked')

    releaseFirst()
    await first
    await sleep(20)

    assert.deepEqual(order, [1, 2])
    assert.deepEqual(acked, ['s1', 's2'])
  })

  test('parks a message whose dependency is itself parked (transitive chain)', async (t) => {
    const order = []
    let releaseFirst

    const firstBlocked = new Promise(resolve => { releaseFirst = resolve })

    const { processor } = createProcessor(t, {
      callback: async (content) => {
        if (content.step === 1) await firstBlocked

        order.push(content.step)
      }
    })

    const first = processor.handle({ step: 1 }, makeMessage('c1'))

    await sleep(20)
    await processor.handle({ step: 2 }, makeMessage('c2', { dependsOn: 'c1' }))
    await processor.handle({ step: 3 }, makeMessage('c3', { dependsOn: 'c2' }))

    releaseFirst()
    await first
    await sleep(50)

    assert.deepEqual(order, [1, 2, 3])
  })

  // The retry policy itself lives in ConsumerManager (#shouldRequeue) so both
  // consumption paths share one rule. What this processor owns is delegating
  // to it faithfully — these tests assert the delegation, not the policy.
  // The policy's own behaviour is covered end-to-end in consumer-manager.test.js.
  test('nacks a failing message and does not ack it', async (t) => {
    const consulted = []

    const { processor, acked, nacked } = createProcessor(t, {
      callback: async () => { throw new Error('boom') },
      options: {
        shouldRequeue: (message, error) => {
          consulted.push({ messageId: message.properties.messageId, reason: error.message })

          return true
        }
      }
    })

    await processor.handle({}, makeMessage('bad'))

    assert.deepEqual(acked, [])
    assert.equal(nacked.length, 1)
    assert.equal(nacked[0].messageId, 'bad')
    assert.equal(nacked[0].requeue, true, 'the policy answer is what settles the message')
    assert.deepEqual(consulted, [{ messageId: 'bad', reason: 'boom' }], 'the policy sees the failing message and its error')
  })

  test('a failing message with no messageId is still reported and settled', async (t) => {
    // messageId is optional: dependency tracking needs it, plain processing
    // does not. The failure path must not assume it exists.
    const errors = []
    const { processor, nacked } = createProcessor(t, {
      callback: async () => { throw new Error('boom') },
      options: { logger: { ...silentLogger, error: (line) => errors.push(line) } }
    })

    await processor.handle({}, { properties: {}, fields: {} })

    assert.equal(nacked.length, 1)
    assert.ok(errors.some(line => line.includes('(no messageId)')))
  })

  test('falls back to the default stale timeout when none is given', async (t) => {
    const { processor } = createProcessor(t, { options: { staleTimeout: undefined } })

    assert.equal(processor.staleTimeout, 30000)
  })

  test('settles a failing message with requeue false when the policy refuses', async (t) => {
    const { processor, acked, nacked } = createProcessor(t, {
      callback: async () => { throw new Error('boom') },
      options: { shouldRequeue: () => false }
    })

    await processor.handle({}, makeMessage('bad'))

    assert.deepEqual(acked, [])
    assert.equal(nacked.length, 1)
    assert.equal(nacked[0].requeue, false)
  })

  test('never requeues when built without a policy', async (t) => {
    // Direct construction has no subscription to inherit a policy from, so the
    // safe answer is the one that cannot hot-loop.
    const { processor, nacked } = createProcessor(t, {
      callback: async () => { throw new Error('boom') }
    })

    await processor.handle({}, makeMessage('bad'))

    assert.equal(nacked[0].requeue, false)
  })

  test('settles stale pending messages through the same policy', async (t) => {
    let releaseFirst

    const firstBlocked = new Promise(resolve => { releaseFirst = resolve })
    const consulted = []

    const { processor, nacked } = createProcessor(t, {
      staleTimeout: 80,
      callback: async (content) => {
        if (content.hold) await firstBlocked
      },
      options: {
        // Answers differently per message, so a hardcoded requeue value on the
        // expiry path could not produce this result.
        shouldRequeue: (message, error) => {
          consulted.push({ messageId: message.properties.messageId, reason: error.message })

          return message.properties.messageId === 'fresh'
        }
      }
    })

    const first = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({}, makeMessage('fresh', { dependsOn: 'dep' }))
    await processor.handle({}, makeMessage('retried', { dependsOn: 'dep' }))

    await sleep(200)

    const fresh = nacked.find(entry => entry.messageId === 'fresh')
    const retried = nacked.find(entry => entry.messageId === 'retried')

    assert.ok(fresh, 'fresh pending message expired')
    assert.equal(fresh.requeue, true)
    assert.ok(retried, 'other pending message expired')
    assert.equal(retried.requeue, false)
    assert.deepEqual(
      consulted.map(entry => entry.reason),
      ['Dependency dep was never resolved', 'Dependency dep was never resolved'],
      'the policy receives the dependency failure, not a synthetic error'
    )

    releaseFirst()
    await first
  })

  test('processes and acks a message that carries no messageId', async (t) => {
    // messageId is only needed for dependency tracking. A plain message
    // without one must still run, still be acked, and leave no bookkeeping
    // entry behind — every `if (messageId)` guard has a live path both ways.
    const processed = []
    const { processor, acked } = createProcessor(t, {
      callback: async (content) => processed.push(content)
    })

    await processor.handle({ n: 1 }, { properties: {}, fields: {} })

    assert.deepEqual(processed, [{ n: 1 }])
    assert.deepEqual(acked, [undefined], 'onSuccess still received the message')
    assert.equal(processor.processing.size, 0, 'nothing was tracked for it')
  })

  test('works with no logger at all', async (t) => {
    // logger is optional and every call site guards with `?.`. Dropping one
    // guard throws on the first message, so this exercises all four paths:
    // parking a dependent, success, failure, and releasing the dependent.
    let releaseFirst
    const firstBlocked = new Promise(resolve => { releaseFirst = resolve })

    const processor = new SequentialProcessor({
      callback: async (content) => {
        if (content.hold) await firstBlocked
        if (content.explode) throw new Error('boom')
      },
      staleTimeout: 30000,
      onSuccess: () => {},
      onFailure: () => {}
    })

    t.after(() => processor.dispose())

    const first = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({}, makeMessage('waiting', { dependsOn: 'dep' }))
    await processor.handle({ explode: true }, makeMessage('bad'))

    releaseFirst()
    await first

    assert.equal(processor.pending.size, 0, 'the dependent was released without a logger')
  })

  test('clears the dependency index once a dependency resolves', async (t) => {
    // The secondary index is what makes releasing dependents O(1). If its
    // entries are not removed it grows for every messageId ever seen.
    const { processor, acked } = createProcessor(t)

    let releaseFirst
    const firstBlocked = new Promise(resolve => { releaseFirst = resolve })

    const { processor: gated } = createProcessor(t, {
      callback: async (content) => { if (content.hold) await firstBlocked }
    })

    const first = gated.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await gated.handle({}, makeMessage('child', { dependsOn: 'dep' }))

    assert.equal(gated.pendingByDependency.size, 1, 'the dependent is indexed while it waits')

    releaseFirst()
    await first
    await sleep(20)

    assert.equal(gated.pendingByDependency.size, 0, 'the index entry is dropped, not left behind')
    assert.equal(gated.pending.size, 0)

    // The unrelated processor keeps the helper's t.after cleanup honest.
    await processor.handle({}, makeMessage('solo'))
    assert.deepEqual(acked, ['solo'])
  })

  test('clears the dependency index when a dependent expires instead of resolving', async (t) => {
    // The resolve path deletes the whole index entry at once; the expiry path
    // goes through #removePending and prunes one dependent at a time. Only
    // this path can leave an empty Set behind for every dependency ever seen.
    let releaseFirst
    const firstBlocked = new Promise(resolve => { releaseFirst = resolve })

    const { processor } = createProcessor(t, {
      staleTimeout: 80,
      callback: async (content) => { if (content.hold) await firstBlocked }
    })

    const first = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({}, makeMessage('child', { dependsOn: 'dep' }))

    assert.equal(processor.pendingByDependency.size, 1)

    await sleep(200)

    assert.equal(processor.pending.size, 0, 'the dependent expired')
    assert.equal(processor.pendingByDependency.size, 0, 'and its index entry went with it')

    releaseFirst()
    await first
  })

  test('dispose clears internal state', async (t) => {
    const { processor } = createProcessor(t)

    await processor.handle({}, makeMessage('x'))
    processor.dispose()

    assert.equal(processor.processing.size, 0)
    assert.equal(processor.pending.size, 0)
  })
})
