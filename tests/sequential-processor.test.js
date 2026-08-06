import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import { ManualClock } from './helpers.js'
import SequentialProcessor from '../src/consumers/sequential-processor.js'

const silentLogger = { info: () => {}, warn: () => {}, error: () => {} }

// Real sleeps here are synchronization only (letting a parked handle() settle);
// everything clock-domain — staleness, the sweep — drives a ManualClock, so no
// test waits out a timeout for real.
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

  test('a handler that throws a non-Error is still settled and reported (issue #18)', async (t) => {
    // Reading error.message on `throw null` used to crash this catch before
    // onFailure ran, leaving the message unacknowledged with no redelivery.
    const errors = []
    const { processor, nacked } = createProcessor(t, {
      callback: async () => {
        throw null // eslint-disable-line no-throw-literal
      },
      options: { logger: { ...silentLogger, error: (line) => errors.push(line) } }
    })

    await processor.handle({}, makeMessage('poison'))

    assert.equal(nacked.length, 1, 'the failure reached onFailure')
    assert.ok(errors.some(line => line.includes('null')), 'the log renders the thrown value')
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
    const clock = new ManualClock()

    const { processor, nacked } = createProcessor(t, {
      staleTimeout: 80,
      callback: async (content) => {
        if (content.hold) await firstBlocked
      },
      options: {
        clock,
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

    // The sweep fires at 80 (entries aged exactly 80 — not stale yet) and at
    // 160, where both pending messages are past the timeout.
    clock.advance(160)

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

  test('a pending message aged exactly staleTimeout is not expired yet', async (t) => {
    // The expiry rule is strictly greater-than: at exactly staleTimeout the
    // dependency still has this instant to arrive. The clock is seeded away
    // from zero so age must be computed from queuedAt — a hardcoded origin
    // would expire the entry on the very first sweep.
    let releaseFirst
    const firstBlocked = new Promise(resolve => { releaseFirst = resolve })
    const clock = new ManualClock(1000)

    const { processor, nacked } = createProcessor(t, {
      staleTimeout: 100,
      callback: async (content) => {
        if (content.hold) await firstBlocked
      },
      options: { clock }
    })

    const first = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({}, makeMessage('child', { dependsOn: 'dep' }))

    clock.advance(100)

    assert.equal(processor.pending.size, 1, 'age == staleTimeout is still within the deadline')
    assert.deepEqual(nacked, [])

    clock.advance(100)

    assert.equal(processor.pending.size, 0, 'the next sweep finds it past the deadline')
    assert.equal(nacked.length, 1)

    releaseFirst()
    await first
  })

  test('removes a stale processing entry without releasing its dependents', async (t) => {
    // A stale processing entry is bookkeeping only: the dependency never
    // actually completed, so its dependents must expire under the pending
    // rule instead of being processed as if it had succeeded.
    const warnings = []
    const clock = new ManualClock(1000)

    const { processor, nacked, acked } = createProcessor(t, {
      staleTimeout: 100,
      callback: async (content) => {
        if (content.hold) await new Promise(() => {})
      },
      options: { clock, logger: { ...silentLogger, warn: (line) => warnings.push(line) } }
    })

    processor.handle({ hold: true }, makeMessage('wedged'))

    await sleep(20)
    await processor.handle({}, makeMessage('child', { dependsOn: 'wedged' }))

    assert.equal(processor.processing.size, 1)

    clock.advance(100)

    assert.equal(processor.processing.size, 1, 'wedged for exactly staleTimeout is not stale yet')

    clock.advance(100)

    assert.equal(processor.processing.size, 0, 'the wedged entry was collected')
    assert.ok(warnings.some(line => line.includes('wedged')), 'and the collection was reported')
    assert.deepEqual(acked, [], 'the dependent was never processed')
    assert.equal(nacked.length, 1, 'it expired under the pending rule')
    assert.equal(nacked[0].messageId, 'child')
  })

  test('reports how long a message took using the clock', async (t) => {
    // Seeded away from zero: the duration must be the delta between two clock
    // reads, and only a nonzero start distinguishes `now - start` from any
    // formula that ignores or misuses the start time.
    const infos = []
    const clock = new ManualClock(1000)

    const { processor, acked } = createProcessor(t, {
      callback: async () => clock.advance(42),
      options: { clock, logger: { ...silentLogger, info: (line) => infos.push(line) } }
    })

    await processor.handle({}, makeMessage('timed'))

    assert.deepEqual(acked, ['timed'])
    assert.ok(infos.some(line => line.includes('in 42ms')), `the elapsed time is the clock delta (got: ${infos.join(' | ')})`)
  })

  test('sweeps every staleTimeout, capped at one minute', (t) => {
    const short = new ManualClock()
    const long = new ManualClock()

    createProcessor(t, { staleTimeout: 500, options: { clock: short } })
    createProcessor(t, { staleTimeout: 120000, options: { clock: long } })

    const intervalOf = (clock) => [...clock.intervals.values()][0].ms

    assert.equal(intervalOf(short), 500)
    assert.equal(intervalOf(long), 60000, 'a huge staleTimeout must not starve the sweep')
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
    const clock = new ManualClock()

    const { processor } = createProcessor(t, {
      staleTimeout: 80,
      callback: async (content) => { if (content.hold) await firstBlocked },
      options: { clock }
    })

    const first = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({}, makeMessage('child', { dependsOn: 'dep' }))

    assert.equal(processor.pendingByDependency.size, 1)

    clock.advance(200)

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
