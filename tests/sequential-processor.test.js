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

  test('a sibling expiring mid-release does not derail the release loop', async (t) => {
    // The dependency resolves and its dependents [d1, d2] are being released
    // in order. Processing d1 takes long enough for the sweep to expire d2 —
    // which happens AFTER #processDependents already deleted the dependency's
    // index entry (exercising #removePending's missing-index tolerance) and
    // leaves the loop holding a pendingId whose entry is gone (exercising the
    // continue guard). d1 must still be acked once and d2 dead-lettered once.
    const clock = new ManualClock(1000)
    let releaseDep
    const depBlocked = new Promise(resolve => { releaseDep = resolve })

    const { processor, acked, nacked } = createProcessor(t, {
      staleTimeout: 100,
      callback: async (content) => {
        if (content.hold) await depBlocked
        if (content.slow) clock.advance(250)
      },
      options: { clock }
    })

    const dep = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({ slow: true }, makeMessage('d1', { dependsOn: 'dep' }))
    await processor.handle({}, makeMessage('d2', { dependsOn: 'dep' }))

    releaseDep()
    await dep
    await sleep(30)

    assert.deepEqual(acked, ['dep', 'd1'], 'the slow sibling still succeeded')
    assert.equal(nacked.length, 1, 'the expired sibling was settled exactly once')
    assert.equal(nacked[0].messageId, 'd2')
    assert.equal(processor.pending.size, 0)
    assert.equal(processor.pendingByDependency.size, 0)
  })

  test('releasing a dependency processes every dependent parked under it', async (t) => {
    // The index entry holds a SET of dependents: registering a second one
    // must extend the set, not replace it.
    let releaseDep
    const depBlocked = new Promise(resolve => { releaseDep = resolve })

    const { processor, acked } = createProcessor(t, {
      callback: async (content) => { if (content.hold) await depBlocked }
    })

    const dep = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({}, makeMessage('c1', { dependsOn: 'dep' }))
    await processor.handle({}, makeMessage('c2', { dependsOn: 'dep' }))

    releaseDep()
    await dep
    await sleep(20)

    assert.deepEqual(acked, ['dep', 'c1', 'c2'], 'both dependents were released')
  })

  test('one dependent expiring keeps the index entry alive for its siblings', async (t) => {
    // The expiry path prunes one dependent at a time and may only drop the
    // index entry once the set is empty — dropping it early orphans the
    // siblings: the dependency resolves and nobody is released.
    const clock = new ManualClock(1000)
    let releaseDep
    const depBlocked = new Promise(resolve => { releaseDep = resolve })

    const { processor, acked, nacked } = createProcessor(t, {
      staleTimeout: 100,
      callback: async (content) => { if (content.hold) await depBlocked },
      options: { clock }
    })

    const dep = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({}, makeMessage('early', { dependsOn: 'dep' }))

    // 'early' ages past the deadline while 'late' stays fresh.
    clock.advance(150)
    await processor.handle({}, makeMessage('late', { dependsOn: 'dep' }))
    clock.advance(50)

    assert.equal(nacked.length, 1, 'only the aged dependent expired')
    assert.equal(nacked[0].messageId, 'early')

    releaseDep()
    await dep
    await sleep(20)

    assert.deepEqual(acked, ['dep', 'late'], 'the fresh sibling was still released on resolution')
  })

  test('a handler that outlives staleTimeout and then succeeds is still acked', async (t) => {
    // Regression: the success path used to read startTime from the processing
    // entry, which the sweep had already collected — the TypeError landed in
    // the catch and the SUCCESSFUL message was nacked (requeued under 'once':
    // its side effects ran twice; dead-lettered under 'none').
    const clock = new ManualClock(1000)
    const warnings = []
    const infos = []

    const { processor, acked, nacked } = createProcessor(t, {
      staleTimeout: 100,
      callback: async (content) => {
        if (content.slow) clock.advance(300)
      },
      options: {
        clock,
        logger: { ...silentLogger, warn: (line) => warnings.push(line), info: (line) => infos.push(line) }
      }
    })

    const slow = processor.handle({ slow: true }, makeMessage('slow-but-fine'))

    await sleep(20)
    await processor.handle({}, makeMessage('waiting', { dependsOn: 'slow-but-fine' }))
    await slow
    await sleep(20)

    assert.ok(warnings.some(line => line.includes('slow-but-fine')), 'the sweep collected the wedged entry mid-flight')
    assert.deepEqual(nacked, [], 'nothing was treated as a failure')
    assert.deepEqual(acked, ['slow-but-fine', 'waiting'], 'the late success is acked and still releases its dependents')
    assert.ok(infos.some(line => line.includes('in 300ms')), 'the duration survives the entry being collected')
  })

  test('a duplicate delivery of a parked messageId is acknowledged, not swallowed', async (t) => {
    // Regression: pending.set used to overwrite the stored entry, so the
    // replaced delivery was never acked nor nacked — its prefetch slot was
    // held until the channel died.
    let releaseDep
    const depBlocked = new Promise(resolve => { releaseDep = resolve })
    const settled = []

    const { processor } = createProcessor(t, {
      callback: async (content) => { if (content.hold) await depBlocked },
      options: {
        onSuccess: (message) => settled.push({ kind: 'ack', message }),
        onFailure: (message) => settled.push({ kind: 'nack', message })
      }
    })

    const dep = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)

    const firstDelivery = makeMessage('child', { dependsOn: 'dep' })
    const duplicateDelivery = makeMessage('child', { dependsOn: 'dep' })

    await processor.handle({ n: 1 }, firstDelivery)
    await processor.handle({ n: 1 }, duplicateDelivery)

    assert.deepEqual(settled, [{ kind: 'ack', message: duplicateDelivery }], 'the duplicate is settled immediately')

    releaseDep()
    await dep
    await sleep(20)

    const refs = settled.map(entry => entry.message)

    assert.equal(settled.length, 3, 'every delivery got exactly one settlement')
    assert.ok(refs.includes(firstDelivery), 'the original parked delivery was settled through its dependency')
    assert.ok(settled.every(entry => entry.kind === 'ack'))
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
    const infos = []
    const { processor, acked } = createProcessor(t, {
      callback: async (content) => processed.push(content),
      options: { logger: { ...silentLogger, info: (line) => infos.push(line) } }
    })

    await processor.handle({ n: 1 }, { properties: {}, fields: {} })

    assert.deepEqual(processed, [{ n: 1 }])
    assert.deepEqual(acked, [undefined], 'onSuccess still received the message')
    assert.equal(processor.processing.size, 0, 'nothing was tracked for it')
    assert.ok(
      !infos.some(line => line.includes('Successfully processed')),
      'no success log for an untracked message — "message undefined" is worse than silence'
    )
  })

  test('a failure leaves no processing entry behind', async (t) => {
    const { processor, nacked } = createProcessor(t, {
      callback: async () => { throw new Error('boom') }
    })

    await processor.handle({}, makeMessage('bad'))

    assert.equal(nacked.length, 1)
    assert.equal(processor.processing.size, 0, 'the failed message was untracked by the catch')
  })

  test('works with no logger at all', async (t) => {
    // logger is optional and every call site guards with `?.`. Dropping one
    // guard throws on the first message, so this exercises every logging
    // path: parking a dependent, duplicate delivery, success, failure,
    // releasing the dependent, and both sweep warnings (wedged processing
    // entry and expired pending message).
    let releaseFirst
    const firstBlocked = new Promise(resolve => { releaseFirst = resolve })
    const clock = new ManualClock(1000)
    const settled = []

    const processor = new SequentialProcessor({
      callback: async (content) => {
        if (content.hold) await firstBlocked
        if (content.explode) throw new Error('boom')
      },
      staleTimeout: 100,
      clock,
      // The settlement KIND is what makes a crashed logging line visible:
      // a throw inside #process lands in the catch and turns a success into
      // a failure, which silent no-op callbacks would never surface.
      onSuccess: (message) => settled.push({ kind: 'ack', id: message.properties.messageId }),
      onFailure: (message) => settled.push({ kind: 'nack', id: message.properties.messageId })
    })

    t.after(() => processor.dispose())

    const first = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({}, makeMessage('early', { dependsOn: 'dep' }))
    await processor.handle({}, makeMessage('early', { dependsOn: 'dep' }))
    await processor.handle({ explode: true }, makeMessage('bad'))

    assert.deepEqual(settled, [
      { kind: 'ack', id: 'early' },
      { kind: 'nack', id: 'bad' }
    ], 'the duplicate was acked and the failure nacked, logger-less')

    // Both sweep paths fire logger-less: 'early' expires as a stale pending
    // message while 'dep' is collected as a wedged processing entry.
    clock.advance(300)

    assert.equal(processor.pending.size, 0, 'the pending message expired without a logger')
    assert.equal(processor.processing.size, 0, 'the wedged entry was collected without a logger')
    assert.deepEqual(settled.at(-1), { kind: 'nack', id: 'early' }, 'the expiry settled the parked message')

    // A fresh dependent parked right before the late resolution exercises
    // the logger-less RELEASE path as well.
    await processor.handle({}, makeMessage('late', { dependsOn: 'dep' }))

    // Late success: the wedged handler finally resolves and must still be
    // ACKED — a crashed success log would divert it into the catch — and its
    // dependent must ride out with it.
    releaseFirst()
    await first
    await sleep(20)

    const outcome = Object.fromEntries(settled.filter(e => e.id === 'dep' || e.id === 'late').map(e => [e.id, e.kind]))

    assert.deepEqual(outcome, { dep: 'ack', late: 'ack' }, 'late success and its dependent are acks, not swallowed crashes')
  })

  test('releases a parked dependent without a logger', async (t) => {
    // The release loop has its own logging line; the wedged-sweep test cannot
    // reach it (the sweep collects the dependency entry first, so nothing is
    // parked by the time the handler resolves).
    let releaseDep
    const depBlocked = new Promise(resolve => { releaseDep = resolve })
    const settled = []

    const processor = new SequentialProcessor({
      callback: async (content) => { if (content.hold) await depBlocked },
      onSuccess: (message) => settled.push(message.properties.messageId),
      onFailure: () => {}
    })

    t.after(() => processor.dispose())

    const dep = processor.handle({ hold: true }, makeMessage('dep'))

    await sleep(20)
    await processor.handle({}, makeMessage('child', { dependsOn: 'dep' }))

    releaseDep()
    await dep
    await sleep(20)

    assert.deepEqual(settled, ['dep', 'child'], 'the dependent was released and acked, logger-less')
  })

  test('tolerates a timer implementation whose handles have no unref', () => {
    // Same probe as the rate limiter's: injectable clocks may hand back bare
    // handles, and calling unref unconditionally would throw at construction.
    const bare = new ManualClock()
    const originalSetInterval = bare.setInterval.bind(bare)

    bare.setInterval = (fn, ms) => {
      const { id } = originalSetInterval(fn, ms)

      return { id }
    }

    const processor = new SequentialProcessor({
      callback: async () => {},
      onSuccess: () => {},
      onFailure: () => {},
      clock: bare
    })

    processor.dispose()
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
