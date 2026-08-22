import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import RateLimiter from '../src/resilience/rate-limiter.js'
import { ManualClock } from './helpers.js'

// Every time-dependent test injects a ManualClock: elapsed time is
// clock.advance(), the sweep fires inside advance() synchronously, and window
// boundaries are exact numbers instead of races against real timers. The few
// tests that build a limiter without a clock exist to pin the default
// systemClock wiring.
const createLimiter = (options) => {
  const clock = new ManualClock()
  const limiter = new RateLimiter({ ...options, clock })

  return { clock, limiter }
}

describe('RateLimiter', () => {
  describe('token-bucket', () => {
    test('allows requests up to maxRequests and then limits', async (t) => {
      // No injected clock: this also covers the systemClock default path.
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 3, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })

    test('keeps independent buckets per key', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 1, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
      assert.equal(await limiter.checkRateLimit('key-b'), true)
    })

    test('refills tokens in proportion to elapsed time', async (t) => {
      // 10 tokens per 1000ms → 1 token per 100ms.
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 1000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 10), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)

      clock.advance(500)

      assert.equal(limiter.getRemainingTokens('key-a'), 5, 'half the window back → half the tokens back')

      assert.equal(await limiter.checkRateLimit('key-a', 5), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })

    test('refill is capped at the bucket size', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 4, windowMs: 1000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 4), true)

      // Idle for many windows: the balance must stop at maxRequests, not grow.
      clock.advance(10000)

      assert.equal(limiter.getRemainingTokens('key-a'), 4)

      // Spend exactly the cap, then ask for one more: had those 10 idle
      // windows kept accruing, this would be the start of a 40-token balance.
      // (Probing with cost 5 would test nothing about the cap — a cost above
      // capacity is now refused up front, before any balance is consulted.)
      assert.equal(await limiter.checkRateLimit('key-a', 4), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'the cap is real, not just reported')
    })

    test('respects cost', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 5, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 5), true)
      assert.equal(await limiter.checkRateLimit('key-a', 1), false)
    })

    test('a clock stepping backwards never shrinks the balance', async (t) => {
      // NTP corrections can move wall time backwards; a negative refill must
      // be skipped, not applied.
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 1000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 4), true)
      assert.equal(limiter.getRemainingTokens('key-a'), 6)

      clock.currentTime -= 500

      assert.equal(limiter.getRemainingTokens('key-a'), 6, 'the balance is frozen while time is behind')
    })
  })

  describe('leaky-bucket', () => {
    test('accepts requests and rejects when queue occupancy exceeds queueLimit', async (t) => {
      const { limiter } = createLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 60000, queueLimit: 2 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })

    test('keeps independent queues per key', async (t) => {
      const { limiter } = createLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 60000, queueLimit: 1 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
      assert.equal(await limiter.checkRateLimit('key-b'), true)
    })

    test('respects cost in queue occupancy', async (t) => {
      const { limiter } = createLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 60000, queueLimit: 5 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 5), true)
      assert.equal(await limiter.checkRateLimit('key-a', 1), false)
    })

    test('delays each request by the queue occupancy ahead of it', async (t) => {
      // 10 req/1000ms → leak rate of 100ms per unit of queued cost. The
      // ManualClock records requested sleeps instead of waiting them out, so
      // the pacing curve is asserted exactly: 0ms, 100ms, 200ms.
      const { clock, limiter } = createLimiter({ strategy: 'leaky-bucket', maxRequests: 10, windowMs: 1000, queueLimit: 10 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)

      assert.deepEqual(clock.sleeps, [100, 200], 'first request undelayed, then one leak interval per queued unit')
    })

    test('releases capacity after the window slides', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 1000, queueLimit: 1 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)

      // An entry expires strictly after windowMs, not at it.
      clock.advance(1000)
      assert.equal(limiter.getRemainingTokens('key-a'), 0, 'still occupied at exactly windowMs')

      clock.advance(1)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
    })
  })

  describe('fixed-window', () => {
    test('limits within the window', async (t) => {
      const limiter = new RateLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })

    test('a request that exactly fills the window is allowed', async (t) => {
      const limiter = new RateLimiter({ strategy: 'fixed-window', maxRequests: 5, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 5), true, 'cost == remaining must pass')
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'and only then is the window full')
    })

    test('an exhausted key is allowed again once its window rolls over', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 2), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'window is exhausted')

      // Windows are epoch-anchored; the ManualClock starts at 0, so the
      // boundary falls exactly at windowMs.
      clock.advance(1000)

      assert.equal(await limiter.checkRateLimit('key-a'), true, 'the new window starts from zero')
      assert.equal(limiter.getRemainingTokens('key-a'), 1, 'the old count was discarded, not carried over')
    })

    test('the window a request lands in is derived from the clock, not from when the key was first seen', async (t) => {
      // Seeded away from zero and advanced by less than a window: both checks
      // must land in the SAME epoch-anchored window. A broken windowStart
      // formula assigns each instant its own window and never limits anyone.
      const clock = new ManualClock(5000)
      const limiter = new RateLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 1000, clock })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 2), true)

      clock.advance(1)

      assert.equal(limiter.getRemainingTokens('key-a'), 0, 'one tick later is still the same window')
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'so the key is still limited')
    })

    test('a check landing after the boundary but before the sweep resets the counter in place', async (t) => {
      // advance() lets the sweep collect the stale counter first, so the
      // rollover above exercises the missing-entry path. jump() models the
      // real race — a check arriving in the gap between the window boundary
      // and the sweep's turn on the event loop — where the stale counter is
      // still present and must be reset inside check itself.
      const { clock, limiter } = createLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 2), true)

      clock.jump(1000)

      assert.equal(limiter.limiter.counters.has('key-a'), true, 'the sweep has not run yet')
      assert.equal(await limiter.checkRateLimit('key-a'), true, 'the stale counter was reset, not trusted')
      assert.equal(limiter.getRemainingTokens('key-a'), 1)
    })
  })

  describe('sliding-window', () => {
    test('limits within the window and releases after it slides', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'sliding-window', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)

      clock.advance(1001)

      assert.equal(await limiter.checkRateLimit('key-a'), true)
    })

    test('slides gradually: only the entries that aged out are released', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'sliding-window', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)

      clock.advance(600)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)

      // 401ms later the first entry (age 1001ms) is out, the second (age
      // 401ms) is still in: exactly one slot frees up.
      clock.advance(401)
      assert.equal(limiter.getRemainingTokens('key-a'), 1)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })
  })

  describe('blockKey', () => {
    test('blocks a key and unblocks exactly at the expiry instant', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 60000 })
      t.after(() => limiter.dispose())

      const unblocked = []
      limiter.on('key-unblocked', ({ key }) => unblocked.push(key))

      limiter.blockKey('key-a', 500)

      assert.equal(await limiter.checkRateLimit('key-a'), false)
      assert.equal(limiter.getStatus('key-a').isBlocked, true)

      clock.advance(499)
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'one tick early is still blocked')

      clock.advance(1)
      assert.equal(limiter.getStatus('key-a').isBlocked, false, 'expiry is inclusive: now == blockedUntil is unblocked')
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.deepEqual(unblocked, ['key-a'], 'unblocking on the check path is announced')
    })

    test('emits blocked with the exact remaining time', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 60000 })
      t.after(() => limiter.dispose())

      const events = []
      limiter.on('blocked', (payload) => events.push(payload))

      limiter.blockKey('key-a', 1000)
      clock.advance(300)
      await limiter.checkRateLimit('key-a')

      assert.deepEqual(events, [{ key: 'key-a', remainingTime: 700 }])
    })

    test('announces the block with its duration', async (t) => {
      const { limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 60000 })
      t.after(() => limiter.dispose())

      const events = []
      limiter.on('key-blocked', (payload) => events.push(payload))

      limiter.blockKey('key-a', 250)

      assert.deepEqual(events, [{ key: 'key-a', duration: 250 }])
    })

    test('defaults the block duration to windowMs', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 800 })
      t.after(() => limiter.dispose())

      limiter.blockKey('key-a')

      clock.advance(799)
      assert.equal(limiter.getStatus('key-a').isBlocked, true)

      clock.advance(1)
      assert.equal(limiter.getStatus('key-a').isBlocked, false)
    })
  })

  describe('reset', () => {
    test('restores capacity for every strategy', async (t) => {
      // Each strategy owns its store, so each has its own reset to prove.
      const configs = [
        { strategy: 'token-bucket', maxRequests: 1, windowMs: 60000 },
        { strategy: 'leaky-bucket', maxRequests: 100, windowMs: 60000, queueLimit: 1 },
        { strategy: 'fixed-window', maxRequests: 1, windowMs: 60000 },
        { strategy: 'sliding-window', maxRequests: 1, windowMs: 60000 }
      ]

      for (const config of configs) {
        const { limiter } = createLimiter(config)
        t.after(() => limiter.dispose())

        assert.equal(await limiter.checkRateLimit('key-a'), true, `${config.strategy}: first request`)
        assert.equal(await limiter.checkRateLimit('key-a'), false, `${config.strategy}: exhausted`)

        limiter.reset('key-a')

        assert.equal(await limiter.checkRateLimit('key-a'), true, `${config.strategy}: capacity restored`)
      }
    })

    test('restores capacity and clears a block for the key', async (t) => {
      const { limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 1, windowMs: 60000 })
      t.after(() => limiter.dispose())

      const events = []
      limiter.on('reset', (payload) => events.push(payload))

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
      limiter.blockKey('key-a', 60000)

      limiter.reset('key-a')

      assert.equal(limiter.getStatus('key-a').isBlocked, false, 'the block went with the counters')
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.deepEqual(events, [{ key: 'key-a' }])
    })
  })

  describe('strategy validation', () => {
    test('rejects an unknown strategy at construction', () => {
      // Failing closed matters: a typo must not become an unlimited limiter —
      // and it must surface at boot, not on the first limited publish.
      assert.throws(
        () => new RateLimiter({ strategy: 'made-up', maxRequests: 1, windowMs: 60000 }),
        /Unknown rate limiting strategy: made-up/
      )
    })

    test('constructs every documented strategy by name', (t) => {
      for (const strategy of ['token-bucket', 'leaky-bucket', 'fixed-window', 'sliding-window']) {
        const limiter = new RateLimiter({ strategy, maxRequests: 2, windowMs: 60000 })
        t.after(() => limiter.dispose())

        assert.equal(limiter.getStatus('fresh').strategy, strategy)
      }
    })
  })

  describe('cleanup sweep', () => {
    test('unrefs its interval so an undisposed limiter cannot hold the process open', (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      assert.equal(clock.unrefs.length, 1, 'the sweep handle was unref\'d')
    })

    test('tolerates a timer implementation whose handles have no unref', () => {
      // Injectable clocks may hand back bare handles; the guard must probe
      // for unref instead of assuming it (calling undefined would throw).
      const bare = new ManualClock()
      const originalSetInterval = bare.setInterval.bind(bare)

      bare.setInterval = (fn, ms) => {
        const { id } = originalSetInterval(fn, ms)

        return { id }
      }

      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 1000, clock: bare })

      limiter.dispose()
    })

    test('runs every windowMs / 10, capped at one minute', () => {
      const fast = createLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 1000 })
      const slow = createLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 6000000 })

      const intervalOf = ({ clock }) => [...clock.intervals.values()][0].ms

      assert.equal(intervalOf(fast), 100)
      assert.equal(intervalOf(slow), 60000, 'a huge window must not starve the sweep')

      fast.limiter.dispose()
      slow.limiter.dispose()
    })

    test('evicts a token bucket idle for more than two windows, and not before', async (t) => {
      // Seeded away from zero: idleness must be the DELTA from lastRefill —
      // with a zero origin, now - lastRefill and now + lastRefill agree.
      const clock = new ManualClock(10000)
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 1000, clock })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a')
      assert.equal(limiter.limiter.buckets.size, 1)

      // Sweeps fire every 100ms during this advance; at exactly windowMs * 2
      // of idleness the bucket is still legal.
      clock.advance(2000)
      assert.equal(limiter.limiter.buckets.size, 1, 'idle == windowMs * 2 is not stale yet')

      clock.advance(101)
      assert.equal(limiter.limiter.buckets.size, 0, 'past windowMs * 2 the bucket is dead weight')
    })

    test('drops fixed-window counters from previous windows', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a')
      assert.equal(limiter.limiter.counters.size, 1)

      clock.advance(1100)

      assert.equal(limiter.limiter.counters.size, 0, 'a counter from an elapsed window is dead weight')
    })

    test('evicts slid-out sliding-window entries and empty leaky queues', async (t) => {
      const sliding = createLimiter({ strategy: 'sliding-window', maxRequests: 2, windowMs: 1000 })
      const leaky = createLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 1000, queueLimit: 4 })

      t.after(() => {
        sliding.limiter.dispose()
        leaky.limiter.dispose()
      })

      await sliding.limiter.checkRateLimit('key-a')
      await leaky.limiter.checkRateLimit('key-a')

      sliding.clock.advance(1101)
      leaky.clock.advance(1101)

      assert.equal(sliding.limiter.limiter.windows.size, 0)
      assert.equal(leaky.limiter.limiter.windows.size, 0)
      assert.equal(sliding.limiter.getRemainingTokens('key-a'), 2, 'capacity fully released')
      assert.equal(leaky.limiter.getRemainingTokens('key-a'), 4, 'queue fully drained')
    })

    test('releases expired blocks and announces each one', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      const unblocked = []
      limiter.on('key-unblocked', ({ key }) => unblocked.push(key))

      limiter.blockKey('key-a', 150)
      limiter.blockKey('key-b', 60000)

      clock.advance(200)

      assert.deepEqual(unblocked, ['key-a'], 'only the expired block was released')
      assert.equal(limiter.getStatus('key-b').isBlocked, true)
    })

    test('a sweep landing exactly on the expiry instant releases the block', async (t) => {
      // Expiry is inclusive (now >= expiresAt): a block whose deadline falls
      // exactly on a sweep tick is released on that tick, not one tick later.
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      const unblocked = []
      limiter.on('key-unblocked', ({ key }) => unblocked.push(key))

      // Sweeps fire every 100ms; the block expires exactly at the first one.
      limiter.blockKey('key-a', 100)

      clock.advance(100)

      assert.deepEqual(unblocked, ['key-a'], 'the tick at the deadline is enough')
    })
  })

  describe('cleanup leaves live state alone', () => {
    // The sweep tests above prove it collects. These prove it does not
    // over-collect: a sweep that drops a live counter silently resets someone's
    // rate limit and lets the traffic it was holding back through. Each test
    // advances far enough for several sweeps to fire, but not far enough for
    // the state to be legitimately stale.
    test('keeps a fixed-window counter that belongs to the current window', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)

      clock.advance(900)

      assert.equal(limiter.getRemainingTokens('key-a'), 0, 'the count was not reset')
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'so the key is still limited')
    })

    test('keeps sliding-window entries that are still inside the window', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'sliding-window', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)

      clock.advance(900)

      assert.equal(limiter.getRemainingTokens('key-a'), 0)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })

    test('keeps a token bucket that was refilled recently', async (t) => {
      const clock = new ManualClock(10000)
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 1000, clock })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)

      clock.advance(1900)

      assert.equal(limiter.limiter.buckets.size, 1, 'a bucket idle for less than two windows is not collectable')
    })

    test('keeps a leaky-bucket queue that has not drained yet', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 1000, queueLimit: 2 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)

      clock.advance(900)

      assert.equal(limiter.getRemainingTokens('key-a'), 0, 'the queue still counts as occupied')
    })

    test('does not release a block that has not expired', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      const unblocked = []
      limiter.on('key-unblocked', ({ key }) => unblocked.push(key))

      limiter.blockKey('key-a', 60000)

      clock.advance(1000)

      assert.deepEqual(unblocked, [], 'nothing was unblocked early')
      assert.equal(limiter.getStatus('key-a').isBlocked, true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })
  })

  describe('limited event', () => {
    test('is emitted once per rejection with the key and strategy', async (t) => {
      const { limiter } = createLimiter({ strategy: 'fixed-window', maxRequests: 1, windowMs: 60000 })
      t.after(() => limiter.dispose())

      const events = []
      limiter.on('limited', (payload) => events.push(payload))

      await limiter.checkRateLimit('key-a')
      await limiter.checkRateLimit('key-a')
      await limiter.checkRateLimit('key-a')

      assert.deepEqual(events, [
        { key: 'key-a', strategy: 'fixed-window' },
        { key: 'key-a', strategy: 'fixed-window' }
      ])
    })
  })

  describe('getStatus per strategy', () => {
    test('reports remaining capacity for every strategy', async (t) => {
      const limiters = {
        'token-bucket': createLimiter({ strategy: 'token-bucket', maxRequests: 4, windowMs: 60000 }).limiter,
        'leaky-bucket': createLimiter({ strategy: 'leaky-bucket', queueLimit: 4, windowMs: 60000 }).limiter,
        'fixed-window': createLimiter({ strategy: 'fixed-window', maxRequests: 4, windowMs: 60000 }).limiter,
        'sliding-window': createLimiter({ strategy: 'sliding-window', maxRequests: 4, windowMs: 60000 }).limiter
      }

      t.after(() => Object.values(limiters).forEach(limiter => limiter.dispose()))

      for (const [strategy, limiter] of Object.entries(limiters)) {
        // Untouched keys report full capacity...
        assert.equal(limiter.getStatus('fresh').remainingTokens, 4, `${strategy}: fresh key`)

        // ...and consumption is reflected.
        await limiter.checkRateLimit('key-a', 3)
        assert.equal(limiter.getStatus('key-a').remainingTokens, 1, `${strategy}: after consuming 3 of 4`)
      }
    })

    test('a fixed-window counter from an elapsed window reports full capacity again', async (t) => {
      const { clock, limiter } = createLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 1000 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)
      assert.equal(limiter.getStatus('key-a').remainingTokens, 0)

      // jump() keeps the sweep from collecting the entry: this must exercise
      // the stale-window branch inside remaining(), not the missing-entry one.
      clock.jump(1000)

      assert.equal(limiter.limiter.counters.has('key-a'), true, 'the entry is still present')
      assert.equal(limiter.getStatus('key-a').remainingTokens, 2, 'a new window starts fresh')
    })

    test('reports the configured shape and the clock time', async (t) => {
      const { clock, limiter } = createLimiter({
        strategy: 'token-bucket',
        maxRequests: 5,
        windowMs: 60000,
        burstable: true,
        burstLimit: 8
      })
      t.after(() => limiter.dispose())

      clock.advance(1234)
      await limiter.checkRateLimit('key-a', 2)

      assert.deepEqual(limiter.getStatus('key-a'), {
        strategy: 'token-bucket',
        remainingTokens: 6,
        isBlocked: false,
        windowMs: 60000,
        maxRequests: 5,
        burstable: true,
        currentTime: 1234
      })
    })
  })

  describe('defaults', () => {
    test('falls back to the token-bucket strategy', async (t) => {
      const limiter = new RateLimiter({ maxRequests: 2, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(limiter.getStatus('key-a').strategy, 'token-bucket')

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'the default strategy actually limits')
    })
  })

  describe('burstable', () => {
    test('is off by default: the bucket holds exactly maxRequests', async (t) => {
      const { limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 4, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(limiter.burstable, false)
      assert.equal(limiter.getStatus('key-a').remainingTokens, 4)
    })

    test('sizes the bucket to burstLimit when enabled', async (t) => {
      const { limiter } = createLimiter({
        strategy: 'token-bucket',
        maxRequests: 4,
        windowMs: 60000,
        burstable: true,
        burstLimit: 10
      })

      t.after(() => limiter.dispose())

      assert.equal(limiter.getStatus('key-a').burstable, true)

      // The burst is real: more than maxRequests calls go through at once.
      for (let i = 0; i < 10; i++) {
        assert.equal(await limiter.checkRateLimit('key-a'), true, `burst call ${i + 1}`)
      }

      assert.equal(await limiter.checkRateLimit('key-a'), false, 'the burst is still bounded')
    })

    test('defaults burstLimit to one and a half times maxRequests', async (t) => {
      const { limiter } = createLimiter({
        strategy: 'token-bucket',
        maxRequests: 4,
        windowMs: 60000,
        burstable: true
      })

      t.after(() => limiter.dispose())

      // Pinned through the observable surface: the limiter no longer keeps a
      // copy of the strategy's burstLimit.
      assert.equal(limiter.getStatus('key-a').remainingTokens, 6, 'multiplied, not divided')
    })
  })

  describe('dispose', () => {
    test('clears internal state and stops the sweep', async () => {
      const { clock, limiter } = createLimiter({ strategy: 'token-bucket', maxRequests: 1, windowMs: 1000 })

      await limiter.checkRateLimit('key-a')
      limiter.blockKey('key-b', 1000)
      limiter.dispose()

      assert.equal(limiter.limiter.buckets.size, 0)
      assert.equal(limiter.blocked.size, 0)
      assert.equal(clock.intervals.size, 0, 'the sweep interval was cleared, not leaked')
    })

    test('empties every strategy store', async () => {
      // Same reason as the per-strategy reset: each store has its own clear.
      const cases = [
        { config: { strategy: 'leaky-bucket', maxRequests: 100, windowMs: 60000, queueLimit: 2 }, storeOf: (limiter) => limiter.limiter.windows },
        { config: { strategy: 'fixed-window', maxRequests: 2, windowMs: 60000 }, storeOf: (limiter) => limiter.limiter.counters },
        { config: { strategy: 'sliding-window', maxRequests: 2, windowMs: 60000 }, storeOf: (limiter) => limiter.limiter.windows }
      ]

      for (const { config, storeOf } of cases) {
        const { limiter } = createLimiter(config)

        await limiter.checkRateLimit('key-a')
        assert.equal(storeOf(limiter).size, 1, `${config.strategy}: consumption is stored`)

        limiter.dispose()

        assert.equal(storeOf(limiter).size, 0, `${config.strategy}: dispose emptied the store`)
      }
    })
  })
})

describe('RateLimiter unsatisfiable cost', () => {
  // Every strategy tops out at a single limit, so a cost above it can never be
  // admitted however long the caller waits. checkRateLimit answered false
  // forever: publishBatch with more messages than maxRequests failed
  // permanently, reported as an ordinary rate limit that would clear on its
  // own, outside any retry or backoff.
  const capacityCases = [
    { strategy: 'token-bucket', options: { maxRequests: 10 } },
    { strategy: 'fixed-window', options: { maxRequests: 10 } },
    { strategy: 'sliding-window', options: { maxRequests: 10 } },
    // The leaky bucket paces rather than refuses, so its ceiling is queue
    // occupancy: queueLimit is what a cost is measured against, not maxRequests.
    { strategy: 'leaky-bucket', options: { maxRequests: 100, queueLimit: 10 } }
  ]

  for (const { strategy, options } of capacityCases) {
    test(`${strategy} refuses a cost above its capacity instead of stalling forever`, async (t) => {
      const limiter = new RateLimiter({ strategy, windowMs: 1000, ...options })
      t.after(() => limiter.dispose())

      await assert.rejects(
        () => limiter.checkRateLimit('key-a', 11),
        (error) => {
          assert.equal(error.code, 'RATE_LIMIT_COST_UNSATISFIABLE')
          assert.match(error.message, /can never be admitted/)

          return true
        }
      )

      // The refusal is a configuration verdict, not consumption: a cost that
      // does fit must still go through untouched.
      assert.equal(await limiter.checkRateLimit('key-a', 10), true)
    })
  }

  test('a burstable token bucket admits up to its burst limit, not just maxRequests', async (t) => {
    // Capacity has to follow the strategy's real ceiling: reporting maxRequests
    // here would refuse costs the bucket can actually serve.
    const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 1000, burstable: true, burstLimit: 20 })
    t.after(() => limiter.dispose())

    assert.equal(await limiter.checkRateLimit('key-a', 20), true, 'the burst capacity is usable')
    await assert.rejects(() => limiter.checkRateLimit('key-b', 21), /can never be admitted/)
  })
})
