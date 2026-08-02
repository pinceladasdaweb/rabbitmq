import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import RateLimiter from '../src/resilience/rate-limiter.js'
import { sleep, waitFor } from './helpers.js'

describe('RateLimiter', () => {
  describe('token-bucket', () => {
    test('allows requests up to maxRequests and then limits', async (t) => {
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

    test('refills tokens over time', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 100 })
      t.after(() => limiter.dispose())

      for (let i = 0; i < 10; i++) {
        assert.equal(await limiter.checkRateLimit('key-a'), true)
      }

      assert.equal(await limiter.checkRateLimit('key-a'), false)

      await sleep(60)

      assert.equal(await limiter.checkRateLimit('key-a'), true)
    })

    test('respects cost', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 5, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 5), true)
      assert.equal(await limiter.checkRateLimit('key-a', 1), false)
    })
  })

  describe('leaky-bucket', () => {
    test('accepts requests and rejects when queue occupancy exceeds queueLimit', async (t) => {
      const limiter = new RateLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 60000, queueLimit: 2 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })

    test('keeps independent queues per key', async (t) => {
      const limiter = new RateLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 60000, queueLimit: 1 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
      assert.equal(await limiter.checkRateLimit('key-b'), true)
    })

    test('respects cost in queue occupancy', async (t) => {
      const limiter = new RateLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 60000, queueLimit: 5 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 5), true)
      assert.equal(await limiter.checkRateLimit('key-a', 1), false)
    })

    test('delays subsequent requests to smooth bursts', async (t) => {
      // 10 req/100ms → leak rate of 10ms per queued request
      const limiter = new RateLimiter({ strategy: 'leaky-bucket', maxRequests: 10, windowMs: 100, queueLimit: 10 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a')
      await limiter.checkRateLimit('key-a')

      const start = Date.now()

      await limiter.checkRateLimit('key-a')

      assert.ok(Date.now() - start >= 15, 'third request must be paced by the queue occupancy')
    })

    test('releases capacity after the window slides', async (t) => {
      const limiter = new RateLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 80, queueLimit: 1 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)

      await sleep(100)

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

    test('an exhausted key is allowed again once its window rolls over', async (t) => {
      // Same technique as the getStatus test below: windowMs stays large so
      // the periodic sweep cannot delete the entry, and the elapsed window is
      // simulated by rewriting windowStart. This exercises the reset branch
      // inside checkRateLimit, not a missing-entry path.
      const limiter = new RateLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a', 2), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'window is exhausted')

      limiter.requests.get('key-a').windowStart -= 60000

      assert.equal(await limiter.checkRateLimit('key-a'), true, 'the new window starts from zero')
      assert.equal(limiter.requests.get('key-a').count, 1, 'the old count was discarded, not carried over')
    })
  })

  describe('sliding-window', () => {
    test('limits within the window and releases after it slides', async (t) => {
      const limiter = new RateLimiter({ strategy: 'sliding-window', maxRequests: 2, windowMs: 80 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)

      await sleep(100)

      assert.equal(await limiter.checkRateLimit('key-a'), true)
    })
  })

  describe('blockKey', () => {
    test('blocks a key and unblocks after the duration', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 60000 })
      t.after(() => limiter.dispose())

      limiter.blockKey('key-a', 50)

      assert.equal(await limiter.checkRateLimit('key-a'), false)
      assert.equal(limiter.getStatus('key-a').isBlocked, true)

      await sleep(60)

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(limiter.getStatus('key-a').isBlocked, false)
    })

    test('emits blocked event while a key is blocked', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 10, windowMs: 60000 })
      t.after(() => limiter.dispose())

      const events = []
      limiter.on('blocked', (payload) => events.push(payload))

      limiter.blockKey('key-a', 1000)
      await limiter.checkRateLimit('key-a')

      assert.equal(events.length, 1)
      assert.equal(events[0].key, 'key-a')
      assert.ok(events[0].remainingTime > 0)
    })
  })

  describe('reset', () => {
    test('restores capacity for a key', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 1, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(await limiter.checkRateLimit('key-a'), true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)

      limiter.reset('key-a')

      assert.equal(await limiter.checkRateLimit('key-a'), true)
    })
  })

  describe('strategy validation', () => {
    test('rejects an unknown strategy instead of silently allowing everything', async (t) => {
      const limiter = new RateLimiter({ strategy: 'made-up', maxRequests: 1, windowMs: 60000 })
      t.after(() => limiter.dispose())

      // Failing closed matters: a typo must not become an unlimited limiter.
      await assert.rejects(() => limiter.checkRateLimit('key-a'), /Unknown rate limiting strategy: made-up/)
      assert.equal(limiter.getStatus('key-a').remainingTokens, 0)
    })
  })

  describe('cleanup', () => {
    test('evicts stale token buckets, expired windows and expired blocks', async (t) => {
      // The periodic sweep is what keeps memory flat for high-cardinality
      // keys; without it every key ever seen is retained forever.
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 30 })
      t.after(() => limiter.dispose())

      const unblocked = []

      limiter.on('key-unblocked', ({ key }) => unblocked.push(key))

      await limiter.checkRateLimit('stale-bucket')
      limiter.blockKey('blocked-key', 10)

      assert.equal(limiter.buckets.size, 1)
      assert.equal(limiter.getStatus('blocked-key').isBlocked, true)

      // The sweep runs on its own interval (windowMs / 10), so this asserts
      // the real wiring rather than poking a private method. A bucket becomes
      // collectable once it is older than windowMs * 2.
      await waitFor(
        () => limiter.buckets.size === 0 && !limiter.getStatus('blocked-key').isBlocked,
        3000,
        'stale bucket evicted and expired block released'
      )

      assert.deepEqual(unblocked, ['blocked-key'], 'unblocking must be announced')
    })

    test('drops fixed-window counters from previous windows and slides sliding-window entries', async (t) => {
      const fixed = new RateLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 30 })
      const sliding = new RateLimiter({ strategy: 'sliding-window', maxRequests: 2, windowMs: 30 })

      t.after(() => {
        fixed.dispose()
        sliding.dispose()
      })

      await fixed.checkRateLimit('key-a')
      await sliding.checkRateLimit('key-a')

      assert.equal(fixed.requests.size, 1)
      assert.equal(sliding.requests.size, 1)

      await waitFor(
        () => fixed.requests.size === 0,
        3000,
        'a counter from an elapsed window is dead weight'
      )
      await waitFor(
        () => {
          const entry = sliding.requests.get('key-a')

          // Either the whole entry went, or its slid-out entries were evicted —
          // both are the sweep doing its job, and neither leaves capacity held.
          return entry === undefined || (entry.total === 0 && entry.entries.length === 0)
        },
        3000,
        'slid-out entries must be evicted'
      )

      assert.equal(sliding.getStatus('key-a').remainingTokens, 2, 'capacity must be fully released')
    })

    test('evicts stale leaky-bucket queues so high-cardinality keys cannot grow unbounded', async (t) => {
      const limiter = new RateLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 30, queueLimit: 4 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a')

      assert.equal(limiter.leakyQueues.size, 1)

      await waitFor(
        () => {
          const entry = limiter.leakyQueues.get('key-a')

          return entry === undefined || entry.total === 0
        },
        3000,
        'leaky queue evicted by the sweep'
      )
    })
  })

  describe('getStatus per strategy', () => {
    test('reports remaining capacity for every strategy and 0 for an unknown one', async (t) => {
      const limiters = {
        'leaky-bucket': new RateLimiter({ strategy: 'leaky-bucket', queueLimit: 4, windowMs: 60000 }),
        'fixed-window': new RateLimiter({ strategy: 'fixed-window', maxRequests: 4, windowMs: 60000 }),
        'sliding-window': new RateLimiter({ strategy: 'sliding-window', maxRequests: 4, windowMs: 60000 }),
        unknown: new RateLimiter({ strategy: 'nope', maxRequests: 4, windowMs: 60000 })
      }

      t.after(() => Object.values(limiters).forEach(limiter => limiter.dispose()))

      // Untouched keys report full capacity.
      assert.equal(limiters['leaky-bucket'].getStatus('fresh').remainingTokens, 4)
      assert.equal(limiters['fixed-window'].getStatus('fresh').remainingTokens, 4)
      assert.equal(limiters['sliding-window'].getStatus('fresh').remainingTokens, 4)
      assert.equal(limiters.unknown.getStatus('fresh').remainingTokens, 0)

      // And consumption is reflected.
      await limiters['leaky-bucket'].checkRateLimit('key-a', 3)
      await limiters['fixed-window'].checkRateLimit('key-a', 3)
      await limiters['sliding-window'].checkRateLimit('key-a', 3)

      assert.equal(limiters['leaky-bucket'].getStatus('key-a').remainingTokens, 1)
      assert.equal(limiters['fixed-window'].getStatus('key-a').remainingTokens, 1)
      assert.equal(limiters['sliding-window'].getStatus('key-a').remainingTokens, 1)
    })

    test('a fixed-window counter from an elapsed window reports full capacity again', async (t) => {
      // windowMs is deliberately large so the periodic sweep cannot fire and
      // delete the key: this must exercise the stale-window branch inside
      // getStatus, not the "no entry at all" one. The window is advanced by
      // rewriting the stored windowStart, which is what elapsed time does.
      const limiter = new RateLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs: 60000 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)
      assert.equal(limiter.getStatus('key-a').remainingTokens, 0)

      const entry = limiter.requests.get('key-a')

      entry.windowStart -= 60000

      assert.equal(limiter.requests.has('key-a'), true, 'the entry must still be present')
      assert.equal(limiter.getStatus('key-a').remainingTokens, 2, 'a new window starts fresh')
    })
  })

  describe('getStatus', () => {
    test('reports remaining tokens', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 5, windowMs: 60000 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)

      const status = limiter.getStatus('key-a')

      assert.equal(status.strategy, 'token-bucket')
      assert.equal(status.remainingTokens, 3)
      assert.equal(status.isBlocked, false)
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
    // The whole burst feature had no assertions: burstable, burstLimit and the
    // bucket sizing they drive were free to be anything.
    test('is off by default: the bucket holds exactly maxRequests', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 4, windowMs: 60000 })
      t.after(() => limiter.dispose())

      assert.equal(limiter.burstable, false)
      assert.equal(limiter.tokenBucketSize, 4)
      assert.equal(limiter.getStatus('key-a').remainingTokens, 4)
    })

    test('sizes the bucket to burstLimit when enabled', async (t) => {
      const limiter = new RateLimiter({
        strategy: 'token-bucket',
        maxRequests: 4,
        windowMs: 60000,
        burstable: true,
        burstLimit: 10
      })

      t.after(() => limiter.dispose())

      assert.equal(limiter.tokenBucketSize, 10)
      assert.equal(limiter.getStatus('key-a').burstable, true)

      // The burst is real: more than maxRequests calls go through at once.
      for (let i = 0; i < 10; i++) {
        assert.equal(await limiter.checkRateLimit('key-a'), true, `burst call ${i + 1}`)
      }

      assert.equal(await limiter.checkRateLimit('key-a'), false, 'the burst is still bounded')
    })

    test('defaults burstLimit to one and a half times maxRequests', async (t) => {
      const limiter = new RateLimiter({
        strategy: 'token-bucket',
        maxRequests: 4,
        windowMs: 60000,
        burstable: true
      })

      t.after(() => limiter.dispose())

      assert.equal(limiter.burstLimit, 6, 'multiplied, not divided')
      assert.equal(limiter.tokenBucketSize, 6)
    })
  })

  describe('cleanup leaves live state alone', () => {
    // The sweep tests above prove it collects. These prove it does not
    // over-collect: a sweep that drops a live counter silently resets someone's
    // rate limit and lets the traffic it was holding back through.
    const sweptAtLeastOnce = (limiter) => sleep(Math.min(limiter.windowMs / 10, 60000) * 3 + 20)

    test('keeps a fixed-window counter that belongs to the current window', async (t) => {
      const windowMs = 3000
      const limiter = new RateLimiter({ strategy: 'fixed-window', maxRequests: 2, windowMs })
      t.after(() => limiter.dispose())

      // Fixed windows are anchored to the clock, not to when the limiter was
      // built: the boundary falls every `windowMs` of epoch time. If one passes
      // while this test sleeps, the sweep drops the entry for the right reason
      // and the assertions below would fail on correct code. Rather than pay an
      // alignment wait up front, the observation is retried on the rare pass
      // that straddles a boundary.
      const windowAt = () => Math.floor(Date.now() / windowMs) * windowMs
      let openedIn, closedIn

      do {
        openedIn = windowAt()

        await limiter.checkRateLimit('key-a', 2)
        assert.equal(limiter.getRemainingTokens('key-a'), 0)

        await sweptAtLeastOnce(limiter)

        closedIn = windowAt()
      } while (openedIn !== closedIn)

      assert.equal(limiter.requests.has('key-a'), true, 'the entry survived the sweep')
      assert.equal(limiter.getRemainingTokens('key-a'), 0, 'and its count was not reset')
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'so the key is still limited')
    })

    test('keeps sliding-window entries that are still inside the window', async (t) => {
      const limiter = new RateLimiter({ strategy: 'sliding-window', maxRequests: 2, windowMs: 3000 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)

      await sweptAtLeastOnce(limiter)

      assert.equal(limiter.requests.has('key-a'), true)
      assert.equal(limiter.getRemainingTokens('key-a'), 0)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })

    test('keeps a token bucket that was refilled recently', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 3000 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)
      assert.equal(limiter.buckets.size, 1)

      await sweptAtLeastOnce(limiter)

      assert.equal(limiter.buckets.size, 1, 'a fresh bucket is not collectable')
      assert.equal(await limiter.checkRateLimit('key-a'), false, 'its spent tokens were not restored')
    })

    test('keeps a leaky-bucket queue that has not drained yet', async (t) => {
      const limiter = new RateLimiter({ strategy: 'leaky-bucket', maxRequests: 100, windowMs: 3000, queueLimit: 2 })
      t.after(() => limiter.dispose())

      await limiter.checkRateLimit('key-a', 2)
      assert.equal(limiter.leakyQueues.has('key-a'), true)

      await sweptAtLeastOnce(limiter)

      assert.equal(limiter.leakyQueues.has('key-a'), true, 'the queue survived the sweep')
      assert.equal(limiter.getRemainingTokens('key-a'), 0, 'and still counts as occupied')
    })

    test('does not release a block that has not expired', async (t) => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 2, windowMs: 3000 })
      t.after(() => limiter.dispose())

      const unblocked = []

      limiter.on('key-unblocked', ({ key }) => unblocked.push(key))
      limiter.blockKey('key-a', 60000)

      await sweptAtLeastOnce(limiter)

      assert.deepEqual(unblocked, [], 'nothing was unblocked early')
      assert.equal(limiter.getStatus('key-a').isBlocked, true)
      assert.equal(await limiter.checkRateLimit('key-a'), false)
    })
  })

  describe('dispose', () => {
    test('clears internal state', async () => {
      const limiter = new RateLimiter({ strategy: 'token-bucket', maxRequests: 1, windowMs: 60000 })

      await limiter.checkRateLimit('key-a')
      limiter.blockKey('key-b', 1000)
      limiter.dispose()

      assert.equal(limiter.buckets.size, 0)
      assert.equal(limiter.blocked.size, 0)
      assert.equal(limiter.requests.size, 0)
    })
  })
})
