import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import RateLimiter from '../src/resilience/rate-limiter.js'

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

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
