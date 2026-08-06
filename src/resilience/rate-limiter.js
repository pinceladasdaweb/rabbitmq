import { EventEmitter } from 'node:events'
import systemClock from '../utils/clock.js'
import TokenBucketStrategy from './strategies/token-bucket.js'
import LeakyBucketStrategy from './strategies/leaky-bucket.js'
import FixedWindowStrategy from './strategies/fixed-window.js'
import SlidingWindowStrategy from './strategies/sliding-window.js'

const STRATEGIES = {
  'token-bucket': TokenBucketStrategy,
  'leaky-bucket': LeakyBucketStrategy,
  'fixed-window': FixedWindowStrategy,
  'sliding-window': SlidingWindowStrategy
}

// Orchestrates one strategy: owns key blocking, event emission and the
// periodic sweep. All algorithm state lives in the strategy object.
class RateLimiter extends EventEmitter {
  constructor (options = {}) {
    super()
    this.windowMs = options.windowMs || 60000
    this.maxRequests = options.maxRequests || 100
    this.strategy = options.strategy || 'token-bucket'
    this.burstable = options.burstable || false
    this.burstLimit = options.burstLimit || this.maxRequests * 1.5
    this.queueLimit = options.queueLimit || 1000
    this.logger = options.logger
    this.clock = options.clock || systemClock
    this.blocked = new Map()

    const Strategy = STRATEGIES[this.strategy]

    // Failing closed at construction: a typo'd strategy must never become an
    // unlimited limiter, and surfacing it at boot beats surfacing it on the
    // first rate-limited publish in production.
    if (!Strategy) {
      throw new Error(`Unknown rate limiting strategy: ${this.strategy}`)
    }

    this.limiter = new Strategy({
      maxRequests: this.maxRequests,
      windowMs: this.windowMs,
      burstable: this.burstable,
      burstLimit: this.burstLimit,
      queueLimit: this.queueLimit
    }, this.clock)

    this.cleanupInterval = this.clock.setInterval(() => this.#cleanup(), Math.min(this.windowMs / 10, 60000))

    // Unref'd so a limiter nobody disposed cannot keep the process alive. Note
    // this is not covered by a unit test: every test disposes its limiter, so
    // the ref'd variant would never get the chance to hold the loop open.
    if (typeof this.cleanupInterval.unref === 'function') {
      this.cleanupInterval.unref()
    }
  }

  // The sweep is what keeps memory flat for high-cardinality keys; without it
  // every key ever seen is retained forever.
  #cleanup () {
    const now = this.clock.now()

    this.limiter.cleanup(now)

    for (const [key, expiresAt] of this.blocked) {
      if (now >= expiresAt) {
        this.blocked.delete(key)
        this.emit('key-unblocked', { key })
      }
    }
  }

  async checkRateLimit (key, cost = 1) {
    const now = this.clock.now()
    const blockedUntil = this.blocked.get(key)

    if (blockedUntil !== undefined) {
      if (now >= blockedUntil) {
        this.blocked.delete(key)
        this.emit('key-unblocked', { key })
      } else {
        this.emit('blocked', { key, remainingTime: blockedUntil - now })

        return false
      }
    }

    const allowed = await this.limiter.check(key, cost, now)

    if (!allowed) {
      this.emit('limited', { key, strategy: this.strategy })
    }

    return allowed
  }

  blockKey (key, duration = this.windowMs) {
    this.blocked.set(key, this.clock.now() + duration)
    this.emit('key-blocked', { key, duration })
  }

  getRemainingTokens (key) {
    return this.limiter.remaining(key, this.clock.now())
  }

  getStatus (key) {
    const now = this.clock.now()
    const blockedUntil = this.blocked.get(key)

    return {
      strategy: this.strategy,
      remainingTokens: this.limiter.remaining(key, now),
      isBlocked: blockedUntil !== undefined && blockedUntil > now,
      windowMs: this.windowMs,
      maxRequests: this.maxRequests,
      burstable: this.burstable,
      currentTime: now
    }
  }

  reset (key) {
    this.limiter.reset(key)
    this.blocked.delete(key)

    this.emit('reset', { key })
  }

  dispose () {
    this.clock.clearInterval(this.cleanupInterval)
    this.limiter.clear()
    this.blocked.clear()
  }
}

export { RateLimiter }
export default RateLimiter
