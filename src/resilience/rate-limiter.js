import { EventEmitter } from 'node:events'
import { setTimeout as sleep } from 'node:timers/promises'

class RateLimiter extends EventEmitter {
  constructor (options = {}) {
    super()
    this.windowMs = options.windowMs || 60000
    this.maxRequests = options.maxRequests || 100
    this.strategy = options.strategy || 'token-bucket'
    this.burstable = options.burstable || false
    this.burstLimit = options.burstLimit || this.maxRequests * 1.5
    this.requests = new Map()
    this.buckets = new Map()
    this.blocked = new Map()
    this.logger = options.logger

    this.tokenRefillRate = this.maxRequests / (this.windowMs / 1000)
    this.tokenBucketSize = this.burstable ? this.burstLimit : this.maxRequests

    this.leakRateMs = this.windowMs / this.maxRequests
    this.leakyQueues = new Map()
    this.queueLimit = options.queueLimit || 1000

    this.cleanupInterval = setInterval(() => this.#cleanup(), Math.min(this.windowMs / 10, 60000))

    if (typeof this.cleanupInterval.unref === 'function') {
      this.cleanupInterval.unref()
    }
  }

  // Sliding-window stores keep a time-ordered entry list plus a running
  // total; expired entries are evicted from the front — O(evicted) per
  // check instead of a full filter+reduce scan of the window.
  #getWindowData (store, key) {
    let windowData = store.get(key)

    if (!windowData) {
      windowData = { entries: [], total: 0 }
      store.set(key, windowData)
    }

    return windowData
  }

  #evictExpired (windowData, now) {
    while (windowData.entries.length > 0 && now - windowData.entries[0].timestamp > this.windowMs) {
      windowData.total -= windowData.entries.shift().cost
    }
  }

  #cleanup () {
    const now = Date.now()

    for (const [key, data] of this.requests) {
      if (data.windowStart !== undefined) {
        const currentWindowStart = Math.floor(now / this.windowMs) * this.windowMs

        if (data.windowStart !== currentWindowStart) {
          this.requests.delete(key)
        }
      } else if (Array.isArray(data.entries)) {
        this.#evictExpired(data, now)

        if (data.entries.length === 0) {
          this.requests.delete(key)
        }
      }
    }

    for (const [key, bucket] of this.buckets) {
      if (now - bucket.lastRefill > this.windowMs * 2) {
        this.buckets.delete(key)
      }
    }

    for (const [key, windowData] of this.leakyQueues) {
      this.#evictExpired(windowData, now)

      if (windowData.entries.length === 0) {
        this.leakyQueues.delete(key)
      }
    }

    for (const [key, expiresAt] of this.blocked) {
      if (now >= expiresAt) {
        this.blocked.delete(key)
        this.emit('key-unblocked', { key })
      }
    }
  }

  async checkRateLimit (key, cost = 1) {
    const now = Date.now()
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

    switch (this.strategy) {
      case 'token-bucket':
        return this.tokenBucketCheck(key, cost)
      case 'leaky-bucket':
        return this.leakyBucketCheck(key, cost)
      case 'fixed-window':
        return this.fixedWindowCheck(key, cost)
      case 'sliding-window':
        return this.slidingWindowCheck(key, cost)
      default:
        throw new Error(`Unknown rate limiting strategy: ${this.strategy}`)
    }
  }

  #getBucket (key) {
    const now = Date.now()
    let bucket = this.buckets.get(key)

    if (!bucket) {
      bucket = { tokens: this.tokenBucketSize, lastRefill: now }
      this.buckets.set(key, bucket)

      return bucket
    }

    const tokensToAdd = ((now - bucket.lastRefill) / 1000) * this.tokenRefillRate

    if (tokensToAdd > 0) {
      bucket.tokens = Math.min(this.tokenBucketSize, bucket.tokens + tokensToAdd)
      bucket.lastRefill = now
    }

    return bucket
  }

  tokenBucketCheck (key, cost) {
    const bucket = this.#getBucket(key)

    if (bucket.tokens >= cost) {
      bucket.tokens -= cost

      return true
    }

    this.emit('limited', { key, strategy: 'token-bucket' })

    return false
  }

  // Leaky bucket with smoothing: accepts the request and delays it in
  // proportion to that key's queue occupancy, spreading bursts over time.
  // Rejects only when the occupancy (sum of costs) exceeds queueLimit.
  async leakyBucketCheck (key, cost) {
    const now = Date.now()
    const windowData = this.#getWindowData(this.leakyQueues, key)

    this.#evictExpired(windowData, now)

    if (windowData.total + cost > this.queueLimit) {
      this.emit('limited', { key, strategy: 'leaky-bucket' })

      return false
    }

    const waitTime = Math.max(0, windowData.total * this.leakRateMs)

    windowData.entries.push({ timestamp: now, cost })
    windowData.total += cost

    if (waitTime > 0) {
      await sleep(waitTime)
    }

    return true
  }

  fixedWindowCheck (key, cost) {
    const windowStart = Math.floor(Date.now() / this.windowMs) * this.windowMs
    const keyData = this.requests.get(key) || { count: 0, windowStart }

    if (keyData.windowStart !== windowStart) {
      keyData.count = 0
      keyData.windowStart = windowStart
    }

    if (keyData.count + cost <= this.maxRequests) {
      keyData.count += cost
      this.requests.set(key, keyData)

      return true
    }

    this.emit('limited', { key, strategy: 'fixed-window' })

    return false
  }

  slidingWindowCheck (key, cost) {
    const now = Date.now()
    const windowData = this.#getWindowData(this.requests, key)

    this.#evictExpired(windowData, now)

    if (windowData.total + cost <= this.maxRequests) {
      windowData.entries.push({ timestamp: now, cost })
      windowData.total += cost

      return true
    }

    this.emit('limited', { key, strategy: 'sliding-window' })

    return false
  }

  blockKey (key, duration = this.windowMs) {
    this.blocked.set(key, Date.now() + duration)
    this.emit('key-blocked', { key, duration })
  }

  getRemainingTokens (key) {
    const now = Date.now()

    switch (this.strategy) {
      case 'token-bucket':
        return Math.floor(this.#getBucket(key).tokens)

      case 'leaky-bucket': {
        const windowData = this.leakyQueues.get(key)

        if (!windowData) return this.queueLimit

        this.#evictExpired(windowData, now)

        return this.queueLimit - windowData.total
      }

      case 'fixed-window': {
        const keyData = this.requests.get(key)
        const windowStart = Math.floor(now / this.windowMs) * this.windowMs

        if (!keyData || keyData.windowStart !== windowStart) return this.maxRequests

        return this.maxRequests - keyData.count
      }

      case 'sliding-window': {
        const windowData = this.requests.get(key)

        if (!windowData) return this.maxRequests

        this.#evictExpired(windowData, now)

        return this.maxRequests - windowData.total
      }

      default:
        return 0
    }
  }

  getStatus (key) {
    const blockedUntil = this.blocked.get(key)

    return {
      strategy: this.strategy,
      remainingTokens: this.getRemainingTokens(key),
      isBlocked: blockedUntil !== undefined && blockedUntil > Date.now(),
      windowMs: this.windowMs,
      maxRequests: this.maxRequests,
      burstable: this.burstable,
      currentTime: Date.now()
    }
  }

  reset (key) {
    this.requests.delete(key)
    this.buckets.delete(key)
    this.blocked.delete(key)
    this.leakyQueues.delete(key)

    this.emit('reset', { key })
  }

  dispose () {
    clearInterval(this.cleanupInterval)
    this.requests.clear()
    this.buckets.clear()
    this.blocked.clear()
    this.leakyQueues.clear()
  }
}

export { RateLimiter }
export default RateLimiter
