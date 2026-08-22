class TokenBucketStrategy {
  constructor ({ maxRequests, windowMs, burstable, burstLimit }) {
    this.bucketSize = burstable ? burstLimit : maxRequests
    this.refillRatePerMs = maxRequests / windowMs
    this.windowMs = windowMs
    this.buckets = new Map()
  }

  // The most a single check can ever be granted: tokens refill up to
  // bucketSize and stop there. See RateLimiter.checkRateLimit.
  get capacity () {
    return this.bucketSize
  }

  // Reads refill too: remaining() must see the tokens accrued since the last
  // check, not the balance frozen at that moment.
  #bucket (key, now) {
    let bucket = this.buckets.get(key)

    if (!bucket) {
      bucket = { tokens: this.bucketSize, lastRefill: now }
      this.buckets.set(key, bucket)

      return bucket
    }

    const tokensToAdd = (now - bucket.lastRefill) * this.refillRatePerMs

    // Strictly greater: zero elapsed means nothing to add, and a NEGATIVE
    // delta (clock stepped backwards) must not shrink the balance.
    if (tokensToAdd > 0) {
      bucket.tokens = Math.min(this.bucketSize, bucket.tokens + tokensToAdd)
      bucket.lastRefill = now
    }

    return bucket
  }

  check (key, cost, now) {
    const bucket = this.#bucket(key, now)

    if (bucket.tokens < cost) return false

    bucket.tokens -= cost

    return true
  }

  remaining (key, now) {
    return Math.floor(this.#bucket(key, now).tokens)
  }

  cleanup (now) {
    for (const [key, bucket] of this.buckets) {
      if (now - bucket.lastRefill > this.windowMs * 2) {
        this.buckets.delete(key)
      }
    }
  }

  reset (key) {
    this.buckets.delete(key)
  }

  clear () {
    this.buckets.clear()
  }
}

export { TokenBucketStrategy }
export default TokenBucketStrategy
