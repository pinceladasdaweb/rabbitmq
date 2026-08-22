// Fixed windows are anchored to the epoch, not to when a key was first seen:
// the boundary falls every windowMs of clock time, so all keys roll over
// together and a counter's windowStart identifies exactly one window.
class FixedWindowStrategy {
  constructor ({ maxRequests, windowMs }) {
    this.maxRequests = maxRequests
    this.windowMs = windowMs
    this.counters = new Map()
  }

  // The most a single check can ever be granted: even an empty window admits
  // at most maxRequests. See RateLimiter.checkRateLimit.
  get capacity () {
    return this.maxRequests
  }

  #windowStart (now) {
    return Math.floor(now / this.windowMs) * this.windowMs
  }

  check (key, cost, now) {
    const windowStart = this.#windowStart(now)
    const counter = this.counters.get(key) || { count: 0, windowStart }

    if (counter.windowStart !== windowStart) {
      counter.count = 0
      counter.windowStart = windowStart
    }

    if (counter.count + cost > this.maxRequests) return false

    counter.count += cost
    this.counters.set(key, counter)

    return true
  }

  remaining (key, now) {
    const counter = this.counters.get(key)

    if (!counter || counter.windowStart !== this.#windowStart(now)) return this.maxRequests

    return this.maxRequests - counter.count
  }

  cleanup (now) {
    const currentWindowStart = this.#windowStart(now)

    for (const [key, counter] of this.counters) {
      if (counter.windowStart !== currentWindowStart) {
        this.counters.delete(key)
      }
    }
  }

  reset (key) {
    this.counters.delete(key)
  }

  clear () {
    this.counters.clear()
  }
}

export { FixedWindowStrategy }
export default FixedWindowStrategy
