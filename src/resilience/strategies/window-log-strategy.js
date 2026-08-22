// Base for the strategies whose whole state is a per-key, time-ordered entry
// log with a running total: occupancy is compared against a single limit, and
// expired entries are evicted from the front — O(evicted) per check instead
// of a full filter+reduce scan of the window. Sliding-window and leaky-bucket
// differ only in which limit they enforce and in what check() does after an
// entry is admitted.
class WindowLogStrategy {
  constructor (limit, windowMs) {
    this.limit = limit
    this.windowMs = windowMs
    this.windows = new Map()
  }

  // The most a single check can ever be granted: occupancy is compared against
  // one limit, so an entry costing more than it never fits, however empty the
  // log. See RateLimiter.checkRateLimit.
  get capacity () {
    return this.limit
  }

  #windowFor (key) {
    let windowData = this.windows.get(key)

    if (!windowData) {
      windowData = { entries: [], total: 0 }
      this.windows.set(key, windowData)
    }

    return windowData
  }

  #evictExpired (windowData, now) {
    while (windowData.entries.length > 0 && now - windowData.entries[0].timestamp > this.windowMs) {
      windowData.total -= windowData.entries.shift().cost
    }
  }

  // Admits the entry and returns the occupancy the key had BEFORE it joined
  // (the leaky bucket paces by it), or null when the limit would be exceeded.
  occupy (key, cost, now) {
    const windowData = this.#windowFor(key)

    this.#evictExpired(windowData, now)

    if (windowData.total + cost > this.limit) return null

    const occupancyBefore = windowData.total

    windowData.entries.push({ timestamp: now, cost })
    windowData.total += cost

    return occupancyBefore
  }

  remaining (key, now) {
    const windowData = this.windows.get(key)

    if (!windowData) return this.limit

    this.#evictExpired(windowData, now)

    return this.limit - windowData.total
  }

  cleanup (now) {
    for (const [key, windowData] of this.windows) {
      this.#evictExpired(windowData, now)

      if (windowData.entries.length === 0) {
        this.windows.delete(key)
      }
    }
  }

  reset (key) {
    this.windows.delete(key)
  }

  clear () {
    this.windows.clear()
  }
}

export { WindowLogStrategy }
export default WindowLogStrategy
