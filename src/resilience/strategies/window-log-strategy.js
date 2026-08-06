import WindowLog from './window-log.js'

// Base for the strategies whose whole state is a WindowLog: occupancy is
// compared against a single limit, and remaining/cleanup/reset/clear are pure
// delegation. Sliding-window and leaky-bucket differ only in which limit they
// enforce and in what check() does after an entry is admitted.
class WindowLogStrategy {
  constructor (limit, windowMs) {
    this.limit = limit
    this.log = new WindowLog(windowMs)
  }

  // Admits the entry and returns the occupancy the key had BEFORE it joined
  // (the leaky bucket paces by it), or null when the limit would be exceeded.
  occupy (key, cost, now) {
    const windowData = this.log.get(key)

    this.log.evictExpired(windowData, now)

    if (windowData.total + cost > this.limit) return null

    const occupancyBefore = windowData.total

    windowData.entries.push({ timestamp: now, cost })
    windowData.total += cost

    return occupancyBefore
  }

  remaining (key, now) {
    const windowData = this.log.peek(key)

    if (!windowData) return this.limit

    this.log.evictExpired(windowData, now)

    return this.limit - windowData.total
  }

  cleanup (now) {
    this.log.cleanup(now)
  }

  reset (key) {
    this.log.delete(key)
  }

  clear () {
    this.log.clear()
  }
}

export { WindowLogStrategy }
export default WindowLogStrategy
