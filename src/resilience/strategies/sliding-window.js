import WindowLog from './window-log.js'

class SlidingWindowStrategy {
  constructor ({ maxRequests, windowMs }) {
    this.maxRequests = maxRequests
    this.log = new WindowLog(windowMs)
  }

  check (key, cost, now) {
    const windowData = this.log.get(key)

    this.log.evictExpired(windowData, now)

    if (windowData.total + cost > this.maxRequests) return false

    windowData.entries.push({ timestamp: now, cost })
    windowData.total += cost

    return true
  }

  remaining (key, now) {
    const windowData = this.log.peek(key)

    if (!windowData) return this.maxRequests

    this.log.evictExpired(windowData, now)

    return this.maxRequests - windowData.total
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

export { SlidingWindowStrategy }
export default SlidingWindowStrategy
