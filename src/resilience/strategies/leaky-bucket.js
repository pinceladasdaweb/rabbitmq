import WindowLog from './window-log.js'

// Leaky bucket with smoothing: accepts the request and delays it in
// proportion to that key's queue occupancy, spreading bursts over time.
// Rejects only when the occupancy (sum of costs) exceeds queueLimit.
class LeakyBucketStrategy {
  constructor ({ maxRequests, windowMs, queueLimit }, clock) {
    this.queueLimit = queueLimit
    this.leakRateMs = windowMs / maxRequests
    this.log = new WindowLog(windowMs)
    this.clock = clock
  }

  async check (key, cost, now) {
    const windowData = this.log.get(key)

    this.log.evictExpired(windowData, now)

    if (windowData.total + cost > this.queueLimit) return false

    // Occupancy is read before this entry joins the queue: the first request
    // in an empty queue goes through undelayed.
    const waitTime = windowData.total * this.leakRateMs

    windowData.entries.push({ timestamp: now, cost })
    windowData.total += cost

    if (waitTime > 0) {
      await this.clock.sleep(waitTime)
    }

    return true
  }

  remaining (key, now) {
    const windowData = this.log.peek(key)

    if (!windowData) return this.queueLimit

    this.log.evictExpired(windowData, now)

    return this.queueLimit - windowData.total
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

export { LeakyBucketStrategy }
export default LeakyBucketStrategy
