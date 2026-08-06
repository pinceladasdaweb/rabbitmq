import WindowLogStrategy from './window-log-strategy.js'

// Leaky bucket with smoothing: accepts the request and delays it in
// proportion to that key's queue occupancy, spreading bursts over time.
// Rejects only when the occupancy (sum of costs) exceeds queueLimit.
class LeakyBucketStrategy extends WindowLogStrategy {
  constructor ({ maxRequests, windowMs, queueLimit }, clock) {
    super(queueLimit, windowMs)
    this.leakRateMs = windowMs / maxRequests
    this.clock = clock
  }

  async check (key, cost, now) {
    // Occupancy is read before this entry joins the queue: the first request
    // in an empty queue goes through undelayed.
    const occupancyBefore = this.occupy(key, cost, now)

    if (occupancyBefore === null) return false

    const waitTime = occupancyBefore * this.leakRateMs

    if (waitTime > 0) {
      await this.clock.sleep(waitTime)
    }

    return true
  }
}

export { LeakyBucketStrategy }
export default LeakyBucketStrategy
