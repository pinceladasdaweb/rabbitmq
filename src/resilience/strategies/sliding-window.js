import WindowLogStrategy from './window-log-strategy.js'

class SlidingWindowStrategy extends WindowLogStrategy {
  constructor ({ maxRequests, windowMs }) {
    super(maxRequests, windowMs)
  }

  check (key, cost, now) {
    return this.occupy(key, cost, now) !== null
  }
}

export { SlidingWindowStrategy }
export default SlidingWindowStrategy
