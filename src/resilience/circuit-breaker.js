import { EventEmitter } from 'node:events'

class CircuitBreaker extends EventEmitter {
  constructor (options = {}) {
    super()
    this.failureThreshold = options.failureThreshold || 5
    this.successThreshold = options.successThreshold || 2
    this.timeout = options.timeout || 60000
    this.failureCount = 0
    this.successCount = 0
    this.nextAttempt = Date.now()
    this.state = 'CLOSED'
  }

  #transitionTo (state) {
    if (this.state === state) return

    // Every state entry starts with clean counters — keeping the resets here
    // (instead of scattered across the transition call sites) preserves the
    // invariant for any future transition path.
    this.failureCount = 0
    this.successCount = 0
    this.state = state
    this.emit('stateChanged', state)
  }

  // Forces the breaker back to a clean CLOSED state — used after a
  // successful reconnection, when failures accumulated against the previous
  // connection no longer say anything about the new one.
  reset () {
    this.failureCount = 0
    this.successCount = 0
    this.nextAttempt = Date.now()
    this.#transitionTo('CLOSED')
  }

  async execute (operation) {
    if (this.state === 'OPEN') {
      if (Date.now() > this.nextAttempt) {
        this.#transitionTo('HALF-OPEN')
      } else {
        const error = new Error('Circuit is OPEN')
        error.code = 'CIRCUIT_OPEN'

        throw error
      }
    }

    try {
      const result = await operation()

      this.onSuccess()

      return result
    } catch (error) {
      this.onFailure()

      throw error
    }
  }

  onSuccess () {
    this.failureCount = 0

    if (this.state === 'HALF-OPEN') {
      this.successCount++

      if (this.successCount >= this.successThreshold) {
        this.#transitionTo('CLOSED')
      }
    }
  }

  onFailure () {
    if (this.state === 'HALF-OPEN') {
      this.nextAttempt = Date.now() + this.timeout
      this.#transitionTo('OPEN')

      return
    }

    this.failureCount++

    if (this.failureCount >= this.failureThreshold) {
      this.nextAttempt = Date.now() + this.timeout
      this.#transitionTo('OPEN')
    }
  }

  getState () {
    return {
      state: this.state,
      failureCount: this.failureCount,
      successCount: this.successCount,
      nextAttempt: this.nextAttempt
    }
  }
}

export { CircuitBreaker }
export default CircuitBreaker
