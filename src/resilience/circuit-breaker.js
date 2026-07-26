import { EventEmitter } from 'node:events'
import { circuitBreaker } from 'breakwater'

const STATE_LABELS = {
  closed: 'CLOSED',
  open: 'OPEN',
  'half-open': 'HALF-OPEN',
  isolated: 'ISOLATED'
}

// Thin adapter over breakwater's circuit breaker preserving this library's
// public contract: uppercase states, the stateChanged event and getState().
class CircuitBreaker extends EventEmitter {
  #options
  #policy

  constructor (options = {}) {
    super()
    this.#options = {
      failureThreshold: options.failureThreshold || 5,
      successThreshold: options.successThreshold || 2,
      timeout: options.timeout || 60000
    }
    this.#policy = this.#createPolicy()
  }

  #createPolicy () {
    const policy = circuitBreaker({
      name: 'rabbitmq-publisher',
      consecutiveFailures: this.#options.failureThreshold,
      halfOpenAfter: this.#options.timeout,
      // breakwater closes after a majority of probe successes
      // (floor(halfOpenCalls / 2) + 1): sizing the probe pool keeps the
      // successThreshold contract intact.
      halfOpenCalls: this.#options.successThreshold * 2 - 1
    })

    policy.on('stateChange', ({ to }) => this.emit('stateChanged', STATE_LABELS[to]))

    return policy
  }

  // The underlying breakwater policy, for composition (e.g. with retry).
  get policy () {
    return this.#policy
  }

  async execute (operation) {
    return await this.#policy.execute(operation)
  }

  // Forces the breaker back to a clean CLOSED state — used after a
  // successful reconnection, when failures accumulated against the previous
  // connection no longer say anything about the new one. Recreating the
  // policy keeps reset synchronous for callers.
  reset () {
    const wasClosed = this.#policy.state === 'closed'

    this.#policy = this.#createPolicy()

    if (!wasClosed) {
      this.emit('stateChanged', 'CLOSED')
    }
  }

  getState () {
    const stats = this.#policy.stats()

    return {
      state: STATE_LABELS[stats.state],
      failureCount: stats.failures,
      successCount: stats.successes,
      nextAttempt: stats.nextAttemptAt ?? Date.now()
    }
  }
}

export { CircuitBreaker }
export default CircuitBreaker
