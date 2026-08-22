import amqp from 'amqplib'
import { EventEmitter } from 'node:events'
import systemClock from '../utils/clock.js'
import emitSafely from '../utils/emit-safely.js'
import describeError from '../utils/describe-error.js'

class RabbitMQConnection extends EventEmitter {
  #logger
  #username
  #password
  #protocol
  #vhost
  #endpoints
  #connectionName
  #connection
  #reconnectTimeout
  #reconnectAttempt
  #reconnectInterval
  #maxReconnectInterval
  #maxReconnectAttempts
  #connectionState
  #currentEndpointIndex
  #isShuttingDown
  #isReconnecting
  #connectPromise
  #clock

  constructor (options, logger) {
    super()

    this.#logger = logger
    this.#username = options.username
    this.#password = options.password
    this.#protocol = options.protocol || 'amqp'
    this.#vhost = options.vhost
    this.#endpoints = options.endpoints
    this.#connectionName = options.connectionName
    this.#connection = null
    this.#reconnectTimeout = null
    this.#reconnectAttempt = 0
    this.#reconnectInterval = options.reconnectInterval || 1000
    this.#maxReconnectInterval = options.maxReconnectInterval || 15000
    // ?? and not ||: a caller asking for 0 attempts wants NO reconnection,
    // and || turned that exact request into its opposite (retry forever).
    this.#maxReconnectAttempts = options.maxReconnectAttempts ?? Infinity
    this.#connectionState = 'disconnected'
    this.#currentEndpointIndex = 0
    this.#isShuttingDown = false
    this.#isReconnecting = false
    this.#connectPromise = null
    // Seam for the reconnection backoff timer. Deliberately NOT wired to the
    // facade's clock: that one is advanced by tests to drive sweeps and
    // staleness, and reconnection cycles must keep running on real time
    // underneath them.
    this.#clock = options.clock ?? systemClock

    if (!['amqp', 'amqps'].includes(this.#protocol)) {
      throw new Error('Invalid protocol. Must be one of: amqp, amqps')
    }

    if (this.#endpoints.length === 0 || this.#endpoints.some(endpoint => !endpoint)) {
      throw new Error('At least one valid RabbitMQ endpoint must be provided')
    }
  }

  // Every lifecycle emit in this class sits at a load-bearing point of the
  // state machine, and a listener is application code running in the middle
  // of it. startReconnection used to announce 'disconnected' BEFORE arming
  // the retry timer, so a throwing listener escaped with #isReconnecting
  // already true and no timer pending: reconnection was disabled for the rest
  // of the process's life, state stuck on 'reconnecting'. The listener's crash
  // is the listener's bug — it must never unwind the caller.
  //
  // Contains synchronous throws only. An async listener's rejection belongs to
  // whoever registered it; the facade's own handlers contain theirs.
  #safeEmit (event, ...args) {
    emitSafely(this, event, args, this.#logger)
  }

  #clearReconnectTimeout () {
    // No guard: clearTimeout tolerates null, and re-nulling is a no-op.
    this.#clock.clearTimeout(this.#reconnectTimeout)
    this.#reconnectTimeout = null
  }

  #buildConnectionString (endpoint) {
    const credentials = `${encodeURIComponent(this.#username)}:${encodeURIComponent(this.#password)}`
    const vhost = this.#vhost ? `/${encodeURIComponent(this.#vhost)}` : ''

    return `${this.#protocol}://${credentials}@${endpoint}${vhost}`
  }

  // Concurrent callers (a user connect() racing the reconnect timer, or two
  // startup paths) funnel into a single in-flight attempt: two parallel
  // loops would create two AMQP connections and leak the losing one.
  async connect () {
    if (!this.#connectPromise) {
      this.#connectPromise = this.#doConnect().finally(() => {
        this.#connectPromise = null
      })
    }

    return this.#connectPromise
  }

  async #doConnect () {
    if (this.#connectionState === 'connected' && this.#connection) {
      this.#logger.info('Already connected to RabbitMQ.')

      return this.#connection
    }

    // An explicit connect() re-enables automatic reconnection after a
    // previous disconnect() (which sets #isShuttingDown to stop it).
    this.#isShuttingDown = false

    this.#setConnectionState('connecting')

    for (let attempt = 0; attempt < this.#endpoints.length; attempt++) {
      try {
        const endpoint = this.#endpoints[this.#currentEndpointIndex]

        const connection = await amqp.connect(this.#buildConnectionString(endpoint), {
          clientProperties: {
            connection_name: this.#connectionName
          }
        })

        // disconnect() cannot cancel a dial already in flight, so the dial
        // checks back on its way in: landing after a shutdown would install a
        // live connection nobody owns (the facade has already torn its pool
        // down) and flip the state machine back to 'connected'.
        if (this.#isShuttingDown) {
          await connection.close().catch(() => {})
          this.#setConnectionState('disconnected')

          return null
        }

        this.#connection = connection

        this.#connection.on('error', (err) => {
          this.#logger.error(`Connection error: ${err.message}`)
        })

        this.#connection.on('close', () => {
          if (!this.#isShuttingDown) {
            this.#logger.error('RabbitMQ connection closed unexpectedly.')
            this.#connection = null
            this.startReconnection()
          }
        })

        this.#logger.info(`Successfully connected to RabbitMQ cluster node: ${endpoint}`)
        this.#clearReconnectTimeout()
        this.#isReconnecting = false
        this.#reconnectAttempt = 0
        this.#setConnectionState('connected')
        this.#safeEmit('connected', endpoint)

        return this.#connection
      } catch (err) {
        this.#logger.error(`Failed to connect to RabbitMQ cluster node ${this.#endpoints[this.#currentEndpointIndex]}: ${err.message}`)
        this.#currentEndpointIndex = (this.#currentEndpointIndex + 1) % this.#endpoints.length
      }
    }

    this.#connection = null

    if (!this.#isShuttingDown) {
      this.startReconnection()
    } else {
      this.#setConnectionState('disconnected')
    }

    return null
  }

  startReconnection () {
    if (this.#isShuttingDown) return

    if (!this.#isReconnecting) {
      this.#isReconnecting = true
      this.#setConnectionState('reconnecting')
      this.#safeEmit('disconnected')
    }

    this.#scheduleReconnect()
  }

  // No shutdown guard: startReconnection, the only caller, checks the same
  // flag one synchronous statement earlier.
  #scheduleReconnect () {
    this.#clearReconnectTimeout()

    if (this.#maxReconnectAttempts !== Infinity && this.#reconnectAttempt >= this.#maxReconnectAttempts) {
      this.#logger.error(`Max reconnect attempts (${this.#maxReconnectAttempts}) reached. Giving up.`)
      this.#isReconnecting = false
      this.#setConnectionState('failed')
      this.#safeEmit('reconnectFailed')

      return
    }

    const delay = Math.min(
      this.#reconnectInterval * Math.pow(2, Math.min(this.#reconnectAttempt, 4)),
      this.#maxReconnectInterval
    )

    if (this.#maxReconnectAttempts === Infinity) {
      this.#logger.info(`Connection attempt ${this.#reconnectAttempt + 1} in ${delay}ms (will try indefinitely)`)
    } else {
      this.#logger.info(`Reconnection attempt ${this.#reconnectAttempt + 1}/${this.#maxReconnectAttempts} in ${delay}ms`)
    }

    this.#reconnectTimeout = this.#clock.setTimeout(() => {
      // Detached: nothing awaits the timer callback, so an unexpected
      // rejection here would be an unhandled rejection that kills the process
      // instead of just failing one attempt.
      this.#attemptReconnect().catch((error) => {
        this.#logger.error(`Reconnection attempt failed unexpectedly: ${describeError(error)}`)
      })
    }, delay)
  }

  async #attemptReconnect () {
    if (this.#connectionState === 'connected') return

    this.#reconnectAttempt++

    const connection = await this.connect()

    if (connection) {
      this.#safeEmit('reconnected')
    }
  }

  #setConnectionState (state) {
    if (this.#connectionState === state) return

    this.#connectionState = state
    this.#safeEmit('connectionStateChanged', state)
  }

  async disconnect () {
    this.#isShuttingDown = true
    this.#isReconnecting = false
    this.#clearReconnectTimeout()

    if (this.#connectionState === 'disconnected') {
      this.#logger.info('Already disconnected from RabbitMQ.')

      return
    }

    this.#setConnectionState('disconnecting')

    if (this.#connection) {
      try {
        this.#connection.removeAllListeners('error')
        this.#connection.removeAllListeners('close')

        await this.#connection.close()

        this.#logger.info('RabbitMQ connection closed gracefully.')
      } catch (err) {
        this.#logger.info('RabbitMQ connection closed.')
      } finally {
        this.#connection = null
      }
    }

    this.#setConnectionState('disconnected')
  }

  getConnection () {
    return this.#connection
  }

  getConnectionState () {
    return this.#connectionState
  }

  getCurrentEndpoint () {
    return this.#endpoints[this.#currentEndpointIndex]
  }

  getAllEndpoints () {
    return this.#endpoints
  }
}

export { RabbitMQConnection }
export default RabbitMQConnection
