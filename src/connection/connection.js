import amqp from 'amqplib'
import { EventEmitter } from 'node:events'

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
    this.#maxReconnectAttempts = options.maxReconnectAttempts || Infinity
    this.#connectionState = 'disconnected'
    this.#currentEndpointIndex = 0
    this.#isShuttingDown = false
    this.#isReconnecting = false
    this.#connectPromise = null

    if (!['amqp', 'amqps'].includes(this.#protocol)) {
      throw new Error('Invalid protocol. Must be one of: amqp, amqps')
    }

    if (this.#endpoints.length === 0 || this.#endpoints.some(endpoint => !endpoint)) {
      throw new Error('At least one valid RabbitMQ endpoint must be provided')
    }
  }

  #clearReconnectTimeout () {
    // No guard: clearTimeout tolerates null, and re-nulling is a no-op.
    clearTimeout(this.#reconnectTimeout)
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

        this.#connection = await amqp.connect(this.#buildConnectionString(endpoint), {
          clientProperties: {
            connection_name: this.#connectionName
          }
        })

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
        this.emit('connected', endpoint)

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
      this.emit('disconnected')
    }

    this.#scheduleReconnect()
  }

  #scheduleReconnect () {
    if (this.#isShuttingDown) return

    this.#clearReconnectTimeout()

    if (this.#maxReconnectAttempts !== Infinity && this.#reconnectAttempt >= this.#maxReconnectAttempts) {
      this.#logger.error(`Max reconnect attempts (${this.#maxReconnectAttempts}) reached. Giving up.`)
      this.#isReconnecting = false
      this.#setConnectionState('failed')
      this.emit('reconnectFailed')

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

    this.#reconnectTimeout = setTimeout(() => {
      this.#attemptReconnect()
    }, delay)
  }

  async #attemptReconnect () {
    if (this.#connectionState === 'connected') return

    this.#reconnectAttempt++

    const connection = await this.connect()

    if (connection) {
      this.emit('reconnected')
    }
  }

  #setConnectionState (state) {
    if (this.#connectionState === state) return

    this.#connectionState = state
    this.emit('connectionStateChanged', state)
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
