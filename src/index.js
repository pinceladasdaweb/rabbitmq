import Rpc from './messaging/rpc.js'
import Logger from './utils/logger.js'
import { EventEmitter } from 'node:events'
import systemClock from './utils/clock.js'
import _NodeCache from '@cacheable/node-cache'
import Topology from './messaging/topology.js'
import emitSafely from './utils/emit-safely.js'
import Publisher from './messaging/publisher.js'
import describeError from './utils/describe-error.js'
import RateLimiter from './resilience/rate-limiter.js'
import ChannelPool from './connection/channel-pool.js'
import MessageCodec from './messaging/message-codec.js'
import RabbitMQConnection from './connection/connection.js'
import CircuitBreaker from './resilience/circuit-breaker.js'
import ConsumerManager from './consumers/consumer-manager.js'

const NodeCache = _NodeCache.default || _NodeCache

class RabbitMQ extends EventEmitter {
  #connection
  #logger
  #exchange
  #channelPool
  #channelPoolSize
  #channelRecoveryInterval
  #codec
  #circuitBreaker
  #cache
  #useCache
  #rateLimiter
  #publisher
  #consumers
  #topology
  #rpc
  #shutdownHandlersInstalled
  #connectPromise
  #restorePromise
  #clock

  constructor (options = {}) {
    super()

    this.#logger = options.logger || Logger
    this.#exchange = options.exchange || {}
    this.#channelPool = null
    this.#channelPoolSize = options.channelPoolSize || 10
    // Left undefined when unset: ChannelPool owns the default, and repeating
    // it here would be a second source of truth that no test could tell apart.
    this.#channelRecoveryInterval = options.channelRecoveryInterval
    this.#useCache = options.useCache || false
    this.#shutdownHandlersInstalled = false
    this.#connectPromise = null
    this.#restorePromise = null
    // One clock for every time-dependent component (rate limiter sweeps,
    // sequential staleness, recovery backoffs, RPC deadlines). Injectable so
    // tests drive all of them from a single fake without sleeping.
    this.#clock = options.clock ?? systemClock

    this.#codec = new MessageCodec({
      serializer: options.serializer,
      deserializer: options.deserializer,
      useCompression: options.useCompression,
      compressionThreshold: options.compressionThreshold,
      logger: this.#logger
    })

    this.#circuitBreaker = new CircuitBreaker(options.circuitBreaker)
    this.#circuitBreaker.on('stateChanged', (state) => {
      // Contained: the breaker transitions state DURING a publish, so a
      // throwing listener would surface as that publish failing.
      this.#safeEmit('circuitBreakerStateChanged', state)
    })

    if (this.#useCache) {
      this.#cache = new NodeCache({
        // ?? and not ||: cacheTTL 0 means "never expire" to node-cache, a
        // legitimate request that || silently rewrote to 60 seconds.
        stdTTL: options.cacheTTL ?? 60,
        checkperiod: options.cacheCheckPeriod || 120,
        ...options.cacheOptions
      })
    }

    if (options.rateLimiter) {
      this.#rateLimiter = new RateLimiter({
        clock: this.#clock,
        ...options.rateLimiter,
        logger: this.#logger
      })

      this.#setupRateLimiterEvents()
    }

    this.#connection = new RabbitMQConnection({
      username: options.username || process.env.RABBITMQ_USER,
      password: options.password || process.env.RABBITMQ_PASS,
      protocol: options.protocol,
      vhost: options.vhost,
      endpoints: Array.isArray(options.endpoints) ? options.endpoints : [options.endpoint || process.env.RABBITMQ_ENDPOINT],
      connectionName: options.connectionName || 'default_connection',
      reconnectInterval: options.reconnectInterval,
      maxReconnectInterval: options.maxReconnectInterval,
      maxReconnectAttempts: options.maxReconnectAttempts
    }, this.#logger)

    this.#connection.on('connected', () => this.emit('connected'))
    this.#connection.on('reconnected', this.#handleReconnection.bind(this))
    this.#connection.on('disconnected', this.#handleDisconnection.bind(this))
    this.#connection.on('reconnectFailed', () => this.emit('reconnectFailed'))

    const context = {
      logger: this.#logger,
      codec: this.#codec,
      clock: this.#clock,
      circuitBreaker: this.#circuitBreaker,
      rateLimiter: this.#rateLimiter,
      maxPriority: options.maxPriority || 10,
      prefetchCount: options.prefetchCount ?? 10,
      consumerRecoveryInterval: options.consumerRecoveryInterval,
      consumerDrainTimeout: options.consumerDrainTimeout,
      deadLetterExchange: options.deadLetterExchange || 'dlx',
      delayExchange: options.delayExchange || 'delayed',
      getExchange: () => this.#exchange,
      getChannel: () => this.getChannel(),
      getChannelPool: () => this.#channelPool,
      getQueueNameByConsumerTag: (consumerTag) => this.#consumers?.findQueueNameByTag(consumerTag) ?? null,
      emit: (event, payload) => this.emit(event, payload),
      // The consume pipeline skips building per-message event payloads when
      // nobody subscribed to them — one integer compare instead of an
      // allocation per delivery.
      listenerCount: (event) => this.listenerCount(event)
    }

    this.#publisher = new Publisher(context)
    this.#consumers = new ConsumerManager(context)
    this.#topology = new Topology(context)
    this.#rpc = new Rpc(context, { publisher: this.#publisher, consumers: this.#consumers })
  }

  #setupRateLimiterEvents () {
    this.#rateLimiter.on('limited', ({ key, strategy }) => {
      this.#logger.warn(`Rate limit exceeded for ${key} using ${strategy} strategy`)
      this.emit('rateLimited', { key, strategy })
    })

    this.#rateLimiter.on('blocked', ({ key, remainingTime }) => {
      this.#logger.warn(`Key ${key} is blocked. Remaining time: ${remainingTime}ms`)
      this.emit('rateBlocked', { key, remainingTime })
    })
  }

  // Deprecated 1.5-era name. Removing it shipped in a minor (1.6.0) and broke
  // callers at boot — the shim restores them and warns once per process
  // toward the real name.
  setupGracefulShutdown (options) {
    this.#logger.warn('setupGracefulShutdown() is deprecated; use enableGracefulShutdown()')

    return this.enableGracefulShutdown(options)
  }

  enableGracefulShutdown ({ signals = ['SIGINT', 'SIGTERM'], exitProcess = true } = {}) {
    if (this.#shutdownHandlersInstalled) return

    this.#shutdownHandlersInstalled = true

    const gracefulShutdown = async (signal) => {
      this.#logger.info(`Received ${signal}. Starting graceful shutdown...`)

      try {
        await this.disconnect()
        this.#logger.info('Disconnected from RabbitMQ successfully.')

        if (exitProcess) process.exit(0)
      } catch (error) {
        this.#logger.error(`Error during graceful shutdown: ${error.message}`)

        if (exitProcess) process.exit(1)
      }
    }

    for (const signal of signals) {
      process.once(signal, () => gracefulShutdown(signal))
    }
  }

  // Everything that must be rebuilt on top of a fresh connection. Both entry
  // points funnel through here — the automatic reconnection AND an explicit
  // connect() that lands while the previous connection is gone — because
  // rebuilding the pool without recreating consumers leaves publishing healthy
  // while every consumer stays silently dead on channels of the old
  // connection.
  //
  // Concurrent callers share one restore. Both recovery paths can be waiting on
  // the same in-flight dial and resume together, in either order:
  //   - the timer's attempt first: it starts the restore, and #doConnect then
  //     sees a pool and skips;
  //   - the manual connect() first: it starts the restore, and the timer's
  //     'reconnected' reaches #handleReconnection, which has no pool check at
  //     all and would restore again.
  // The second ordering is why the caller-side `if (!this.#channelPool)` guard
  // is not enough on its own. Two overlapping recreateAll() runs issue
  // channel.consume twice per consumer, so every message is delivered twice.
  async #restoreState () {
    if (!this.#restorePromise) {
      const restore = this.#doRestoreState().finally(() => {
        // Only release the slot if this restore still owns it: a stale restore
        // (one #handleDisconnection already dropped) finishing late must not
        // clear a newer one, or the next caller would start a second restore
        // alongside it and duplicate every consumer.
        //
        // NOT covered by a test: reaching it needs a third #restoreState()
        // caller while a restore is in flight, and today there is none —
        // #handleReconnection needs a disconnection first (which drops the slot
        // anyway) and #doConnect is gated by the pool the in-flight restore
        // already assigned. Kept because the check costs three lines while the
        // failure it prevents is silent duplicate processing.
        if (this.#restorePromise === restore) {
          this.#restorePromise = null
        }
      })

      this.#restorePromise = restore
    }

    return this.#restorePromise
  }

  // The whole restore is one unit: #doConnect gates on `!this.#channelPool`,
  // so "pool present" has to mean "state fully restored". Leaving the pool
  // behind after a later step failed satisfied that gate with the exchange
  // unasserted and the consumers never recreated — a retried connect()
  // resolved as a success onto a half-dead instance. On failure the pool goes
  // back to null so the next attempt genuinely retries.
  async #doRestoreState () {
    let installedPool = null

    try {
      installedPool = await this.#setupChannelPool()

      await this.#topology.ensureExchange()
      await this.#consumers.recreateAll()
    } catch (error) {
      // Only tear down the pool THIS restore installed: a stale restore
      // failing late (the flapping-broker case, fenced inside
      // #setupChannelPool) must not clear the pool a newer restore already
      // put in place — same ownership discipline as the fence itself.
      if (installedPool && this.#channelPool === installedPool) {
        this.#channelPool = null
        // ChannelPool.close() swallows per-channel teardown failures by
        // contract (pinned by its own test), so this cannot reject and mask
        // the restore error we are about to rethrow.
        await installedPool.close()
      }

      throw error
    }

    // Failures accumulated against the previous connection say nothing
    // about the fresh one — do not keep publishing blocked after recovery.
    this.#circuitBreaker.reset()
  }

  // Lifecycle notifications sit inside recovery paths where a listener's crash
  // must not be mistaken for a failure of the step it follows, and where
  // nothing is waiting to catch it: this handler is invoked by the
  // connection's emit, so an escaping throw would unwind the state machine
  // that called us.
  // Per listener, so a throwing application handler cannot starve the internal
  // ones registered after it — connect({ waitForConnection }) parks on exactly
  // these events (see emitSafely).
  #safeEmit (event, ...args) {
    emitSafely(this, event, args, this.#logger)
  }

  async #handleReconnection () {
    try {
      await this.#restoreState()
    } catch (error) {
      this.#logger.error(`Failed to restore state after reconnection: ${error.message}`)
      this.#safeEmit('reconnectError', error)

      return
    }

    // Outside the try, deliberately: inside it, a throwing 'reconnected'
    // listener was caught by the handler above and reported as
    // 'reconnectError' — rejecting an in-flight connect({ waitForConnection })
    // with "failed to restore state" on a client that had just recovered
    // perfectly. State was restored; only the notification failed.
    this.#safeEmit('reconnected')
  }

  #handleDisconnection () {
    // Direct reply-to routes died with the connection: settle in-flight RPC
    // requests now instead of leaving them to hit their timeouts.
    this.#rpc.handleConnectionLoss()

    // A restore still in flight was building on the connection that just died,
    // so its result says nothing about the next one. Dropping it here stops the
    // next recovery from joining it and concluding that state was restored when
    // no pool or consumer exists — which leaves publishing broken until yet
    // another disconnection happens to come along.
    this.#restorePromise = null

    if (this.#channelPool) {
      // Marks the pool as closed so channel-replacement retry loops stop;
      // closing dead channels is best-effort.
      const staleChannelPool = this.#channelPool

      this.#channelPool = null
      staleChannelPool.close()
    }

    // Contained: this runs synchronously inside the connection's own
    // 'disconnected' emit, which has just set #isReconnecting and is about to
    // arm the retry timer. A throwing app listener must not reach it.
    this.#safeEmit('disconnected')
  }

  async #setupChannelPool () {
    const connection = this.#connection.getConnection()

    if (!connection) {
      throw new Error('No active connection to RabbitMQ')
    }

    // Assigned only AFTER a successful initialize: a half-built pool left in
    // the field would satisfy #doConnect's `if (!this.#channelPool)` gate and
    // silently skip the restore a manual retry is asking for — leaving
    // publishing and consumers broken until the next disconnection.
    const channelPool = new ChannelPool(connection, {
      logger: this.#logger,
      size: this.#channelPoolSize,
      recoveryInterval: this.#channelRecoveryInterval,
      clock: this.#clock
    })

    await channelPool.initialize()

    // The connection can turn over while channels were being created (a
    // flapping broker): a pool built on the dead connection must not clobber
    // the one the newer recovery installed.
    if (this.#connection.getConnection() !== connection) {
      await channelPool.close()

      throw new Error('Connection changed while the channel pool was being built')
    }

    this.#channelPool = channelPool

    return channelPool
  }

  // Concurrent connect() callers share a single in-flight attempt: two
  // parallel setups would each build a channel pool and leak the loser's
  // channels on the broker.
  async connect (options = {}) {
    if (!this.#connectPromise) {
      this.#connectPromise = this.#doConnect(options).finally(() => {
        this.#connectPromise = null
      })
    }

    return this.#connectPromise
  }

  async #doConnect (options) {
    const connection = await this.#connection.connect()

    if (connection) {
      // No pool means this connection is new to the facade: either the very
      // first connect (where recreating consumers and resetting the breaker are
      // no-ops) or a manual connect() that beat the automatic reconnection
      // timer to it — which the 'reconnected' event never covers, since that
      // fires only from the timer path.
      if (!this.#channelPool) {
        await this.#restoreState()
      }

      return connection
    }

    if (!options.waitForConnection) {
      return null
    }

    // All endpoints failed and reconnection keeps running in the background:
    // wait for the next successful cycle (or for reconnection to give up).
    return new Promise((resolve, reject) => {
      let timer = null

      const abort = (error) => {
        cleanup()
        reject(error)
      }

      const cleanup = () => {
        this.off('reconnected', onReconnected)
        this.off('reconnectFailed', onReconnectFailed)
        this.off('reconnectError', onReconnectError)
        this.off('disconnecting', onDisconnecting)

        if (timer) this.#clock.clearTimeout(timer)
      }

      // disconnect() stops the reconnection cycles this promise is waiting on,
      // so without an explicit abort it would never settle — and since
      // #connectPromise is only released in its finally, EVERY later connect()
      // would receive the same dead promise. One mechanism for all four
      // signals: anything else waiting on the cycle hears the shutdown too.
      const onDisconnecting = () => {
        abort(new Error('Connection wait aborted: the client was disconnected'))
      }

      const onReconnected = () => {
        cleanup()
        resolve(this.#connection.getConnection())
      }

      const onReconnectFailed = () => {
        cleanup()
        reject(new Error('Unable to connect: all reconnection attempts failed'))
      }

      // Without this, a reconnect whose post-connection setup fails would
      // leave the waitForConnection promise hanging forever ('reconnected'
      // never fires and no further reconnect cycle runs).
      const onReconnectError = (error) => {
        cleanup()
        reject(new Error(`Connected, but failed to restore state: ${error.message}`))
      }

      this.on('reconnected', onReconnected)
      this.on('reconnectFailed', onReconnectFailed)
      this.on('reconnectError', onReconnectError)
      this.on('disconnecting', onDisconnecting)

      if (options.timeout > 0) {
        timer = this.#clock.setTimeout(() => {
          cleanup()
          reject(new Error(`Timed out after ${options.timeout}ms waiting for connection`))
        }, options.timeout)
      }
    })
  }

  async disconnect () {
    // Fired before any teardown, unlike 'disconnected' (which also fires on
    // transient losses): this is the explicit-shutdown signal. Whoever is
    // parked in connect({ waitForConnection }) is waiting on a reconnection
    // cycle this shutdown is about to end.
    //
    // Contained, and it matters most here: this emit sits BEFORE the teardown,
    // so a throwing listener used to abort the whole shutdown — no RPC
    // settled, no consumer disposed, and the connection left open.
    this.#safeEmit('disconnecting')

    try {
      this.#rpc.handleConnectionLoss('client disconnected')

      await this.#consumers.disposeAll()

      if (this.#rateLimiter) {
        this.#rateLimiter.dispose()
      }

      if (this.#cache && typeof this.#cache.close === 'function') {
        this.#cache.close()
      }

      if (this.#channelPool) {
        await this.#channelPool.close()
        this.#channelPool = null
      }

      this.#logger.info('Disconnecting from RabbitMQ...')

      await this.#connection.disconnect()

      // Contained like every other lifecycle emit: uncontained, a throwing
      // listener landed in the catch below, which logged a clean shutdown as
      // 'Error during disconnection' and dialed the teardown a second time.
      this.#safeEmit('disconnected')
    } catch (error) {
      // No message-substring filtering here: it used to swallow genuine
      // broker errors whose text happened to contain 'Channel closed'.
      // Expected channel-teardown noise is already silenced in ChannelPool.
      this.#logger.warn(`Error during disconnection: ${error.message}`)

      try {
        await this.#connection.disconnect()
      } catch (disconnectError) {
        this.#logger.warn(`Error during final disconnection attempt: ${disconnectError.message}`)
      }
    }
  }

  async getChannel () {
    if (!this.#channelPool) {
      throw new Error('Not connected to RabbitMQ. Connection establishing/recovery in progress.')
    }

    return this.#channelPool.getChannel()
  }

  getClusterStatus () {
    return {
      connectedTo: this.#connection.getCurrentEndpoint(),
      allEndpoints: this.#connection.getAllEndpoints(),
      connectionState: this.#connection.getConnectionState()
    }
  }

  setExchange (name, type = 'direct', options = {}) {
    if (typeof name !== 'string' || name.trim() === '') {
      throw new Error('Exchange name must be a non-empty string')
    }

    // No typeof arm: a non-string is never in the list, so includes() already
    // rejects it with the same error.
    if (!['direct', 'topic', 'fanout', 'headers'].includes(type)) {
      throw new Error('Invalid exchange type. Must be one of: direct, topic, fanout, headers')
    }

    this.#exchange = { name, type, options }
    this.#logger.info(`Exchange set to ${name} (${type})`)
  }

  // --- Publishing (delegated to Publisher) ---

  async publish (routingKey, message, options = {}) {
    return this.#publisher.publish(routingKey, message, options)
  }

  async publishBatch (routingKey, messages, options = {}) {
    return this.#publisher.publishBatch(routingKey, messages, options)
  }

  async publishAsync (routingKey, message, options = {}) {
    return this.#publisher.publishAsync(routingKey, message, options)
  }

  async publishAsyncBatch (routingKey, messages, options = {}) {
    return this.#publisher.publishAsyncBatch(routingKey, messages, options)
  }

  async publishDelayed (routingKey, message, delayMs, options = {}) {
    return this.#publisher.publishDelayed(routingKey, message, delayMs, options)
  }

  async publishWithCache (routingKey, messageGenerator, options = {}) {
    this.#publisher.validateRoutingKey(routingKey, this.#exchange)

    const cacheKey = this.#cacheKey(routingKey)

    if (this.#useCache) {
      const cachedMessage = this.#cache.get(cacheKey)

      if (cachedMessage !== undefined) {
        this.#logger.info(`Cache hit for key: ${cacheKey}`)

        return cachedMessage
      }
    }

    const message = typeof messageGenerator === 'function' ? await messageGenerator() : messageGenerator

    await this.publish(routingKey, message, {
      ...options,
      rateLimitKey: options.rateLimitKey || `cached:${routingKey}`,
      headers: {
        ...options.headers,
        'x-cached': true
      }
    })

    if (this.#useCache) {
      // ?? and not ||: the other half of the cacheTTL 0 problem. A per-publish
      // 0 ("never expire this entry") was silently replaced by the configured
      // default, exactly as the constructor used to do.
      const ttl = options.cacheTTL ?? this.#cache.options?.stdTTL

      this.#cache.set(cacheKey, message, ttl)
      this.#logger.info(`Cached message for key: ${cacheKey}, TTL: ${ttl}s`)
    }

    return message
  }

  // --- Request/response (RPC) over direct reply-to (delegated to Rpc) ---

  async request (routingKey, message, options = {}) {
    return this.#rpc.request(routingKey, message, options)
  }

  async respond (queueName, handler, options = {}) {
    return this.#rpc.respond(queueName, handler, options)
  }

  // --- Consumption (delegated to ConsumerManager) ---

  async subscribe (queueName, callback, options = {}) {
    return this.#consumers.subscribe(queueName, callback, options)
  }

  async subscribeWithOptimizedPrefetch (queueName, callback, options = {}) {
    return this.#consumers.subscribeWithOptimizedPrefetch(queueName, callback, options)
  }

  async subscribeParallel (queueName, processorFile, options = {}) {
    return this.#consumers.subscribeParallel(queueName, processorFile, options)
  }

  async subscribeSequential (queueName, callback, options = {}) {
    return this.#consumers.subscribeSequential(queueName, callback, options)
  }

  async unsubscribe (consumerTag) {
    return this.#consumers.unsubscribe(consumerTag)
  }

  async acknowledgeMessage (message) {
    return this.#consumers.ackMessage(message)
  }

  async negativeAcknowledgeMessage (message, options = {}) {
    return this.#consumers.nackMessage(message, options)
  }

  // --- Topology: exchanges, queues, DLQ and delay (delegated to Topology) ---

  async setupDeadLetterExchange () {
    return this.#topology.setupDeadLetterExchange()
  }

  async createQueue (queueName, options = {}) {
    return this.#topology.createQueue(queueName, options)
  }

  async moveToDeadLetter (message, reason) {
    return this.#topology.moveToDeadLetter(message, reason)
  }

  async processDeadLetterQueue (originalQueueName, processor, options = {}) {
    const deadLetterQueueName = `${originalQueueName}_dlq`

    const consumer = await this.subscribe(deadLetterQueueName, async (message) => {
      try {
        await processor(message)
      } catch (error) {
        this.#logger.error(`Error processing dead letter message: ${describeError(error)}`)

        // Rethrown so this behaves like every other subscription: swallowing
        // here made retryPolicy a silent no-op on this method alone.
        //
        // The default 'none' keeps the previous outcome. createQueue declares
        // `${queue}_dlq` with no dead letter exchange of its own, so a nack
        // without requeue discards the message exactly as the old ack did —
        // only now 'once' can buy the processor a retry, and a DLQ the caller
        // gave its own DLX routes onward instead of vanishing.
        throw error
      }
    }, options)

    this.#logger.info(`Started processing dead letter queue: ${deadLetterQueueName}`)

    return consumer
  }

  async setupDelayExchange (options = {}) {
    return this.#topology.setupDelayExchange(options)
  }

  async setupDelayPlugin () {
    return this.#topology.setupDelayPlugin()
  }

  async isDelayPluginEnabled () {
    return this.#topology.isDelayPluginEnabled()
  }

  // --- Configuration and observability ---

  setCompression (useCompression) {
    this.#codec.useCompression = useCompression
    this.#logger.info(`Message compression ${useCompression ? 'enabled' : 'disabled'}`)
  }

  setCompressionThreshold (threshold) {
    if (typeof threshold !== 'number' || threshold < 0) {
      throw new Error('Compression threshold must be a non-negative number')
    }

    this.#codec.compressionThreshold = threshold
    this.#logger.info(`Compression threshold set to ${threshold} bytes`)
  }

  setSerializer (serializer) {
    if (typeof serializer !== 'function') {
      throw new Error('Serializer must be a function')
    }

    this.#codec.serializer = serializer
    this.#logger.info('Custom serializer set')
  }

  setDeserializer (deserializer) {
    if (typeof deserializer !== 'function') {
      throw new Error('Deserializer must be a function')
    }

    this.#codec.deserializer = deserializer
    this.#logger.info('Custom deserializer set')
  }

  getCircuitBreakerState () {
    return this.#circuitBreaker.getState()
  }

  #requireRateLimiter () {
    if (!this.#rateLimiter) {
      throw new Error('Rate limiter is not enabled')
    }

    return this.#rateLimiter
  }

  #requireCache () {
    if (!this.#useCache) {
      throw new Error('Cache is not enabled')
    }

    return this.#cache
  }

  #cacheKey (routingKey) {
    return `${this.#exchange.name}:${routingKey}`
  }

  getRateLimitStatus (key) {
    return this.#requireRateLimiter().getStatus(key)
  }

  resetRateLimit (key) {
    this.#requireRateLimiter().reset(key)
  }

  blockRateLimit (key, duration) {
    return this.#requireRateLimiter().blockKey(key, duration)
  }

  async getFromCache (routingKey) {
    return this.#requireCache().get(this.#cacheKey(routingKey))
  }

  invalidateCache (routingKey) {
    this.#requireCache().del(this.#cacheKey(routingKey))
    this.#logger.info(`Cache invalidated for key: ${this.#cacheKey(routingKey)}`)
  }

  clearCache () {
    this.#requireCache().flushAll()
    this.#logger.info('Entire cache cleared')
  }
}

export { RabbitMQ }
export default RabbitMQ
