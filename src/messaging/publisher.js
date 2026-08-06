import describeError from '../utils/describe-error.js'
import { compose, exponential, isRetryExhaustedError, retry } from 'breakwater'

class Publisher {
  constructor (context) {
    this.logger = context.logger
    this.codec = context.codec
    this.circuitBreaker = context.circuitBreaker
    this.rateLimiter = context.rateLimiter
    this.maxPriority = context.maxPriority
    this.delayExchange = context.delayExchange
    this.getChannel = context.getChannel
    this.getExchange = context.getExchange
  }

  validateRoutingKey (routingKey, exchange) {
    if ((typeof routingKey !== 'string') || (!['fanout', 'headers'].includes(exchange.type) && routingKey.trim() === '')) {
      throw new Error('Invalid routing key. It must be a non-empty string.')
    }
  }

  validatePriority (options) {
    if (options.priority !== undefined && (options.priority < 0 || options.priority > this.maxPriority)) {
      throw new Error(`Invalid priority value. Must be between 0 and ${this.maxPriority}`)
    }
  }

  async applyRateLimit (key, cost) {
    if (!this.rateLimiter) return

    const canProceed = await this.rateLimiter.checkRateLimit(key, cost)

    if (!canProceed) {
      const error = new Error(`Rate limit exceeded for key: ${key}`)
      error.code = 'RATE_LIMIT_EXCEEDED'
      error.status = this.rateLimiter.getStatus(key)

      throw error
    }
  }

  // Shared pre-publish pipeline: validation, fail-fast connection probe and
  // rate limiting. The probe runs BEFORE tokens are consumed and OUTSIDE the
  // circuit breaker, so publishing while disconnected neither drains the
  // rate limit nor trips the breaker — the reconnection state machine
  // already owns that failure mode.
  async preflight (exchange, routingKey, options, defaultRateKey, cost) {
    this.validateRoutingKey(routingKey, exchange)
    this.validatePriority(options)

    await this.getChannel()
    await this.applyRateLimit(options.rateLimitKey ?? defaultRateKey, cost)
  }

  buildOptions (options, compressed, extraHeaders) {
    return {
      persistent: true,
      ...options,
      headers: {
        ...options.headers,
        'x-compressed': compressed,
        ...extraHeaders
      }
    }
  }

  publishOnChannel (channel, exchange, routingKey, content, options) {
    return new Promise((resolve, reject) => {
      channel.publish(exchange, routingKey, content, options, (err) => {
        if (err) {
          reject(new Error(`Message was not confirmed by the broker: ${err.message}`))
        } else {
          resolve()
        }
      })
    })
  }

  async publishFireAndForget (channel, exchangeName, routingKey, content, options) {
    const keepGoing = channel.publish(exchangeName, routingKey, content, options)

    if (!keepGoing) {
      // Waiting only for 'drain' would hang forever if the channel dies with
      // a full write buffer — settle on close/error as well.
      await new Promise((resolve, reject) => {
        const onDrain = () => {
          cleanup()
          resolve()
        }

        const onClose = () => {
          cleanup()
          reject(new Error('Channel closed while waiting for drain'))
        }

        const onError = (error) => {
          cleanup()
          reject(error instanceof Error ? error : new Error('Channel error while waiting for drain'))
        }

        const cleanup = () => {
          channel.off('drain', onDrain)
          channel.off('close', onClose)
          channel.off('error', onError)
        }

        channel.once('drain', onDrain)
        channel.once('close', onClose)
        channel.once('error', onError)
      })
    }
  }

  // Builds the per-call resilience pipeline: retry sits OUTSIDE the breaker,
  // so every attempt feeds the breaker's stats individually and an open
  // circuit stops the retry cycle immediately (CircuitOpenError is not
  // retryable) instead of sleeping through backoff against a dead broker.
  publishPolicy (options = {}) {
    // The operation must run at least once: maxRetries <= 0 would otherwise
    // resolve without ever publishing.
    const attempts = Math.max(1, options.maxRetries ?? 3)
    const initial = options.retryDelay ?? 1000

    const retryPolicy = retry({
      attempts,
      backoff: exponential({ initial, factor: 2, max: Infinity, jitter: 'none' })
    })

    retryPolicy.on('retry', ({ attempt, error }) => {
      // describeError, not error.message: the failing operation runs the
      // user's serializer (codec.encode), which can throw a non-Error — a
      // crash here is swallowed by breakwater and silently kills this log.
      this.logger.warn(`Operation failed, retrying (${attempt}/${attempts}): ${describeError(error)}`)
    })

    return compose(retryPolicy, this.circuitBreaker.policy)
  }

  // Callers of this library receive the last real error, not the retry
  // envelope breakwater throws when every attempt failed.
  async runProtected (policy, operation) {
    try {
      return await policy.execute(operation)
    } catch (error) {
      throw isRetryExhaustedError(error) ? error.cause : error
    }
  }

  async publish (routingKey, message, options = {}) {
    const exchange = this.getExchange()

    await this.preflight(exchange, routingKey, options, routingKey, options.rateLimitCost ?? 1)

    const publishOperation = async () => {
      const channel = await this.getChannel()
      const { content, compressed } = await this.codec.encode(message)

      await this.publishOnChannel(channel, exchange.name, routingKey, content, this.buildOptions(options, compressed))

      this.logger.debug?.('Message published and confirmed')
    }

    return this.runProtected(this.publishPolicy(options), publishOperation)
  }

  async publishBatch (routingKey, messages, options = {}) {
    const exchange = this.getExchange()

    if (!Array.isArray(messages) || messages.length === 0) {
      throw new Error('Messages must be a non-empty array.')
    }

    await this.preflight(exchange, routingKey, options, routingKey, messages.length * (options.rateLimitCost ?? 1))

    const batchOperation = async () => {
      const channel = await this.getChannel()
      const preparedMessages = await Promise.all(messages.map(message => this.codec.encode(message)))

      await Promise.all(preparedMessages.map(({ content, compressed }) =>
        this.publishOnChannel(channel, exchange.name, routingKey, content, this.buildOptions(options, compressed))
      ))

      this.logger.info(`Batch of ${messages.length} messages published to ${routingKey}`)
    }

    return this.runProtected(this.publishPolicy(options), batchOperation)
  }

  async publishAsync (routingKey, message, options = {}) {
    const exchange = this.getExchange()

    await this.preflight(exchange, routingKey, options, `async:${routingKey}`, options.rateLimitCost ?? 1)

    const publishOperation = async () => {
      const channel = await this.getChannel()
      const { content, compressed } = await this.codec.encode(message)

      await this.publishFireAndForget(channel, exchange.name, routingKey, content, this.buildOptions(options, compressed, { 'x-async': true }))
    }

    try {
      await this.circuitBreaker.execute(publishOperation)
    } catch (error) {
      // describeError: reading .message on a non-Error thrown by the user's
      // serializer would replace the caller's error with a TypeError.
      this.logger.error(`Failed to publish message asynchronously: ${describeError(error)}`)

      throw error
    }
  }

  async publishAsyncBatch (routingKey, messages, options = {}) {
    const exchange = this.getExchange()

    if (!Array.isArray(messages) || messages.length === 0) {
      throw new Error('Messages must be a non-empty array.')
    }

    await this.preflight(exchange, routingKey, options, `async-batch:${routingKey}`, messages.length * (options.rateLimitCost ?? 1))

    const publishOperation = async () => {
      const channel = await this.getChannel()
      const preparedMessages = await Promise.all(messages.map(message => this.codec.encode(message)))

      for (const { content, compressed } of preparedMessages) {
        await this.publishFireAndForget(channel, exchange.name, routingKey, content, this.buildOptions(options, compressed, { 'x-async-batch': true }))
      }

      this.logger.info(`Batch of ${messages.length} messages published asynchronously.`)
    }

    try {
      await this.circuitBreaker.execute(publishOperation)
    } catch (error) {
      // Same non-Error tolerance as publishAsync.
      this.logger.error(`Failed to publish batch asynchronously: ${describeError(error)}`)

      throw error
    }
  }

  async publishDelayed (routingKey, message, delayMs, options = {}) {
    const exchange = this.getExchange()

    if (typeof delayMs !== 'number' || !Number.isFinite(delayMs) || delayMs < 0) {
      throw new Error('Delay must be a non-negative number of milliseconds')
    }

    await this.preflight(exchange, routingKey, options, `delayed:${routingKey}`, options.rateLimitCost ?? 1)

    const publishOperation = async () => {
      const channel = await this.getChannel()
      const { content, compressed } = await this.codec.encode(message)

      await this.publishOnChannel(channel, this.delayExchange, routingKey, content, this.buildOptions(options, compressed, { 'x-delay': delayMs }))

      this.logger.debug?.(`Delayed message published (${delayMs}ms) and confirmed`)
    }

    return this.runProtected(this.publishPolicy(options), publishOperation)
  }
}

export { Publisher }
export default Publisher
