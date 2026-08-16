import describeError from '../utils/describe-error.js'
import { publishConfirmed, publishWatched } from './routable-publish.js'
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
    return publishConfirmed(channel, exchange, routingKey, content, options)
  }

  // A publish the broker cannot route is DROPPED IN SILENCE: the confirm still
  // arrives, so the caller is told everything went fine while the message went
  // nowhere. `mandatory` makes the broker hand it back as a basic.return
  // instead, and this turns that into an error the caller can act on.
  async publishRoutable (channel, exchangeName, routingKey, content, options) {
    if (!options.mandatory) {
      return this.publishOnChannel(channel, exchangeName, routingKey, content, options)
    }

    const returned = await publishWatched(channel, exchangeName, routingKey, content, options)

    if (returned) {
      const error = new Error(`Message to '${routingKey}' was returned by the broker: no queue is bound for that routing key on exchange '${exchangeName}'`)
      error.code = 'UNROUTABLE'

      throw error
    }
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

      await this.publishRoutable(channel, exchange.name, routingKey, content, this.buildOptions(options, compressed))

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

    // Retries republish ONLY what the broker has not confirmed. Resending the
    // whole batch because one message was nacked delivered every already
    // confirmed message again — up to three times by default — so consumers
    // saw duplicates of messages that never failed. Encoding is memoized for
    // the same reason: the payloads cannot change between attempts.
    let prepared = null
    const unconfirmed = new Set(messages.keys())

    const batchOperation = async () => {
      const channel = await this.getChannel()

      prepared ??= await Promise.all(messages.map(message => this.codec.encode(message)))

      const attempts = [...unconfirmed].map(async (index) => {
        const { content, compressed } = prepared[index]

        await this.publishRoutable(channel, exchange.name, routingKey, content, this.buildOptions(options, compressed))

        unconfirmed.delete(index)
      })

      const results = await Promise.allSettled(attempts)
      const failed = results.find(result => result.status === 'rejected')

      if (failed) throw failed.reason

      this.logger.info(`Batch of ${messages.length} messages published to ${routingKey}`)
    }

    return this.runProtected(this.publishPolicy(options), batchOperation)
  }

  // Fire-and-forget cannot honor `mandatory`: detecting the broker's
  // basic.return requires waiting on the confirm, which is exactly what this
  // path exists to skip. Accepting the flag and dropping its result would be
  // the silent loss the option promises to prevent, so it is refused loudly.
  #rejectMandatory (options, method) {
    if (options.mandatory) {
      throw new Error(`The 'mandatory' option needs a broker confirm to report the return — ${method} is fire-and-forget. Use publish(), publishBatch() or publishDelayed() instead.`)
    }
  }

  async publishAsync (routingKey, message, options = {}) {
    this.#rejectMandatory(options, 'publishAsync')

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
    this.#rejectMandatory(options, 'publishAsyncBatch')

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

    // Number.isFinite is type-strict (no coercion), so it already rejects
    // every non-number — a separate typeof check would be dead weight.
    if (!Number.isFinite(delayMs) || delayMs < 0) {
      throw new Error('Delay must be a non-negative number of milliseconds')
    }

    await this.preflight(exchange, routingKey, options, `delayed:${routingKey}`, options.rateLimitCost ?? 1)

    const publishOperation = async () => {
      const channel = await this.getChannel()
      const { content, compressed } = await this.codec.encode(message)

      await this.publishRoutable(channel, this.delayExchange, routingKey, content, this.buildOptions(options, compressed, { 'x-delay': delayMs }))

      this.logger.debug?.(`Delayed message published (${delayMs}ms) and confirmed`)
    }

    return this.runProtected(this.publishPolicy(options), publishOperation)
  }
}

export { Publisher }
export default Publisher
