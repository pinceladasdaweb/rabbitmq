import { randomUUID } from 'node:crypto'

// RabbitMQ's direct reply-to pseudo-queue: consuming from it (noAck) turns the
// consumer's channel into a private, zero-declaration reply route.
const DIRECT_REPLY_QUEUE = 'amq.rabbitmq.reply-to'

const DEFAULT_TIMEOUT = 30000

class Rpc {
  constructor (context, { publisher, consumers }) {
    this.logger = context.logger
    this.codec = context.codec
    this.circuitBreaker = context.circuitBreaker
    this.getExchange = context.getExchange
    this.getChannel = context.getChannel
    this.getChannelPool = context.getChannelPool
    this.publisher = publisher
    this.consumers = consumers
    this.pendingRequests = new Map()
    this.replyChannel = null
    this.replySetupPromise = null
  }

  #rpcError (code, message) {
    const error = new Error(message)
    error.code = code

    return error
  }

  #settlePending (correlationId, settle) {
    const pending = this.pendingRequests.get(correlationId)

    if (!pending) return false

    this.pendingRequests.delete(correlationId)
    clearTimeout(pending.timer)
    settle(pending)

    return true
  }

  // Direct reply-to routes are connection-scoped and cannot survive a
  // reconnect: in-flight requests lost their way back, so they are rejected
  // immediately instead of silently never resolving — the caller decides
  // whether to retry. The reply consumer itself is recreated lazily by the
  // next request().
  handleConnectionLoss (reason = 'connection to RabbitMQ lost') {
    this.replyChannel = null
    this.rejectAllPending(reason)
  }

  rejectAllPending (reason) {
    for (const [, pending] of this.pendingRequests.entries()) {
      clearTimeout(pending.timer)
      pending.reject(this.#rpcError('RPC_CONNECTION_LOST', `RPC request aborted: ${reason}`))
    }

    this.pendingRequests.clear()
  }

  #invalidateReplyChannel (channel, reason) {
    if (this.replyChannel !== channel) return

    this.replyChannel = null
    this.rejectAllPending(reason)
  }

  // Concurrent first requests share a single consumer setup: two parallel
  // setups would consume the pseudo-queue twice on different channels and
  // strand one set of pending requests.
  async #ensureReplyConsumer () {
    if (this.replyChannel) return this.replyChannel

    if (!this.replySetupPromise) {
      this.replySetupPromise = this.#setupReplyConsumer().finally(() => {
        this.replySetupPromise = null
      })
    }

    return this.replySetupPromise
  }

  async #setupReplyConsumer () {
    const channelPool = this.getChannelPool()

    if (!channelPool) {
      throw new Error('Not connected to RabbitMQ. Connection establishing/recovery in progress.')
    }

    const channel = await channelPool.getDedicatedChannel('rpc-reply')

    await channel.consume(DIRECT_REPLY_QUEUE, (msg) => {
      if (!msg) {
        this.#invalidateReplyChannel(channel, 'reply consumer cancelled by the broker')

        return
      }

      this.#handleReply(msg)
    }, { noAck: true })

    channel.on('close', () => {
      this.#invalidateReplyChannel(channel, 'reply channel closed')
    })

    this.replyChannel = channel

    return channel
  }

  async #handleReply (msg) {
    const correlationId = msg.properties.correlationId
    const pending = correlationId ? this.pendingRequests.get(correlationId) : null

    if (!pending) {
      // Most likely a reply that arrived after its request already timed out.
      this.logger.debug?.(`Discarding RPC reply with unknown correlationId: ${correlationId}`)

      return
    }

    this.pendingRequests.delete(correlationId)
    clearTimeout(pending.timer)

    try {
      const isCompressed = Boolean(msg.properties.headers && msg.properties.headers['x-compressed'])
      const content = await this.codec.decode(msg.content, isCompressed)

      if (msg.properties.headers && msg.properties.headers['x-rpc-error']) {
        pending.reject(this.#rpcError('RPC_RESPONDER_ERROR', content?.message || 'RPC responder failed'))

        return
      }

      pending.resolve(content)
    } catch (error) {
      // A reply that cannot be decoded fails the request right away instead
      // of leaving it to run into its timeout.
      pending.reject(error)
    }
  }

  async request (routingKey, message, options = {}) {
    const exchange = this.getExchange()
    const timeout = options.timeout ?? DEFAULT_TIMEOUT

    if (typeof timeout !== 'number' || !Number.isFinite(timeout) || timeout <= 0) {
      throw new Error('RPC timeout must be a positive number of milliseconds')
    }

    await this.publisher.preflight(exchange, routingKey, options, `rpc:${routingKey}`, options.rateLimitCost ?? 1)

    const channel = await this.#ensureReplyConsumer()
    const correlationId = randomUUID()
    const { content, compressed } = await this.codec.encode(message)

    // Requests default to transient with a per-message TTL matching the
    // requester's timeout: a request nobody picked up in time has no business
    // being processed after its caller already gave up.
    const publishOptions = this.publisher.buildOptions({
      persistent: false,
      expiration: String(Math.ceil(timeout)),
      ...options
    }, compressed)

    publishOptions.correlationId = correlationId
    publishOptions.replyTo = DIRECT_REPLY_QUEUE

    // Registered BEFORE publishing so a reply cannot outrun the bookkeeping.
    const responsePromise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(correlationId)
        reject(this.#rpcError('RPC_TIMEOUT', `RPC request to ${routingKey} timed out after ${timeout}ms`))
      }, timeout)

      // An in-flight RPC must never keep the process alive on its own.
      timer.unref?.()

      this.pendingRequests.set(correlationId, { resolve, reject, timer })
    })

    try {
      // The request MUST go out on the same channel that consumes the direct
      // reply-to pseudo-queue — that is what scopes the reply route back to
      // this consumer.
      await this.circuitBreaker.execute(() =>
        this.publisher.publishOnChannel(channel, exchange.name, routingKey, content, publishOptions)
      )
    } catch (error) {
      this.#settlePending(correlationId, (pending) => pending.reject(error))
    }

    return responsePromise
  }

  async respond (queueName, handler, options = {}) {
    if (typeof handler !== 'function') {
      throw new Error('Handler must be a function')
    }

    const { replyOnError = false, ...subscribeOptions } = options

    return this.consumers.subscribe(queueName, async (content, message) => {
      const { replyTo, correlationId } = message.properties

      if (!replyTo) {
        // Not an RPC message: process it normally, there is nowhere to reply.
        this.logger.warn(`Message on RPC queue ${queueName} has no replyTo property — processed without a reply`)
        await handler(content, message)

        return
      }

      let result

      try {
        result = await handler(content, message)
      } catch (error) {
        if (!replyOnError) {
          // Poison-message policy: rethrowing lets the subscribe pipeline
          // nack to the DLQ (no hot requeue loops); the requester's timeout
          // surfaces the failure on the other side.
          throw error
        }

        await this.#publishReply(replyTo, correlationId, { message: error.message }, { 'x-rpc-error': true })

        return
      }

      await this.#publishReply(replyTo, correlationId, result)
    }, subscribeOptions)
  }

  async #publishReply (replyTo, correlationId, payload, extraHeaders = {}) {
    const channel = await this.getChannel()
    // undefined means the handler had nothing to return — normalize to null
    // so the codec (which rejects undefined) still produces a reply and the
    // requester settles instead of timing out.
    const { content, compressed } = await this.codec.encode(payload === undefined ? null : payload)

    // Replies go through the default exchange: the replyTo value IS the
    // routing key of the requester's private reply route. If the requester is
    // already gone the broker drops the reply, which is exactly right — its
    // timeout has spoken for it.
    await this.publisher.publishOnChannel(channel, '', replyTo, content, {
      persistent: false,
      correlationId,
      headers: { 'x-compressed': compressed, ...extraHeaders }
    })
  }
}

export { Rpc }
export default Rpc
