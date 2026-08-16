import WorkerPool from './worker-pool.js'
import systemClock from '../utils/clock.js'
import describeError from '../utils/describe-error.js'
import SequentialProcessor from './sequential-processor.js'

class ConsumerManager {
  constructor (context) {
    this.logger = context.logger
    this.codec = context.codec
    this.circuitBreaker = context.circuitBreaker
    this.prefetchCount = context.prefetchCount
    this.clock = context.clock ?? systemClock
    // Base backoff between attempts to recover a broker-cancelled consumer
    // (attempt N waits N * this value).
    this.recoveryInterval = context.consumerRecoveryInterval ?? 1000
    this.getChannelPool = context.getChannelPool
    this.getChannel = context.getChannel
    this.emit = context.emit
    this.activeConsumers = new Map()
    this.consumersByTag = new Map()
    this.workerPools = new Map()
    this.consumerSequence = 0
  }

  async getDedicatedChannel (consumerId) {
    const channelPool = this.getChannelPool()

    if (!channelPool) {
      throw new Error('Not connected to RabbitMQ. Connection establishing/recovery in progress.')
    }

    return channelPool.getDedicatedChannel(consumerId)
  }

  attachAckControls (msg, channel) {
    Object.defineProperty(msg, '__channel', { value: channel, enumerable: false, configurable: true })
    Object.defineProperty(msg, '__ackSettled', { value: false, enumerable: false, configurable: true, writable: true })
  }

  settleAck (msg, channel, action, requeue = false) {
    if (msg.__ackSettled) return

    try {
      if (action === 'ack') {
        channel.ack(msg)
      } else {
        channel.nack(msg, false, requeue)
      }

      msg.__ackSettled = true
    } catch (error) {
      this.logger.error(`Failed to ${action} message: ${error.message}`)
    }
  }

  // Delivery tags are scoped to the channel that delivered them, so settlement
  // must go back to that exact channel. Falling back to a pool channel would
  // make the broker answer PRECONDITION_FAILED and close it, taking unrelated
  // in-flight publishes down with it — failing loudly is the safer contract.
  #settlementChannel (message) {
    if (!message.__channel) {
      throw new Error('Cannot settle a message that was not delivered by this consumer: its channel is unknown')
    }

    return message.__channel
  }

  async ackMessage (message) {
    if (message.__ackSettled) return

    try {
      this.#settlementChannel(message).ack(message)
      message.__ackSettled = true
    } catch (err) {
      this.logger.error(`Failed to acknowledge message: ${err.message}`)

      throw err
    }
  }

  async nackMessage (message, { requeue = false } = {}) {
    if (message.__ackSettled) return

    try {
      this.#settlementChannel(message).nack(message, false, requeue)
      message.__ackSettled = true
    } catch (err) {
      this.logger.error(`Failed to negatively acknowledge message: ${err.message}`)

      throw err
    }
  }

  #validateSubscribeArgs (queueName, callback) {
    if (typeof queueName !== 'string' || queueName.trim() === '') {
      throw new Error('Queue name must be a non-empty string')
    }

    if (typeof callback !== 'function') {
      throw new Error('Callback must be a function')
    }
  }

  // Rejected at subscribe time rather than defaulted: a typo silently falling
  // back to 'none' would quietly disable retries the caller asked for.
  #resolveRetryPolicy (retryPolicy, defaultPolicy) {
    const policy = retryPolicy ?? defaultPolicy

    if (policy === 'none' || policy === 'once') return policy

    // { attempts: N } — a real budget, honoured exactly on quorum queues
    // (see #shouldRequeue).
    if (Number.isInteger(policy?.attempts) && policy.attempts > 0) return policy

    const shown = typeof policy === 'object' && policy !== null ? JSON.stringify(policy) : String(policy)

    throw new Error(`Invalid retryPolicy '${shown}': expected 'none', 'once' or { attempts: <positive integer> }`)
  }

  // The single place that decides whether a failed message goes back to the
  // queue. Both consumption paths route through it, so subscribe() and
  // subscribeSequential() cannot drift apart again.
  //
  // 'once' is a ceiling, not a guarantee: `error.retryable === false` opts out
  // of the retry, and a delivery already marked `redelivered` never gets
  // another one — without that check an always failing callback hot-loops
  // (nack -> requeue -> redeliver -> nack).
  //
  // Caveat of the `redelivered` flag: the broker sets it on ANY requeue, not
  // just ours. An unacked message returned to the queue after a connection
  // drop arrives redelivered too, so an infrastructure event can consume the
  // retry budget before the handler ever fails. Classic queues offer no
  // redelivery counter at all — { attempts: N } uses a quorum queue's
  // x-delivery-count, which counts real deliveries and is immune to that.
  #shouldRequeue (message, error, retryPolicy) {
    if (retryPolicy === 'none') return false
    if (error?.retryable === false) return false

    if (retryPolicy === 'once') return !message.fields?.redelivered

    // x-delivery-count is the number of PRIOR deliveries, so this delivery is
    // number deliveryCount + 1: requeue while the budget still has room.
    const deliveryCount = Number(message.properties?.headers?.['x-delivery-count'])

    if (Number.isFinite(deliveryCount)) return deliveryCount + 1 < retryPolicy.attempts

    // No counter. Verified against a real broker: a quorum queue OMITS the
    // header on the first delivery and only starts sending it (at 1) from the
    // redelivery on — so an absent header on a first delivery is normal and
    // the budget still applies, with attempts: 1 meaning "no retry at all".
    if (!message.fields?.redelivered) return retryPolicy.attempts > 1

    // Absent on a REDELIVERY means the queue does not count at all (classic):
    // fall back to the one-shot ceiling rather than looping forever on a
    // budget the broker cannot track.
    return false
  }

  registerConsumer (queueName, setup) {
    const consumerId = `consumer-${queueName}-${++this.consumerSequence}`

    this.activeConsumers.set(consumerId, {
      queueName,
      setup,
      channel: null,
      consumerTag: null,
      // Every tag this consumer has ever answered to (see #trackConsumerTag).
      knownTags: new Set(),
      // The channel whose lifecycle we are already watching (#watchChannelLoss).
      watchedChannel: null,
      cancelled: false,
      sequentialProcessor: null,
      epoch: 0
    })

    return consumerId
  }

  // Recreating a consumer gets a NEW tag from the broker, but the caller only
  // ever holds the one subscribe() returned. Retiring the old tag here made
  // unsubscribe(originalTag) silently answer false after the first
  // reconnection, leaving the consumer (and its worker pool) running with no
  // way to stop it — the tag it now answers to is not exposed anywhere. So
  // every tag a consumer has held stays a valid handle for its whole life;
  // the set is bounded by that consumer's recreation count and is dropped
  // wholesale with it.
  #trackConsumerTag (consumerId, consumerInfo, consumerTag) {
    consumerInfo.consumerTag = consumerTag
    consumerInfo.knownTags.add(consumerTag)
    this.consumersByTag.set(consumerTag, consumerId)
  }

  // The single place a consumer is removed, so it owns every resource the
  // consumer holds. The worker pool used to be terminated only on the
  // unsubscribe path, which left threads running (and the process unable to
  // exit) whenever recovery gave up on a consumer instead.
  async #dropConsumer (consumerId, consumerInfo) {
    for (const tag of consumerInfo.knownTags) {
      this.consumersByTag.delete(tag)
    }

    consumerInfo.knownTags.clear()
    consumerInfo.sequentialProcessor?.dispose()
    this.activeConsumers.delete(consumerId)

    const workerPool = this.workerPools.get(consumerId)

    if (workerPool) {
      this.workerPools.delete(consumerId)
      await workerPool.terminate()
    }
  }

  // Every consumer (re)creation goes through here. The epoch lets concurrent
  // recovery paths (recreateAll after a reconnect vs handleBrokerCancel's
  // retry loop) detect that someone else already recreated the consumer,
  // instead of issuing a duplicate channel.consume.
  async runSetup (consumerInfo) {
    consumerInfo.epoch++

    return consumerInfo.setup()
  }

  async startConsumer (consumerId) {
    const consumerInfo = this.activeConsumers.get(consumerId)

    try {
      return await this.runSetup(consumerInfo)
    } catch (error) {
      await this.#dropConsumer(consumerId, consumerInfo)

      throw error
    }
  }

  findConsumerIdByTag (consumerTag) {
    return this.consumersByTag.get(consumerTag) ?? null
  }

  findQueueNameByTag (consumerTag) {
    if (!consumerTag) return null

    const consumerId = this.findConsumerIdByTag(consumerTag)

    return consumerId ? this.activeConsumers.get(consumerId).queueName : null
  }

  // A consumer's dedicated channel can die on its own — a channel-level
  // exception (PRECONDITION_FAILED, ACCESS_REFUSED) closes it while the
  // connection stays perfectly healthy. amqplib only delivers the null
  // message on a broker basic.cancel, never on a channel close, so without
  // this watcher the consumer simply stopped draining its queue in silence:
  // no cancel, no reconnection, no log.
  //
  // Registered per CHANNEL, not per setup: recoveries that reuse the cached
  // channel would otherwise stack a listener each time.
  #watchChannelLoss (consumerId, consumerInfo, channel) {
    if (consumerInfo.watchedChannel === channel) return

    consumerInfo.watchedChannel = channel

    channel.once('close', () => {
      // State tied to the dead channel goes with it either way.
      consumerInfo.sequentialProcessor?.dispose()

      // amqplib closes every channel synchronously BEFORE the connection
      // announces its own close, so at this instant a connection-level drop
      // is indistinguishable from a channel-level one — the pool still looks
      // healthy. The whole teardown runs in one synchronous stack, so one
      // microtask later the facade has already nulled the pool if the whole
      // connection went; only then can the two losses be told apart.
      queueMicrotask(() => {
        const channelPool = this.getChannelPool()

        // Pool gone or closed: the connection dropped (or a disconnect is in
        // flight), and recovery belongs to recreateAll — racing it here would
        // duplicate consumers or burn the retry budget against a broker that
        // is not there, permanently dropping consumers the reconnection
        // would have restored.
        if (!channelPool || channelPool.closed) return
        if (consumerInfo.cancelled || consumerInfo.channel !== channel) return

        this.handleConsumerLoss(consumerId, `channel for queue ${consumerInfo.queueName} closed unexpectedly`)
      })
    })
  }

  // Broker-initiated cancellation (e.g. queue deleted): notify and try to
  // recreate the consumer with backoff; on giving up, remove it and emit consumerLost.
  async handleBrokerCancel (consumerId) {
    const consumerInfo = this.activeConsumers.get(consumerId)

    if (!consumerInfo) return

    return this.handleConsumerLoss(consumerId, `consumer for queue ${consumerInfo.queueName} was cancelled by the broker`)
  }

  async handleConsumerLoss (consumerId, reason) {
    const consumerInfo = this.activeConsumers.get(consumerId)

    if (!consumerInfo || consumerInfo.cancelled) return

    this.logger.warn(`Recovering consumer: ${reason}`)
    this.emit('consumerCancelled', { queueName: consumerInfo.queueName, consumerTag: consumerInfo.consumerTag, reason })

    const maxAttempts = 3
    let knownEpoch = consumerInfo.epoch
    // Ownership fence: this loop only recovers on the pool that lost the
    // consumer. A different (or missing) pool means a reconnection cycle is
    // running and recreateAll owns every registered consumer — recovering
    // here as well would consume the same queue twice, and the epoch check
    // alone cannot see a recreation that has not happened YET (the window
    // between the new pool being installed and recreateAll reaching this
    // consumer).
    const ownedPool = this.getChannelPool()

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      await this.clock.sleep(this.recoveryInterval * attempt)

      const currentInfo = this.activeConsumers.get(consumerId)

      if (!currentInfo || currentInfo.cancelled) return
      if (this.getChannelPool() !== ownedPool) return

      // Someone else (e.g. recreateAll after a reconnection) already
      // recreated this consumer while we were backing off — recovering
      // again would create a duplicate consumer on the queue.
      if (currentInfo.epoch !== knownEpoch) return

      try {
        await this.runSetup(currentInfo)

        this.logger.info(`Consumer for queue ${currentInfo.queueName} recovered after broker cancellation`)
        this.emit('consumerRecovered', { queueName: currentInfo.queueName, consumerTag: currentInfo.consumerTag })

        return
      } catch (error) {
        knownEpoch = currentInfo.epoch
        this.logger.warn(`Failed to recover consumer for queue ${currentInfo.queueName} (attempt ${attempt}/${maxAttempts}): ${error.message}`)
      }
    }

    await this.#dropConsumer(consumerId, consumerInfo)
    this.logger.error(`Consumer for queue ${consumerInfo.queueName} could not be recovered and was removed`)
    this.emit('consumerLost', { queueName: consumerInfo.queueName })
  }

  // Shared consume pipeline used by subscribe and subscribeSequential: the
  // only variable part is how a decoded message is processed and settled,
  // provided by hooks.createProcessor({ channel, consumerInfo, noAck, shouldRequeue }).
  async #subscribeCore (queueName, callback, options, hooks) {
    this.#validateSubscribeArgs(queueName, callback)

    const { retryPolicy: requestedPolicy, ...consumeOptions } = options
    const retryPolicy = this.#resolveRetryPolicy(requestedPolicy, hooks.defaultRetryPolicy)
    const noAck = options.noAck === true
    const prefetchCount = options.prefetchCount ?? hooks.defaultPrefetch
    const shouldRequeue = (message, error) => this.#shouldRequeue(message, error, retryPolicy)

    // Per-message observability, one payload shape for every subscribe
    // variant. `requeued` reports what actually happened to the delivery:
    // under noAck nothing is ever requeued, whatever the retry policy says.
    const events = {
      processed: (msg, durationMs) => this.emit('messageProcessed', {
        queue: queueName,
        messageId: msg.properties?.messageId,
        consumerTag: msg.fields?.consumerTag,
        durationMs
      }),
      failed: (msg, error, requeue, durationMs) => this.emit('messageFailed', {
        queue: queueName,
        messageId: msg.properties?.messageId,
        consumerTag: msg.fields?.consumerTag,
        durationMs,
        error,
        requeued: noAck ? false : requeue
      })
    }

    const consumerId = this.registerConsumer(queueName, async () => {
      const channel = await this.getDedicatedChannel(consumerId)
      const consumerInfo = this.activeConsumers.get(consumerId)
      const processMessage = hooks.createProcessor({ channel, consumerInfo, noAck, shouldRequeue, events })

      if (!noAck) {
        await channel.prefetch(prefetchCount)
      }

      const wrappedCallback = async (msg) => {
        if (!msg) {
          this.handleBrokerCancel(consumerId)

          return
        }

        this.attachAckControls(msg, channel)

        const startedAt = this.clock.now()

        try {
          const isCompressed = Boolean(msg.properties.headers && msg.properties.headers['x-compressed'])
          const decodedContent = await this.codec.decode(msg.content, isCompressed)

          await processMessage(decodedContent, msg)

          // The sequential processor settles asynchronously (a message can be
          // parked behind its dependency), so a clean return here says nothing
          // about the outcome — that path reports through the events helper.
          if (!hooks.outcomeFromProcessor) {
            events.processed(msg, this.clock.now() - startedAt)
          }
        } catch (error) {
          // describeError, not error.message: a handler can throw null or a
          // string, and a crash here would leave the delivery unsettled
          // forever — no ack, no nack, no redelivery until the channel dies.
          this.logger.error(`Error processing message: ${describeError(error)}`)

          // The retry policy governs every failure in the pipeline, decode
          // errors included. A decode failure is deterministic, so under
          // 'once' it costs one pointless redelivery before the DLQ — the
          // price of a single rule with no carve-outs to remember.
          const requeue = shouldRequeue(msg, error)

          events.failed(msg, error, requeue, this.clock.now() - startedAt)

          if (!noAck) {
            this.settleAck(msg, channel, 'nack', requeue)
          }
        }
      }

      const consumer = await channel.consume(queueName, wrappedCallback, { ...consumeOptions, noAck })

      consumerInfo.channel = channel
      this.#trackConsumerTag(consumerId, consumerInfo, consumer.consumerTag)
      this.#watchChannelLoss(consumerId, consumerInfo, channel)

      return consumer
    })

    try {
      const consumer = await this.startConsumer(consumerId)

      this.logger.info(hooks.successLog(prefetchCount))

      return consumer
    } catch (error) {
      this.logger.error(`Failed to subscribe to queue ${queueName}: ${error.message}`)

      throw error
    }
  }

  async subscribe (queueName, callback, options = {}) {
    return this.#subscribeCore(queueName, callback, options, {
      defaultPrefetch: this.prefetchCount,
      // Never requeues by default: a handler that already applied part of its
      // side effect would apply it again on the redelivery. Opt into
      // 'once' when the handler is idempotent.
      defaultRetryPolicy: 'none',
      successLog: (prefetchCount) => `Subscribed to queue: ${queueName} with prefetch count: ${prefetchCount}`,
      createProcessor: ({ channel, noAck }) => async (content, msg) => {
        await callback(content, msg)

        if (!noAck) {
          this.settleAck(msg, channel, 'ack')
        }
      }
    })
  }

  async subscribeSequential (queueName, callback, options = {}) {
    // No default here: SequentialProcessor owns it, and a second one (even
    // for staleTimeout: 0, which its || also maps to the default) would be a
    // shadowed copy that could silently drift.
    const staleTimeout = options.staleTimeout

    return this.#subscribeCore(queueName, callback, options, {
      defaultPrefetch: 1,
      // Historical default, kept for compatibility. Note the tension: the
      // requeued message goes back to the queue while later ones keep being
      // processed, so the retry can break the very ordering this method
      // exists to provide. Pass 'none' when order matters more than the retry.
      defaultRetryPolicy: 'once',
      successLog: () => `Subscribed to queue ${queueName} with sequential processing`,
      // handle() returning cleanly can mean "parked behind a dependency", so
      // the outcome events come from onSuccess/onFailure below, where the
      // settlement actually happens.
      outcomeFromProcessor: true,
      createProcessor: ({ channel, consumerInfo, noAck, shouldRequeue, events }) => {
        // Recreation (reconnect): discard state tied to the previous channel.
        consumerInfo.sequentialProcessor?.dispose()

        const processor = new SequentialProcessor({
          callback,
          logger: this.logger,
          staleTimeout,
          shouldRequeue,
          clock: this.clock,
          onSuccess: (message, meta) => {
            if (!noAck) {
              this.settleAck(message, channel, 'ack')
            }

            // A duplicate delivery of a parked message is acked and dropped —
            // the original will report when it actually completes, and a
            // second messageProcessed would double-count it.
            if (!meta?.duplicate) {
              events.processed(message, meta?.durationMs)
            }
          },
          // Consumer callback failures must NOT feed the circuit breaker: it
          // gates publishing, and poison messages on one queue would block
          // every publish in the application.
          onFailure: (message, error, requeue, meta) => {
            if (!noAck) {
              this.settleAck(message, channel, 'nack', requeue)
            }

            // meta.durationMs is absent for a parked message that expired
            // waiting for its dependency: it never ran, so there is no
            // duration to report.
            events.failed(message, error, requeue, meta?.durationMs)
          }
        })

        // The channel's own teardown is handled by #watchChannelLoss, which
        // disposes whatever processor is current — attaching a listener here
        // would stack one more on every recovery that reuses this channel.
        consumerInfo.sequentialProcessor = processor

        return (content, msg) => processor.handle(content, msg)
      }
    })
  }

  async unsubscribe (consumerTag) {
    const consumerId = this.findConsumerIdByTag(consumerTag)

    if (!consumerId) return false

    const consumerInfo = this.activeConsumers.get(consumerId)

    consumerInfo.cancelled = true

    try {
      if (consumerInfo.channel) {
        // The broker only knows the tag from the CURRENT consume — the caller
        // may legitimately be holding an older alias (see #trackConsumerTag),
        // and cancelling that one would leave the consumer running.
        await consumerInfo.channel.cancel(consumerInfo.consumerTag)
      }
    } catch (error) {
      this.logger.warn(`Failed to cancel consumer ${consumerTag}: ${error.message}`)
    }

    await this.#dropConsumer(consumerId, consumerInfo)

    // The dedicated channel exists for this consumer alone; leaving it open
    // leaked one channel per subscribe/unsubscribe cycle until the connection
    // hit channel_max and the broker refused every new one.
    await this.getChannelPool()?.releaseDedicatedChannel(consumerId)

    this.logger.info(`Unsubscribed consumer ${consumerTag} from queue ${consumerInfo.queueName}`)

    return true
  }

  async subscribeWithOptimizedPrefetch (queueName, callback, options = {}) {
    const {
      initialPrefetch = 10,
      maxPrefetch = 1000,
      minPrefetch = 1,
      optimizationInterval = 1000,
      increaseFactor = 1.5,
      decreaseFactor = 0.75,
      ...subscribeOptions
    } = options

    let currentPrefetch = initialPrefetch
    let lastOptimizationTime = this.clock.now()
    let processingTimes = []
    let consumerId = null
    let knownEpoch = null

    const applyPrefetch = async (consumerInfo) => {
      // The optimization runs after the user callback already succeeded:
      // a prefetch failure here must never bubble into the message path,
      // or a successfully processed message would be nacked to the DLQ.
      try {
        await consumerInfo.channel.prefetch(currentPrefetch)
        this.logger.info(`Adjusted prefetch to: ${currentPrefetch}`)
      } catch (error) {
        this.logger.warn(`Failed to adjust prefetch to ${currentPrefetch}: ${error.message}`)
      }
    }

    const optimizePrefetch = async () => {
      const consumerInfo = this.activeConsumers.get(consumerId)

      if (!consumerInfo?.channel) return

      // After a reconnection the recreated channel starts back at the
      // initial prefetch: re-apply the optimized value so reality matches
      // what this closure believes.
      if (consumerInfo.epoch !== knownEpoch) {
        knownEpoch = consumerInfo.epoch

        if (currentPrefetch !== initialPrefetch) {
          await applyPrefetch(consumerInfo)
        }
      }

      const now = this.clock.now()
      const elapsed = now - lastOptimizationTime

      // No empty-samples guard: the only caller pushes its own measurement
      // before invoking the optimizer, so the window always has at least one.
      if (elapsed < optimizationInterval) return

      const avgProcessingTime = processingTimes.reduce((sum, time) => sum + time, 0) / processingTimes.length
      let newPrefetch = currentPrefetch

      if (avgProcessingTime < 100 && currentPrefetch < maxPrefetch) {
        newPrefetch = Math.min(Math.floor(currentPrefetch * increaseFactor), maxPrefetch)
      } else if (avgProcessingTime > 500 && currentPrefetch > minPrefetch) {
        newPrefetch = Math.max(Math.floor(currentPrefetch * decreaseFactor), minPrefetch)
      }

      lastOptimizationTime = now
      processingTimes = []

      if (newPrefetch === currentPrefetch) return

      currentPrefetch = newPrefetch

      await applyPrefetch(consumerInfo)
    }

    const wrappedCallback = async (content, message) => {
      const startTime = this.clock.now()

      try {
        await callback(content, message)
      } finally {
        processingTimes.push(this.clock.now() - startTime)
      }

      await optimizePrefetch()
    }

    const consumer = await this.subscribe(queueName, wrappedCallback, {
      ...subscribeOptions,
      prefetchCount: currentPrefetch
    })

    // subscribe() registered this tag one line ago, so the lookup cannot miss.
    consumerId = this.findConsumerIdByTag(consumer.consumerTag)
    knownEpoch = this.activeConsumers.get(consumerId).epoch

    return consumer
  }

  async subscribeParallel (queueName, processorFile, options = {}) {
    const {
      workerCount,
      prefetch = 10,
      maxRespawns,
      createWorker,
      ...subscribeOptions
    } = options

    const workerPool = new WorkerPool(processorFile, {
      workerCount,
      maxRespawns,
      workerData: { queueName },
      logger: this.logger,
      // The pool's spawn seam, threaded through so the parallel path is
      // testable without real threads.
      createWorker
    })

    const messageHandler = async (content, message) => {
      const result = await workerPool.run({ content })

      if (!result || result.success === false) {
        throw new Error(result?.error || 'Worker processing failed')
      }
    }

    try {
      const consumer = await this.subscribe(queueName, messageHandler, {
        ...subscribeOptions,
        prefetchCount: prefetch * workerPool.size
      })

      const consumerId = this.findConsumerIdByTag(consumer.consumerTag)

      if (consumerId) {
        this.workerPools.set(consumerId, workerPool)
      }

      return consumer
    } catch (error) {
      await workerPool.terminate()

      throw error
    }
  }

  async recreateAll () {
    const recreations = Array.from(this.activeConsumers.entries()).map(async ([consumerId, consumerInfo]) => {
      try {
        await this.runSetup(consumerInfo)

        this.logger.info(`Consumer ${consumerId} for queue ${consumerInfo.queueName} recreated successfully`)
      } catch (error) {
        this.logger.error(`Failed to recreate consumer for queue ${consumerInfo.queueName}: ${error.message}`)
      }
    })

    await Promise.all(recreations)
  }

  async disposeAll () {
    for (const [, workerPool] of this.workerPools.entries()) {
      await workerPool.terminate()
    }

    this.workerPools.clear()

    for (const [, consumerInfo] of this.activeConsumers.entries()) {
      consumerInfo.cancelled = true
      consumerInfo.sequentialProcessor?.dispose()
    }

    this.activeConsumers.clear()
    this.consumersByTag.clear()
  }
}

export { ConsumerManager }
export default ConsumerManager
