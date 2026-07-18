import WorkerPool from './worker-pool.js'
import { setTimeout as sleep } from 'node:timers/promises'
import SequentialProcessor from './sequential-processor.js'

class ConsumerManager {
  constructor (context) {
    this.logger = context.logger
    this.codec = context.codec
    this.circuitBreaker = context.circuitBreaker
    this.prefetchCount = context.prefetchCount
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

  async ackMessage (message) {
    if (message.__ackSettled) return

    try {
      const channel = message.__channel || await this.getChannel()

      channel.ack(message)
      message.__ackSettled = true
    } catch (err) {
      this.logger.error(`Failed to acknowledge message: ${err.message}`)

      throw err
    }
  }

  async nackMessage (message, { requeue = false } = {}) {
    if (message.__ackSettled) return

    try {
      const channel = message.__channel || await this.getChannel()

      channel.nack(message, false, requeue)
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

  registerConsumer (queueName, setup) {
    const consumerId = `consumer-${queueName}-${++this.consumerSequence}`

    this.activeConsumers.set(consumerId, {
      queueName,
      setup,
      channel: null,
      consumerTag: null,
      cancelled: false,
      sequentialProcessor: null,
      epoch: 0
    })

    return consumerId
  }

  #trackConsumerTag (consumerId, consumerInfo, consumerTag) {
    if (consumerInfo.consumerTag) {
      this.consumersByTag.delete(consumerInfo.consumerTag)
    }

    consumerInfo.consumerTag = consumerTag
    this.consumersByTag.set(consumerTag, consumerId)
  }

  #dropConsumer (consumerId, consumerInfo) {
    if (consumerInfo.consumerTag) {
      this.consumersByTag.delete(consumerInfo.consumerTag)
    }

    consumerInfo.sequentialProcessor?.dispose()
    this.activeConsumers.delete(consumerId)
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
      this.#dropConsumer(consumerId, consumerInfo)

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

  // Broker-initiated cancellation (e.g. queue deleted): notify and try to
  // recreate the consumer with backoff; on giving up, remove it and emit consumerLost.
  async handleBrokerCancel (consumerId) {
    const consumerInfo = this.activeConsumers.get(consumerId)

    if (!consumerInfo || consumerInfo.cancelled) return

    this.logger.warn(`Consumer for queue ${consumerInfo.queueName} was cancelled by the broker`)
    this.emit('consumerCancelled', { queueName: consumerInfo.queueName, consumerTag: consumerInfo.consumerTag })

    const maxAttempts = 3
    let knownEpoch = consumerInfo.epoch

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      await sleep(1000 * attempt)

      const currentInfo = this.activeConsumers.get(consumerId)

      if (!currentInfo || currentInfo.cancelled) return

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

    this.#dropConsumer(consumerId, consumerInfo)
    this.logger.error(`Consumer for queue ${consumerInfo.queueName} could not be recovered and was removed`)
    this.emit('consumerLost', { queueName: consumerInfo.queueName })
  }

  // Shared consume pipeline used by subscribe and subscribeSequential: the
  // only variable part is how a decoded message is processed and settled,
  // provided by hooks.createProcessor({ channel, consumerInfo, noAck }).
  async #subscribeCore (queueName, callback, options, hooks) {
    this.#validateSubscribeArgs(queueName, callback)

    const noAck = options.noAck === true
    const prefetchCount = options.prefetchCount ?? hooks.defaultPrefetch

    const consumerId = this.registerConsumer(queueName, async () => {
      const channel = await this.getDedicatedChannel(consumerId)
      const consumerInfo = this.activeConsumers.get(consumerId)
      const processMessage = hooks.createProcessor({ channel, consumerInfo, noAck })

      if (!noAck) {
        await channel.prefetch(prefetchCount)
      }

      const wrappedCallback = async (msg) => {
        if (!msg) {
          this.handleBrokerCancel(consumerId)

          return
        }

        this.attachAckControls(msg, channel)

        try {
          const isCompressed = Boolean(msg.properties.headers && msg.properties.headers['x-compressed'])
          const decodedContent = await this.codec.decode(msg.content, isCompressed)

          await processMessage(decodedContent, msg)
        } catch (error) {
          this.logger.error(`Error processing message: ${error.message}`)

          // Requeueing here would hot-loop poison messages (e.g. content
          // that fails to decode) — dead-letter them instead.
          if (!noAck) {
            this.settleAck(msg, channel, 'nack', false)
          }
        }
      }

      const consumer = await channel.consume(queueName, wrappedCallback, { ...options, noAck })

      consumerInfo.channel = channel
      this.#trackConsumerTag(consumerId, consumerInfo, consumer.consumerTag)

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
    const staleTimeout = options.staleTimeout ?? 30000

    return this.#subscribeCore(queueName, callback, options, {
      defaultPrefetch: 1,
      successLog: () => `Subscribed to queue ${queueName} with sequential processing`,
      createProcessor: ({ channel, consumerInfo, noAck }) => {
        // Recreation (reconnect): discard state tied to the previous channel.
        consumerInfo.sequentialProcessor?.dispose()

        const processor = new SequentialProcessor({
          callback,
          logger: this.logger,
          staleTimeout,
          onSuccess: (message) => {
            if (!noAck) {
              this.settleAck(message, channel, 'ack')
            }
          },
          // Consumer callback failures must NOT feed the circuit breaker: it
          // gates publishing, and poison messages on one queue would block
          // every publish in the application.
          onFailure: (message, error, requeue) => {
            if (!noAck) {
              this.settleAck(message, channel, 'nack', requeue)
            }
          }
        })

        consumerInfo.sequentialProcessor = processor
        channel.on('close', () => processor.dispose())

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
        await consumerInfo.channel.cancel(consumerTag)
      }
    } catch (error) {
      this.logger.warn(`Failed to cancel consumer ${consumerTag}: ${error.message}`)
    }

    const workerPool = this.workerPools.get(consumerId)

    if (workerPool) {
      await workerPool.terminate()
      this.workerPools.delete(consumerId)
    }

    this.#dropConsumer(consumerId, consumerInfo)
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
    let lastOptimizationTime = Date.now()
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

      const now = Date.now()
      const elapsed = now - lastOptimizationTime

      if (elapsed < optimizationInterval || processingTimes.length === 0) return

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
      const startTime = Date.now()

      try {
        await callback(content, message)
      } finally {
        processingTimes.push(Date.now() - startTime)
      }

      await optimizePrefetch()
    }

    const consumer = await this.subscribe(queueName, wrappedCallback, {
      ...subscribeOptions,
      prefetchCount: currentPrefetch
    })

    consumerId = this.findConsumerIdByTag(consumer.consumerTag)
    knownEpoch = consumerId ? this.activeConsumers.get(consumerId).epoch : null

    return consumer
  }

  async subscribeParallel (queueName, processorFile, options = {}) {
    const {
      workerCount,
      prefetch = 10,
      maxRespawns,
      ...subscribeOptions
    } = options

    const workerPool = new WorkerPool(processorFile, {
      workerCount,
      maxRespawns,
      workerData: { queueName },
      logger: this.logger
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
