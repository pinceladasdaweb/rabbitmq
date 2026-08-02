import describeError from '../utils/describe-error.js'

class SequentialProcessor {
  constructor (options) {
    this.callback = options.callback
    this.logger = options.logger
    this.staleTimeout = options.staleTimeout || 30000
    this.onSuccess = options.onSuccess
    this.onFailure = options.onFailure
    // Supplied by ConsumerManager so the retry policy is decided in one place
    // for every consumption path. Defaults to never requeueing, which is the
    // safe answer when this processor is built directly.
    this.shouldRequeue = options.shouldRequeue ?? (() => false)
    this.processing = new Map()
    this.pending = new Map()
    // Secondary index (dependsOn -> Set of pending messageIds) so releasing
    // dependents after a completion is O(1) instead of a full pending scan.
    this.pendingByDependency = new Map()

    this.cleanupInterval = setInterval(() => this.cleanup(), Math.min(this.staleTimeout, 60000))

    if (typeof this.cleanupInterval.unref === 'function') {
      this.cleanupInterval.unref()
    }
  }

  async handle (content, message) {
    const messageId = message.properties.messageId
    const dependsOn = message.properties.headers?.['depends-on']
    const dependencyUnresolved = dependsOn && (this.processing.has(dependsOn) || this.pending.has(dependsOn))

    if (messageId && dependencyUnresolved) {
      this.logger?.info(`Message ${messageId} waiting for dependency ${dependsOn}`)
      this.pending.set(messageId, { content, message, dependsOn, queuedAt: Date.now() })
      this.#indexPending(messageId, dependsOn)

      return
    }

    await this.#process(content, message)
  }

  #indexPending (messageId, dependsOn) {
    let dependents = this.pendingByDependency.get(dependsOn)

    if (!dependents) {
      dependents = new Set()
      this.pendingByDependency.set(dependsOn, dependents)
    }

    dependents.add(messageId)
  }

  #removePending (messageId, dependsOn) {
    this.pending.delete(messageId)

    const dependents = this.pendingByDependency.get(dependsOn)

    if (dependents) {
      dependents.delete(messageId)

      if (dependents.size === 0) {
        this.pendingByDependency.delete(dependsOn)
      }
    }
  }

  async #process (content, message) {
    const messageId = message.properties.messageId

    try {
      if (messageId) {
        this.processing.set(messageId, { startTime: Date.now() })
      }

      await this.callback(content, message)

      if (messageId) {
        const processingTime = Date.now() - this.processing.get(messageId).startTime

        this.processing.delete(messageId)
        this.logger?.info(`Successfully processed message ${messageId} in ${processingTime}ms`)
      }

      this.onSuccess(message)

      if (messageId) {
        await this.#processDependents(messageId)
      }
    } catch (error) {
      if (messageId) {
        this.processing.delete(messageId)
      }

      const requeue = this.shouldRequeue(message, error)

      // describeError tolerates a handler throwing null or a string; reading
      // .message here used to crash the catch and skip onFailure entirely,
      // leaving the message unacknowledged (issue #18).
      this.logger?.error(`Error processing message ${messageId || '(no messageId)'}: ${describeError(error)}`)
      this.onFailure(message, error, requeue)
    }
  }

  async #processDependents (messageId) {
    const dependents = this.pendingByDependency.get(messageId)

    if (!dependents) return

    this.pendingByDependency.delete(messageId)

    for (const pendingId of dependents) {
      const pendingData = this.pending.get(pendingId)

      if (!pendingData) continue

      this.logger?.info(`Processing dependent message ${pendingId}`)

      this.pending.delete(pendingId)
      await this.handle(pendingData.content, pendingData.message)
    }
  }

  cleanup () {
    const now = Date.now()

    // A stale processing entry is bookkeeping only: it does not release its
    // dependents (the dependency never actually completed) — they expire via
    // the rule below and are settled under the subscription's retry policy.
    for (const [messageId, data] of this.processing.entries()) {
      if (now - data.startTime > this.staleTimeout) {
        this.logger?.warn(`Removing stale processing entry for message ${messageId}`)
        this.processing.delete(messageId)
      }
    }

    for (const [messageId, data] of this.pending.entries()) {
      if (now - data.queuedAt > this.staleTimeout) {
        this.#removePending(messageId, data.dependsOn)

        // Same retry policy as a handler failure: under 'once' a first expiry
        // goes back to the queue (the dependency may arrive later), a
        // redelivery is dead-lettered instead of looping forever.
        const error = new Error(`Dependency ${data.dependsOn} was never resolved`)
        const requeue = this.shouldRequeue(data.message, error)

        this.logger?.warn(`Pending message ${messageId} timed out waiting for ${data.dependsOn}. ${requeue ? 'Requeueing' : 'Dead-lettering'}`)
        this.onFailure(data.message, error, requeue)
      }
    }
  }

  dispose () {
    clearInterval(this.cleanupInterval)
    this.processing.clear()
    this.pending.clear()
    this.pendingByDependency.clear()
  }
}

export { SequentialProcessor }
export default SequentialProcessor
