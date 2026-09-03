import WorkerPool from './worker-pool.js'
import systemClock from '../utils/clock.js'
import detached from '../utils/detached.js'
import describeError from '../utils/describe-error.js'
import { notConnectedError } from '../utils/errors.js'
import SequentialProcessor from './sequential-processor.js'

// Constant per process, not per delivery: defineProperty only reads it.
const ACK_SETTLED_DESCRIPTOR = { value: false, enumerable: false, configurable: true, writable: true }

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
    // How long unsubscribe waits for in-flight handlers before closing the
    // consumer's dedicated channel anyway (see #drainInFlight).
    this.drainTimeout = context.consumerDrainTimeout ?? 30000
    // Spawn seam for subscribeParallel's worker pools; tests inject fakes,
    // production leaves it undefined and WorkerPool spawns real threads.
    this.createWorker = context.createWorker
    this.getChannelPool = context.getChannelPool
    this.getChannel = context.getChannel
    this.emit = context.emit
    // Lets the hot per-message path skip building event payloads nobody is
    // listening for. Defaults to "someone is listening" so a context without
    // the hook (tests constructing the manager directly) still sees every
    // event.
    this.listenerCount = context.listenerCount ?? (() => 1)
    this.activeConsumers = new Map()
    this.consumersByTag = new Map()
    this.workerPools = new Map()
    this.consumerSequence = 0
  }

  async getDedicatedChannel (consumerId) {
    const channelPool = this.getChannelPool()

    if (!channelPool) {
      throw notConnectedError()
    }

    return channelPool.getDedicatedChannel(consumerId)
  }

  attachAckControls (msg, channel) {
    Object.defineProperty(msg, '__channel', { value: channel, enumerable: false, configurable: true })
    Object.defineProperty(msg, '__ackSettled', ACK_SETTLED_DESCRIPTOR)
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
      // The channel whose lifecycle we are already watching (#watchChannelLoss).
      watchedChannel: null,
      cancelled: false,
      sequentialProcessor: null,
      epoch: 0,
      // Deliveries currently inside the consume pipeline; unsubscribe drains
      // them before closing the dedicated channel (see #drainInFlight).
      inFlight: 0,
      drainWaiters: []
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
    this.consumersByTag.set(consumerTag, consumerId)
  }

  // The single place a consumer is removed, so it owns every resource the
  // consumer holds. The worker pool used to be terminated only on the
  // unsubscribe path, which left threads running (and the process unable to
  // exit) whenever recovery gave up on a consumer instead.
  async #dropConsumer (consumerId, consumerInfo) {
    // Every exit path drains before anything is torn down — see #drainInFlight.
    // This used to be unsubscribe's job alone, so recovery giving up closed the
    // channel under handlers still running: their late acks died with it and
    // the broker redelivered work that had actually completed. First, so the
    // worker pool is still alive for a handler mid-run to finish on.
    if (!await this.#drainInFlight(consumerInfo)) {
      this.logger.warn(`Consumer for queue ${consumerInfo.queueName} still has ${consumerInfo.inFlight} handler(s) in flight after ${this.drainTimeout}ms; closing its channel anyway`)
    }

    // consumersByTag is the single source of truth for every tag this
    // consumer ever answered to — a per-consumer reverse index drifted, and
    // consumer counts are small enough that the scan is free.
    for (const [tag, ownerId] of this.consumersByTag) {
      if (ownerId === consumerId) {
        this.consumersByTag.delete(tag)
      }
    }

    consumerInfo.sequentialProcessor?.dispose()
    this.activeConsumers.delete(consumerId)

    const workerPool = this.workerPools.get(consumerId)

    if (workerPool) {
      this.workerPools.delete(consumerId)
      await workerPool.terminate()
    }

    // The dedicated channel exists for this consumer alone, so it dies with
    // it. This used to live in unsubscribe(), which meant the other two ways a
    // consumer goes away — recovery giving up after three attempts, and a
    // subscribe that failed during setup — each leaked one channel toward
    // channel_max, after which the broker refuses every new one.
    await this.getChannelPool()?.releaseDedicatedChannel(consumerId)
  }

  // Every consumer (re)creation goes through here. The epoch lets concurrent
  // recovery paths (recreateAll after a reconnect vs handleBrokerCancel's
  // retry loop) detect that someone else already recreated the consumer,
  // instead of issuing a duplicate channel.consume. The increment is
  // synchronous and happens BEFORE the setup starts — callers that fence on
  // the epoch record it right after this call, as their own.
  runSetup (consumerInfo) {
    consumerInfo.epoch++

    return consumerInfo.setup()
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

        const reason = `channel for queue ${consumerInfo.queueName} closed unexpectedly`

        this.#recoverDetached(this.handleConsumerLoss(consumerId, reason))
      })
    })
  }

  // Last-resort containment for the event bridge. In production context.emit is
  // the facade's own emit, which already contains per listener (emitSafely), so
  // this catch never fires there; it exists for a context whose emit is a bare
  // function (the unit-test harness), where a throwing listener would otherwise
  // leave a delivery unsettled, misreport a settled outcome, or abort a
  // recovery midway. Every emit this class makes goes through here.
  #emitOutcome (event, payload) {
    try {
      this.emit(event, payload)
    } catch (error) {
      this.logger.error(`A '${event}' listener threw: ${describeError(error)}`)
    }
  }

  // Recovery is triggered from places that cannot await it: a channel 'close'
  // handler and amqplib's null-message delivery. See utils/detached.js.
  #recoverDetached (recovery) {
    detached(recovery, this.logger, 'Consumer recovery failed unexpectedly')
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
    this.#emitOutcome('consumerCancelled', { queueName: consumerInfo.queueName, consumerTag: consumerInfo.consumerTag, reason })

    // Deliberately NOT shared with ChannelPool's replacement loop despite the
    // similar shape: this one sleeps BEFORE each attempt and re-fetches state
    // through three fences (existence/cancelled, pool identity, epoch) whose
    // placement is load-bearing — a generic retry helper would bury exactly
    // the parts that have already carried bugs.
    const maxAttempts = 3
    // Ownership fence: this loop only recovers on the pool that lost the
    // consumer. A different (or missing) pool means a reconnection cycle is
    // running and recreateAll owns every registered consumer — recovering
    // here as well would consume the same queue twice, and the epoch check
    // alone cannot see a recreation that has not happened YET (the window
    // between the new pool being installed and recreateAll reaching this
    // consumer). The consumer object never changes identity, only its
    // presence in the map and its flags do, so "still ours" is one test.
    const ownedPool = this.getChannelPool()
    const stillOurs = () => this.activeConsumers.get(consumerId) === consumerInfo &&
      !consumerInfo.cancelled &&
      this.getChannelPool() === ownedPool

    let knownEpoch = consumerInfo.epoch

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      await this.clock.sleep(this.recoveryInterval * attempt)

      if (!stillOurs()) return

      // Someone else (e.g. recreateAll after a reconnection) already
      // recreated this consumer while we were backing off — recovering
      // again would create a duplicate consumer on the queue.
      if (consumerInfo.epoch !== knownEpoch) return

      // runSetup stamps the epoch synchronously, before the setup runs, so
      // this attempt's own bump is recorded as ours right here — read it back
      // any later and the fence above would mistake it for someone else's
      // recreation and abandon the recovery after a single failure.
      const setup = this.runSetup(consumerInfo)

      knownEpoch = consumerInfo.epoch

      try {
        await setup

        this.logger.info(`Consumer for queue ${consumerInfo.queueName} recovered after broker cancellation`)
        this.#emitOutcome('consumerRecovered', { queueName: consumerInfo.queueName, consumerTag: consumerInfo.consumerTag })

        return
      } catch (error) {
        this.logger.warn(`Failed to recover consumer for queue ${consumerInfo.queueName} (attempt ${attempt}/${maxAttempts}): ${error.message}`)
      }
    }

    // Fenced before giving up as well: the LAST attempt can fail because the
    // connection dropped under it, and from that instant recreateAll owns this
    // consumer. Dropping it here removed a consumer the reconnection was about
    // to restore — the queue went silent for good behind a healthy reconnect.
    if (!stillOurs()) return

    await this.#dropConsumer(consumerId, consumerInfo)
    this.logger.error(`Consumer for queue ${consumerInfo.queueName} could not be recovered and was removed`)
    this.#emitOutcome('consumerLost', { queueName: consumerInfo.queueName })
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
    // Reporting never interferes with the pipeline: payloads are only built
    // when someone is listening, and both emits are crash-proofed — every
    // caller settles the delivery BEFORE reporting, and #emitOutcome keeps a
    // throwing listener from rerouting a settled outcome (issue #18's class).
    const events = {
      processed: (msg, durationMs) => {
        if (this.listenerCount('messageProcessed') === 0) return

        this.#emitOutcome('messageProcessed', {
          queue: queueName,
          messageId: msg.properties?.messageId,
          consumerTag: msg.fields?.consumerTag,
          durationMs
        })
      },
      failed: (msg, error, requeue, durationMs) => {
        if (this.listenerCount('messageFailed') === 0) return

        this.#emitOutcome('messageFailed', {
          queue: queueName,
          messageId: msg.properties?.messageId,
          consumerTag: msg.fields?.consumerTag,
          durationMs,
          error,
          requeued: noAck ? false : requeue
        })
      }
    }

    const consumerId = this.registerConsumer(queueName, async () => {
      const channel = await this.getDedicatedChannel(consumerId)
      const consumerInfo = this.activeConsumers.get(consumerId)

      // Re-checked AFTER the await, and before anything is asked of the
      // broker. Opening a channel is a round trip, and every RECREATION of
      // this consumer (recreateAll after a reconnect, handleConsumerLoss's
      // retry loop) runs this closure while the caller already holds a tag it
      // can unsubscribe with. An unsubscribe landing in that window left the
      // consume below issuing a live broker consumer on a channel nobody
      // tracks: every delivery then threw on the missing consumerInfo and was
      // never settled, so the queue quietly filled with unacked messages that
      // only a connection drop could release.
      const cancelledEarly = () => new Error(`Consumer for queue ${queueName} was cancelled before its channel was ready`)

      if (!consumerInfo) {
        // Already dropped: #dropConsumer released the previous channel before
        // this await resolved, so the one we just reopened is ours to close or
        // it leaks toward channel_max.
        await this.getChannelPool()?.releaseDedicatedChannel(consumerId)

        throw cancelledEarly()
      }

      if (consumerInfo.cancelled) {
        // An unsubscribe is mid flight: it still needs this channel to cancel
        // and drain on, and releases it itself in #dropConsumer.
        throw cancelledEarly()
      }

      const processMessage = hooks.createProcessor({ channel, consumerInfo, noAck, shouldRequeue, events })

      if (!noAck) {
        await channel.prefetch(prefetchCount)
      }

      const wrappedCallback = async (msg) => {
        if (!msg) {
          this.#recoverDetached(this.handleBrokerCancel(consumerId))

          return
        }

        this.attachAckControls(msg, channel)

        const startedAt = this.clock.now()

        consumerInfo.inFlight++

        try {
          const isCompressed = Boolean(msg.properties.headers && msg.properties.headers['x-compressed'])
          const decodedContent = await this.codec.decode(msg.content, isCompressed)

          // Outcome reporting belongs to the processor, uniformly: only it
          // knows when a message actually settled (the sequential one can
          // park a message behind its dependency and settle it much later).
          await processMessage(decodedContent, msg)
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

          if (!noAck) {
            this.settleAck(msg, channel, 'nack', requeue)
          }

          events.failed(msg, error, requeue, this.clock.now() - startedAt)
        } finally {
          consumerInfo.inFlight--

          // The length check is the hot-path guard: at prefetch 1 inFlight
          // returns to 0 on every delivery, and splice allocates an array each
          // time even when nobody is waiting.
          if (consumerInfo.inFlight === 0 && consumerInfo.drainWaiters.length > 0) {
            for (const resolve of consumerInfo.drainWaiters.splice(0)) resolve()
          }
        }
      }

      const consumer = await channel.consume(queueName, wrappedCallback, { ...consumeOptions, noAck })

      // Fenced once more: prefetch and consume are two further round trips, and
      // an unsubscribe that completed #dropConsumer during them has already
      // swept this consumer's tags and released its channel. Tracking the new
      // tag would register it for a consumer that no longer exists —
      // findQueueNameByTag and unsubscribe then throw on undefined for that
      // tag, while the broker keeps delivering to a callback whose owner is
      // gone. The consume just issued is cancelled so it does not.
      if (this.activeConsumers.get(consumerId) !== consumerInfo || consumerInfo.cancelled) {
        try {
          await channel.cancel(consumer.consumerTag)
        } catch {}

        throw new Error(`Consumer for queue ${queueName} was cancelled while its consume was in flight`)
      }

      consumerInfo.channel = channel
      this.#trackConsumerTag(consumerId, consumerInfo, consumer.consumerTag)
      this.#watchChannelLoss(consumerId, consumerInfo, channel)

      return consumer
    })

    const consumerInfo = this.activeConsumers.get(consumerId)

    try {
      const consumer = await this.runSetup(consumerInfo)

      this.logger.info(hooks.successLog(prefetchCount))

      return consumer
    } catch (error) {
      // A subscribe that never got going owns nothing: the registration is
      // undone and the channel it may have opened goes back.
      await this.#dropConsumer(consumerId, consumerInfo)
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
      createProcessor: ({ channel, noAck, events }) => async (content, msg) => {
        const startedAt = this.clock.now()

        await callback(content, msg)

        if (!noAck) {
          this.settleAck(msg, channel, 'ack')
        }

        // After the ack, always: reporting must never reroute a message that
        // already succeeded into the failure path.
        events.processed(msg, this.clock.now() - startedAt)
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
      // this processor reports from onSuccess/onFailure below, where the
      // settlement actually happens.
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

  // basic.cancel only stops NEW deliveries: a handler already running keeps
  // its delivery tag valid only while the channel lives, so the channel must
  // outlive every in-flight handler or their late acks die and the broker
  // redelivers work that actually succeeded. Bounded, because a wedged
  // handler is the application's bug and must not hang unsubscribe forever.
  async #drainInFlight (consumerInfo) {
    if (consumerInfo.inFlight > 0) {
      const drained = new Promise((resolve) => consumerInfo.drainWaiters.push(resolve))

      await Promise.race([drained, this.clock.sleep(this.drainTimeout)])
    }

    // Re-read rather than trust which promise won: the counter is the truth
    // about whether the grace period expired with work still running.
    return consumerInfo.inFlight === 0
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

    // Drains in-flight handlers, drops the consumer and releases its dedicated
    // channel, in that order.
    await this.#dropConsumer(consumerId, consumerInfo)

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
      ...subscribeOptions
    } = options

    const workerPool = new WorkerPool(processorFile, {
      workerCount,
      maxRespawns,
      workerData: { queueName },
      logger: this.logger,
      // The pool's spawn seam. A construction-time dependency, so it arrives
      // through the manager's context like the clock does — never through the
      // per-subscription options, which belong to the caller.
      createWorker: this.createWorker
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
    // Pools are independent: terminating them one after another made shutdown
    // pay the sum of every pool's slowest worker instead of the single slowest.
    await Promise.allSettled([...this.workerPools.values()].map(workerPool => workerPool.terminate()))

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
