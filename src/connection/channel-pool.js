import systemClock from '../utils/clock.js'
import detached from '../utils/detached.js'

const MAX_REPLACE_ATTEMPTS = 5

class ChannelPool {
  // Options object rather than positionals: the facade forwards whatever the
  // application configured and the pool owns every default, so a new knob is
  // additive instead of shifting an arity every caller must respell.
  //
  // recoveryInterval is the base backoff between attempts to recreate a dead
  // pool channel (attempt N waits N × this value). Configurable for the same
  // reason consumerRecoveryInterval is: five attempts at the default add up to
  // 7.5s, which is right for a broker restart and far too slow for a test.
  constructor (connection, { logger, size = 10, recoveryInterval = 500, clock = systemClock } = {}) {
    this.connection = connection
    this.logger = logger
    this.size = size
    this.recoveryInterval = recoveryInterval
    this.clock = clock
    this.channels = []
    this.index = 0
    this.dedicatedChannels = new Map()
    this.closed = false
  }

  async initialize () {
    await this.close()
    this.closed = false

    // All slots at once: each open is a broker round trip, and this runs on
    // every reconnect with #channelPool still null — publishing refused and
    // consumers not yet recreated. Serially, a pool of 10 stretched that outage
    // by 10 round trips; slots are index-keyed, so order is immaterial.
    //
    // allSettled, not all: the failure path must wait for EVERY open to land.
    // Closing on the first rejection, while other opens were still in flight,
    // left the late ones unclaimed (the pool was already closed) and unclosed —
    // a channel leak per failed initialize.
    const outcomes = await Promise.allSettled(Array.from({ length: this.size }, (_, index) => this.#createPoolChannel(index)))
    const failure = outcomes.find(outcome => outcome.status === 'rejected')

    if (failure) {
      // Without this the channels that did open stay live on a pool nobody
      // owns — and since `closed` is still false, their close listeners keep
      // dialing replacements: a zombie pool eating the connection's channels.
      await this.close()

      throw failure.reason
    }
  }

  async #createPoolChannel (index) {
    const channel = await this.connection.createConfirmChannel()

    // The slot is claimed BEFORE any further await: the close guard below
    // recognises its channel by slot identity, so a channel dying while the
    // caller was still awaiting used to be installed permanently dead — its
    // close event had already decided the slot was not its own, and no
    // further one would ever come. A closed pool claims nothing: close()
    // already emptied the array and writing into it would resurrect it.
    if (!this.closed) {
      this.channels[index] = channel
    }

    channel.on('error', (error) => {
      if (this.closed) return

      this.logger.error(`Channel ${index} encountered an error: ${error.message}`)
    })

    // Every lost channel (errored or cleanly closed) emits 'close', so the
    // replacement lives here — a dead channel must never keep being handed
    // out by getChannel().
    channel.on('close', () => {
      if (this.closed || this.channels[index] !== channel) return

      this.channels[index] = null
      // Nothing awaits a close handler — see utils/detached.js.
      detached(this.#replacePoolChannel(index), this.logger, `Pool channel ${index} replacement failed unexpectedly`)
    })

    return channel
  }

  // Hand-rolled on purpose, like ConsumerManager.handleConsumerLoss: the loop
  // must re-read `closed` between attempts AND after a successful open (a pool
  // closed mid-attempt has nothing to hand the channel to), and a generic retry
  // helper buries exactly those two fences.
  async #replacePoolChannel (index) {
    for (let attempt = 1; attempt <= MAX_REPLACE_ATTEMPTS && !this.closed; attempt++) {
      try {
        const channel = await this.#createPoolChannel(index)

        if (this.closed) {
          // close() has already emptied `channels`, and #createPoolChannel
          // claims no slot once the pool is closed — there is nothing to give
          // back, only a late channel to discard.
          await this.#closeChannel(channel)

          return
        }

        this.logger.info(`Pool channel ${index} recreated successfully`)

        return
      } catch (error) {
        this.logger.warn(`Failed to recreate pool channel ${index} (attempt ${attempt}/${MAX_REPLACE_ATTEMPTS}): ${error.message}`)

        await this.clock.sleep(this.recoveryInterval * attempt)
      }
    }

    if (!this.closed) {
      this.logger.error(`Pool channel ${index} could not be recreated and its slot is out of rotation`)
    }
  }

  async getDedicatedChannel (id) {
    if (this.dedicatedChannels.has(id)) {
      return this.dedicatedChannels.get(id)
    }

    const channel = await this.connection.createConfirmChannel()

    channel.on('error', (error) => {
      if (!this.closed) {
        this.logger.error(`Dedicated channel ${id} encountered an error: ${error.message}`)
      }

      this.dedicatedChannels.delete(id)
    })

    channel.on('close', () => {
      this.dedicatedChannels.delete(id)
    })

    this.dedicatedChannels.set(id, channel)

    return channel
  }

  // A dedicated channel belongs to exactly one owner (a consumer, the RPC
  // reply route). When that owner goes away the channel has to go with it, or
  // every subscribe/unsubscribe cycle leaks one until the connection reaches
  // channel_max and the broker refuses to open any more.
  async releaseDedicatedChannel (id) {
    const channel = this.dedicatedChannels.get(id)

    if (!channel) return

    this.dedicatedChannels.delete(id)

    await this.#closeChannel(channel)
  }

  getChannel () {
    if (this.channels.length === 0) {
      throw new Error('Channel pool is not initialized')
    }

    // Skip slots whose channel died and is awaiting (or failed) replacement.
    for (let i = 0; i < this.channels.length; i++) {
      const channel = this.channels[this.index]
      this.index = (this.index + 1) % this.channels.length

      if (channel) return channel
    }

    throw new Error('No usable channels available in the pool')
  }

  // Listeners are deliberately NOT stripped before closing: amqplib's own
  // internal 'close' listener is what fails still-unconfirmed publish
  // callbacks — removing it would leave in-flight confirm promises pending
  // forever. The pool's own listeners already no-op once `closed` is set,
  // and external listeners (e.g. the RPC reply channel's) NEED the event.
  async #closeChannel (channel) {
    if (!channel || typeof channel.close !== 'function') return

    try {
      await channel.close()
    } catch (error) {
      // Teardown errors are expected when the connection is already gone.
    }
  }

  async close () {
    this.closed = true

    // #closeChannel never rejects, so every close can be in flight at once: a
    // graceful shutdown then costs the slowest close-ok rather than the sum of
    // size + one-per-consumer round trips, which under a fixed SIGTERM grace
    // period is time the process could not spend draining.
    await Promise.all([...this.channels, ...this.dedicatedChannels.values()].map(channel => this.#closeChannel(channel)))

    this.channels = []
    this.index = 0
    this.dedicatedChannels.clear()
  }
}

export { ChannelPool }
export default ChannelPool
