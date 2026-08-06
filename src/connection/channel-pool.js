import { setTimeout as sleep } from 'node:timers/promises'

const MAX_REPLACE_ATTEMPTS = 5

class ChannelPool {
  // recoveryInterval is the base backoff between attempts to recreate a dead
  // pool channel (attempt N waits N × this value). Configurable for the same
  // reason consumerRecoveryInterval is: five attempts at the default add up to
  // 7.5s, which is right for a broker restart and far too slow for a test.
  constructor (connection, logger, size = 10, recoveryInterval = 500) {
    this.connection = connection
    this.logger = logger
    this.size = size
    this.recoveryInterval = recoveryInterval
    this.channels = []
    this.index = 0
    this.dedicatedChannels = new Map()
    this.closed = false
  }

  async initialize () {
    await this.close()
    this.closed = false

    for (let i = 0; i < this.size; i++) {
      this.channels.push(await this.#createPoolChannel(i))
    }
  }

  async #createPoolChannel (index) {
    const channel = await this.connection.createConfirmChannel()

    channel.on('error', (error) => {
      if (this.closed) return

      // Stryker disable next-line StringLiteral: log phrasing is not contract
      this.logger.error(`Channel ${index} encountered an error: ${error.message}`)
    })

    // Every lost channel (errored or cleanly closed) emits 'close', so the
    // replacement lives here — a dead channel must never keep being handed
    // out by getChannel().
    channel.on('close', () => {
      if (this.closed || this.channels[index] !== channel) return

      this.channels[index] = null
      this.#replacePoolChannel(index)
    })

    return channel
  }

  async #replacePoolChannel (index) {
    for (let attempt = 1; attempt <= MAX_REPLACE_ATTEMPTS && !this.closed; attempt++) {
      try {
        const channel = await this.#createPoolChannel(index)

        if (this.closed) {
          await this.#closeChannel(channel)

          return
        }

        this.channels[index] = channel
        // Stryker disable next-line StringLiteral: log phrasing is not contract
        this.logger.info(`Pool channel ${index} recreated successfully`)

        return
      } catch (error) {
        // Stryker disable next-line StringLiteral: log phrasing is not contract
        this.logger.warn(`Failed to recreate pool channel ${index} (attempt ${attempt}/${MAX_REPLACE_ATTEMPTS}): ${error.message}`)

        await sleep(this.recoveryInterval * attempt, undefined, { ref: false })
      }
    }

    if (!this.closed) {
      // Stryker disable next-line StringLiteral: log phrasing is not contract
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
        // Stryker disable next-line StringLiteral: log phrasing is not contract
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

    for (const channel of this.channels) {
      await this.#closeChannel(channel)
    }

    for (const [, channel] of this.dedicatedChannels.entries()) {
      await this.#closeChannel(channel)
    }

    this.channels = []
    this.index = 0
    this.dedicatedChannels.clear()
  }
}

export { ChannelPool }
export default ChannelPool
