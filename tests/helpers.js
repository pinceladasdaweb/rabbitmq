import { EventEmitter } from 'node:events'

export const silentLogger = { info () {}, warn () {}, error () {}, debug () {} }

// A logger that records what was written, for tests asserting on log output.
export const recordingLogger = () => {
  const records = { info: [], warn: [], error: [], debug: [] }

  return {
    records,
    info: (message) => records.info.push(message),
    warn: (message) => records.warn.push(message),
    error: (message) => records.error.push(message),
    debug: (message) => records.debug.push(message)
  }
}

export const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

export const waitFor = async (predicate, timeoutMs = 3000, label = 'condition') => {
  const start = Date.now()

  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return

    await sleep(5)
  }

  throw new Error(`Timeout waiting for: ${label}`)
}

// Deterministic stand-in for src/utils/clock.js. Time only moves when the
// test calls advance(), which also fires any interval whose turn came up —
// so a sweep that used to need a real 900ms sleep is now a synchronous call.
// sleep() resolves immediately and records the requested duration, letting a
// test assert pacing (the leaky bucket's smoothing) without waiting it out.
export class ManualClock {
  constructor (start = 0) {
    this.currentTime = start
    this.intervals = new Map()
    this.nextIntervalId = 1
    this.sleeps = []
  }

  now () {
    return this.currentTime
  }

  setInterval (fn, ms) {
    const id = this.nextIntervalId++

    this.intervals.set(id, { fn, ms, nextAt: this.currentTime + ms })

    return { id, unref: () => {} }
  }

  clearInterval (handle) {
    if (handle) this.intervals.delete(handle.id)
  }

  sleep (ms) {
    this.sleeps.push(ms)

    return Promise.resolve()
  }

  advance (ms) {
    const target = this.currentTime + ms

    // Fire due intervals in timestamp order, with now() set to each firing
    // time — an interval crossing several periods fires once per period, as
    // the real timer would.
    for (;;) {
      let earliest = null

      for (const interval of this.intervals.values()) {
        if (interval.nextAt <= target && (!earliest || interval.nextAt < earliest.nextAt)) {
          earliest = interval
        }
      }

      if (!earliest) break

      this.currentTime = earliest.nextAt
      earliest.nextAt += earliest.ms
      earliest.fn()
    }

    this.currentTime = target
  }
}

// Single fake channel shared by every unit test, kept deliberately faithful to
// amqplib's ConfirmChannel contract so a lenient fake cannot green-light
// broken production code:
//
//   - publish() returns a boolean (false = write buffer full) and invokes the
//     confirm callback asynchronously, never synchronously;
//   - the confirm callback is the ONLY way a publish settles, so a caller that
//     forgets to await it is observable through pendingConfirms;
//   - consume() resolves with { consumerTag }.
//
// Failure injection knobs:
//   confirmErrors      FIFO of errors handed to successive confirm callbacks
//   neverConfirm       confirm callbacks are never invoked (dead broker)
//   manualConfirms     confirms are held until releaseConfirms() is called
//   keepGoingResults   FIFO of publish() return values (false triggers drain)
//   consumeError       consume() rejects with this
//   assertExchangeError / assertQueueError  assertion failures
export class FakeChannel extends EventEmitter {
  constructor () {
    super()
    this.published = []
    this.consumers = []
    this.acked = []
    this.nacked = []
    this.cancelled = []
    this.prefetches = []
    this.assertedExchanges = []
    this.assertedQueues = []
    this.boundQueues = []
    this.deletedExchanges = []
    this.deletedQueues = []
    this.closed = false

    this.confirmErrors = []
    this.neverConfirm = false
    this.manualConfirms = false
    this.confirmDelayMs = 0
    this.keepGoingResults = []
    this.consumeError = null
    this.assertExchangeError = null
    this.assertQueueError = null
    this.returnRoutingKey = null

    this.pendingConfirms = []
    this.consumeSequence = 0

    // Faithful to amqplib (lib/channel.js): the channel's own 'close' listener
    // fails every still-unconfirmed publish callback. Production relies on
    // this — ChannelPool.close() must not strip channel listeners, or
    // in-flight confirm promises would never settle. Modelling it here is what
    // makes that invariant testable at all.
    this.on('close', () => {
      const unconfirmed = this.pendingConfirms.splice(0)

      for (const settle of unconfirmed) settle(new Error('channel closed'))
    })
  }

  publish (exchange, routingKey, content, options, confirmCallback) {
    this.published.push({ exchange, routingKey, content, options })

    if (this.returnRoutingKey) {
      // basic.return reaches the client before the confirm, as the broker does.
      this.emit('return', { fields: { routingKey: this.returnRoutingKey } })
    }

    if (confirmCallback) {
      const error = this.confirmErrors.shift() ?? null
      let settled = false
      const settle = (override) => {
        if (settled) return

        settled = true
        confirmCallback(override ?? error)
      }

      // Every publish is unconfirmed until its callback runs, so a channel
      // close can fail it (see the 'close' listener in the constructor).
      this.pendingConfirms.push(settle)

      const release = () => {
        const index = this.pendingConfirms.indexOf(settle)

        if (index !== -1) this.pendingConfirms.splice(index, 1)

        settle()
      }

      if (!this.manualConfirms && !this.neverConfirm) {
        if (this.confirmDelayMs > 0) {
          setTimeout(release, this.confirmDelayMs)
        } else {
          setImmediate(release)
        }
      }
    }

    return this.keepGoingResults.length > 0 ? this.keepGoingResults.shift() : true
  }

  // Releases confirms held by manualConfirms, oldest first.
  releaseConfirms (count = Infinity) {
    const released = this.pendingConfirms.splice(0, count)

    for (const settle of released) settle()

    return released.length
  }

  get unconfirmedCount () {
    return this.pendingConfirms.length
  }

  async consume (queue, callback, options) {
    if (this.consumeError) throw this.consumeError

    const consumer = { queue, callback, options, consumerTag: `tag-${++this.consumeSequence}` }

    this.consumers.push(consumer)

    return { consumerTag: consumer.consumerTag }
  }

  async cancel (consumerTag) {
    this.cancelled.push(consumerTag)
  }

  async prefetch (count) {
    this.prefetches.push(count)
  }

  ack (msg) {
    this.acked.push(msg)
  }

  nack (msg, allUpTo, requeue) {
    this.nacked.push({ msg, allUpTo, requeue })
  }

  async assertExchange (name, type, options) {
    if (this.assertExchangeError) throw this.assertExchangeError

    this.assertedExchanges.push({ name, type, options })
  }

  async assertQueue (name, options) {
    if (this.assertQueueError) throw this.assertQueueError

    this.assertedQueues.push({ name, options })
  }

  async bindQueue (queue, exchange, routingKey) {
    this.boundQueues.push({ queue, exchange, routingKey })
  }

  async deleteExchange (name) {
    this.deletedExchanges.push(name)
  }

  async deleteQueue (name) {
    this.deletedQueues.push(name)
  }

  async close () {
    this.closed = true
    this.emit('close')
  }
}

// Fake amqplib connection: hands out FakeChannels and records them.
export class FakeAmqpConnection extends EventEmitter {
  constructor () {
    super()
    this.channels = []
    this.channelError = null
    this.closed = false
  }

  async createConfirmChannel () {
    if (this.channelError) throw this.channelError

    const channel = new FakeChannel()

    this.channels.push(channel)

    return channel
  }

  async close () {
    this.closed = true
  }

  publishedOn () {
    return this.channels.flatMap(channel => channel.published)
  }

  consumersOn () {
    return this.channels.flatMap(channel => channel.consumers)
  }
}

// Scripted dialer standing in for amqp.connect. `plan` is a list of outcomes
// ('ok' or an Error) consumed in order; the last entry repeats.
export const createDialer = (plan = ['ok']) => {
  const remaining = [...plan]

  const dialer = {
    dials: 0,
    urls: [],
    connections: [],
    // The connection captures the dial function at construction time, so
    // tests customize freshly dialed connections through this hook rather
    // than by reassigning dialer.connect (which would never be seen).
    onConnection: null,
    connect: async (url, socketOptions) => {
      dialer.dials++
      dialer.urls.push(url)
      dialer.socketOptions = socketOptions

      const outcome = remaining.length > 1 ? remaining.shift() : remaining[0]

      if (outcome !== 'ok') throw outcome

      const connection = new FakeAmqpConnection()

      dialer.connections.push(connection)
      dialer.onConnection?.(connection)

      return connection
    }
  }

  return dialer
}
