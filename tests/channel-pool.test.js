import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import { EventEmitter } from 'node:events'
import ChannelPool from '../src/connection/channel-pool.js'
import { FakeChannel, ManualClock, recordingLogger, silentLogger, waitFor as waitForCondition } from './helpers.js'

function createFakeChannel () {
  const channel = new EventEmitter()
  channel.closed = false
  channel.close = async () => {
    channel.closed = true
  }

  return channel
}

function createFakeConnection () {
  const connection = {
    createdChannels: [],
    createConfirmChannel: async () => {
      const channel = createFakeChannel()
      connection.createdChannels.push(channel)

      return channel
    }
  }

  return connection
}

// Hands out the shared FakeChannel, which models amqplib's confirm bookkeeping
// (unconfirmed callbacks are failed by the channel's own 'close' listener) —
// required by the tests that pin ChannelPool's teardown contract.
function createConfirmAwareConnection () {
  const connection = {
    createdChannels: [],
    createConfirmChannel: async () => {
      const channel = new FakeChannel()
      connection.createdChannels.push(channel)

      return channel
    }
  }

  return connection
}

describe('ChannelPool', () => {
  test('initialize creates the configured number of channels', async () => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 4)

    await pool.initialize()

    assert.equal(pool.channels.length, 4)
    assert.equal(connection.createdChannels.length, 4)
  })

  test('getChannel throws before initialization', () => {
    const pool = new ChannelPool(createFakeConnection(), silentLogger, 2)

    assert.throws(() => pool.getChannel(), /not initialized/)
  })

  test('getChannel rotates channels round-robin', async () => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 2)

    await pool.initialize()

    const first = pool.getChannel()
    const second = pool.getChannel()
    const third = pool.getChannel()

    assert.notEqual(first, second)
    assert.equal(first, third)
  })

  test('getDedicatedChannel reuses the channel for the same id', async () => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1)

    const channelA = await pool.getDedicatedChannel('consumer-1')
    const channelB = await pool.getDedicatedChannel('consumer-1')
    const channelC = await pool.getDedicatedChannel('consumer-2')

    assert.equal(channelA, channelB)
    assert.notEqual(channelA, channelC)
  })

  test('dedicated channel is removed from the pool on error and on close', async () => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1)

    const channelA = await pool.getDedicatedChannel('consumer-1')
    channelA.emit('error', new Error('channel error'))

    assert.equal(pool.dedicatedChannels.has('consumer-1'), false)

    const channelB = await pool.getDedicatedChannel('consumer-2')
    channelB.emit('close')

    assert.equal(pool.dedicatedChannels.has('consumer-2'), false)
  })

  test('pool channel is recreated after it dies (error followed by close)', async () => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 2)

    await pool.initialize()

    const original = pool.channels[0]

    original.emit('error', new Error('channel error'))
    original.emit('close')

    await new Promise(resolve => setImmediate(resolve))

    assert.notEqual(pool.channels[0], original)
    assert.ok(pool.channels[0], 'slot must hold a live replacement channel')
    assert.equal(pool.channels.length, 2)
  })

  test('pool channel closed without an error event is also replaced', async () => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1)

    await pool.initialize()

    const original = pool.channels[0]

    original.emit('close')

    await new Promise(resolve => setImmediate(resolve))

    assert.notEqual(pool.channels[0], original)
    assert.ok(pool.channels[0])
  })

  test('a failed channel recreation takes the slot out of rotation instead of crashing', async (t) => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 2)

    await pool.initialize()
    t.after(() => pool.close())

    connection.createConfirmChannel = async () => {
      throw new Error('connection lost')
    }

    const survivor = pool.channels[1]

    pool.channels[0].emit('close')

    await new Promise(resolve => setImmediate(resolve))

    assert.equal(pool.channels[0], null, 'dead slot must not keep the stale channel')

    // getChannel must skip the dead slot and only hand out the live channel
    assert.equal(pool.getChannel(), survivor)
    assert.equal(pool.getChannel(), survivor)
  })

  test('getChannel throws when every channel is dead', async (t) => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1)

    await pool.initialize()
    t.after(() => pool.close())

    connection.createConfirmChannel = async () => {
      throw new Error('connection lost')
    }

    pool.channels[0].emit('close')

    await new Promise(resolve => setImmediate(resolve))

    assert.throws(() => pool.getChannel(), /No usable channels/)
  })

  test('close shuts down every channel and clears state', async () => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 2)

    await pool.initialize()
    await pool.getDedicatedChannel('consumer-1')

    const allChannels = [...connection.createdChannels]

    await pool.close()

    assert.equal(pool.channels.length, 0)
    assert.equal(pool.dedicatedChannels.size, 0)
    assert.ok(allChannels.every(channel => channel.closed))
  })

  test('a pool channel is replaced after a transient failure and returns to rotation', async (t) => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1)

    await pool.initialize()
    // The replacement loop sleeps between attempts; closing the pool stops it.
    t.after(() => pool.close())

    const original = pool.channels[0]
    const realCreate = connection.createConfirmChannel
    // One failure is enough to prove the retry loop exists; the give-up test
    // below walks all five attempts.
    let failures = 1

    // The first replacement attempt fails, the second succeeds.
    connection.createConfirmChannel = async () => {
      if (failures-- > 0) throw new Error('connection not ready')

      return realCreate()
    }

    original.emit('close')

    await waitForCondition(() => pool.channels[0] && pool.channels[0] !== original, 5000, 'channel replaced after retries')

    assert.notEqual(pool.channels[0], original)
    assert.equal(pool.getChannel(), pool.channels[0], 'the replacement must be back in rotation')
  })

  test('a replacement that lands after the pool closed is discarded, not left open', async (t) => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1)

    await pool.initialize()

    const original = pool.channels[0]
    let releaseCreate
    const gate = new Promise(resolve => { releaseCreate = resolve })
    const realCreate = connection.createConfirmChannel

    // Symmetric with the sibling test: an assertion failing before the explicit
    // close below must not strand the gate or the pool.
    t.after(() => {
      releaseCreate?.()

      return pool.close()
    })

    connection.createConfirmChannel = async () => {
      await gate

      return realCreate()
    }

    original.emit('close')

    // The pool shuts down while the replacement is still being created.
    await pool.close()
    releaseCreate()

    await waitForCondition(() => connection.createdChannels.length === 2, 3000, 'replacement channel created')

    const late = connection.createdChannels[1]

    await waitForCondition(() => late.closed, 3000, 'late replacement closed')

    assert.throws(() => pool.getChannel(), /not initialized/, 'a closed pool must not adopt the late channel')
  })

  test('close must not strip channel listeners: in-flight publish confirms still settle', async () => {
    // Regression, and the reason #closeChannel deliberately does NOT call
    // removeAllListeners: amqplib's own 'close' listener is what fails every
    // still-unconfirmed publish callback. Stripping it left in-flight confirm
    // promises pending forever — a publish that never resolves nor rejects.
    //
    // FakeChannel models that amqplib behaviour, so this test fails the moment
    // the listeners are stripped again.
    const connection = createConfirmAwareConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1)

    await pool.initialize()

    const channel = pool.getChannel()
    // A publish whose confirm the broker has not answered yet.
    const inFlight = new Promise((resolve, reject) => {
      channel.publish('ex', 'rk', Buffer.from('x'), {}, (err) => (err ? reject(err) : resolve()))
    })

    assert.equal(channel.unconfirmedCount, 1, 'the publish must be unconfirmed before closing')

    await pool.close()

    await assert.rejects(() => inFlight, /channel closed/, 'the confirm must be failed, never left pending')
  })

  test('channel errors after the pool closed are not reported', async () => {
    // Closing a pool makes the broker error its channels. Logging those would
    // bury the real cause of a shutdown under noise the operator cannot act on.
    const logger = recordingLogger()
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, logger, 1)

    await pool.initialize()

    const channel = pool.channels[0]

    channel.emit('error', new Error('while the pool was alive'))
    assert.ok(logger.records.error.some(line => line.includes('while the pool was alive')), 'a live pool reports')

    await pool.close()

    channel.emit('error', new Error('after the pool closed'))

    assert.equal(
      logger.records.error.some(line => line.includes('after the pool closed')),
      false,
      'teardown noise stays quiet'
    )
  })

  test('close tolerates a channel that refuses to close', async () => {
    // Teardown errors are routine when the connection is already gone: they
    // must not abort the loop and leave the remaining channels open.
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 2)

    await pool.initialize()

    const [first, second] = pool.channels

    first.close = async () => { throw new Error('connection already gone') }

    await pool.close()

    assert.equal(second.closed, true, 'the second channel was still closed')
    assert.deepEqual(pool.channels, [], 'the pool emptied despite the failure')
  })

  test('a slot whose replacement never succeeds is reported and left out of rotation', async (t) => {
    // The injected clock records every requested backoff instead of waiting
    // it out, so the loop runs at the PRODUCTION interval and the exact
    // `recoveryInterval * attempt` sequence is pinned — a mutated multiplier,
    // a dropped attempt or a constant delay all change the recorded list.
    // (This test used to sleep 300ms of real time for a weaker >= bound.)
    const logger = recordingLogger()
    const connection = createFakeConnection()
    const clock = new ManualClock()
    const pool = new ChannelPool(connection, logger, 1, 500, clock)

    await pool.initialize()
    t.after(() => pool.close())

    const original = pool.channels[0]

    connection.createConfirmChannel = async () => { throw new Error('connection not ready') }

    original.emit('close')

    await waitForCondition(
      () => logger.records.error.some(line => line.includes('could not be recreated')),
      15000,
      'the exhausted slot is reported'
    )

    assert.equal(pool.channels[0], null, 'the dead slot is left empty, never handed out')
    assert.deepEqual(clock.sleeps, [500, 1000, 1500, 2000, 2500], 'five attempts, each backing off progressively')
  })

  test('a stale close event from an already-replaced channel does not replace again', async () => {
    // The same channel can emit 'close' more than once (error paths often
    // re-emit); only the event from the CURRENT occupant of the slot may
    // trigger a replacement, or every stale event dials one more channel.
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1, new ManualClock())

    await pool.initialize()

    const original = pool.channels[0]

    original.emit('close')
    await waitForCondition(() => pool.channels[0] && pool.channels[0] !== original, 3000, 'first replacement')

    const dialed = connection.createdChannels.length

    original.emit('close')
    await new Promise(resolve => setImmediate(resolve))

    assert.equal(connection.createdChannels.length, dialed, 'the stale event dialed nothing')
  })

  test('close resolves cleanly with an exhausted (null) slot in the pool', async () => {
    // A slot whose recreation gave up holds null; close() must skip it
    // instead of calling close on nothing.
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1, new ManualClock())

    await pool.initialize()

    connection.createConfirmChannel = async () => { throw new Error('gone') }
    pool.channels[0].emit('close')

    await waitForCondition(() => pool.channels[0] === null, 3000, 'slot exhausted')

    await pool.close()

    assert.deepEqual(pool.channels, [], 'teardown completed past the empty slot')
  })

  test('closing the pool mid-recovery silences the exhaustion report', async () => {
    // The operator asked for the shutdown; reporting the interrupted recovery
    // as a dead slot would point them at a problem that no longer exists.
    const logger = recordingLogger()
    const connection = createFakeConnection()
    const clock = new ManualClock()
    const pool = new ChannelPool(connection, logger, 1, 1, clock)

    await pool.initialize()

    let releaseDial
    const dialBlocked = new Promise(resolve => { releaseDial = resolve })

    connection.createConfirmChannel = async () => {
      await dialBlocked

      throw new Error('gone')
    }

    pool.channels[0].emit('close')

    await pool.close()
    releaseDial()
    await new Promise(resolve => setImmediate(resolve))

    assert.equal(
      logger.records.error.some(line => line.includes('could not be recreated')),
      false,
      'an interrupted recovery is not an exhausted slot'
    )
  })

  test('dedicated channel errors are reported while the pool is open and silenced after close', async () => {
    const logger = recordingLogger()
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, logger, 1)

    // Before initialize: dedicated channels are usable and their errors real.
    const early = await pool.getDedicatedChannel('early')

    early.emit('error', new Error('early trouble'))
    assert.ok(logger.records.error.some(line => line.includes('early trouble')), 'a fresh pool reports dedicated errors')

    await pool.initialize()

    const dedicated = await pool.getDedicatedChannel('worker')

    await pool.close()

    dedicated.emit('error', new Error('post-close noise'))

    assert.equal(
      logger.records.error.some(line => line.includes('post-close noise')),
      false,
      'teardown noise stays quiet on dedicated channels too'
    )
  })

  test('defaults the recovery backoff to 500ms', () => {
    // Every other test pins the interval to keep the suite fast, which leaves
    // the production default asserted nowhere.
    const pool = new ChannelPool(createFakeConnection(), silentLogger, 1)

    assert.equal(pool.recoveryInterval, 500)
  })

  test('a channel closing during pool shutdown does not trigger a replacement', async () => {
    // Without the `closed` guard in the channel 'close' handler, pool.close()
    // would kick off replacement loops that recreate channels on a connection
    // that is going away.
    const connection = createConfirmAwareConnection()
    const pool = new ChannelPool(connection, silentLogger, 2)

    await pool.initialize()

    const createdDuringSetup = connection.createdChannels.length

    await pool.close()
    await new Promise(resolve => setImmediate(resolve))

    assert.equal(
      connection.createdChannels.length,
      createdDuringSetup,
      'shutting the pool down must not create replacement channels'
    )
  })
})

describe('ChannelPool resource ownership', () => {
  test('a failure mid-initialize closes the channels already created', async () => {
    // Left open, they sit on a pool nobody owns whose `closed` is still false
    // — so their close listeners keep dialing replacements forever.
    const connection = createFakeConnection()
    let created = 0

    const realCreate = connection.createConfirmChannel.bind(connection)

    connection.createConfirmChannel = async () => {
      if (++created === 3) throw new Error('channel_max reached')

      return realCreate()
    }

    const pool = new ChannelPool(connection, silentLogger, 5, 1, new ManualClock())

    await assert.rejects(() => pool.initialize(), /channel_max reached/)

    assert.equal(pool.closed, true, 'the orphan pool is marked closed so replacements stop')
    assert.deepEqual(pool.channels, [], 'no half-built pool left behind')
    assert.equal(
      connection.createdChannels.every(channel => channel.closed),
      true,
      'every channel created before the failure was closed'
    )
  })

  test('releaseDedicatedChannel closes it and lets the next request build a fresh one', async () => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1, 1, new ManualClock())

    await pool.initialize()

    const first = await pool.getDedicatedChannel('worker')

    await pool.releaseDedicatedChannel('worker')

    assert.equal(first.closed, true, 'the released channel was closed')

    const second = await pool.getDedicatedChannel('worker')

    assert.notEqual(second, first, 'the id is free for a fresh channel')

    await pool.close()
  })

  test('releasing an unknown id is a no-op', async () => {
    const pool = new ChannelPool(createFakeConnection(), silentLogger, 1, 1, new ManualClock())

    await pool.initialize()
    await pool.releaseDedicatedChannel('never-existed')
    await pool.close()
  })
})
