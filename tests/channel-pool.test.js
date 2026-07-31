import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import { EventEmitter } from 'node:events'
import ChannelPool from '../src/connection/channel-pool.js'
import { FakeChannel, silentLogger, waitFor as waitForCondition } from './helpers.js'

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
    const pool = new ChannelPool(connection, silentLogger, 1)

    const channelA = await pool.getDedicatedChannel('consumer-1')
    const channelB = await pool.getDedicatedChannel('consumer-1')
    const channelC = await pool.getDedicatedChannel('consumer-2')

    assert.equal(channelA, channelB)
    assert.notEqual(channelA, channelC)
  })

  test('dedicated channel is removed from the pool on error and on close', async () => {
    const connection = createFakeConnection()
    const pool = new ChannelPool(connection, silentLogger, 1)

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
    const pool = new ChannelPool(connection, silentLogger, 1)

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
    const pool = new ChannelPool(connection, silentLogger, 1)

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
    const pool = new ChannelPool(connection, silentLogger, 1)

    await pool.initialize()
    // The replacement loop sleeps between attempts; closing the pool stops it.
    t.after(() => pool.close())

    const original = pool.channels[0]
    const realCreate = connection.createConfirmChannel
    // One failure is enough to prove the retry loop exists; each extra attempt
    // costs a real 500ms * attempt backoff and this file is the suite's
    // critical path.
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
    const pool = new ChannelPool(connection, silentLogger, 1)

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
    const pool = new ChannelPool(connection, silentLogger, 1)

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
