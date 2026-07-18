import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import { EventEmitter } from 'node:events'
import ChannelPool from '../src/connection/channel-pool.js'

const silentLogger = {
  info: () => {},
  warn: () => {},
  error: () => {}
}

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
})
