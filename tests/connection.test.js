import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import { EventEmitter } from 'node:events'
import RabbitMQConnection from '../src/connection/connection.js'

const silentLogger = { info () {}, warn () {}, error () {}, debug () {} }

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const waitFor = async (predicate, timeoutMs = 3000, label = 'condition') => {
  const start = Date.now()

  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return

    await sleep(5)
  }

  throw new Error(`Timeout waiting for: ${label}`)
}

class FakeAmqpConnection extends EventEmitter {
  async close () {
    this.closed = true
  }
}

// A controllable dialer: `plan` is a list of outcomes ('ok' or an Error) that
// dials consume in order; the last entry repeats for further dials.
const createDialer = (plan = ['ok']) => {
  const dialer = {
    dials: [],
    connections: [],
    connect: async (url, socketOptions) => {
      dialer.dials.push({ url, socketOptions })

      const outcome = plan.length > 1 ? plan.shift() : plan[0]

      if (outcome !== 'ok') throw outcome

      const connection = new FakeAmqpConnection()

      dialer.connections.push(connection)

      return connection
    }
  }

  return dialer
}

const createConnection = (dialer, options = {}) => new RabbitMQConnection({
  username: 'admin',
  password: 'admin',
  endpoints: ['node-a:5672'],
  connectionName: 'unit-test',
  reconnectInterval: 10,
  maxReconnectInterval: 20,
  amqpConnect: dialer.connect,
  ...options
}, silentLogger)

describe('RabbitMQConnection constructor', () => {
  test('rejects invalid protocols', () => {
    assert.throws(() => createConnection(createDialer(), { protocol: 'http' }), /Invalid protocol/)
  })

  test('rejects empty or falsy endpoints', () => {
    assert.throws(() => createConnection(createDialer(), { endpoints: [] }), /At least one valid/)
    assert.throws(() => createConnection(createDialer(), { endpoints: [undefined] }), /At least one valid/)
  })
})

describe('RabbitMQConnection connect', () => {
  test('dials with encoded credentials, vhost and the connection name', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(dialer, {
      username: 'u ser',
      password: 'p@ss/word',
      vhost: 'my/vhost',
      protocol: 'amqps'
    })

    t.after(() => connection.disconnect())

    const result = await connection.connect()

    assert.ok(result, 'connect must return the connection')
    assert.equal(dialer.dials[0].url, 'amqps://u%20ser:p%40ss%2Fword@node-a:5672/my%2Fvhost')
    assert.equal(dialer.dials[0].socketOptions.clientProperties.connection_name, 'unit-test')
    assert.equal(connection.getConnectionState(), 'connected')
  })

  test('emits connected with the endpoint and reports cluster status', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(dialer)

    t.after(() => connection.disconnect())

    const events = []

    connection.on('connected', (endpoint) => events.push(endpoint))

    await connection.connect()

    assert.deepEqual(events, ['node-a:5672'])
    assert.equal(connection.getCurrentEndpoint(), 'node-a:5672')
    assert.deepEqual(connection.getAllEndpoints(), ['node-a:5672'])
    assert.equal(connection.getConnection(), dialer.connections[0])
  })

  test('a second connect() while connected reuses the connection without dialing again', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(dialer)

    t.after(() => connection.disconnect())

    const first = await connection.connect()
    const second = await connection.connect()

    assert.equal(first, second)
    assert.equal(dialer.dials.length, 1)
  })

  test('concurrent connect() callers share a single in-flight dial', async (t) => {
    const dialer = createDialer()
    let release
    const gate = new Promise(resolve => { release = resolve })
    const gatedConnect = dialer.connect

    dialer.connect = async (...args) => {
      await gate

      return gatedConnect(...args)
    }

    const connection = createConnection(dialer)

    t.after(() => connection.disconnect())

    const attempts = [connection.connect(), connection.connect()]

    release()

    const [first, second] = await Promise.all(attempts)

    assert.equal(first, second)
    assert.equal(dialer.dials.length, 1, 'two parallel loops would leak a connection')
  })

  test('rotates to the next endpoint when the first one fails', async (t) => {
    const dialer = createDialer([new Error('ECONNREFUSED'), 'ok'])
    const connection = createConnection(dialer, { endpoints: ['node-a:5672', 'node-b:5672'] })

    t.after(() => connection.disconnect())

    const result = await connection.connect()

    assert.ok(result)
    assert.equal(dialer.dials.length, 2)
    assert.match(dialer.dials[1].url, /node-b:5672/)
    assert.equal(connection.getCurrentEndpoint(), 'node-b:5672')
  })
})

describe('RabbitMQConnection reconnection', () => {
  test('starts background reconnection when every endpoint fails, then recovers', async (t) => {
    const dialer = createDialer([new Error('down'), new Error('still down'), 'ok'])
    const connection = createConnection(dialer)

    t.after(() => connection.disconnect())

    const events = []

    connection.on('disconnected', () => events.push('disconnected'))
    connection.on('reconnected', () => events.push('reconnected'))

    const result = await connection.connect()

    assert.equal(result, null, 'connect() returns null when every endpoint fails')
    assert.equal(connection.getConnectionState(), 'reconnecting')

    await waitFor(() => events.includes('reconnected'), 3000, 'background reconnection')

    assert.deepEqual(events, ['disconnected', 'reconnected'])
    assert.equal(connection.getConnectionState(), 'connected')
  })

  test('an unexpected connection close triggers reconnection', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(dialer)

    t.after(() => connection.disconnect())

    await connection.connect()

    const reconnected = new Promise(resolve => connection.once('reconnected', resolve))
    const disconnected = new Promise(resolve => connection.once('disconnected', resolve))

    dialer.connections[0].emit('close')

    await disconnected
    await reconnected

    assert.equal(connection.getConnectionState(), 'connected')
    assert.equal(dialer.dials.length, 2)
  })

  test('gives up with reconnectFailed after maxReconnectAttempts', async (t) => {
    const dialer = createDialer([new Error('permanently down')])
    const connection = createConnection(dialer, { maxReconnectAttempts: 2 })

    t.after(() => connection.disconnect())

    const failed = new Promise(resolve => connection.once('reconnectFailed', resolve))

    await connection.connect()
    await failed

    assert.equal(connection.getConnectionState(), 'failed')
    // 1 initial dial + 2 reconnect attempts, nothing further scheduled.
    assert.equal(dialer.dials.length, 3)

    await sleep(50)
    assert.equal(dialer.dials.length, 3, 'no dials after giving up')
  })

  test('emits connectionStateChanged through the lifecycle', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(dialer)

    t.after(() => connection.disconnect())

    const states = []

    connection.on('connectionStateChanged', (state) => states.push(state))

    await connection.connect()
    await connection.disconnect()

    assert.deepEqual(states, ['connecting', 'connected', 'disconnecting', 'disconnected'])
  })
})

describe('RabbitMQConnection disconnect', () => {
  test('closes the connection and stops reacting to its close event', async () => {
    const dialer = createDialer()
    const connection = createConnection(dialer)

    await connection.connect()

    const amqpConnection = dialer.connections[0]
    let reconnectionStarted = false

    connection.on('disconnected', () => { reconnectionStarted = true })

    await connection.disconnect()

    assert.equal(amqpConnection.closed, true)
    assert.equal(connection.getConnectionState(), 'disconnected')
    assert.equal(connection.getConnection(), null)

    // A late close from the dead socket must not restart reconnection.
    amqpConnection.emit('close')
    await sleep(30)

    assert.equal(reconnectionStarted, false)
    assert.equal(dialer.dials.length, 1)
  })

  test('disconnect while reconnecting cancels the retry loop', async () => {
    const dialer = createDialer([new Error('down')])
    const connection = createConnection(dialer)

    await connection.connect()
    assert.equal(connection.getConnectionState(), 'reconnecting')

    await connection.disconnect()

    const dialsAtDisconnect = dialer.dials.length

    await sleep(60)

    assert.equal(dialer.dials.length, dialsAtDisconnect, 'no dials after disconnect')
    assert.equal(connection.getConnectionState(), 'disconnected')
  })

  test('disconnect on an already disconnected instance is a no-op', async () => {
    const connection = createConnection(createDialer())

    await connection.disconnect()

    assert.equal(connection.getConnectionState(), 'disconnected')
  })

  test('connect() after disconnect() re-enables automatic reconnection', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(dialer)

    t.after(() => connection.disconnect())

    await connection.connect()
    await connection.disconnect()
    await connection.connect()

    const reconnected = new Promise(resolve => connection.once('reconnected', resolve))

    dialer.connections.at(-1).emit('close')

    await reconnected

    assert.equal(connection.getConnectionState(), 'connected')
  })
})
