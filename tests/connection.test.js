import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import RabbitMQConnection from '../src/connection/connection.js'
import { installDialer } from './fake-amqp.js'
import { createDialer, recordingLogger, silentLogger, sleep, waitFor } from './helpers.js'

// The dialer is installed onto amqplib itself (see fake-amqp.js), so nothing
// test-specific reaches the constructor.
const createConnection = (t, dialer, { logger = silentLogger, ...options } = {}) => {
  installDialer(t, dialer)

  return new RabbitMQConnection({
    username: 'admin',
    password: 'admin',
    endpoints: ['node-a:5672'],
    connectionName: 'unit-test',
    reconnectInterval: 10,
    maxReconnectInterval: 20,
    ...options
  }, logger)
}

describe('RabbitMQConnection constructor', () => {
  // These reject before anything is dialed, so they need no fake dialer.
  const construct = (options) => new RabbitMQConnection({
    username: 'admin',
    password: 'admin',
    endpoints: ['node-a:5672'],
    ...options
  }, silentLogger)

  test('rejects invalid protocols', () => {
    assert.throws(() => construct({ protocol: 'http' }), /Invalid protocol/)
  })

  test('rejects empty or falsy endpoints', () => {
    assert.throws(() => construct({ endpoints: [] }), /At least one valid/)
    assert.throws(() => construct({ endpoints: [undefined] }), /At least one valid/)
  })
})

describe('RabbitMQConnection connect', () => {
  test('dials with encoded credentials, vhost and the connection name', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(t, dialer, {
      username: 'u ser',
      password: 'p@ss/word',
      vhost: 'my/vhost',
      protocol: 'amqps'
    })

    t.after(() => connection.disconnect())

    const result = await connection.connect()

    assert.ok(result, 'connect must return the connection')
    assert.equal(dialer.urls[0], 'amqps://u%20ser:p%40ss%2Fword@node-a:5672/my%2Fvhost')
    assert.equal(dialer.socketOptions.clientProperties.connection_name, 'unit-test')
    assert.equal(connection.getConnectionState(), 'connected')
  })

  test('emits connected with the endpoint and reports cluster status', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(t, dialer)

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
    const connection = createConnection(t, dialer)

    t.after(() => connection.disconnect())

    const first = await connection.connect()
    const second = await connection.connect()

    assert.equal(first, second)
    assert.equal(dialer.dials, 1)
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

    const connection = createConnection(t, dialer)

    t.after(() => connection.disconnect())

    const attempts = [connection.connect(), connection.connect()]

    release()

    const [first, second] = await Promise.all(attempts)

    assert.equal(first, second)
    assert.equal(dialer.dials, 1, 'two parallel loops would leak a connection')
  })

  test('rotates to the next endpoint when the first one fails', async (t) => {
    const dialer = createDialer([new Error('ECONNREFUSED'), 'ok'])
    const connection = createConnection(t, dialer, { endpoints: ['node-a:5672', 'node-b:5672'] })

    t.after(() => connection.disconnect())

    const result = await connection.connect()

    assert.ok(result)
    assert.equal(dialer.dials, 2)
    assert.match(dialer.urls[1], /node-b:5672/)
    assert.equal(connection.getCurrentEndpoint(), 'node-b:5672')
  })
})

describe('RabbitMQConnection reconnection', () => {
  test('starts background reconnection when every endpoint fails, then recovers', async (t) => {
    const dialer = createDialer([new Error('down'), new Error('still down'), 'ok'])
    const connection = createConnection(t, dialer)

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
    const connection = createConnection(t, dialer)

    t.after(() => connection.disconnect())

    await connection.connect()

    const reconnected = new Promise(resolve => connection.once('reconnected', resolve))
    const disconnected = new Promise(resolve => connection.once('disconnected', resolve))

    dialer.connections[0].emit('close')

    await disconnected
    await reconnected

    assert.equal(connection.getConnectionState(), 'connected')
    assert.equal(dialer.dials, 2)
  })

  test('a successful connection resets the attempt counter so the next outage gets a full budget', async (t) => {
    // Regression: without the reset on success, attempts accumulate across
    // outages and a client with a finite budget gives up prematurely (here it
    // would abandon the second outage without dialing at all).
    const dialer = createDialer([new Error('down'), 'ok', new Error('down again')])
    const connection = createConnection(t, dialer, { maxReconnectAttempts: 2 })

    t.after(() => connection.disconnect())

    const recovered = new Promise(resolve => connection.once('reconnected', resolve))

    await connection.connect()
    await recovered

    assert.equal(dialer.dials, 2, 'first outage: one failed dial plus one successful retry')

    const failed = new Promise(resolve => connection.once('reconnectFailed', resolve))

    dialer.connections.at(-1).emit('close')

    await failed

    assert.equal(dialer.dials, 4, 'second outage must still get its two full attempts')
  })

  test('gives up with reconnectFailed after maxReconnectAttempts', async (t) => {
    const dialer = createDialer([new Error('permanently down')])
    const connection = createConnection(t, dialer, { maxReconnectAttempts: 2 })

    t.after(() => connection.disconnect())

    const failed = new Promise(resolve => connection.once('reconnectFailed', resolve))

    await connection.connect()
    await failed

    assert.equal(connection.getConnectionState(), 'failed')
    // 1 initial dial + 2 reconnect attempts, nothing further scheduled.
    assert.equal(dialer.dials, 3)

    await sleep(50)
    assert.equal(dialer.dials, 3, 'no dials after giving up')
  })

  test('emits connectionStateChanged through the lifecycle', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(t, dialer)

    t.after(() => connection.disconnect())

    const states = []

    connection.on('connectionStateChanged', (state) => states.push(state))

    await connection.connect()
    await connection.disconnect()

    assert.deepEqual(states, ['connecting', 'connected', 'disconnecting', 'disconnected'])
  })
})

describe('RabbitMQConnection disconnect', () => {
  test('closes the connection and stops reacting to its close event', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(t, dialer)

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
    assert.equal(dialer.dials, 1)
  })

  test('disconnect while reconnecting cancels the retry loop', async (t) => {
    const dialer = createDialer([new Error('down')])
    const connection = createConnection(t, dialer)

    await connection.connect()
    assert.equal(connection.getConnectionState(), 'reconnecting')

    await connection.disconnect()

    const dialsAtDisconnect = dialer.dials

    await sleep(60)

    assert.equal(dialer.dials, dialsAtDisconnect, 'no dials after disconnect')
    assert.equal(connection.getConnectionState(), 'disconnected')
  })

  test('logs a connection error without tearing the connection down', async (t) => {
    const logger = recordingLogger()
    const dialer = createDialer()
    const connection = createConnection(t, dialer, { logger })

    t.after(() => connection.disconnect())

    await connection.connect()

    // amqplib always follows a connection 'error' with 'close', so the error
    // handler must only report: 'close' owns the recovery decision. The
    // load-bearing assertion is therefore "no redial", not the transient state.
    dialer.connections[0].emit('error', new Error('socket hiccup'))

    assert.ok(logger.records.error.some(message => /Connection error: socket hiccup/.test(message)))

    // Recovery is scheduled on a timer, so asserting right after the emit would
    // pass even if the handler had started one. Wait past the backoff window
    // (reconnectInterval 10ms, maxReconnectInterval 20ms) before concluding.
    await sleep(80)

    assert.equal(dialer.dials, 1, 'the error handler must not start recovery on its own')
    assert.equal(connection.getConnectionState(), 'connected')
  })

  test('the real error-then-close sequence reconnects exactly once', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(t, dialer)

    t.after(() => connection.disconnect())

    await connection.connect()

    const reconnected = new Promise(resolve => connection.once('reconnected', resolve))

    // This is the ordering amqplib actually produces on a failing socket.
    dialer.connections[0].emit('error', new Error('ECONNRESET'))
    dialer.connections[0].emit('close')

    await reconnected

    assert.equal(connection.getConnectionState(), 'connected')
    assert.equal(dialer.dials, 2, 'error + close together must produce one redial, not two')
  })

  test('a close() that throws still leaves the instance cleanly disconnected', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(t, dialer)

    await connection.connect()

    dialer.connections[0].close = async () => {
      throw new Error('socket already gone')
    }

    await connection.disconnect()

    assert.equal(connection.getConnectionState(), 'disconnected')
    assert.equal(connection.getConnection(), null, 'the dead connection must be released regardless')
  })

  test('disconnect on an already disconnected instance is a no-op', async (t) => {
    const connection = createConnection(t, createDialer())

    await connection.disconnect()

    assert.equal(connection.getConnectionState(), 'disconnected')
  })

  test('a dial that fails after disconnect() was called settles as disconnected, not reconnecting', async (t) => {
    // disconnect() lands while connect() is still dialing. Starting a
    // reconnection loop at that point would resurrect a connection the caller
    // explicitly shut down, and nothing would ever stop it.
    let releaseDial
    const dialGate = new Promise(resolve => { releaseDial = resolve })

    const dialer = {
      connections: [],
      connect: async () => {
        await dialGate

        throw new Error('broker refused the connection')
      }
    }

    const connection = createConnection(t, dialer)
    const connecting = connection.connect()

    await connection.disconnect()

    releaseDial()

    assert.equal(await connecting, null)
    assert.equal(connection.getConnectionState(), 'disconnected')

    // A reconnection loop would keep dialing; nothing must be scheduled.
    await sleep(60)
    assert.equal(connection.getConnectionState(), 'disconnected', 'no reconnection cycle was started')
  })

  test('connect() after disconnect() re-enables automatic reconnection', async (t) => {
    const dialer = createDialer()
    const connection = createConnection(t, dialer)

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
