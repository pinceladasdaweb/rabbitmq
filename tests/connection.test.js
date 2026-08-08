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
    // One bad entry among good ones is still a misconfiguration: it would dial
    // an undefined host on whichever attempt rotated onto it, long after start.
    assert.throws(() => construct({ endpoints: ['node-a:5672', ''] }), /At least one valid/)
    assert.doesNotThrow(() => construct({ endpoints: ['node-a:5672', 'node-b:5672'] }))
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
    const logger = recordingLogger()
    const dialer = createDialer([new Error('ECONNREFUSED'), 'ok'])
    const connection = createConnection(t, dialer, { logger, endpoints: ['node-a:5672', 'node-b:5672'] })

    t.after(() => connection.disconnect())

    const result = await connection.connect()

    assert.ok(result)
    assert.equal(dialer.dials, 2)
    assert.match(dialer.urls[1], /node-b:5672/)
    assert.equal(connection.getCurrentEndpoint(), 'node-b:5672')

    // Which node refused matters: in a cluster this is how an operator tells a
    // single sick broker from a network-wide outage.
    assert.ok(
      logger.records.error.some(line => line.includes('node-a:5672') && line.includes('ECONNREFUSED')),
      'the failed node and the reason are both named'
    )
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
    const logger = recordingLogger()
    const dialer = createDialer([new Error('permanently down')])
    const connection = createConnection(t, dialer, { logger, maxReconnectAttempts: 2 })

    t.after(() => connection.disconnect())

    const failed = new Promise(resolve => connection.once('reconnectFailed', resolve))

    await connection.connect()
    await failed

    assert.equal(connection.getConnectionState(), 'failed')
    assert.ok(
      logger.records.error.some(line => /Max reconnect attempts \(2\)/.test(line)),
      'giving up is announced with the budget that was exhausted'
    )
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

  test('backs off exponentially and caps at maxReconnectInterval', async (t) => {
    // The schedule had no assertion at all: doubling could have been halving,
    // the cap could have been a floor, and nothing would have noticed. The
    // scheduler logs the delay it picked, which makes this deterministic
    // rather than a race against real elapsed time.
    const logger = recordingLogger()
    const dialer = createDialer([new Error('broker down')])
    const connection = createConnection(t, dialer, {
      logger,
      reconnectInterval: 10,
      maxReconnectInterval: 40,
      maxReconnectAttempts: 5
    })

    t.after(() => connection.disconnect())

    const failed = new Promise(resolve => connection.once('reconnectFailed', resolve))

    await connection.connect()
    await failed

    const delays = logger.records.info
      .map(line => /Reconnection attempt \d+\/5 in (\d+)ms/.exec(line))
      .filter(Boolean)
      .map(match => Number(match[1]))

    assert.deepEqual(delays, [10, 20, 40, 40, 40], 'doubles, then holds at the ceiling')
  })

  test('defaults the backoff interval to one second and its ceiling to fifteen', async (t) => {
    // Constructed directly: the helper above pins both values, so it can never
    // exercise the defaults. Only the first scheduled delay is inspected —
    // enough to pin both, and it costs nothing to observe.
    const firstDelay = async (options) => {
      const logger = recordingLogger()

      installDialer(t, createDialer([new Error('broker down')]))

      const connection = new RabbitMQConnection({
        username: 'admin',
        password: 'admin',
        endpoints: ['node-a:5672'],
        maxReconnectAttempts: 5,
        ...options
      }, logger)

      await connection.connect()
      await connection.disconnect()

      const line = logger.records.info.find(entry => /Reconnection attempt 1\/5 in/.test(entry))

      assert.ok(line, 'a reconnection was scheduled')

      return Number(/in (\d+)ms/.exec(line)[1])
    }

    assert.equal(await firstDelay({}), 1000, 'the default interval is one second')

    // An interval above the default ceiling is clamped on the very first
    // schedule, which pins the ceiling without waiting for the backoff to grow.
    assert.equal(await firstDelay({ reconnectInterval: 20000 }), 15000, 'the default ceiling is fifteen seconds')
  })

  test('startReconnection is a no-op once disconnect() has been called', async (t) => {
    // It is a public method, so an application (or a stale callback) can call
    // it after shutdown. Reconnecting there would resurrect a connection the
    // caller deliberately closed, with nothing left to stop it.
    const dialer = createDialer()
    const connection = createConnection(t, dialer)

    await connection.connect()
    await connection.disconnect()

    const dialsBefore = dialer.dials

    connection.startReconnection()
    await sleep(80)

    assert.equal(dialer.dials, dialsBefore, 'no dial was attempted')
    assert.equal(connection.getConnectionState(), 'disconnected')
  })

  test('a reconnect timer that fires after a manual connect() does not dial again', async (t) => {
    // The timer and a user calling connect() race. If the timer does not check
    // the state first, it opens a second AMQP connection and leaks the loser.
    const dialer = createDialer([new Error('broker down'), 'ok'])
    const connection = createConnection(t, dialer, { reconnectInterval: 120, maxReconnectInterval: 120 })

    t.after(() => connection.disconnect())

    await connection.connect()

    // The user reconnects by hand well before the scheduled retry fires.
    await connection.connect()

    assert.equal(connection.getConnectionState(), 'connected')

    const dialsBefore = dialer.dials

    // Now let the timer fire into an already connected client.
    await sleep(200)

    assert.equal(dialer.dials, dialsBefore, 'the timer found the connection healthy and stood down')
    assert.equal(connection.getConnectionState(), 'connected')
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

describe('RabbitMQConnection lifecycle guards', () => {
  test('startReconnection works on a freshly constructed instance', (t) => {
    // #isShuttingDown must start false: a connection that boots believing it
    // is shutting down silently refuses to reconnect.
    const dialer = createDialer([new Error('down')])
    const connection = createConnection(t, dialer, { reconnectInterval: 60000, maxReconnectInterval: 60000 })

    t.after(() => connection.disconnect())

    const events = []

    connection.on('disconnected', () => events.push('disconnected'))

    connection.startReconnection()

    assert.deepEqual(events, ['disconnected'])
    assert.equal(connection.getConnectionState(), 'reconnecting')
  })

  test('a dial that lands after disconnect() does not resurrect reconnection on close', async (t) => {
    // disconnect() during an in-flight dial cannot cancel it; the connection
    // still lands and attaches its close handler. When THAT connection later
    // closes, the shutdown flag must keep it from scheduling a reconnect —
    // and from alarming the operator about a close they asked for.
    const logger = recordingLogger()
    const dialer = createDialer()
    let releaseDial
    const dialGate = new Promise(resolve => { releaseDial = resolve })
    const originalConnect = dialer.connect

    dialer.connect = async (...args) => {
      await dialGate

      return originalConnect(...args)
    }

    const connection = createConnection(t, dialer, { logger })

    t.after(() => connection.disconnect())

    const pending = connection.connect()

    await connection.disconnect()
    releaseDial()
    await pending

    dialer.connections.at(-1).emit('close')
    await sleep(50)

    assert.equal(dialer.dials, 1, 'the post-shutdown close scheduled no reconnect dial')
    assert.equal(
      logger.records.error.some(line => line.includes('closed unexpectedly')),
      false,
      'a close during shutdown is not unexpected'
    )
  })

  test('disconnect() during a failing dial settles the state as disconnected', async (t) => {
    const dialer = createDialer()
    let releaseDial
    const dialGate = new Promise(resolve => { releaseDial = resolve })

    dialer.connect = async () => {
      await dialGate

      throw new Error('unreachable')
    }

    const connection = createConnection(t, dialer)

    const pending = connection.connect()

    await connection.disconnect()
    releaseDial()
    await pending

    assert.equal(connection.getConnectionState(), 'disconnected', 'a shutdown mid-dial must not leave the state at connecting')

    await sleep(50)
    assert.equal(dialer.dials, 0, 'and must not schedule reconnection')
  })

  test('after giving up, an explicit startReconnection announces a fresh cycle', async (t) => {
    const dialer = createDialer([new Error('down')])
    const connection = createConnection(t, dialer, { maxReconnectAttempts: 1 })

    t.after(() => connection.disconnect())

    const failed = new Promise(resolve => connection.once('reconnectFailed', resolve))

    await connection.connect()
    await failed

    const events = []

    connection.on('disconnected', () => events.push('disconnected'))

    connection.startReconnection()

    assert.deepEqual(events, ['disconnected'], 'the new cycle is announced — giving up cleared the reconnecting latch')
    // The fresh cycle immediately re-evaluates the exhausted budget and gives
    // up again; what matters is that it was ANNOUNCED (the latch was reset).
    assert.equal(connection.getConnectionState(), 'failed')
  })

  test('an unlimited retry budget is announced as such', async (t) => {
    // Which log line an operator sees is decided by the Infinity branch: the
    // capped wording implies the client may give up, the unlimited one that
    // it will not — telling them apart matters during an outage.
    const logger = recordingLogger()
    const dialer = createDialer([new Error('down')])
    const connection = createConnection(t, dialer, { logger })

    t.after(() => connection.disconnect())

    await connection.connect()

    assert.ok(logger.records.info.some(line => line.includes('indefinitely')), 'the unlimited budget is stated')

    const capped = recordingLogger()
    const cappedConnection = createConnection(t, createDialer([new Error('down')]), { logger: capped, maxReconnectAttempts: 3 })

    t.after(() => cappedConnection.disconnect())

    await cappedConnection.connect()

    assert.ok(capped.records.info.some(line => line.includes('1/3')), 'a capped budget states the ceiling')
  })

  test('a reconnect timer firing after a manual connect succeeds does not re-emit reconnected', async (t) => {
    const dialer = createDialer([new Error('down'), 'ok'])
    const connection = createConnection(t, dialer)

    t.after(() => connection.disconnect())

    const reconnects = []

    connection.on('reconnected', () => reconnects.push('reconnected'))

    // First dial fails and schedules a retry; the manual connect() wins the
    // race and succeeds. The timer then fires against a connected client.
    await connection.connect()
    await connection.connect()

    assert.equal(connection.getConnectionState(), 'connected')

    await sleep(60)

    assert.deepEqual(reconnects, [], 'the late timer must not announce a reconnection that never happened')
  })

  test('repeated retry cycles do not repeat the connecting state announcement', async (t) => {
    const dialer = createDialer([new Error('down')])
    const connection = createConnection(t, dialer)

    t.after(() => connection.disconnect())

    const states = []

    connection.on('connectionStateChanged', (state) => states.push(state))

    await connection.connect()
    await waitFor(() => dialer.dials >= 3, 3000, 'two retry cycles')

    assert.deepEqual(
      states.slice(0, 3),
      ['connecting', 'reconnecting', 'connecting'],
      'the second retry re-enters connecting silently — the state did not change'
    )
    assert.equal(states.filter(state => state === 'connecting').length, 2, 'no duplicate announcements for an unchanged state')
  })

  test('disconnect() resets the reconnecting latch for the next cycle', async (t) => {
    const dialer = createDialer([new Error('down')])
    const connection = createConnection(t, dialer, { reconnectInterval: 60000, maxReconnectInterval: 60000 })

    await connection.connect()
    assert.equal(connection.getConnectionState(), 'reconnecting')

    await connection.disconnect()

    const events = []

    connection.on('disconnected', () => events.push('disconnected'))

    await connection.connect()

    assert.deepEqual(events, ['disconnected'], 'the failing cycle after a disconnect is announced anew')

    await connection.disconnect()
  })

  test('disconnect() on a never-connected instance changes no state', (t) => {
    const connection = createConnection(t, createDialer())
    const states = []

    connection.on('connectionStateChanged', (state) => states.push(state))

    connection.disconnect()

    assert.deepEqual(states, [], 'already disconnected: announcing disconnecting/disconnected again is noise')
  })

  test('a connection that fails to close still reports the shutdown', async (t) => {
    const logger = recordingLogger()
    const dialer = createDialer()
    const connection = createConnection(t, dialer, { logger })

    await connection.connect()

    dialer.connections[0].close = async () => { throw new Error('already gone') }

    await connection.disconnect()

    assert.equal(connection.getConnectionState(), 'disconnected')
    assert.ok(
      logger.records.info.some(line => line.includes('closed')),
      'the operator still learns the connection is gone'
    )
  })
})

describe('RabbitMQConnection shutdown fencing', () => {
  test('a dial that SUCCEEDS after disconnect() is closed, not installed', async (t) => {
    // disconnect() cannot cancel a dial already in flight. Without the check
    // on the way in, the connection lands live and unowned — the facade has
    // already torn its pool down — and the state machine reports 'connected'.
    const dialer = createDialer()
    let releaseDial
    const dialGate = new Promise(resolve => { releaseDial = resolve })
    const originalConnect = dialer.connect

    dialer.connect = async (...args) => {
      await dialGate

      return originalConnect(...args)
    }

    const connection = createConnection(t, dialer)

    const pending = connection.connect()

    await connection.disconnect()
    releaseDial()

    assert.equal(await pending, null, 'the dial reports no connection')
    assert.equal(connection.getConnectionState(), 'disconnected', 'the shutdown stands')
    assert.equal(connection.getConnection(), null, 'nothing was installed')
    assert.equal(dialer.connections[0].closed, true, 'the orphan was closed, not leaked')
  })
})

describe('RabbitMQConnection teardown failures', () => {
  test('an orphan dial whose close() rejects is still abandoned', async (t) => {
    // The close is best-effort: a broker that refuses to close the connection
    // we no longer want must not turn a clean shutdown into a rejection.
    const dialer = createDialer()
    let releaseDial
    const dialGate = new Promise(resolve => { releaseDial = resolve })
    const originalConnect = dialer.connect

    dialer.connect = async (...args) => {
      await dialGate

      return originalConnect(...args)
    }

    dialer.onConnection = (connection) => {
      connection.close = async () => { throw new Error('already gone') }
    }

    const connection = createConnection(t, dialer)

    const pending = connection.connect()

    await connection.disconnect()
    releaseDial()

    assert.equal(await pending, null, 'the dial still reports no connection')
    assert.equal(connection.getConnectionState(), 'disconnected')
    assert.equal(connection.getConnection(), null)
  })
})

describe('RabbitMQConnection reconnect timer races', () => {
  test('a reconnect callback already dequeued when the connect succeeded stands down', async (t) => {
    // A successful connect clears the timer, so this guard only matters for
    // the race where the callback had ALREADY been taken off the loop. The
    // injected clock is what makes that instant reproducible: capture the
    // scheduled callback, connect by hand, then run it.
    const { ManualClock } = await import('./helpers.js')
    const clock = new ManualClock()
    const dialer = createDialer([new Error('down'), 'ok'])
    const connection = createConnection(t, dialer, { clock })

    t.after(() => connection.disconnect())

    const reconnects = []

    connection.on('reconnected', () => reconnects.push('reconnected'))

    await connection.connect()

    const scheduled = [...clock.timeouts.values()][0]

    assert.ok(scheduled, 'the failed dial scheduled a retry')

    await connection.connect()
    assert.equal(connection.getConnectionState(), 'connected')

    const dialsBefore = dialer.dials

    // fn() is the timer's arrow, which does not return the async work — give
    // it a turn to finish or the assertions race it.
    scheduled.fn()
    await sleep(50)

    assert.equal(dialer.dials, dialsBefore, 'the stale callback dialed nothing')
    assert.deepEqual(reconnects, [], 'and announced no reconnection that never happened')
  })

  test('the reconnect backoff is exponential and capped', async (t) => {
    // With the timer under our control the whole curve is observable, instead
    // of inferred from one slow observation.
    const { ManualClock } = await import('./helpers.js')
    const clock = new ManualClock()
    const dialer = createDialer([new Error('down')])
    const connection = createConnection(t, dialer, {
      clock,
      reconnectInterval: 100,
      maxReconnectInterval: 800
    })

    t.after(() => connection.disconnect())

    const delays = []

    await connection.connect()

    for (let cycle = 0; cycle < 6; cycle++) {
      const [scheduled] = [...clock.timeouts.values()]

      delays.push(scheduled.at - clock.now())
      clock.advance(scheduled.at - clock.now())
      await sleep(5)
    }

    assert.deepEqual(delays, [100, 200, 400, 800, 800, 800], 'doubles until the ceiling, then holds')
  })
})
