import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import CircuitBreaker from '../src/resilience/circuit-breaker.js'

const failingOperation = async () => {
  throw new Error('boom')
}

const succeedingOperation = async () => 'ok'

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

describe('CircuitBreaker', () => {
  test('starts CLOSED and executes operations', async () => {
    const breaker = new CircuitBreaker()

    const result = await breaker.execute(succeedingOperation)

    assert.equal(result, 'ok')
    assert.equal(breaker.getState().state, 'CLOSED')
  })

  test('opens after reaching the failure threshold', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 3, timeout: 1000 })

    for (let i = 0; i < 3; i++) {
      await assert.rejects(() => breaker.execute(failingOperation), /boom/)
    }

    assert.equal(breaker.getState().state, 'OPEN')
  })

  test('rejects immediately while OPEN', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, timeout: 60000 })

    await assert.rejects(() => breaker.execute(failingOperation), /boom/)

    await assert.rejects(() => breaker.execute(succeedingOperation), (error) => {
      assert.equal(error.code, 'CIRCUIT_OPEN')

      return true
    })
  })

  test('transitions to HALF-OPEN after the timeout and closes after enough successes', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, successThreshold: 2, timeout: 20 })

    await assert.rejects(() => breaker.execute(failingOperation), /boom/)
    assert.equal(breaker.getState().state, 'OPEN')

    await sleep(30)

    await breaker.execute(succeedingOperation)
    assert.equal(breaker.getState().state, 'HALF-OPEN')

    await breaker.execute(succeedingOperation)
    assert.equal(breaker.getState().state, 'CLOSED')
  })

  test('reopens immediately on a single failure while HALF-OPEN', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 5, successThreshold: 2, timeout: 20 })

    for (let i = 0; i < 5; i++) {
      await assert.rejects(() => breaker.execute(failingOperation), /boom/)
    }

    assert.equal(breaker.getState().state, 'OPEN')

    await sleep(30)

    await assert.rejects(() => breaker.execute(failingOperation), /boom/)
    assert.equal(breaker.getState().state, 'OPEN')
  })

  test('emits stateChanged on transitions', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, timeout: 1000 })
    const states = []

    breaker.on('stateChanged', (state) => states.push(state))

    await assert.rejects(() => breaker.execute(failingOperation), /boom/)

    assert.deepEqual(states, ['OPEN'])
  })

  test('reset() returns an OPEN breaker to a clean CLOSED state', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, timeout: 60000 })

    await assert.rejects(() => breaker.execute(failingOperation), /boom/)
    assert.equal(breaker.getState().state, 'OPEN')

    breaker.reset()

    const state = breaker.getState()

    assert.equal(state.state, 'CLOSED')
    assert.equal(state.failureCount, 0)
    assert.equal(await breaker.execute(succeedingOperation), 'ok')
  })

  test('reset announces the transition only when there was one to announce', async () => {
    // reset() runs after every successful reconnection. Emitting CLOSED on an
    // already closed breaker would tell the application the circuit just
    // recovered, on a connection that never tripped it.
    const breaker = new CircuitBreaker({ failureThreshold: 1, timeout: 60000 })
    const states = []

    breaker.on('stateChanged', (state) => states.push(state))

    breaker.reset()
    assert.deepEqual(states, [], 'resetting a healthy breaker says nothing')

    await assert.rejects(() => breaker.execute(failingOperation), /boom/)
    assert.deepEqual(states, ['OPEN'])

    breaker.reset()
    assert.deepEqual(states, ['OPEN', 'CLOSED'], 'recovering from OPEN is announced exactly once')

    breaker.reset()
    assert.deepEqual(states, ['OPEN', 'CLOSED'], 'and not repeated')
  })

  test('getState exposes counters', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 5 })

    await assert.rejects(() => breaker.execute(failingOperation), /boom/)

    const state = breaker.getState()

    assert.equal(state.state, 'CLOSED')
    assert.equal(state.failureCount, 1)
    assert.equal(state.successCount, 0)
  })

  test('a closed breaker reports nextAttempt as the present, never undefined', () => {
    // breakwater only stamps nextAttemptAt once the circuit opens; while
    // closed the caller still gets a number (now), not a hole in the shape.
    const breaker = new CircuitBreaker()
    const before = Date.now()
    const state = breaker.getState()

    assert.ok(state.nextAttempt >= before && state.nextAttempt <= Date.now(), 'nextAttempt defaults to now while closed')
  })

  test('isolating the composed policy surfaces the ISOLATED label', async () => {
    // The `policy` getter exists for composition; anyone holding it can call
    // isolate(), so the state map must translate that state too.
    const breaker = new CircuitBreaker()
    const states = []

    breaker.on('stateChanged', (state) => states.push(state))

    breaker.policy.isolate()

    // breakwater emits stateChange asynchronously.
    await new Promise(resolve => setImmediate(resolve))

    assert.deepEqual(states, ['ISOLATED'])
    assert.equal(breaker.getState().state, 'ISOLATED')
  })
})
