import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import { safeLogger } from '../src/utils/safe-logger.js'

describe('safeLogger', () => {
  test('forwards every level to the wrapped logger', () => {
    const calls = []
    const logger = safeLogger({
      info: (...a) => calls.push(['info', ...a]),
      warn: (...a) => calls.push(['warn', ...a]),
      error: (...a) => calls.push(['error', ...a]),
      debug: (...a) => calls.push(['debug', ...a])
    })

    logger.info('a', 1)
    logger.warn('b')
    logger.error('c')
    logger.debug('d')

    assert.deepEqual(calls, [['info', 'a', 1], ['warn', 'b'], ['error', 'c'], ['debug', 'd']])
  })

  test('a level that throws is contained and reported once on console.error', (t) => {
    const consoleError = t.mock.method(console, 'error', () => {})
    const logger = safeLogger({ info () { throw new Error('transport closed') }, warn () {}, error () {}, debug () {} })

    assert.doesNotThrow(() => logger.info('anything'))
    assert.equal(consoleError.mock.callCount(), 1)
    assert.match(String(consoleError.mock.calls[0].arguments[0]), /application logger threw on info\(\)/)
  })

  test('a logger missing a level is tolerated, and not reported as broken', (t) => {
    // A minimal application logger may only implement what it cares about: a
    // missing level is a choice, not a failure, so the last-resort report must
    // stay silent — calling the missing method and catching the TypeError
    // would flood console.error on every debug() line.
    const consoleError = t.mock.method(console, 'error', () => {})
    const logger = safeLogger({ info () {}, error () {} })

    assert.doesNotThrow(() => logger.debug('nobody home'))
    assert.doesNotThrow(() => logger.warn('nobody home'))
    assert.equal(consoleError.mock.callCount(), 0)
  })

  test('wrapping an already wrapped logger returns it as is', () => {
    // Applied once at the facade boundary; components that receive the wrapped
    // logger and defensively wrap again must not stack another layer per hop.
    const once = safeLogger({ info () {}, warn () {}, error () {}, debug () {} })

    assert.equal(safeLogger(once), once)
  })

  test('even the last-resort report cannot throw', (t) => {
    // If console.error itself is broken there is nowhere left to report — and
    // that still must not become the caller's problem.
    t.mock.method(console, 'error', () => { throw new Error('console gone too') })
    const logger = safeLogger({ info () { throw new Error('transport closed') } })

    assert.doesNotThrow(() => logger.info('anything'))
  })
})
