import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import defaultLogger, { createLogger } from '../src/utils/logger.js'

describe('logger', () => {
  test('default export exposes all four level methods', () => {
    for (const method of ['error', 'warn', 'info', 'debug']) {
      assert.equal(typeof defaultLogger[method], 'function')
    }
  })

  test('routes levels to the matching console methods', (t) => {
    const errorMock = t.mock.method(console, 'error', () => {})
    const warnMock = t.mock.method(console, 'warn', () => {})
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('debug')

    logger.error('boom')
    logger.warn('careful')
    logger.info('hello')
    logger.debug('details')

    assert.equal(errorMock.mock.callCount(), 1)
    assert.equal(warnMock.mock.callCount(), 1)
    assert.equal(logMock.mock.callCount(), 2)
  })

  test('suppresses messages below the configured level', (t) => {
    const errorMock = t.mock.method(console, 'error', () => {})
    const warnMock = t.mock.method(console, 'warn', () => {})
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('warn')

    logger.error('shown')
    logger.warn('shown')
    logger.info('hidden')
    logger.debug('hidden')

    assert.equal(errorMock.mock.callCount(), 1)
    assert.equal(warnMock.mock.callCount(), 1)
    assert.equal(logMock.mock.callCount(), 0)
  })

  test('defaults to info level, hiding debug output', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger(undefined)

    logger.info('shown')
    logger.debug('hidden')

    assert.equal(logMock.mock.callCount(), 1)
  })

  test('reads the level from LOG_LEVEL when the caller gives none', (t) => {
    // The default parameter is `process.env.LOG_LEVEL || 'info'`. Without the
    // environment read, setting LOG_LEVEL=debug in a deployment silently does
    // nothing and the diagnostics people turned on never appear.
    const previous = process.env.LOG_LEVEL

    process.env.LOG_LEVEL = 'debug'
    t.after(() => {
      if (previous === undefined) delete process.env.LOG_LEVEL
      else process.env.LOG_LEVEL = previous
    })

    // debug is written through console.log, like info.
    const logMock = t.mock.method(console, 'log', () => {})
    const logger = createLogger()

    logger.debug('now visible')

    assert.equal(logMock.mock.callCount(), 1, 'LOG_LEVEL=debug lets debug through')
  })

  test('falls back to info for unknown levels', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('bananas')

    logger.info('shown')
    logger.debug('hidden')

    assert.equal(logMock.mock.callCount(), 1)
  })

  test('prefixes messages with a timestamp and the level name', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('info')

    logger.info('formatted message')

    const line = logMock.mock.calls[0].arguments[0]

    assert.match(line, /^\d{4}-\d{2}-\d{2}T[\d:.]+Z \[info\] formatted message$/)
  })

  test('forwards extra arguments to the console', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('info')
    const details = { queue: 'orders' }

    logger.info('with context', details)

    assert.equal(logMock.mock.calls[0].arguments[1], details)
  })
})
