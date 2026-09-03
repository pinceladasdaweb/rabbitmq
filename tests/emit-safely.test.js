import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { test, describe } from 'node:test'
import { emitSafely } from '../src/utils/emit-safely.js'
import { recordingLogger } from './helpers.js'

const emitterWith = (...listeners) => {
  const emitter = new EventEmitter()

  for (const listener of listeners) emitter.on('ping', listener)

  return emitter
}

describe('emitSafely', () => {
  test('a synchronous listener that returns nothing, or a plain value, is not mistaken for a failure', () => {
    // The thenable check must be a check: forcing it on would call .then on
    // undefined (or on 42) inside the try and log a spurious "listener threw"
    // for every well-behaved listener in the process.
    const logger = recordingLogger()
    const seen = []

    emitSafely(emitterWith(() => { seen.push('void') }, () => 42, () => 'text'), 'ping', [], logger)

    assert.deepEqual(seen, ['void'])
    assert.deepEqual(logger.records.error, [], 'nothing threw, so nothing is reported')
  })

  test('reports whether anyone was listening, exactly like EventEmitter.emit', () => {
    const logger = recordingLogger()

    assert.equal(emitSafely(emitterWith(() => {}), 'ping', [], logger), true)
    assert.equal(emitSafely(new EventEmitter(), 'ping', [], logger), false)
  })

  test('a rejecting async listener is reported and the others still run', async () => {
    const logger = recordingLogger()
    const seen = []

    emitSafely(emitterWith(async () => { throw new Error('flush failed') }, () => seen.push('second')), 'ping', [], logger)

    assert.deepEqual(seen, ['second'])

    // The rejection lands a microtask later.
    await new Promise(resolve => setImmediate(resolve))

    assert.ok(logger.records.error.some(line => line.includes("'ping' listener threw") && line.includes('flush failed')))
  })

  test('arguments reach every listener and a throwing one does not starve the next', () => {
    const logger = recordingLogger()
    const seen = []

    emitSafely(emitterWith(() => { throw new Error('boom') }, (a, b) => seen.push([a, b])), 'ping', [1, 2], logger)

    assert.deepEqual(seen, [[1, 2]])
    assert.ok(logger.records.error.some(line => line.includes('boom')))
  })
})
