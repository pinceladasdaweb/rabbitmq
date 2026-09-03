import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import { nonNegativeNumber } from '../src/utils/options.js'

describe('nonNegativeNumber', () => {
  test('absent means the default', () => {
    assert.equal(nonNegativeNumber(undefined, 'x', 7), 7)
    assert.equal(nonNegativeNumber(null, 'x', 7), 7)
  })

  test('zero and Infinity are real requests, not absences', () => {
    // The whole reason this exists: `|| fallback` rewrote 0 into the default.
    assert.equal(nonNegativeNumber(0, 'x', 7), 0)
    assert.equal(nonNegativeNumber(Infinity, 'x', 7), Infinity)
    assert.equal(nonNegativeNumber(3.5, 'x', 7), 3.5)
  })

  test('junk fails loudly, naming the option', () => {
    // And the reason a bare `??` is not enough: NaN is neither null nor undefined.
    for (const junk of [NaN, -1, '5', true, {}]) {
      assert.throws(() => nonNegativeNumber(junk, 'cacheTTL', 7), /^Error: cacheTTL must be a non-negative number$/)
    }
  })
})
