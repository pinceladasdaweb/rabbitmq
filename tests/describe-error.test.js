import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import describeError from '../src/utils/describe-error.js'

describe('describeError', () => {
  test('turns every throwable shape into a loggable string', () => {
    // The table from issue #18: everything a handler can throw. The exact
    // rendering matters less than never throwing while producing it — but
    // pinning the rendering is what keeps this from regressing to .message.
    assert.equal(describeError(new Error('boom')), 'boom')
    assert.equal(describeError(null), 'null')
    assert.equal(describeError(undefined), 'undefined')
    assert.equal(describeError('just a string'), 'just a string')
    assert.equal(describeError(0), '0')
    assert.equal(describeError({ message: 'shaped like an error' }), 'shaped like an error')
    assert.equal(describeError({}), '[object Object]')
  })

  test('an empty Error message stays empty rather than falling through', () => {
    // '' is not nullish: the fallback must not replace a real (if useless)
    // message with the stringified Error.
    assert.equal(describeError(new Error()), '')
  })
})
