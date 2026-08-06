import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import systemClock from '../src/utils/clock.js'

// The production clock is trivial by design, but it is the one implementation
// every ManualClock-based test stops exercising — so its contract is pinned
// here against real time, in the only file that is allowed to wait.
describe('systemClock', () => {
  test('now() is the epoch clock', () => {
    const before = Date.now()
    const reported = systemClock.now()
    const after = Date.now()

    assert.ok(reported >= before && reported <= after)
  })

  test('sleep() actually waits for the requested duration', async () => {
    const start = Date.now()

    await systemClock.sleep(25)

    assert.ok(Date.now() - start >= 20, 'a sleep that resolves early breaks the leaky bucket pacing')
  })

  test('setInterval() fires repeatedly and clearInterval() stops it', async () => {
    let fired = 0
    const handle = systemClock.setInterval(() => { fired++ }, 5)

    assert.equal(typeof handle.unref, 'function', 'callers unref the handle so it cannot hold the process open')

    await systemClock.sleep(40)
    systemClock.clearInterval(handle)

    assert.ok(fired >= 2, `the interval must recur, not fire once (fired ${fired}x)`)

    const atClear = fired

    await systemClock.sleep(25)

    assert.equal(fired, atClear, 'a cleared interval must never fire again')
  })
})
