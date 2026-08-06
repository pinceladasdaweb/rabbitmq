import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { test, describe } from 'node:test'
import systemClock from '../src/utils/clock.js'

const run = promisify(execFile)

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

  test('setTimeout() fires exactly once and clearTimeout() cancels it', async () => {
    let fired = 0
    const kept = systemClock.setTimeout(() => { fired++ }, 5)
    const cancelled = systemClock.setTimeout(() => { fired += 100 }, 5)

    assert.equal(typeof kept.unref, 'function', 'RPC unrefs its timeout so in-flight requests cannot hold the process')

    systemClock.clearTimeout(cancelled)
    await systemClock.sleep(30)

    assert.equal(fired, 1, 'the kept timer fired once, the cancelled one never')
  })

  test('a pending sleep does not hold the process open', async () => {
    // The leaky bucket's smoothing delay scales with queue occupancy; a ref'd
    // timer would keep a shutting-down process alive until the last one fired.
    const script = 'import(process.argv[1]).then(({ default: clock }) => { clock.sleep(60000) })'

    const { stdout } = await run(
      process.execPath,
      ['--input-type=module', '-e', script, new URL('../src/utils/clock.js', import.meta.url).pathname],
      { timeout: 5000 }
    )

    assert.equal(stdout, '', 'the child exited on its own instead of waiting out the sleep')
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
