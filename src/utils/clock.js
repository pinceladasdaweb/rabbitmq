import { setTimeout as delay } from 'node:timers/promises'

// The single seam for time. Modules that branch on the clock take one of
// these instead of calling Date.now()/setInterval directly, so every
// time-dependent branch is testable without sleeping (see issue #17: the
// sleeps those branches used to force are what made the suite slow).
const systemClock = {
  now: () => Date.now(),
  setInterval: (fn, ms) => setInterval(fn, ms),
  clearInterval: (handle) => clearInterval(handle),
  setTimeout: (fn, ms) => setTimeout(fn, ms),
  clearTimeout: (handle) => clearTimeout(handle),
  // Unref'd: a pending sleep (e.g. the leaky bucket's smoothing delay, which
  // scales with queue occupancy) must never hold the process open through
  // shutdown after its owner was disposed.
  sleep: (ms) => delay(ms, undefined, { ref: false })
}

export { systemClock }
export default systemClock
