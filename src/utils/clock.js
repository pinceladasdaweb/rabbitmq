// The single seam for time. Modules that branch on the clock take one of
// these instead of calling Date.now()/setInterval directly, so every
// time-dependent branch is testable without sleeping (see issue #17: the
// sleeps those branches used to force are what made the suite slow).
const systemClock = {
  now: () => Date.now(),
  setInterval: (fn, ms) => setInterval(fn, ms),
  clearInterval: (handle) => clearInterval(handle),
  sleep: (ms) => new Promise(resolve => setTimeout(resolve, ms))
}

export { systemClock }
export default systemClock
