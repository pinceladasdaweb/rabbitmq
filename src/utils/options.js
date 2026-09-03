// Numeric option defaults, done once. `|| fallback` rewrote a caller's 0 into
// the default — the exact inverse of the request for maxReconnectAttempts
// (0 = never reconnect), compressionThreshold (0 = compress everything) and
// cacheTTL (0 = never expire). `?? fallback` alone then let NaN through:
// `Number(process.env.UNSET)` is NaN, `length <= NaN` is always false, and a
// misconfigured threshold started compressing every message in silence. So:
// absent means the default, a non-negative number means itself, and anything
// else fails at construction, where a misconfiguration is cheapest to see.
const nonNegativeNumber = (value, name, fallback) => {
  if (value === undefined || value === null) return fallback

  if (typeof value !== 'number' || Number.isNaN(value) || value < 0) {
    throw new Error(`${name} must be a non-negative number`)
  }

  return value
}

export { nonNegativeNumber }
