// Dependency-free default logger. The library logs through whatever the
// application injects via the `logger` option; this console-based fallback
// only exists so the out-of-the-box experience still has visible, leveled
// logs. See the README ("Logging Options") for injecting pino/winston/etc.

const LEVELS = { error: 0, warn: 1, info: 2, debug: 3 }

// No default on `level` beyond the env var: an unknown or missing level is
// already mapped to info by the ?? below — a second default would be dead
// weight (and mutation testing flagged it as such).
const createLogger = (level = process.env.LOG_LEVEL) => {
  const threshold = LEVELS[level] ?? LEVELS.info

  const write = (method, levelName) => (message, ...args) => {
    if (LEVELS[levelName] > threshold) return

    console[method](`${new Date().toISOString()} [${levelName}] ${message}`, ...args)
  }

  return {
    error: write('error', 'error'),
    warn: write('warn', 'warn'),
    info: write('log', 'info'),
    debug: write('log', 'debug')
  }
}

const logger = createLogger()

export { createLogger }
export default logger
