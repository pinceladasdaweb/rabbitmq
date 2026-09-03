// Wraps the application's logger so that a logging failure can never become a
// client failure. Same rule as emit-safely.js, same reason: the logger is
// application code the library calls from inside its own state machine, and it
// used to be able to unwind it. Found empirically — an `info` that threw on the
// "connected" line sat inside the dial's try/catch, so a SUCCESSFUL connection
// was treated as a failed endpoint: the live AMQP connection was nulled without
// a close, the endpoint rotated, and the reconnect scheduler threw on its own
// first log line. A logging transport whose stream has closed is an ordinary
// production event; it must cost log lines, not the connection.
//
// The wrapped logger is what every component receives, so the containment is
// structural: nothing downstream can call the raw one. Applied once, at the
// facade boundary; a logger already wrapped is returned as is.
//
// Last-resort reporting goes to console.error and is itself guarded — when the
// logger is the thing that is broken there is nowhere better to say so, and
// saying nothing would hide the outage that took the logs down.
const WRAPPED = Symbol.for('rabbitmq.safeLogger')
const LEVELS = ['info', 'warn', 'error', 'debug']

const safeLogger = (logger) => {
  if (logger?.[WRAPPED]) return logger

  const wrapped = { [WRAPPED]: true }

  for (const level of LEVELS) {
    wrapped[level] = (...args) => {
      try {
        logger[level]?.(...args)
      } catch (error) {
        try {
          console.error(`[rabbitmq] the application logger threw on ${level}():`, error)
        } catch {}
      }
    }
  }

  return wrapped
}

export { safeLogger }
export default safeLogger
