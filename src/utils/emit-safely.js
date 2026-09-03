import describeError from './describe-error.js'

// Emits an event so that one misbehaving listener decides the fate of neither
// the operation being reported on nor the OTHER listeners. Installed as the
// emitter's own emit() (see RabbitMQ and RabbitMQConnection), so containment is
// structural: no emit, present or future, can be raw by omission — three review
// rounds of converting call sites one at a time each left some behind.
//
// Two failure modes, one fix. EventEmitter.emit lets a listener's exception
// unwind the caller — and these events are emitted from load-bearing points
// (one statement before the reconnect timer is armed, just before a shutdown
// tears the client down, inside a publish's rate-limit check), so the crash
// used to abort or misreport the very operation it was announcing. It also
// ABANDONS its listener loop at the first throw, which would starve the
// library's own internal waiters: connect({ waitForConnection }) parks on
// 'reconnected'/'reconnectError'/'reconnectFailed'/'disconnecting' with
// listeners registered after the application's, so any throwing app listener
// left that promise hanging — with no timeout set, forever.
//
// An async listener's rejection is contained the same way: the returned
// thenable gets a rejection handler that logs. Node's own emit would have
// left it unhandled (or routed it to captureRejections, which this emitter
// does not enable), and a metrics flush that fails during a reconnection must
// not take the process down any more than a synchronous throw would.
//
// rawListeners and not listeners: the former returns `once` wrappers, which
// remove themselves when called. Calling the unwrapped originals would leave
// every once-listener installed for good.
//
// Returns what EventEmitter.emit returns: whether anyone was listening.
const emitSafely = (emitter, event, args, logger) => {
  const listeners = emitter.rawListeners(event)
  const report = (error) => logger.error(`A '${event}' listener threw: ${describeError(error)}`)

  for (const listener of listeners) {
    try {
      const result = listener.apply(emitter, args)

      if (result && typeof result.then === 'function') {
        result.then(undefined, report)
      }
    } catch (error) {
      report(error)
    }
  }

  return listeners.length > 0
}

export { emitSafely }
export default emitSafely
