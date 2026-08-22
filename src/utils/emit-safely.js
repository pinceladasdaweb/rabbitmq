import describeError from './describe-error.js'

// Emits an event so that one misbehaving listener decides the fate of neither
// the operation being reported on nor the OTHER listeners.
//
// Two failure modes, one fix. EventEmitter.emit lets a listener's exception
// unwind the caller — and these events are emitted from load-bearing points
// (one statement before the reconnect timer is armed, just before a shutdown
// tears the client down), so the crash used to abort the very operation it was
// announcing. It also ABANDONS its listener loop at the first throw, which
// would starve the library's own internal waiters: connect({ waitForConnection })
// parks on 'reconnected'/'reconnectError'/'reconnectFailed'/'disconnecting'
// with listeners registered after the application's, so any throwing app
// listener left that promise hanging — with no timeout set, forever.
//
// rawListeners and not listeners: the former returns `once` wrappers, which
// remove themselves when called. Calling the unwrapped originals would leave
// every once-listener installed for good.
const emitSafely = (emitter, event, args, logger) => {
  for (const listener of emitter.rawListeners(event)) {
    try {
      listener.apply(emitter, args)
    } catch (error) {
      logger.error(`A '${event}' listener threw: ${describeError(error)}`)
    }
  }
}

export { emitSafely }
export default emitSafely
