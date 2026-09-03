import describeError from './describe-error.js'

// For a promise nothing awaits. A rejection there is an unhandled rejection,
// which under Node's default --unhandled-rejections=throw kills the process —
// over work whose failure no caller could have acted on anyway (a consumer
// recovery kicked off from a channel 'close' handler, a reconnect attempt fired
// by a timer, a pool slot replacement started from a channel's own teardown).
// The rejection is logged under the caller's label and goes no further.
const detached = (promise, logger, label) => {
  promise.catch((error) => {
    logger.error(`${label}: ${describeError(error)}`)
  })
}

export { detached }
export default detached
