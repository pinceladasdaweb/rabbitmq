// JavaScript lets a handler throw anything — null, undefined, a string, a
// bare object. Every catch block on the message path used to read
// `error.message` directly, so a `throw null` crashed the catch itself: the
// message was never settled and sat unacknowledged until the connection
// dropped (issue #18). This is the single place that turns whatever was
// thrown into something loggable, so settlement can never be derailed by the
// shape of the error.
const describeError = (error) => error?.message ?? String(error)

export { describeError }
export default describeError
