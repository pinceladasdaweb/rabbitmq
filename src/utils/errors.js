// The one place the library's shared error shapes are built. Errors here are
// identified by `code` (see index.d.ts PublishErrorCode); the message is for
// operators and may be reworded, the code is contract and may not.

// Publish, subscribe and RPC all refuse to start while no channel pool exists.
// Three hand-typed copies of this message drifted toward three wordings and
// none of them carried a code, so a caller could branch on every other error
// a publish can raise except the most common one.
const notConnectedError = () => {
  const error = new Error('Not connected to RabbitMQ. Connection establishing/recovery in progress.')
  error.code = 'NOT_CONNECTED'

  return error
}

export { notConnectedError }
