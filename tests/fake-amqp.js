import amqp from 'amqplib'

// Lets unit tests stand in for the real broker dialer WITHOUT any test hook in
// production code.
//
// How it works: amqplib is CommonJS, so `import amqp from 'amqplib'` binds to
// its module.exports object, and the module cache hands src/ and this file the
// very same object. src/connection/connection.js calls `amqp.connect(...)` —
// a property lookup at call time, not a captured binding — so replacing that
// property here is observed by production code unchanged.
//
// Two properties keep this safe:
//   - with no dialer installed, calls fall through to the real amqplib, so
//     importing this module can never break integration tests;
//   - each test file runs in its own process and tests within a file run
//     sequentially, so a single installed dialer at a time is unambiguous.
//
// If connection.js ever switches to a named import (`import { connect } from
// 'amqplib'`), this stops intercepting — loudly, because every test here would
// try to reach a real broker and fail.

const realConnect = amqp.connect

let currentDialer = null

amqp.connect = (url, socketOptions) => (currentDialer ?? realConnect)(url, socketOptions)

// Installs `dialer` (from createDialer()) for the duration of one test.
export const installDialer = (t, dialer) => {
  currentDialer = dialer.connect

  t.after(() => {
    currentDialer = null
  })

  return dialer
}

export const uninstallDialer = () => {
  currentDialer = null
}
