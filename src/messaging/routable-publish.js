import { randomUUID } from 'node:crypto'

// The one implementation of the mandatory-publish/basic.return dance, shared
// by Publisher.publishRoutable and Topology.moveToDeadLetter — two copies had
// already drifted apart (different token headers, different error shapes),
// and a fix to the subtle parts of this protocol must reach both.

// The token correlates a return with its exact publish: a pool channel
// carries many concurrent publishes, and matching on the routing key alone
// would let one publish's return fail another that was delivered.
const RETURN_TOKEN_HEADER = 'x-return-token'

// One persistent 'return' listener per channel, with a token registry behind
// it. A listener per in-flight publish instead made every mandatory batch
// over 10 messages trip MaxListenersExceededWarning and scanned O(N)
// listeners per return. The registry is keyed weakly: it dies with the
// channel.
const registries = new WeakMap()

const registryFor = (channel) => {
  let registry = registries.get(channel)

  if (!registry) {
    registry = new Map()

    channel.on('return', (message) => {
      const entry = registry.get(message?.properties?.headers?.[RETURN_TOKEN_HEADER])

      if (entry) {
        entry.returned = true
      }
    })

    registries.set(channel, registry)
  }

  return registry
}

// Confirm-callback publish as a promise. The broker's refusal reason rides in
// the error so callers can log or wrap it.
const publishConfirmed = (channel, exchange, routingKey, content, options) => {
  return new Promise((resolve, reject) => {
    channel.publish(exchange, routingKey, content, options, (err) => {
      if (err) {
        reject(new Error(`Message was not confirmed by the broker: ${err.message}`))
      } else {
        resolve()
      }
    })
  })
}

// Publishes with a return watch and reports whether the broker handed THIS
// message back. amqplib dispatches frames in arrival order and emits 'return'
// synchronously before the same delivery's confirm callback runs, and the
// broker sends basic.return before basic.ack for a returned mandatory
// message — so by the time the confirm resolves, the watch already knows.
// (Pinned by the integration test publishing mandatory to an unbound key.)
const publishWatched = async (channel, exchange, routingKey, content, options) => {
  const token = randomUUID()
  const registry = registryFor(channel)
  const entry = { returned: false }
  const guarded = { ...options, headers: { ...options.headers, [RETURN_TOKEN_HEADER]: token } }

  registry.set(token, entry)

  try {
    await publishConfirmed(channel, exchange, routingKey, content, guarded)
  } finally {
    registry.delete(token)
  }

  return entry.returned
}

// Introspection for tests: the registry is otherwise invisible, and its
// cleanup is a real contract — a token left behind is a memory leak per
// mandatory publish on a long-lived pool channel.
const watchCount = (channel) => registries.get(channel)?.size ?? 0

export { publishConfirmed, publishWatched, watchCount, RETURN_TOKEN_HEADER }
