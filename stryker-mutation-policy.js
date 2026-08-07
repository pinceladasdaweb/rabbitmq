import { declareValuePlugin, PluginKind } from '@stryker-mutator/api/plugin'

// The mutation POLICY for this repository, declared in one tool-owned file so
// the library source carries zero Stryker annotations. Two kinds of rule:
//
// 1. Log phrasing is not contract. Errors in this lib are identified by
//    `code`, events by name — a mutant that rewrites a log message was never
//    a missing defense. Covers logger.* call arguments and the successLog
//    hook factories that exist only to build log lines.
//
// 2. Named equivalent mutants: places where the mutation provably cannot
//    change behavior. Each matcher is structural and narrow, with the proof
//    in its comment.
//
// Known equivalent SURVIVORS deliberately NOT ignored here (ignoring their
// node would also suppress killable siblings on the same expression):
//   - token-bucket.js `tokensToAdd > 0` -> `>= 0`: at exactly zero the
//     refill is a no-op either way. The `true` mutant on the same node IS
//     killable (a clock stepping backwards must not shrink the balance), so
//     the node stays in the gate and the >= twin is accepted as a survivor.
//   - fixed-window.js `Math.floor(now / windowMs) * windowMs` outer operator:
//     windowStart is only ever compared for identity, never used as a value,
//     so any injective transform of floor(now / windowMs) is an equally valid
//     window id — every mutation of the OUTER operator is equivalent. The
//     inner `/` is load-bearing (its mutants break which instants share a
//     window) and stays killable, which is why the node is not ignored.
//   - rate-limiter.js `blockedUntil !== undefined && blockedUntil > now`:
//     the `true &&` mutant reduces to `blockedUntil > now`, and undefined
//     compares false against any number — the guard is semantically
//     redundant, kept for readability over coercion arcana.
//   - channel-pool.js getChannel's ring scan `i < length` -> `<=`: the extra
//     iteration re-reads a slot the ring already visited — it can neither
//     find a channel the scan missed nor change the throw.
//   - channel-pool.js #closeChannel's `!channel || typeof close` guard: the
//     try/catch right below already swallows the TypeError a null slot (or a
//     close-less channel) would produce — the guard is an early exit whose
//     removal cannot be observed.
//   - topology.js default `(() => null)` -> `() => undefined`: the only
//     consumer feeds it into `sourceQueue || routingKey`, where null and
//     undefined are the same falsy.
//   - publisher.js validatePriority `priority !== undefined &&`: the
//     `true &&` twin hands undefined to the range check, and undefined
//     compares false against any number — same shape as the rate limiter's
//     readability guard.
//   - connection.js `state === 'connected' && #connection` (and its || twin):
//     the state machine maintains state === 'connected' iff #connection is
//     set (the close handler nulls both together, synchronously), so the two
//     operands never disagree.
//   - connection.js #scheduleReconnect's shutdown guard: its only caller
//     (startReconnection) checks the same flag one synchronous statement
//     earlier — kept as defense in depth on a private entry point.
//   - connection.js `#maxReconnectAttempts !== Infinity &&`: the `true &&`
//     twin asks whether attempt >= Infinity, which no attempt count ever is.
//   - connection.js disconnect's listener stripping and the close handler's
//     #isShuttingDown guard are a defense-in-depth PAIR: each one alone
//     prevents the shutdown-reconnect loop, so mutating either in isolation
//     is unobservable (and the null-connection branch is swallowed by the
//     surrounding catch).
//   - connection.js #doConnect's failed-dial else branch: disconnect() has
//     already settled the state at 'disconnected' by the time a shut-down
//     dial loop finishes, so re-setting it is deduplicated — only an
//     interleaving where disconnect is still awaiting close() could tell
//     the branches apart.
//   - connection.js #attemptReconnect's connected-guard: successful connects
//     clear the reconnect timer, so the guard only matters for the microtask
//     race where the timer callback was already dequeued when the connect
//     resolved — the clear and the guard are another defense-in-depth pair.
//   - worker-pool.js acquire's `workers.has` guard and terminate's idle
//     reset: the exit handler already removes dead workers from the idle
//     list and the terminated flag rejects before idle is ever read, so
//     stale idle content is unobservable — both are backstops for the same
//     invariant the exit handler maintains.
//   - rpc.js `connectionEpoch++` -> `--` and consumer-manager.js
//     `consumerInfo.epoch++` -> `--`: epoch fences only ever test INEQUALITY
//     between a captured value and the current one — direction is immaterial.
//   - consumer-manager.js `++this.consumerSequence` -> `--`: consumer ids
//     only need uniqueness; negative sequence numbers are as unique as
//     positive ones.
//   - consumer-manager.js the two `if (consumerInfo.consumerTag)` guards
//     around consumersByTag.delete: Map.delete of an absent (undefined) key
//     is a no-op, so forcing the branch changes nothing.
//   - consumer-manager.js findQueueNameByTag's `if (!consumerTag)` early
//     return: falling through hands undefined to Map.get, whose `?? null`
//     and the ternary below produce the same null.
//   - consumer-manager.js settleAck's 'nack' action string: any non-'ack'
//     value routes to the nack branch; the name only feeds the failure log.
//   - consumer-manager.js unsubscribe's `if (consumerInfo.channel)` guard:
//     cancelling on null throws inside the try whose catch already tolerates
//     cancel failures — forcing the branch is swallowed.
//   - consumer-manager.js the prefetch cap operands (`current < maxPrefetch`
//     -> true/<=, `current > minPrefetch` -> true/>=): Math.min/Math.max
//     clamp the proposal and the newPrefetch === currentPrefetch early
//     return discards the no-op — the guards only save the arithmetic.
//   - consumer-manager.js subscribeParallel's `if (consumerId)`: subscribe
//     just registered the tag, so the lookup cannot miss; the guard is
//     defensive against a bookkeeping break that would already fail louder
//     elsewhere.
//   - consumer-manager.js disposeAll's cancelled = true: the loop is
//     synchronous and activeConsumers.clear() follows immediately, so no
//     concurrent observer can see the flag between the two.
//   - consumer-manager.js __ackSettled's `configurable: true`: the property
//     is writable, and a non-configurable-but-writable property still
//     accepts value changes — which is all a re-attach performs on it.
//     Configurability only governs flag changes and deletes, which no code
//     path performs. (__channel's configurable IS load-bearing and tested:
//     that property is non-writable, so its re-attach value swap depends on
//     it.)
//   - index.js constructor's `if (#useCache)` around cache creation: every
//     cache consumer re-checks #useCache, so an eagerly built (unused) cache
//     is dead weight the disconnect-close guard tolerates.
//   - index.js checkperiod (cacheCheckPeriod || 120): node-cache's sweep
//     cadence is internal; observing a wrong value needs real-time waits.
//   - index.js context's `#consumers?.` arrow: the arrow only runs after the
//     constructor finished assigning #consumers — construction order makes
//     the optional chaining unreachable-false.
//   - index.js connect()'s promise funnel: defense in depth over TWO inner
//     funnels (RabbitMQConnection's own connectPromise and #restoreState's
//     slot) — a duplicated #doConnect converges on the same dial and the
//     pool gate skips the second restore.
//   - index.js the stale-restore ownership check (#restorePromise ===
//     restore -> true): reaching a non-owning finally needs a third restore
//     caller that today's call graph cannot produce — kept per the comment
//     in the source, three lines against silent duplicate consumers.
//   - index.js waitForConnection's `if (timer)` -> true: both clocks
//     tolerate clearing a null timer handle.
//   - index.js disconnect's cache-close shape guards: @cacheable/node-cache
//     always ships close(), and skipping it only leaves node-cache's
//     internal (unref'd) sweep running — unobservable without real waits.
//   - index.js disconnect's catch-retry: the primary #connection.disconnect
//     has already run by the time anything can throw (a throwing
//     'disconnected' listener), and the connection state machine dedups the
//     second call — the retry exists for mid-teardown failures that the
//     public API cannot produce deterministically.
//   - index.js publishWithCache's `#cache.options?.stdTTL`: node-cache
//     instances always expose options — the chain cannot miss.

const LOG_METHODS = new Set(['info', 'warn', 'error', 'debug'])

const isLoggerObject = (node) => {
  // logger.warn(...) — a local or destructured logger.
  if (node.type === 'Identifier') return node.name === 'logger'

  // this.logger.warn(...) / this.#logger.info(...) / context.logger.error(...)
  if (node.type === 'MemberExpression' || node.type === 'OptionalMemberExpression') {
    const property = node.property

    if (property.type === 'Identifier') return property.name === 'logger'
    if (property.type === 'PrivateName') return property.id.name === 'logger'
  }

  return false
}

const isLoggerCall = (callPath) => {
  const callee = callPath.node.callee

  if (!callee || (callee.type !== 'MemberExpression' && callee.type !== 'OptionalMemberExpression')) {
    return false
  }

  const method = callee.property

  if (method.type !== 'Identifier' || !LOG_METHODS.has(method.name)) return false

  return isLoggerObject(callee.object)
}

// String/TemplateLiteral inside a logger.* call. A ternary passed to a logger
// keeps its ConditionalExpression mutant — only the string arms are ignored.
const logString = (path) => {
  if (!path.isStringLiteral() && !path.isTemplateLiteral()) return undefined

  const call = path.findParent((parent) => parent.isCallExpression() || parent.isOptionalCallExpression())

  if (call && isLoggerCall(call)) {
    return 'Log phrasing is not contract: errors are identified by code, events by name.'
  }

  return undefined
}

// The successLog hooks in consumer-manager.js are factories whose only output
// is a log line — same policy as the logger calls that consume them.
const successLogHook = (path) => {
  if (!path.isObjectProperty()) return undefined

  const key = path.node.key

  if (key?.type === 'Identifier' && key.name === 'successLog') {
    return 'successLog builds a log line: phrasing is not contract.'
  }

  return undefined
}

// Buffer.from(value, 'utf-8'): Node treats a falsy encoding as utf-8, so the
// emptied-string mutant is equivalent by the platform's own contract.
const bufferEncoding = (path) => {
  if (!path.isStringLiteral()) return undefined

  const parent = path.parentPath

  if (!parent?.isCallExpression()) return undefined

  const { callee, arguments: args } = parent.node

  const isBufferFrom = callee.type === 'MemberExpression' &&
    callee.object.type === 'Identifier' && callee.object.name === 'Buffer' &&
    callee.property.type === 'Identifier' && callee.property.name === 'from'

  if (isBufferFrom && args[1] === path.node) {
    return 'Buffer.from treats a falsy encoding as utf-8 — the mutant is equivalent.'
  }

  return undefined
}

// circuitBreaker({ name: ... }): breakwater keeps the name internal — absent
// from stats(), stateChange payloads and its error messages — so no test can
// observe the mutation.
const breakerName = (path) => {
  if (!path.isObjectProperty()) return undefined

  const key = path.node.key

  if (key?.type !== 'Identifier' || key.name !== 'name') return undefined

  const call = path.findParent((parent) => parent.isCallExpression())
  const callee = call?.node.callee

  if (callee?.type === 'Identifier' && callee.name === 'circuitBreaker') {
    return 'breakwater keeps the policy name internal — unobservable, so the mutant is equivalent.'
  }

  return undefined
}

// fixed-window.js: `counters.get(key) || { count: 0, windowStart }` — an
// emptied default object is repaired one line below (undefined !== the
// current windowStart resets count and stamps windowStart), so the mutant
// cannot change behavior. The object shape is unique to that site.
const repairedWindowCounter = (path) => {
  if (!path.isObjectExpression()) return undefined

  const keys = path.node.properties.map((property) => property.key?.name).sort()

  if (keys.length === 2 && keys[0] === 'count' && keys[1] === 'windowStart' && path.parentPath?.isLogicalExpression()) {
    return 'An emptied default is repaired by the window roll-over branch — equivalent.'
  }

  return undefined
}

const RULES = [logString, successLogHook, bufferEncoding, breakerName, repairedWindowCounter]

export const strykerPlugins = [
  declareValuePlugin(PluginKind.Ignore, 'mutation-policy', {
    shouldIgnore (path) {
      for (const rule of RULES) {
        const reason = rule(path)

        if (reason) return reason
      }

      return undefined
    }
  })
]
