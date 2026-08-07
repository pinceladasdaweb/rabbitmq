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
