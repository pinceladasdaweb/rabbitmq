import { declareValuePlugin, PluginKind } from '@stryker-mutator/api/plugin'

// Declares once what 84 per-line comments used to declare: string literals
// inside logger calls are log phrasing, not contract — errors in this library
// are identified by `code`, events by name. Mutating a log message was never
// a missing defense, and pinning the wording with asserts would fossilize it.
//
// Scope is deliberately narrow. Only String/TemplateLiteral nodes whose
// enclosing call is logger.info/warn/error/debug are ignored, so:
//   - a ternary passed to a logger keeps its ConditionalExpression mutant
//     (each string arm is ignored individually);
//   - thrown error messages, event names and strategy keys stay in the gate;
//   - literals produced OUTSIDE the call (e.g. a log-line factory such as
//     ConsumerManager's successLog hooks) are not matched — those carry an
//     explicit per-line disable at the definition site instead.
// Ignoring a template does take its `${...}` children along (Stryker ignores
// node + subtree): acceptable, because logic that only ever renders into a
// log line is phrasing too — anything load-bearing also exists outside it.

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

export const strykerPlugins = [
  declareValuePlugin(PluginKind.Ignore, 'log-strings', {
    shouldIgnore (path) {
      if (!path.isStringLiteral() && !path.isTemplateLiteral()) return undefined

      const call = path.findParent((parent) => parent.isCallExpression() || parent.isOptionalCallExpression())

      if (call && isLoggerCall(call)) {
        return 'Log phrasing is not contract: errors are identified by code, events by name.'
      }

      return undefined
    }
  })
]
