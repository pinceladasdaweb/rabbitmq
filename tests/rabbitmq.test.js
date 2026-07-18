import RabbitMQ from '../src/index.js'
import assert from 'node:assert/strict'
import { test, describe } from 'node:test'

const silentLogger = { info: () => {}, warn: () => {}, error: () => {} }

const createInstance = (options = {}) => new RabbitMQ({
  username: 'user',
  password: 'pass',
  endpoints: ['localhost:5672'],
  exchange: { name: 'unit-exchange', type: 'direct' },
  logger: silentLogger,
  ...options
})

describe('RabbitMQ (validation, no broker required)', () => {
  describe('constructor', () => {
    test('throws when no endpoint is provided', () => {
      assert.throws(() => new RabbitMQ({ username: 'u', password: 'p', logger: silentLogger }), /endpoint/)
    })

    test('throws on invalid protocol', () => {
      assert.throws(() => createInstance({ protocol: 'http' }), /Invalid protocol/)
    })
  })

  describe('setExchange', () => {
    test('rejects empty names and invalid types', () => {
      const rabbitMQ = createInstance()

      assert.throws(() => rabbitMQ.setExchange(''), /non-empty string/)
      assert.throws(() => rabbitMQ.setExchange('ok', 'bogus'), /Invalid exchange type/)
    })
  })

  describe('getChannel', () => {
    test('rejects before connect', async () => {
      const rabbitMQ = createInstance()

      await assert.rejects(() => rabbitMQ.getChannel(), /Not connected/)
    })
  })

  describe('publish validation', () => {
    test('rejects invalid routing keys', async () => {
      const rabbitMQ = createInstance()

      await assert.rejects(() => rabbitMQ.publish('', { a: 1 }), /Invalid routing key/)
      await assert.rejects(() => rabbitMQ.publish(null, { a: 1 }), /Invalid routing key/)
    })

    test('allows empty routing key on fanout exchanges', async () => {
      const rabbitMQ = createInstance({ exchange: { name: 'fan', type: 'fanout' } })

      // Passes validation and fails later due to the missing connection.
      await assert.rejects(() => rabbitMQ.publish('', { a: 1 }, { maxRetries: 1 }), /Not connected/)
    })

    test('rejects out-of-range priority', async () => {
      const rabbitMQ = createInstance({ maxPriority: 5 })

      await assert.rejects(() => rabbitMQ.publish('route', {}, { priority: 6 }), /Invalid priority/)
      await assert.rejects(() => rabbitMQ.publish('route', {}, { priority: -1 }), /Invalid priority/)
    })

    test('publishBatch rejects empty or non-array messages', async () => {
      const rabbitMQ = createInstance()

      await assert.rejects(() => rabbitMQ.publishBatch('route', []), /non-empty array/)
      await assert.rejects(() => rabbitMQ.publishBatch('route', 'not-an-array'), /non-empty array/)
    })

    test('publishDelayed rejects invalid delays', async () => {
      const rabbitMQ = createInstance()

      await assert.rejects(() => rabbitMQ.publishDelayed('route', {}, -1), /non-negative number/)
      await assert.rejects(() => rabbitMQ.publishDelayed('route', {}, 'soon'), /non-negative number/)
      await assert.rejects(() => rabbitMQ.publishDelayed('route', {}, Infinity), /non-negative number/)
    })

    test('publish with maxRetries: 0 still attempts the operation instead of resolving silently', async () => {
      const rabbitMQ = createInstance()

      // Regression: maxRetries: 0 used to skip the retry loop entirely and
      // resolve without ever publishing. It must attempt at least once —
      // here that attempt fails loudly because there is no connection.
      await assert.rejects(() => rabbitMQ.publish('route', { a: 1 }, { maxRetries: 0 }), /Not connected/)
    })
  })

  describe('subscribe validation', () => {
    test('rejects invalid queue names and callbacks', async () => {
      const rabbitMQ = createInstance()

      await assert.rejects(() => rabbitMQ.subscribe('', () => {}), /non-empty string/)
      await assert.rejects(() => rabbitMQ.subscribe('queue', 'not-a-function'), /must be a function/)
      await assert.rejects(() => rabbitMQ.subscribeSequential('', () => {}), /non-empty string/)
      await assert.rejects(() => rabbitMQ.subscribeSequential('queue', null), /must be a function/)
    })
  })

  describe('unsubscribe', () => {
    test('returns false for unknown consumer tags', async () => {
      const rabbitMQ = createInstance()

      assert.equal(await rabbitMQ.unsubscribe('unknown-tag'), false)
    })
  })

  describe('rate limiting on publish', () => {
    test('publishing while disconnected does not consume rate limit tokens', async () => {
      const rabbitMQ = createInstance({
        rateLimiter: { strategy: 'token-bucket', maxRequests: 1, windowMs: 60000 }
      })

      // The connection probe fails BEFORE the rate limiter runs, so tokens
      // are never burned for publishes that could not possibly be sent.
      await assert.rejects(() => rabbitMQ.publish('route', { a: 1 }), /Not connected/)
      await assert.rejects(() => rabbitMQ.publish('route', { a: 2 }), /Not connected/)

      assert.equal(rabbitMQ.getRateLimitStatus('route').remainingTokens, 1)

      await rabbitMQ.disconnect()
    })

    test('rate limiter management methods throw when the feature is disabled', () => {
      const rabbitMQ = createInstance()

      assert.throws(() => rabbitMQ.getRateLimitStatus('k'), /not enabled/)
      assert.throws(() => rabbitMQ.resetRateLimit('k'), /not enabled/)
      assert.throws(() => rabbitMQ.blockRateLimit('k'), /not enabled/)
    })
  })

  describe('cache management', () => {
    test('cache methods throw when the feature is disabled', async () => {
      const rabbitMQ = createInstance()

      await assert.rejects(() => rabbitMQ.getFromCache('route'), /not enabled/)
      assert.throws(() => rabbitMQ.invalidateCache('route'), /not enabled/)
      assert.throws(() => rabbitMQ.clearCache(), /not enabled/)
    })
  })

  describe('configuration setters', () => {
    test('validate their inputs', () => {
      const rabbitMQ = createInstance()

      assert.throws(() => rabbitMQ.setCompressionThreshold(-1), /non-negative/)
      assert.throws(() => rabbitMQ.setCompressionThreshold('big'), /non-negative/)
      assert.throws(() => rabbitMQ.setSerializer('not-a-function'), /must be a function/)
      assert.throws(() => rabbitMQ.setDeserializer(42), /must be a function/)
    })
  })

  describe('circuit breaker', () => {
    test('exposes state without a connection', () => {
      const rabbitMQ = createInstance()
      const state = rabbitMQ.getCircuitBreakerState()

      assert.equal(state.state, 'CLOSED')
      assert.equal(state.failureCount, 0)
    })
  })

  describe('cluster status', () => {
    test('reports disconnected before connect', () => {
      const rabbitMQ = createInstance()
      const status = rabbitMQ.getClusterStatus()

      assert.equal(status.connectionState, 'disconnected')
      assert.deepEqual(status.allEndpoints, ['localhost:5672'])
    })
  })
})
