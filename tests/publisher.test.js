import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import { EventEmitter } from 'node:events'
import Publisher from '../src/messaging/publisher.js'
import MessageCodec from '../src/messaging/message-codec.js'
import CircuitBreaker from '../src/resilience/circuit-breaker.js'

const silentLogger = { info () {}, warn () {}, error () {}, debug () {} }

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

class FakeChannel extends EventEmitter {
  constructor () {
    super()
    this.published = []
    this.confirmErrors = []
    // publishFireAndForget path: publish() returning false signals a full
    // write buffer, making the publisher wait for drain/close/error.
    this.keepGoingResults = []
  }

  publish (exchange, routingKey, content, options, confirmCallback) {
    this.published.push({ exchange, routingKey, content, options })

    if (confirmCallback) {
      const error = this.confirmErrors.shift() ?? null

      setImmediate(() => confirmCallback(error))
    }

    return this.keepGoingResults.length > 0 ? this.keepGoingResults.shift() : true
  }
}

const createPublisher = (overrides = {}) => {
  const channel = new FakeChannel()

  const context = {
    logger: silentLogger,
    codec: new MessageCodec({ logger: silentLogger }),
    circuitBreaker: new CircuitBreaker(),
    rateLimiter: undefined,
    maxPriority: 10,
    delayExchange: 'delayed',
    getChannel: async () => channel,
    getExchange: () => ({ name: 'main-exchange', type: 'direct' }),
    ...overrides
  }

  return { publisher: new Publisher(context), channel, context }
}

describe('Publisher validation', () => {
  test('rejects an empty routing key on direct exchanges but allows it on fanout/headers', () => {
    const { publisher } = createPublisher()

    assert.throws(() => publisher.validateRoutingKey('', { type: 'direct' }), /Invalid routing key/)
    assert.throws(() => publisher.validateRoutingKey('   ', { type: 'topic' }), /Invalid routing key/)
    assert.throws(() => publisher.validateRoutingKey(42, { type: 'fanout' }), /Invalid routing key/)

    assert.doesNotThrow(() => publisher.validateRoutingKey('', { type: 'fanout' }))
    assert.doesNotThrow(() => publisher.validateRoutingKey('', { type: 'headers' }))
  })

  test('rejects priorities outside [0, maxPriority]', () => {
    const { publisher } = createPublisher()

    assert.throws(() => publisher.validatePriority({ priority: -1 }), /Invalid priority/)
    assert.throws(() => publisher.validatePriority({ priority: 11 }), /Invalid priority/)
    assert.doesNotThrow(() => publisher.validatePriority({ priority: 10 }))
    assert.doesNotThrow(() => publisher.validatePriority({}))
  })
})

describe('Publisher publish', () => {
  test('encodes, publishes persistent by default and resolves on the confirm', async () => {
    const { publisher, channel } = createPublisher()

    await publisher.publish('orders', { id: 1 })

    const [published] = channel.published

    assert.equal(published.exchange, 'main-exchange')
    assert.equal(published.routingKey, 'orders')
    assert.equal(published.options.persistent, true)
    assert.equal(published.options.headers['x-compressed'], false)
    assert.deepEqual(JSON.parse(published.content.toString()), { id: 1 })
  })

  test('retries failed confirms and succeeds within maxRetries', async () => {
    const { publisher, channel } = createPublisher()

    channel.confirmErrors.push(new Error('nack 1'), new Error('nack 2'))

    await publisher.publish('orders', { id: 2 }, { maxRetries: 3, retryDelay: 5 })

    assert.equal(channel.published.length, 3)
  })

  test('surfaces the last real broker error when retries are exhausted', async () => {
    const { publisher, channel } = createPublisher()

    channel.confirmErrors.push(new Error('nacked'), new Error('nacked'), new Error('nacked'))

    await assert.rejects(
      () => publisher.publish('orders', { id: 3 }, { maxRetries: 3, retryDelay: 5 }),
      /not confirmed by the broker: nacked/
    )
  })

  test('maxRetries: 0 still publishes exactly once (no silent message loss)', async () => {
    const { publisher, channel } = createPublisher()

    await publisher.publish('orders', { id: 4 }, { maxRetries: 0 })

    assert.equal(channel.published.length, 1)
  })

  test('an open circuit rejects immediately without touching the channel', async () => {
    const { publisher, channel } = createPublisher({
      circuitBreaker: new CircuitBreaker({ failureThreshold: 2 })
    })

    channel.confirmErrors.push(new Error('down'), new Error('down'))

    await assert.rejects(() => publisher.publish('orders', { id: 1 }, { maxRetries: 1 }))
    await assert.rejects(() => publisher.publish('orders', { id: 2 }, { maxRetries: 1 }))

    const publishesBeforeOpen = channel.published.length

    await assert.rejects(() => publisher.publish('orders', { id: 3 }, { maxRetries: 1 }))

    assert.equal(channel.published.length, publishesBeforeOpen, 'open circuit must not publish')
  })
})

describe('Publisher rate limiting', () => {
  const createRateLimiter = (allow) => {
    const calls = []

    return {
      calls,
      checkRateLimit: async (key, cost) => {
        calls.push({ key, cost })

        return allow
      },
      getStatus: () => ({ remainingTokens: 0 })
    }
  }

  test('rejects with RATE_LIMIT_EXCEEDED and does not publish when the limit is hit', async () => {
    const rateLimiter = createRateLimiter(false)
    const { publisher, channel } = createPublisher({ rateLimiter })

    await assert.rejects(
      () => publisher.publish('orders', { id: 1 }),
      (error) => error.code === 'RATE_LIMIT_EXCEEDED' && error.status.remainingTokens === 0
    )

    assert.deepEqual(rateLimiter.calls, [{ key: 'orders', cost: 1 }])
    assert.equal(channel.published.length, 0)
  })

  test('honors custom rateLimitKey and rateLimitCost, batches multiply the cost', async () => {
    const rateLimiter = createRateLimiter(true)
    const { publisher } = createPublisher({ rateLimiter })

    await publisher.publish('orders', { id: 1 }, { rateLimitKey: 'custom', rateLimitCost: 4 })
    await publisher.publishBatch('orders', [{ n: 1 }, { n: 2 }, { n: 3 }])

    assert.deepEqual(rateLimiter.calls, [
      { key: 'custom', cost: 4 },
      { key: 'orders', cost: 3 }
    ])
  })

  test('does not burn tokens when the connection probe fails (fail-fast ordering)', async () => {
    const rateLimiter = createRateLimiter(true)
    const { publisher } = createPublisher({
      rateLimiter,
      getChannel: async () => {
        throw new Error('Not connected to RabbitMQ')
      }
    })

    await assert.rejects(() => publisher.publish('orders', { id: 1 }), /Not connected/)

    assert.equal(rateLimiter.calls.length, 0, 'tokens must not be consumed while disconnected')
  })
})

describe('Publisher publishBatch', () => {
  test('rejects an empty or non-array batch', async () => {
    const { publisher } = createPublisher()

    await assert.rejects(() => publisher.publishBatch('orders', []), /non-empty array/)
    await assert.rejects(() => publisher.publishBatch('orders', 'nope'), /non-empty array/)
  })

  test('publishes every message of the batch and awaits all confirms', async () => {
    const { publisher, channel } = createPublisher()

    await publisher.publishBatch('orders', [{ n: 1 }, { n: 2 }, { n: 3 }])

    assert.equal(channel.published.length, 3)
  })
})

describe('Publisher fire-and-forget (publishAsync)', () => {
  test('publishes with the x-async header and resolves without a confirm', async () => {
    const { publisher, channel } = createPublisher()

    await publisher.publishAsync('orders', { id: 1 })

    assert.equal(channel.published[0].options.headers['x-async'], true)
  })

  test('waits for drain when the write buffer is full', async () => {
    const { publisher, channel } = createPublisher()

    channel.keepGoingResults.push(false)

    let resolved = false
    const pending = publisher.publishAsync('orders', { id: 1 }).then(() => { resolved = true })

    await sleep(20)
    assert.equal(resolved, false, 'must wait for drain')

    channel.emit('drain')
    await pending

    assert.equal(resolved, true)
  })

  test('a channel close while waiting for drain rejects instead of hanging', async () => {
    const { publisher, channel } = createPublisher()

    channel.keepGoingResults.push(false)

    const pending = publisher.publishAsync('orders', { id: 1 })

    await sleep(10)
    channel.emit('close')

    await assert.rejects(() => pending, /Channel closed while waiting for drain/)
  })

  test('a channel error while waiting for drain rejects with that error', async () => {
    const { publisher, channel } = createPublisher()

    channel.keepGoingResults.push(false)

    const pending = publisher.publishAsync('orders', { id: 1 })

    await sleep(10)
    channel.emit('error', new Error('socket reset'))

    await assert.rejects(() => pending, /socket reset/)
  })

  test('publishAsyncBatch validates input and publishes sequentially with the batch header', async () => {
    const { publisher, channel } = createPublisher()

    await assert.rejects(() => publisher.publishAsyncBatch('orders', []), /non-empty array/)

    await publisher.publishAsyncBatch('orders', [{ n: 1 }, { n: 2 }])

    assert.equal(channel.published.length, 2)
    assert.ok(channel.published.every(p => p.options.headers['x-async-batch'] === true))
  })
})

describe('Publisher publishDelayed', () => {
  test('validates the delay', async () => {
    const { publisher } = createPublisher()

    await assert.rejects(() => publisher.publishDelayed('orders', {}, -1), /non-negative number/)
    await assert.rejects(() => publisher.publishDelayed('orders', {}, Infinity), /non-negative number/)
    await assert.rejects(() => publisher.publishDelayed('orders', {}, 'soon'), /non-negative number/)
  })

  test('publishes through the delay exchange with the x-delay header', async () => {
    const { publisher, channel } = createPublisher()

    await publisher.publishDelayed('orders', { id: 1 }, 5000)

    const [published] = channel.published

    assert.equal(published.exchange, 'delayed')
    assert.equal(published.options.headers['x-delay'], 5000)
  })
})
