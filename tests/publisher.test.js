import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import Publisher from '../src/messaging/publisher.js'
import MessageCodec from '../src/messaging/message-codec.js'
import CircuitBreaker from '../src/resilience/circuit-breaker.js'
import { FakeChannel, recordingLogger, silentLogger, sleep, waitFor } from './helpers.js'

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
    assert.doesNotThrow(() => publisher.validatePriority({ priority: 10 }), 'maxPriority itself is valid')
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

  test('publishing while disconnected leaves the circuit breaker closed', async () => {
    // The preflight probe runs OUTSIDE the breaker on purpose: the
    // reconnection state machine owns that failure mode. If the probe fed the
    // breaker, an outage would trip it and keep blocking publishes after
    // recovery.
    const circuitBreaker = new CircuitBreaker({ failureThreshold: 2 })
    const { publisher } = createPublisher({
      circuitBreaker,
      getChannel: async () => {
        throw new Error('Not connected to RabbitMQ')
      }
    })

    for (let i = 0; i < 5; i++) {
      await assert.rejects(() => publisher.publish('orders', { id: i }), /Not connected/)
    }

    assert.equal(circuitBreaker.getState().state, 'CLOSED', 'a disconnected broker must not trip the breaker')
    assert.equal(circuitBreaker.getState().failureCount, 0)
  })
})

describe('Publisher publishBatch', () => {
  test('rejects an empty or non-array batch', async () => {
    const { publisher } = createPublisher()

    await assert.rejects(() => publisher.publishBatch('orders', []), /non-empty array/)
    await assert.rejects(() => publisher.publishBatch('orders', 'nope'), /non-empty array/)
  })

  test('does not resolve until every confirm in the batch has been answered', async () => {
    const { publisher, channel } = createPublisher()

    // Confirms are held so a caller that forgets to await them is observable.
    channel.manualConfirms = true

    let resolved = false
    const pending = publisher.publishBatch('orders', [{ n: 1 }, { n: 2 }, { n: 3 }]).then(() => { resolved = true })

    await waitFor(() => channel.published.length === 3, 2000, 'all three messages published')
    await sleep(20)

    assert.equal(resolved, false, 'must not resolve while confirms are outstanding')

    channel.releaseConfirms(2)
    await sleep(20)

    assert.equal(resolved, false, 'one outstanding confirm must still block resolution')

    channel.releaseConfirms()
    await pending

    assert.equal(resolved, true)
  })

  test('rejects when the broker nacks one message of the batch', async () => {
    const { publisher, channel } = createPublisher()

    channel.confirmErrors.push(null, new Error('nacked'))

    await assert.rejects(
      () => publisher.publishBatch('orders', [{ n: 1 }, { n: 2 }, { n: 3 }], { maxRetries: 1 }),
      /not confirmed by the broker: nacked/
    )
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

  test('publishAsyncBatch rejects an empty or non-array batch', async () => {
    const { publisher } = createPublisher()

    await assert.rejects(() => publisher.publishAsyncBatch('orders', []), /non-empty array/)
    await assert.rejects(() => publisher.publishAsyncBatch('orders', 'nope'), /non-empty array/)
  })

  test('publishAsyncBatch logs and rethrows when the batch cannot be published', async () => {
    const logger = recordingLogger()
    const { publisher, channel } = createPublisher({ logger })

    // The failure has to come from the publish itself: preflight's connection
    // probe calls getChannel() before the try, so breaking that would never
    // reach the reporting path this test is about.
    channel.publish = () => { throw new Error('channel is closed') }

    await assert.rejects(() => publisher.publishAsyncBatch('orders', [{ n: 1 }]), /channel is closed/)

    assert.ok(
      logger.records.error.some(line => line.includes('Failed to publish batch asynchronously')),
      'the failure is reported before being rethrown'
    )
  })

  test('publishAsyncBatch respects back-pressure: it waits for drain before the next message', async () => {
    const { publisher, channel } = createPublisher()

    // The first publish reports a full write buffer, so the loop must stop
    // there. Publishing the whole batch concurrently would defeat the
    // serialization the sequential await exists for.
    channel.keepGoingResults.push(false)

    let resolved = false
    const pending = publisher.publishAsyncBatch('orders', [{ n: 1 }, { n: 2 }, { n: 3 }]).then(() => { resolved = true })

    await sleep(20)

    assert.equal(channel.published.length, 1, 'must not publish ahead while back-pressured')
    assert.equal(resolved, false)

    channel.emit('drain')
    await pending

    assert.equal(channel.published.length, 3)
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
