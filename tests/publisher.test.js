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
    // Both ends of the range are inclusive. Zero is the AMQP default priority,
    // so rejecting it would break the most ordinary explicit value there is.
    assert.doesNotThrow(() => publisher.validatePriority({ priority: 0 }), 'zero is a valid priority')
    assert.doesNotThrow(() => publisher.validatePriority({}), 'omitting priority is always fine')
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

describe('Publisher with a serializer that throws a non-Error', () => {
  // The serializer is user code (setSerializer). Same contract as issue #18
  // on the consumer side: a `throw null` must not crash the library's own
  // logging nor replace the caller's error with a TypeError.
  const throwingCodec = { encode: async () => { throw null } } // eslint-disable-line no-throw-literal

  test('publish still writes the retry log and rethrows the original value', async () => {
    const logger = recordingLogger()
    const { publisher } = createPublisher({ codec: throwingCodec, logger })

    await assert.rejects(
      () => publisher.publish('orders', { id: 1 }, { maxRetries: 2, retryDelay: 1 }),
      (error) => error === null
    )

    assert.equal(logger.records.warn.length, 1, 'one retry between two attempts, and its log survived the null')
    assert.ok(logger.records.warn[0].includes('null'), 'the log renders the thrown value')
  })

  test('publishAsync logs the failure and rethrows the original value', async () => {
    const logger = recordingLogger()
    const { publisher } = createPublisher({ codec: throwingCodec, logger })

    await assert.rejects(
      () => publisher.publishAsync('orders', { id: 1 }),
      (error) => error === null,
      'the caller must receive what the serializer threw, not a TypeError'
    )

    assert.equal(logger.records.error.length, 1)
    assert.ok(logger.records.error[0].includes('null'))
  })

  test('publishAsyncBatch logs the failure and rethrows the original value', async () => {
    const logger = recordingLogger()
    const { publisher } = createPublisher({ codec: throwingCodec, logger })

    await assert.rejects(
      () => publisher.publishAsyncBatch('orders', [{ id: 1 }]),
      (error) => error === null
    )

    assert.equal(logger.records.error.length, 1)
    assert.ok(logger.records.error[0].includes('null'))
  })
})

describe('Publisher rate-limit accounting', () => {
  // Each publish flavour reserves its own key namespace and pays a cost that
  // scales with the batch. rateLimitCost 2 keeps multiplication apart from
  // division (with the default 1 they agree), and omitting it must charge
  // exactly 1 — not undefined.
  const allowAll = () => {
    const rateLimiter = { calls: [] }

    rateLimiter.checkRateLimit = async (key, cost) => {
      rateLimiter.calls.push({ key, cost })

      return true
    }
    rateLimiter.getStatus = () => ({})

    return rateLimiter
  }

  test('every flavour charges its namespaced key and batch-scaled cost', async () => {
    const rateLimiter = allowAll()
    const { publisher, channel } = createPublisher({ rateLimiter })

    channel.confirmDelayMs = 0

    await publisher.publish('orders', { n: 1 }, { rateLimitCost: 2 })
    await publisher.publishBatch('orders', [{ n: 1 }, { n: 2 }, { n: 3 }], { rateLimitCost: 2 })
    await publisher.publishAsync('orders', { n: 1 }, { rateLimitCost: 2 })
    await publisher.publishAsyncBatch('orders', [{ n: 1 }, { n: 2 }, { n: 3 }], { rateLimitCost: 2 })
    await publisher.publishDelayed('orders', { n: 1 }, 5, { rateLimitCost: 2 })

    assert.deepEqual(rateLimiter.calls, [
      { key: 'orders', cost: 2 },
      { key: 'orders', cost: 6 },
      { key: 'async:orders', cost: 2 },
      { key: 'async-batch:orders', cost: 6 },
      { key: 'delayed:orders', cost: 2 }
    ])
  })

  test('omitting rateLimitCost charges exactly one per message', async () => {
    const rateLimiter = allowAll()
    const { publisher } = createPublisher({ rateLimiter })

    await publisher.publishAsync('orders', { n: 1 })
    await publisher.publishAsyncBatch('orders', [{ n: 1 }, { n: 2 }])
    await publisher.publishDelayed('orders', { n: 1 }, 5)

    assert.deepEqual(rateLimiter.calls.map(call => call.cost), [1, 2, 1])
  })
})

describe('Publisher retry pacing', () => {
  test('retries are spaced by retryDelay with deterministic (jitter-free) backoff', async () => {
    // The backoff config is only observable through WHEN attempts run:
    // jitter 'none' with initial 400 puts the second attempt ~200ms out,
    // while breakwater's defaults (full jitter over a much smaller initial)
    // land far under 150ms and a broken jitter value yields no delay at all.
    const attempts = []
    const { publisher, channel } = createPublisher()

    channel.confirmErrors.push(new Error('transient'))

    const originalPublish = channel.publish.bind(channel)

    channel.publish = (...args) => {
      attempts.push(Date.now())

      return originalPublish(...args)
    }

    await publisher.publish('orders', { n: 1 }, { maxRetries: 2, retryDelay: 400 })

    assert.equal(attempts.length, 2)
    assert.ok(attempts[1] - attempts[0] >= 150, `the retry must respect the configured pacing (waited ${attempts[1] - attempts[0]}ms)`)
  })
})

describe('Publisher with a debug-less logger', () => {
  test('confirmed publishes tolerate a logger without debug', async () => {
    // Injected loggers only promise error/warn/info; debug is reached with
    // optional chaining precisely because it may not exist.
    const { logger } = { logger: { info: () => {}, warn: () => {}, error: () => {} } }
    const { publisher } = createPublisher({ logger })

    await publisher.publish('orders', { n: 1 })
    await publisher.publishDelayed('orders', { n: 1 }, 5)
  })
})

describe('Publisher publishDelayed validation', () => {
  test('accepts zero, rejects negatives and non-numbers', async () => {
    const { publisher, channel } = createPublisher()

    await publisher.publishDelayed('orders', { n: 1 }, 0)
    assert.equal(channel.published[0].options.headers['x-delay'], 0, 'zero delay is a legal immediate delivery')

    await assert.rejects(() => publisher.publishDelayed('orders', { n: 1 }, -1), /non-negative number/)
    await assert.rejects(() => publisher.publishDelayed('orders', { n: 1 }, 'soon'), /non-negative number/)
    await assert.rejects(() => publisher.publishDelayed('orders', { n: 1 }, Infinity), /non-negative number/)
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

    // The drain machinery attaches three one-shot listeners per stalled
    // publish; each settled wait must remove all of them or a long-lived
    // channel accumulates dead listeners on every buffer stall.
    assert.equal(channel.listenerCount('drain'), 0)
    assert.equal(channel.listenerCount('error'), 0)
    assert.equal(channel.listenerCount('close') <= 1, true, 'only the FakeChannel constructor listener remains')
  })

  test('a channel close while waiting for drain rejects instead of hanging', async () => {
    const { publisher, channel } = createPublisher()

    channel.keepGoingResults.push(false)

    const pending = publisher.publishAsync('orders', { id: 1 })

    await sleep(10)
    channel.emit('close')

    await assert.rejects(() => pending, /Channel closed while waiting for drain/)

    // 'close' consumed its own once-listener; 'drain' and 'error' did not
    // fire, so only an explicit cleanup removes them.
    assert.equal(channel.listenerCount('drain'), 0, 'the unfired drain listener was cleaned up')
    assert.equal(channel.listenerCount('error'), 0, 'the unfired error listener was cleaned up')
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

  test('a non-Error channel failure during drain still rejects with an Error', async () => {
    // amqplib emits whatever the broker handed it. A thrown string would leave
    // the caller with `undefined` for error.message on the retry path.
    const { publisher, channel } = createPublisher()

    channel.keepGoingResults.push(false)

    const pending = publisher.publishAsyncBatch('orders', [{ n: 1 }, { n: 2 }])
      .then(() => null, (error) => error)

    await sleep(20)

    channel.emit('error', 'connection reset by peer')

    const error = await pending

    assert.ok(error instanceof Error, 'the caller always receives an Error')
    assert.match(error.message, /Channel error while waiting for drain/)
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

describe('Publisher publishBatch retry granularity', () => {
  test('a retry republishes only the messages the broker did not confirm', async () => {
    // Resending the whole batch delivered every already-confirmed message
    // again — consumers saw duplicates of messages that never failed.
    const { publisher, channel } = createPublisher()

    // Three publishes go out; the second confirm fails.
    channel.confirmErrors.push(null, new Error('nacked'), null)

    await publisher.publishBatch('orders', [{ n: 1 }, { n: 2 }, { n: 3 }], { maxRetries: 2, retryDelay: 1 })

    assert.equal(channel.published.length, 4, 'three first attempts plus exactly one retry')

    const payloads = channel.published.map(entry => JSON.parse(entry.content.toString()).n)

    assert.deepEqual(payloads, [1, 2, 3, 2], 'only the unconfirmed message went out again')
  })

  test('a batch that fully succeeds publishes each message exactly once', async () => {
    const { publisher, channel } = createPublisher()

    await publisher.publishBatch('orders', [{ n: 1 }, { n: 2 }])

    assert.equal(channel.published.length, 2)
  })
})
