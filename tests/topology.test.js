import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import Topology from '../src/messaging/topology.js'
import { FakeChannel, silentLogger } from './helpers.js'

const createTopology = (overrides = {}) => {
  const channel = new FakeChannel()

  const context = {
    logger: silentLogger,
    deadLetterExchange: 'dlx',
    delayExchange: 'delayed',
    getChannel: async () => channel,
    getExchange: () => ({ name: 'main-exchange', type: 'topic' }),
    getQueueNameByConsumerTag: () => null,
    ...overrides
  }

  return { topology: new Topology(context), channel }
}

describe('Topology ensureExchange', () => {
  test('asserts the configured exchange with its type and options', async () => {
    const { topology, channel } = createTopology({
      getExchange: () => ({ name: 'orders', type: 'fanout', options: { durable: false } })
    })

    await topology.ensureExchange()

    assert.deepEqual(channel.assertedExchanges, [
      { name: 'orders', type: 'fanout', options: { durable: false } }
    ])
  })

  test('defaults to a durable direct exchange when type/options are omitted', async () => {
    const { topology, channel } = createTopology({
      getExchange: () => ({ name: 'orders' })
    })

    await topology.ensureExchange()

    assert.deepEqual(channel.assertedExchanges, [
      { name: 'orders', type: 'direct', options: { durable: true } }
    ])
  })

  test('is a no-op when no exchange is configured', async () => {
    let channelRequested = false
    const { topology, channel } = createTopology({
      getExchange: () => ({}),
      getChannel: async () => {
        channelRequested = true

        return channel
      }
    })

    await topology.ensureExchange()

    assert.equal(channelRequested, false, 'must not touch the broker without an exchange name')
  })

  test('propagates assertion failures', async () => {
    const { topology, channel } = createTopology()

    channel.assertExchangeError = new Error('access refused')

    await assert.rejects(() => topology.ensureExchange(), /access refused/)
  })
})

describe('Topology setupDeadLetterExchange', () => {
  test('asserts the configured DLX as durable direct', async () => {
    const { topology, channel } = createTopology({ deadLetterExchange: 'my-dlx' })

    await topology.setupDeadLetterExchange()

    assert.deepEqual(channel.assertedExchanges, [
      { name: 'my-dlx', type: 'direct', options: { durable: true } }
    ])
  })
})

describe('Topology createQueue', () => {
  test('creates the queue wired to the DLX plus its bound DLQ', async () => {
    const { topology, channel } = createTopology()

    await topology.createQueue('orders')

    const [main, dlq] = channel.assertedQueues

    assert.equal(main.name, 'orders')
    assert.equal(main.options.durable, true)
    assert.equal(main.options.arguments['x-dead-letter-exchange'], 'dlx')
    assert.equal(main.options.arguments['x-dead-letter-routing-key'], 'orders_dlq')
    assert.deepEqual(dlq, { name: 'orders_dlq', options: { durable: true } })
    assert.deepEqual(channel.boundQueues, [{ queue: 'orders_dlq', exchange: 'dlx', routingKey: 'orders_dlq' }])
  })

  test('honors maxPriority and merges custom arguments', async () => {
    const { topology, channel } = createTopology()

    await topology.createQueue('orders', {
      durable: false,
      maxPriority: 5,
      arguments: { 'x-queue-mode': 'lazy' }
    })

    const main = channel.assertedQueues[0]

    assert.equal(main.options.durable, false)
    assert.equal(main.options.arguments['x-max-priority'], 5)
    assert.equal(main.options.arguments['x-queue-mode'], 'lazy')
    assert.equal(main.options.arguments['x-dead-letter-exchange'], 'dlx')
  })

  test('reports which queue could not be created', async () => {
    const errors = []
    const { topology, channel } = createTopology({
      logger: { ...silentLogger, error: (line) => errors.push(line) }
    })

    channel.assertQueueError = new Error('PRECONDITION_FAILED')

    await assert.rejects(() => topology.createQueue('orders'), /PRECONDITION_FAILED/)

    // Naming the queue is what makes this actionable during a deploy that
    // declares dozens of them.
    assert.ok(errors.some(line => line.includes("'orders'") && line.includes('PRECONDITION_FAILED')))
  })

  test('propagates queue assertion failures', async () => {
    const { topology, channel } = createTopology()

    channel.assertQueueError = new Error('inequivalent arg')

    await assert.rejects(() => topology.createQueue('orders'), /inequivalent arg/)
  })
})

describe('Topology moveToDeadLetter', () => {
  const buildMessage = () => ({
    content: Buffer.from('{"id":1}'),
    fields: { consumerTag: 'tag-1', routingKey: 'orders-route', exchange: 'main-exchange' },
    properties: { headers: { original: true } }
  })

  test('falls back to the routing-key convention when no tag resolver is wired', async () => {
    // Topology defaults getQueueNameByConsumerTag to () => null when the
    // context omits it, so a message moved by a standalone Topology still
    // resolves a DLQ instead of throwing on an undefined lookup.
    const channel = new FakeChannel()

    const topology = new Topology({
      logger: silentLogger,
      deadLetterExchange: 'dlx',
      delayExchange: 'delayed',
      getChannel: async () => channel,
      getExchange: () => ({ name: 'main-exchange', type: 'topic' })
    })

    await topology.moveToDeadLetter(buildMessage())

    assert.equal(channel.published[0].routingKey, 'orders-route_dlq')
  })

  test('publishes to the DLQ resolved through the consumer tag with tracking headers', async () => {
    const { topology, channel } = createTopology({
      getQueueNameByConsumerTag: (tag) => (tag === 'tag-1' ? 'orders' : null)
    })

    await topology.moveToDeadLetter(buildMessage(), 'quarantine')

    const [published] = channel.published

    assert.equal(published.exchange, 'dlx')
    assert.equal(published.routingKey, 'orders_dlq')
    assert.equal(published.options.persistent, true)
    assert.equal(published.options.mandatory, true)
    assert.equal(published.options.headers['x-death-reason'], 'quarantine')
    assert.equal(published.options.headers['x-original-exchange'], 'main-exchange')
    assert.equal(published.options.headers['x-original-routing-key'], 'orders-route')
    assert.equal(published.options.headers.original, true, 'original headers are preserved')

    const deathTime = published.options.headers['x-death-time']

    assert.match(deathTime, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/, 'x-death-time must be an ISO timestamp')
    assert.ok(Math.abs(Date.parse(deathTime) - Date.now()) < 60000, 'x-death-time must be the actual death time')
  })

  test('falls back to the routing-key convention when the consumer tag is unknown', async () => {
    const { topology, channel } = createTopology()

    await topology.moveToDeadLetter(buildMessage())

    assert.equal(channel.published[0].routingKey, 'orders-route_dlq')
  })

  test('rejects when the broker does not confirm the publish', async () => {
    const { topology, channel } = createTopology()

    channel.confirmErrors.push(new Error('channel closed'))

    await assert.rejects(() => topology.moveToDeadLetter(buildMessage()), /Failed to move message to dead letter queue/)
  })

  test('rejects when the DLQ routing has no binding (message returned)', async () => {
    const { topology, channel } = createTopology()

    channel.returnRoutingKey = 'orders-route_dlq'

    await assert.rejects(() => topology.moveToDeadLetter(buildMessage()), /no binding/)
  })

  test('ignores basic.return events for other routing keys', async () => {
    const { topology, channel } = createTopology()

    channel.returnRoutingKey = 'some-other-queue_dlq'

    await topology.moveToDeadLetter(buildMessage())

    assert.equal(channel.published.length, 1)
  })
})

describe('Topology delay exchange and plugin', () => {
  test('setupDelayExchange asserts x-delayed-message with the explicit type', async () => {
    const { topology, channel } = createTopology()

    await topology.setupDelayExchange({ type: 'topic' })

    const [exchange] = channel.assertedExchanges

    assert.equal(exchange.name, 'delayed')
    assert.equal(exchange.type, 'x-delayed-message')
    assert.equal(exchange.options.arguments['x-delayed-type'], 'topic')
    assert.equal(exchange.options.durable, true)
  })

  test('setupDelayExchange falls back to the main exchange type, then direct', async () => {
    const fanout = createTopology({ getExchange: () => ({ name: 'x', type: 'fanout' }) })

    await fanout.topology.setupDelayExchange()
    assert.equal(fanout.channel.assertedExchanges[0].options.arguments['x-delayed-type'], 'fanout')

    const bare = createTopology({ getExchange: () => ({}) })

    await bare.topology.setupDelayExchange()
    assert.equal(bare.channel.assertedExchanges[0].options.arguments['x-delayed-type'], 'direct')
  })

  test('isDelayPluginEnabled probes with a throwaway exchange and cleans it up', async () => {
    const { topology, channel } = createTopology()

    assert.equal(await topology.isDelayPluginEnabled(), true)
    assert.equal(channel.assertedExchanges[0].name, 'test.delay')
    assert.deepEqual(channel.deletedExchanges, ['test.delay'])
  })

  test('isDelayPluginEnabled returns false for both RabbitMQ 3 and 4 error texts', async () => {
    for (const message of [
      "NOT_FOUND - no exchange type 'x-delayed-message'... exchange type",
      "PRECONDITION_FAILED - unknown exchange type 'x-delayed-message'"
    ]) {
      const { topology, channel } = createTopology()

      channel.assertExchangeError = new Error(message)

      assert.equal(await topology.isDelayPluginEnabled(), false)
    }
  })

  test('isDelayPluginEnabled rethrows unrelated errors', async () => {
    const { topology, channel } = createTopology()

    channel.assertExchangeError = new Error('ACCESS_REFUSED - login refused')

    await assert.rejects(() => topology.isDelayPluginEnabled(), /ACCESS_REFUSED/)
  })

  test('setupDelayPlugin resolves when the plugin is available and throws when it is not', async () => {
    const enabled = createTopology()

    await enabled.topology.setupDelayPlugin()

    const disabled = createTopology()

    disabled.channel.assertExchangeError = new Error("unknown exchange type 'x-delayed-message'")

    await assert.rejects(() => disabled.topology.setupDelayPlugin(), /not enabled/)
  })
})
