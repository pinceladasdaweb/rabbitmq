import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import { EventEmitter } from 'node:events'
import Topology from '../src/messaging/topology.js'

const silentLogger = { info () {}, warn () {}, error () {}, debug () {} }

class FakeChannel extends EventEmitter {
  constructor () {
    super()
    this.assertedExchanges = []
    this.assertedQueues = []
    this.boundQueues = []
    this.deletedExchanges = []
    this.published = []
    // Failure knobs
    this.assertExchangeError = null
    this.assertQueueError = null
    this.confirmError = null
    this.returnRoutingKey = null
  }

  async assertExchange (name, type, options) {
    if (this.assertExchangeError) throw this.assertExchangeError

    this.assertedExchanges.push({ name, type, options })
  }

  async assertQueue (name, options) {
    if (this.assertQueueError) throw this.assertQueueError

    this.assertedQueues.push({ name, options })
  }

  async bindQueue (queue, exchange, routingKey) {
    this.boundQueues.push({ queue, exchange, routingKey })
  }

  async deleteExchange (name) {
    this.deletedExchanges.push(name)
  }

  publish (exchange, routingKey, content, options, confirmCallback) {
    this.published.push({ exchange, routingKey, content, options })

    // basic.return (if configured) arrives before the confirm ack,
    // mirroring the broker's ordering.
    if (this.returnRoutingKey) {
      this.emit('return', { fields: { routingKey: this.returnRoutingKey } })
    }

    if (confirmCallback) setImmediate(() => confirmCallback(this.confirmError))

    return true
  }
}

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
    assert.ok(published.options.headers['x-death-time'])
  })

  test('falls back to the routing-key convention when the consumer tag is unknown', async () => {
    const { topology, channel } = createTopology()

    await topology.moveToDeadLetter(buildMessage())

    assert.equal(channel.published[0].routingKey, 'orders-route_dlq')
  })

  test('rejects when the broker does not confirm the publish', async () => {
    const { topology, channel } = createTopology()

    channel.confirmError = new Error('channel closed')

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
