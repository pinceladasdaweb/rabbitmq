import assert from 'node:assert/strict'
import RabbitMQ from '../../src/index.js'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { test, describe } from 'node:test'

// Cluster integration tests against a real three-node RabbitMQ cluster.
//
//   docker compose -f docker-compose.cluster.yml up -d --wait
//   npm run test:cluster
//
// These cover what a single broker cannot: what happens to a client when the
// node it is talking to disappears. Everything here kills a container for
// real — no fakes, no mocked failures.

const RUN_CLUSTER = process.env.RABBITMQ_CLUSTER_INTEGRATION === '1'

const USERNAME = process.env.RMQ_USERNAME || 'admin'
const PASSWORD = process.env.RMQ_PASSWORD || 'admin'

const NODES = [
  { container: 'rabbitmq-cluster-1', endpoint: 'localhost:5681' },
  { container: 'rabbitmq-cluster-2', endpoint: 'localhost:5682' },
  { container: 'rabbitmq-cluster-3', endpoint: 'localhost:5683' }
]

const EXCHANGE = 'cluster-exchange'

const run = promisify(execFile)
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const silentLogger = { info () {}, warn () {}, error () {}, debug () {} }

const waitFor = async (predicate, timeoutMs = 40000, label = 'condition') => {
  const start = Date.now()

  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return

    await sleep(200)
  }

  throw new Error(`Timeout waiting for: ${label}`)
}

const stopNode = (container) => run('docker', ['stop', '-t', '2', container])
const startNode = async (container) => {
  await run('docker', ['start', container])

  // Wait until the node has rejoined and is serving, or the next test starts
  // against a broker that is still booting.
  await waitFor(async () => {
    try {
      const { stdout } = await run('docker', ['exec', container, 'rabbitmq-diagnostics', '-q', 'ping'])

      return stdout.includes('Ping succeeded')
    } catch {
      return false
    }
  }, 60000, `${container} back up`)
}

const createClient = (options = {}) => new RabbitMQ({
  username: USERNAME,
  password: PASSWORD,
  endpoints: NODES.map(node => node.endpoint),
  connectionName: 'cluster-test',
  exchange: { name: EXCHANGE, type: 'direct' },
  channelPoolSize: 2,
  reconnectInterval: 500,
  maxReconnectInterval: 2000,
  logger: silentLogger,
  ...options
})

describe('RabbitMQ cluster integration', { skip: !RUN_CLUSTER && 'set RABBITMQ_CLUSTER_INTEGRATION=1 (needs docker-compose.cluster.yml)' }, () => {
  test('a node going down rotates to a survivor and rebuilds pool and consumers there', async (t) => {
    // Endpoint rotation is a documented feature that had only ever run against
    // fakes. This is the sacred test at cluster scale: the broker the client
    // is attached to is killed outright, and the client has to come back on a
    // different node with its consumers actually draining again.
    const queue = 'cluster-failover-queue'
    const routingKey = 'cluster-failover'
    const client = createClient()

    let victim = null

    t.after(async () => {
      await client.disconnect().catch(() => {})

      if (victim) await startNode(victim)
    })

    await client.connect()

    const channel = await client.getChannel()

    await channel.deleteQueue(queue).catch(() => {})
    await client.setupDeadLetterExchange()
    await client.createQueue(queue, { arguments: { 'x-queue-type': 'quorum' } })
    await channel.bindQueue(queue, EXCHANGE, routingKey)

    const received = []

    await client.subscribe(queue, async (content) => {
      received.push(content)
    })

    await client.publish(routingKey, { before: true })
    await waitFor(() => received.length === 1, 20000, 'message before the failover')

    // Whichever node the client actually landed on is the one to kill.
    const attached = client.getClusterStatus().connectedTo
    const node = NODES.find(candidate => candidate.endpoint === attached)

    assert.ok(node, `the client reported an endpoint outside the cluster: ${attached}`)

    victim = node.container

    const reconnected = new Promise(resolve => client.once('reconnected', resolve))

    await stopNode(victim)
    await reconnected

    assert.notEqual(client.getClusterStatus().connectedTo, attached, 'it must have rotated to another node')

    // The real proof is not the state field: it is a message flowing again.
    await waitFor(async () => {
      try {
        await client.publish(routingKey, { after: true })

        return true
      } catch {
        return false
      }
    }, 30000, 'publishing works on the surviving node')

    await waitFor(
      () => received.some(message => message.after === true),
      30000,
      'the recreated consumer drains on the surviving node'
    )
  })

  test('the retry budget survives a quorum leader failover', async (t) => {
    // { attempts: N } trusts x-delivery-count, a broker-side counter. On one
    // node we proved the header's shape; only a cluster can answer whether
    // the count survives the leader moving to another node — if it reset, a
    // failover mid-retry would silently restart every message's budget.
    const queue = 'cluster-budget-queue'
    const routingKey = 'cluster-budget'
    const client = createClient()

    let victim = null

    t.after(async () => {
      await client.disconnect().catch(() => {})

      if (victim) await startNode(victim)
    })

    await client.connect()

    const channel = await client.getChannel()

    await channel.deleteQueue(queue).catch(() => {})
    await channel.deleteQueue(`${queue}_dlq`).catch(() => {})
    await client.setupDeadLetterExchange()
    await client.createQueue(queue, { arguments: { 'x-queue-type': 'quorum' } })
    await channel.bindQueue(queue, EXCHANGE, routingKey)

    const counts = []
    let openGate = null

    // Holds the first delivery so the failover lands mid-retry; every later
    // delivery fails immediately.
    const gate = new Promise(resolve => { openGate = resolve })

    await client.subscribe(queue, async (content, message) => {
      counts.push(message.properties.headers?.['x-delivery-count'] ?? 0)

      if (counts.length === 1) await gate

      throw new Error('always fails')
    }, { retryPolicy: { attempts: 4 } })

    await client.publish(routingKey, { n: 1 })
    await waitFor(() => counts.length === 1, 20000, 'first delivery')

    const attached = client.getClusterStatus().connectedTo

    victim = NODES.find(candidate => candidate.endpoint === attached).container

    const reconnected = new Promise(resolve => client.once('reconnected', resolve))

    openGate()
    await stopNode(victim)
    await reconnected

    // The budget is four deliveries TOTAL. If the counter reset on failover
    // the message would be redelivered forever; if it is preserved, the
    // sequence keeps climbing and stops at four.
    await waitFor(() => counts.length >= 4, 40000, 'the budget is spent')
    await sleep(3000)

    assert.equal(counts.length, 4, 'exactly four deliveries across the failover, not a restarted budget')
    assert.deepEqual(
      counts.slice(1),
      [1, 2, 3],
      'x-delivery-count kept climbing after the leader moved — it is not per-node state'
    )
  })

  test('a publish confirm survives the node it was issued against going away', async (t) => {
    // Confirms are per-channel and the channel dies with its node. The client
    // must surface that as a failed publish (retryable) rather than resolving
    // a message the broker never persisted.
    const queue = 'cluster-confirm-queue'
    const routingKey = 'cluster-confirm'
    const client = createClient()

    let victim = null

    t.after(async () => {
      await client.disconnect().catch(() => {})

      if (victim) await startNode(victim)
    })

    await client.connect()

    const channel = await client.getChannel()

    await channel.deleteQueue(queue).catch(() => {})
    await client.setupDeadLetterExchange()
    await client.createQueue(queue, { arguments: { 'x-queue-type': 'quorum' } })
    await channel.bindQueue(queue, EXCHANGE, routingKey)

    victim = NODES.find(node => node.endpoint === client.getClusterStatus().connectedTo).container

    const reconnected = new Promise(resolve => client.once('reconnected', resolve))

    await stopNode(victim)
    await reconnected

    // Every message published after recovery must be durably accepted by the
    // surviving nodes — a resolved confirm has to mean something.
    for (let n = 0; n < 5; n++) {
      await client.publish(routingKey, { n })
    }

    const drained = []

    await client.subscribe(queue, async (content) => {
      drained.push(content)
    })

    await waitFor(() => drained.length === 5, 30000, 'every confirmed message is really in the queue')
  })
})
