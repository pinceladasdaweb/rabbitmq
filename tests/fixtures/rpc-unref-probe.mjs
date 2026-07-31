// Probe for the unref'd RPC timeout timer, run as a child process.
//
// It issues one request() with a 60s timeout and then does nothing else. If the
// pending-request timer were ref'd, this process would stay alive for the whole
// timeout; because it is unref'd, the event loop drains and the process exits
// immediately. The parent test asserts the exit, which is the only way to
// observe the property.
import { EventEmitter } from 'node:events'
import Rpc from '../../src/messaging/rpc.js'
import Publisher from '../../src/messaging/publisher.js'
import MessageCodec from '../../src/messaging/message-codec.js'
import CircuitBreaker from '../../src/resilience/circuit-breaker.js'

const silentLogger = { info () {}, warn () {}, error () {}, debug () {} }

class Channel extends EventEmitter {
  async consume () { return { consumerTag: 'tag-1' } }
  publish (exchange, routingKey, content, options, confirmCallback) {
    if (confirmCallback) setImmediate(() => confirmCallback(null))

    return true
  }
}

const channel = new Channel()
const context = {
  logger: silentLogger,
  codec: new MessageCodec({ logger: silentLogger }),
  circuitBreaker: new CircuitBreaker(),
  maxPriority: 10,
  getExchange: () => ({ name: 'probe-exchange', type: 'direct' }),
  getChannel: async () => channel,
  getChannelPool: () => ({ getDedicatedChannel: async () => channel })
}

const rpc = new Rpc(context, { publisher: new Publisher(context), consumers: null })

// A reply never arrives. The promise is deliberately never settled; the point
// is that its timer must not hold the process open.
rpc.request('probe.route', { ping: true }, { timeout: 60000 }).catch(() => {})

process.on('exit', () => {
  process.stdout.write('EXITED_WITHOUT_WAITING_FOR_TIMEOUT')
})
