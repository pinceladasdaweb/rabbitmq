import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

// RPC requester over direct reply-to (amq.rabbitmq.reply-to):
//
//   request(routingKey, message, options) publishes the request with an
//   automatic correlationId and resolves with whatever the responder's
//   handler returned. No reply queues to declare, no correlation bookkeeping.
//
//   Every request has an escape route — it settles on reply, on timeout
//   (error.code = 'RPC_TIMEOUT'), on an unroutable routing key
//   (error.code = 'RPC_UNROUTABLE') or on connection loss
//   (error.code = 'RPC_CONNECTION_LOST'). It never hangs.
//
// Run the responder first:  node 'examples/23 - request-response/responder.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-rpc-requester',
    exchange: {
      name: 'rpc-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const ROUTING_KEY = 'rpc.users.get'

  async function runScenarios () {
    // 1. Happy path: the responder replies with the user.
    console.log('\n🚀 Requesting user 1...')
    const user = await rabbitMQ.request(ROUTING_KEY, { id: 1 }, { timeout: 5000 })
    console.log(`   ✅ Response: ${user.name} (${user.role})`)

    // 2. Concurrent requests: each one gets its own correlated reply.
    console.log('\n🚀 Requesting users 2 and 3 concurrently...')
    const [second, third] = await Promise.all([
      rabbitMQ.request(ROUTING_KEY, { id: 2 }, { timeout: 5000 }),
      rabbitMQ.request(ROUTING_KEY, { id: 3 }, { timeout: 5000 })
    ])
    console.log(`   ✅ Responses: ${second.name} and ${third.name}`)

    // 3. Responder error: the handler throws and, because the responder uses
    //    { replyOnError: true }, the error comes back as RPC_RESPONDER_ERROR.
    console.log('\n🚀 Requesting a user that does not exist (id 99)...')
    try {
      await rabbitMQ.request(ROUTING_KEY, { id: 99 }, { timeout: 5000 })
    } catch (error) {
      console.log(`   💥 Rejected as expected: [${error.code}] ${error.message}`)
    }

    // 4. Unroutable: no queue is bound to this routing key. Requests are
    //    published with mandatory, so the broker returns it and the request
    //    fails fast with RPC_UNROUTABLE — no timeout burned on a typo.
    console.log('\n🚀 Requesting a route with nothing bound to it...')
    try {
      await rabbitMQ.request('rpc.nobody.home', { ping: true }, { timeout: 5000 })
    } catch (error) {
      console.log(`   🚫 Rejected as expected: [${error.code}] ${error.message}`)
    }

    // 5. Timeout: the queue exists (routable) but nobody consumes it, so the
    //    request expires with RPC_TIMEOUT after 2 seconds instead of hanging.
    console.log('\n🚀 Requesting a bound route nobody answers (2s timeout)...')

    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue('rpc-limbo', { durable: false, autoDelete: true })
    await channel.bindQueue('rpc-limbo', rabbitConfig.exchange.name, 'rpc.limbo')

    try {
      await rabbitMQ.request('rpc.limbo', { ping: true }, { timeout: 2000 })
    } catch (error) {
      console.log(`   ⏰ Rejected as expected: [${error.code}] ${error.message}`)
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')

    try {
      await rabbitMQ.disconnect()
      console.log('✅ Connection closed successfully')

      setTimeout(() => {
        process.exit(0)
      }, 100)
    } catch (error) {
      console.error('❌ Error during shutdown:', error.message)
      process.exit(1)
    }
  }

  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)

  try {
    console.log('📡 Connecting to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully')

    await runScenarios()

    console.log('\n✅ All scenarios complete!')

    await shutdown()
  } catch (error) {
    console.error('❌ Error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
