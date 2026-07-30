import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

// RPC responder over direct reply-to (amq.rabbitmq.reply-to):
//
//   respond(queueName, handler, options) subscribes to the queue and, for
//   each request, publishes the handler's return value back to the private
//   reply route of whichever requester sent it — correlation is automatic.
//
//   With { replyOnError: true }, a handler crash is sent back to the
//   requester as a structured error (rejects with code RPC_RESPONDER_ERROR)
//   instead of dead-lettering the request.

const USERS = {
  1: { id: 1, name: 'Ada Lovelace', role: 'admin' },
  2: { id: 2, name: 'Alan Turing', role: 'analyst' },
  3: { id: 3, name: 'Grace Hopper', role: 'developer' }
}

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-rpc-responder',
    exchange: {
      name: 'rpc-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const QUEUE_NAME = 'rpc-users'
  const ROUTING_KEY = 'rpc.users.get'

  const stats = { answered: 0, failed: 0 }

  async function setupInfrastructure () {
    console.log('\n🔧 Ensuring infrastructure...')

    const channel = await rabbitMQ.getChannel()

    await channel.assertQueue(QUEUE_NAME, { durable: true })
    await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, ROUTING_KEY)

    console.log('✅ Infrastructure ready')
  }

  async function startResponder () {
    console.log(`\n👂 Answering RPC requests on '${QUEUE_NAME}'...`)

    await rabbitMQ.respond(QUEUE_NAME, async (content) => {
      console.log(`\n📨 Request received: user id ${content.id}`)

      const user = USERS[content.id]

      if (!user) {
        stats.failed++
        console.log('   💥 Unknown user → error goes back to the requester')

        throw new Error(`User ${content.id} not found`)
      }

      stats.answered++
      console.log(`   ✅ Replying with ${user.name}`)

      return user
    }, { replyOnError: true })
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Stats:')
    console.log(`   Requests answered: ${stats.answered}`)
    console.log(`   Requests failed (error reply): ${stats.failed}`)

    try {
      await rabbitMQ.disconnect()
      console.log('\n✅ Connection closed successfully')

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

    await setupInfrastructure()
    await startResponder()

    console.log('\nℹ️  Waiting for requests... Press CTRL+C to exit')
  } catch (error) {
    console.error('❌ Error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
