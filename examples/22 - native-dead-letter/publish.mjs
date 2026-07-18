import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

// Demonstrates the lib's native support for Dead Letter Queues:
//
//   - setupDeadLetterExchange(): creates the configured DLX (default: 'dlx')
//   - createQueue(): creates the main queue ALREADY set up for dead-lettering
//     plus the matching '<queue>_dlq' bound to the DLX
//
// The consumer (consumer.mjs) completes the demonstration with
// moveToDeadLetter() and processDeadLetterQueue().

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-native-dlq-producer',
    exchange: {
      name: 'native-dlq-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const QUEUE_NAME = 'orders'

  // IMPORTANT: moveToDeadLetter() derives the DLQ from the message's ROUTING
  // KEY (`<routingKey>_dlq`). We use a routing key equal to the queue name so
  // the destination is exactly the DLQ created by createQueue().
  const ROUTING_KEY = QUEUE_NAME

  const stats = { published: 0 }

  async function setupInfrastructure () {
    console.log('\n🔧 Setting up infrastructure via the native API...')

    // Creates the dead letter exchange ('dlx' by default)
    await rabbitMQ.setupDeadLetterExchange()
    console.log('   ✓ Dead letter exchange created')

    // Creates the main queue with dead-lettering + the 'orders_dlq' queue
    await rabbitMQ.createQueue(QUEUE_NAME)
    console.log(`   ✓ Queues '${QUEUE_NAME}' and '${QUEUE_NAME}_dlq' created`)

    const channel = await rabbitMQ.getChannel()
    await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, ROUTING_KEY)
    console.log('   ✓ Queue bound to the exchange')

    console.log('✅ Infrastructure set up successfully')
  }

  async function publishScenarios () {
    const messages = [
      { id: 1, item: 'Notebook', status: 'ok' },
      { id: 2, item: 'Mouse', status: 'ok' },
      { id: 3, item: 'Keyboard', status: 'ok', shouldFail: true },
      { id: 4, item: 'Monitor', status: 'ok' },
      { id: 5, item: 'Webcam', status: 'ok', shouldFail: true },
      { id: 6, item: 'Suspicious headset', status: 'ok', quarantine: true }
    ]

    console.log(`\n🚀 Publishing ${messages.length} orders...`)
    console.log('   - shouldFail: the consumer will reject it → automatic DLQ via nack')
    console.log('   - quarantine: the consumer uses moveToDeadLetter() → manual DLQ\n')

    for (const message of messages) {
      await rabbitMQ.publish(ROUTING_KEY, message, {
        persistent: true,
        messageId: `order-${message.id}`
      })

      stats.published++

      const flag = message.shouldFail ? ' (will fail)' : message.quarantine ? ' (quarantine)' : ''
      console.log(`   ✓ Order ${message.id}: ${message.item}${flag}`)
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log(`\n📊 Total orders published: ${stats.published}`)

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

    await setupInfrastructure()
    await publishScenarios()

    console.log('\n✅ Publishing complete!')

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
