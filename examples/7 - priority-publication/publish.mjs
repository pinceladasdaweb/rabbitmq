import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-priority-publication',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    maxPriority: 10, // Sets the maximum number of priority levels
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const QUEUE_NAME = 'priority-queue' // New dedicated queue for priority messages

  async function publishWithPriority (message, priority) {
    try {
      console.log(`\n📨 Publishing message with priority ${priority}...`)
      await rabbitMQ.publish('priority-route', message, {
        persistent: true,
        priority,
        messageId: new Date().getTime().toString(),
        timestamp: new Date().getTime(),
        contentType: 'application/json'
      })
      console.log('✅ Message published successfully!')
      return true
    } catch (error) {
      console.error('❌ Error publishing message:', error.message)
      return false
    }
  }

  async function setupQueue () {
    try {
      const channel = await rabbitMQ.getChannel()

      console.log(`\n🔄 Setting up queue ${QUEUE_NAME}...`)
      console.log('   Configuring with priority support (maxPriority: 10)')

      // Creates the queue with priority support
      await channel.assertQueue(QUEUE_NAME, {
        durable: true,
        maxPriority: 10 // Must match the maxPriority from the configuration
      })

      // Binds the queue to the exchange using a specific routing key
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'priority-route')

      console.log('✅ Queue set up successfully')
    } catch (error) {
      console.error('❌ Error setting up queue:', error.message)
      throw error
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
    // 1. Establishes the connection
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully')

    // 2. Sets up the queue with priority support
    await setupQueue()

    // 3. Prepares messages with different priorities
    console.log('\n📝 Preparing messages with different priorities...')

    // Array of messages with different priorities
    const messages = [
      { priority: 1, data: { id: 1, type: 'LOW', text: 'Low priority message' } },
      { priority: 5, data: { id: 2, type: 'MEDIUM', text: 'Medium priority message' } },
      { priority: 10, data: { id: 3, type: 'HIGH', text: 'High priority message' } },
      { priority: 1, data: { id: 4, type: 'LOW', text: 'Another low priority message' } },
      { priority: 10, data: { id: 5, type: 'HIGH', text: 'Another high priority message' } },
      { priority: 5, data: { id: 6, type: 'MEDIUM', text: 'Another medium priority message' } }
    ]

    console.log('\n⚡ Starting publication with priorities...')
    console.log(`   Queue: ${QUEUE_NAME}`)
    console.log('   Messages will be published in random order')
    console.log('   But will be consumed according to their priorities\n')

    // Shuffles the messages to demonstrate that publication order does not matter
    const shuffledMessages = messages.sort(() => Math.random() - 0.5)

    // Publishes the messages
    for (const msg of shuffledMessages) {
      console.log(`\n📦 Message: ${msg.data.id}`)
      console.log(`   Type: ${msg.data.type}`)
      console.log(`   Priority: ${msg.priority}`)

      await publishWithPriority(msg.data, msg.priority)

      // Small delay between publications for better visualization
      await new Promise(resolve => setTimeout(resolve, 1000))
    }

    console.log('\n✨ Priority publication completed!')
    console.log(`   All messages were published to queue: ${QUEUE_NAME}`)
    console.log('   Messages will be consumed in the following order:')
    console.log('   1. High priority (10)')
    console.log('   2. Medium priority (5)')
    console.log('   3. Low priority (1)')

    // Waits a moment before shutting down
    await new Promise(resolve => setTimeout(resolve, 2000))
    await shutdown()
  } catch (error) {
    console.error('❌ Error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Fatal error:', error)
  process.exit(1)
})
