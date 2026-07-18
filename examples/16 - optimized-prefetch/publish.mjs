import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-optimized-prefetch',
    exchange: {
      name: 'optimized-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let messagesPublished = 0
  const QUEUE_NAME = 'optimized-queue'
  const TOTAL_MESSAGES = 100

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()
      console.log('\n🔧 Setting up infrastructure...')

      // Creates the queue
      console.log(`   Creating queue: ${QUEUE_NAME}`)
      await channel.assertQueue(QUEUE_NAME, {
        durable: true
      })

      // Binds the queue to the exchange
      console.log('   Binding queue to the exchange...')
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'optimized-route')

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function publishMessages () {
    try {
      console.log('\n📨 Starting message publishing...')

      for (let i = 1; i <= TOTAL_MESSAGES; i++) {
        const processingTime = Math.random() * 1000 // Random processing time between 0 and 1000ms
        const priority = Math.random() < 0.3 ? 'high' : 'normal' // 30% of the messages are high priority

        const message = {
          id: i,
          content: `Message ${i} of ${TOTAL_MESSAGES}`,
          expectedProcessingTime: processingTime,
          priority,
          timestamp: Date.now()
        }

        await rabbitMQ.publish('optimized-route', message)
        messagesPublished++

        if (messagesPublished % 10 === 0) {
          console.log(`   ✓ Published ${messagesPublished} messages`)
        }

        // Small delay between publishes to avoid overloading
        await new Promise(resolve => setTimeout(resolve, 100))
      }

      console.log('\n✅ All messages were published successfully!')
    } catch (error) {
      console.error('❌ Error publishing messages:', error.message)
      throw error
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Publishing statistics:')
    console.log(`   Total messages published: ${messagesPublished}`)

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
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully')

    await setupInfrastructure()

    console.log('\n🚀 Starting to publish messages...')
    await publishMessages()

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
