import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-delay-publication',
    // Exchange dedicated to delayed messages, used by publishDelayed().
    // The lib creates the x-delayed-message exchange via setupDelayExchange().
    delayExchange: 'delayed-exchange',
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const QUEUE_NAME = 'delayed-queue'

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()

      console.log('\n🔄 Checking delay plugin...')
      await rabbitMQ.setupDelayPlugin()
      console.log('✅ Delay plugin is active')

      console.log('\n🔄 Setting up exchange and queue...')

      // Creates the x-delayed-message exchange configured in delayExchange
      await rabbitMQ.setupDelayExchange({ type: 'direct' })

      // Sets up the queue
      await channel.assertQueue(QUEUE_NAME, {
        durable: true
      })

      // Binds the queue to the delay exchange
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.delayExchange, 'delay-route')

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function publishWithDelay (message, delayMs) {
    try {
      const delaySeconds = delayMs / 1000
      console.log(`\n📨 Publishing message with a ${delaySeconds}s delay...`)
      console.log(`   Current time: ${new Date().toISOString()}`)
      console.log(`   Will be delivered at: ${new Date(Date.now() + delayMs).toISOString()}`)

      // publishDelayed handles the x-delay header and publishes to the delayExchange
      await rabbitMQ.publishDelayed('delay-route', message, delayMs, {
        persistent: true,
        messageId: new Date().getTime().toString(),
        timestamp: new Date().getTime()
      })

      console.log('✅ Message published successfully!')
      return true
    } catch (error) {
      console.error('❌ Error publishing message:', error.message)
      return false
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

    // 2. Sets up the required infrastructure
    await setupInfrastructure()

    // 3. Prepares messages with different delays
    console.log('\n📝 Preparing messages with different delays...')

    const messages = [
      {
        delay: 5000, // 5 seconds
        data: {
          id: 1,
          text: 'Message with a 5 second delay',
          type: 'SHORT_DELAY'
        }
      },
      {
        delay: 10000, // 10 seconds
        data: {
          id: 2,
          text: 'Message with a 10 second delay',
          type: 'MEDIUM_DELAY'
        }
      },
      {
        delay: 15000, // 15 seconds
        data: {
          id: 3,
          text: 'Message with a 15 second delay',
          type: 'LONG_DELAY'
        }
      }
    ]

    console.log('\n⚡ Starting delayed publication...')
    console.log(`   Queue: ${QUEUE_NAME}`)
    console.log('   Messages will be delivered after their respective delays\n')

    // Publishes the messages
    for (const msg of messages) {
      console.log(`\n📦 Message: ${msg.data.id}`)
      console.log(`   Type: ${msg.data.type}`)
      console.log(`   Delay: ${msg.delay}ms`)

      await publishWithDelay(msg.data, msg.delay)

      // Small delay between publications for better visualization
      await new Promise(resolve => setTimeout(resolve, 1000))
    }

    console.log('\n✨ Delayed publication completed!')
    console.log('   Messages will be delivered at:')
    messages.forEach(msg => {
      const deliveryTime = new Date(Date.now() + msg.delay)
      console.log(`   - ID ${msg.data.id}: ${deliveryTime.toISOString()} (delay: ${msg.delay}ms)`)
    })

    // Waits a moment before shutting down
    await new Promise(resolve => setTimeout(resolve, 2000))
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
