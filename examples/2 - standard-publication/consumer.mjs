import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitMQ = new RabbitMQ({
    ...baseConfig,
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    prefetchCount: 1
  })

  let shutdownInProgress = false
  let messagesProcessed = 0

  async function handleMessage (content, message) {
    try {
      console.log('\n📥 Processing message:')
      console.log('   ID:', content.id)
      console.log('   Text:', content.text)
      console.log('   Timestamp:', new Date(content.timestamp).toISOString())
      console.log('   Consumer Tag:', message.fields.consumerTag)
      console.log('   Delivery Tag:', message.fields.deliveryTag)

      await new Promise(resolve => setTimeout(resolve, 500))
      messagesProcessed++
      console.log('✅ Message processed successfully')

      return true
    } catch (error) {
      console.error('❌ Error processing message:', error.message)
      return false
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n🛑 Starting graceful shutdown...')
    console.log(`📊 Total messages processed: ${messagesProcessed}`)

    try {
      // 1. Short pause to finish in-flight processing
      await new Promise(resolve => setTimeout(resolve, 500))

      // 2. Performs the disconnection
      console.log('👋 Disconnecting from RabbitMQ...')
      await rabbitMQ.disconnect()
      console.log('✅ Disconnected successfully')

      // 3. Short pause before exiting
      await new Promise(resolve => setTimeout(resolve, 100))
    } catch (error) {
      // Ignores errors during shutdown
      console.log('✅ Disconnected successfully')
    } finally {
      process.exit(0)
    }
  }

  // Handlers for graceful shutdown
  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)

  try {
    console.log('🔄 Connecting to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connected to RabbitMQ')

    rabbitMQ.on('disconnected', () => {
      if (!shutdownInProgress) {
        console.log('❌ Disconnected from RabbitMQ')
      }
    })

    rabbitMQ.on('reconnected', () => {
      if (!shutdownInProgress) {
        console.log('🔄 Reconnected to RabbitMQ')
      }
    })

    await new Promise(resolve => setTimeout(resolve, 1000))

    const queueName = 'example-queue'
    console.log(`\n🔄 Setting up consumer for queue: ${queueName}`)

    await rabbitMQ.subscribe(queueName, handleMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'example-consumer-' + Date.now()
    })

    console.log('✅ Consumer set up successfully')
    console.log('\n👂 Waiting for messages...')
    console.log('   Press Ctrl+C to stop the consumer\n')
  } catch (error) {
    console.error('❌ Fatal error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
