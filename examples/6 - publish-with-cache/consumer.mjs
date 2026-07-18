import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-cache-consumer',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let messagesProcessed = 0
  let lastMessageTime = null
  const IDLE_NOTIFICATION_TIMEOUT = 5000 // 5 seconds without messages = idle notification

  async function handleMessage (content, message) {
    try {
      if (messagesProcessed === 0) {
        console.log('\n🚀 Starting message processing...\n')
      }

      lastMessageTime = Date.now()
      messagesProcessed++

      console.log('\n📥 Processing message:')
      console.log('   ID:', content.id)
      console.log('   Text:', content.text)
      console.log('   Timestamp:', new Date(content.timestamp).toISOString())

      // Cache-specific information
      console.log('\n📦 Complex data:')
      console.log('   Data size:', content.data.complexData.length)
      console.log('   Processed at:', new Date(content.data.processedAt).toISOString())
      console.log('   Cached:', content.data.cached)

      // RabbitMQ information
      console.log('\n🐰 RabbitMQ metadata:')
      console.log('   Consumer Tag:', message.fields.consumerTag)
      console.log('   Delivery Tag:', message.fields.deliveryTag)
      console.log('   Exchange:', message.fields.exchange)
      console.log('   Routing Key:', message.fields.routingKey)

      // Simulates some processing
      await new Promise(resolve => setTimeout(resolve, 500))

      console.log('\n✅ Message processed successfully!')

      // Checks for idleness
      const now = Date.now()
      if (now - lastMessageTime > IDLE_NOTIFICATION_TIMEOUT) {
        console.log('\n⌛ Consumer idle...')
        console.log(`   Last message received ${((now - lastMessageTime) / 1000).toFixed(1)}s ago`)
        console.log(`   Total messages processed: ${messagesProcessed}`)
      }

      return true
    } catch (error) {
      console.error('❌ Error processing message:', error.message)
      return false
    }
  }

  function showStatus () {
    console.log('\n📊 Current status:')
    console.log(`   Messages processed: ${messagesProcessed}`)
    if (lastMessageTime) {
      const idleTime = (Date.now() - lastMessageTime) / 1000
      console.log(`   Idle time: ${idleTime.toFixed(1)}s`)
    }
    console.log('   Consumer remains active and waiting for messages...\n')
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log(`\n📊 Total messages processed: ${messagesProcessed}`)

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
    // 1. Connection
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully')

    // 2. Event Listeners
    rabbitMQ.on('disconnected', () => {
      if (!shutdownInProgress) {
        console.log('\n❌ Disconnected from RabbitMQ')
      }
    })

    rabbitMQ.on('reconnected', () => {
      if (!shutdownInProgress) {
        console.log('\n🔄 Reconnected to RabbitMQ')
        showStatus()
      }
    })

    // 3. Consumer setup
    const queueName = 'example-queue'
    console.log(`\n🔄 Setting up consumer for queue: ${queueName}`)

    await rabbitMQ.subscribe(queueName, handleMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'consumer-cache-' + Date.now()
    })

    console.log('✅ Consumer set up successfully')
    console.log('\n⚡ Starting message consumption')
    console.log('   Consumer will remain active waiting for messages')
    console.log('   Cache details will be displayed for each message')
    console.log('   Status will be updated every minute')
    console.log('   Press Ctrl+C to stop\n')

    // 4. Periodic status
    const statusInterval = setInterval(() => {
      if (!shutdownInProgress && messagesProcessed > 0) {
        showStatus()
      }
    }, 60000)

    // Cleanup on exit
    process.on('exit', () => {
      clearInterval(statusInterval)
    })
  } catch (error) {
    console.error('❌ Fatal error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
