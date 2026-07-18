import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-priority-consumer',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  const QUEUE_NAME = 'priority-queue'
  let shutdownInProgress = false
  let messagesProcessed = 0
  let lastMessageTime = null
  const messagesByPriority = {
    high: 0, // Priority 10
    medium: 0, // Priority 5
    low: 0 // Priority 1
  }

  function getPriorityLevel (priority) {
    if (priority >= 10) return 'high'
    if (priority >= 5) return 'medium'
    return 'low'
  }

  function showPriorityStats () {
    console.log('\n📊 Statistics by priority:')
    console.log(`   High (10): ${messagesByPriority.high} messages`)
    console.log(`   Medium (5): ${messagesByPriority.medium} messages`)
    console.log(`   Low (1): ${messagesByPriority.low} messages`)
    console.log(`   Total: ${messagesProcessed} messages`)
  }

  async function handleMessage (content, message) {
    try {
      const priority = message.properties.priority || 0
      const priorityLevel = getPriorityLevel(priority)
      lastMessageTime = Date.now()
      messagesProcessed++
      messagesByPriority[priorityLevel]++

      console.log('\n📥 Processing message:')
      console.log(`   ID: ${content.id}`)
      console.log(`   Type: ${content.type}`)
      console.log(`   Text: ${content.text}`)
      console.log(`   Priority: ${priority}`)
      console.log(`   Level: ${priorityLevel.toUpperCase()}`)

      // Simulates processing
      const processingTime = 500
      await new Promise(resolve => setTimeout(resolve, processingTime))

      console.log('\n✅ Message processed:')
      console.log(`   Processing time: ${processingTime}ms`)

      // Shows statistics every 2 messages
      if (messagesProcessed % 2 === 0) {
        showPriorityStats()
      }

      return true
    } catch (error) {
      console.error('❌ Error processing message:', error.message)
      return false
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')

    if (messagesProcessed > 0) {
      console.log('\n📊 Final statistics:')
      console.log(`   Total messages processed: ${messagesProcessed}`)
      showPriorityStats()
    }

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

    rabbitMQ.on('disconnected', () => {
      if (!shutdownInProgress) {
        console.log('\n❌ Disconnected from RabbitMQ')
      }
    })

    rabbitMQ.on('reconnected', () => {
      if (!shutdownInProgress) {
        console.log('\n🔄 Reconnected to RabbitMQ')
        showPriorityStats()
      }
    })

    console.log(`\n🔄 Setting up consumer for queue: ${QUEUE_NAME}`)

    await rabbitMQ.subscribe(QUEUE_NAME, handleMessage, {
      prefetchCount: 1, // Processes one message at a time to demonstrate priority
      noAck: false,
      consumerTag: 'consumer-priority-' + Date.now()
    })

    console.log('✅ Consumer set up successfully')
    console.log('\n⚡ Starting priority message consumption')
    console.log('   RabbitMQ will deliver messages in the following order:')
    console.log('   1. High priority (10)')
    console.log('   2. Medium priority (5)')
    console.log('   3. Low priority (1)')
    console.log('   Consumer will remain active waiting for messages')
    console.log('   Press Ctrl+C to stop\n')

    // Periodic status every minute
    const statusInterval = setInterval(() => {
      if (!shutdownInProgress && messagesProcessed > 0) {
        const idleTime = (Date.now() - lastMessageTime) / 1000
        console.log(`\n⏱️  Status: ${idleTime.toFixed(1)}s without new messages`)
        showPriorityStats()
      }
    }, 60000)

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
