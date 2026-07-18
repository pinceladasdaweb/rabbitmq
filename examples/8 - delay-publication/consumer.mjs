import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-delay-consumer',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  const QUEUE_NAME = 'delayed-queue'
  let shutdownInProgress = false
  let messagesProcessed = 0
  let lastMessageTime = null

  function formatTimeDifference (timestamp) {
    const diff = Date.now() - timestamp
    return `${(diff / 1000).toFixed(1)}s`
  }

  async function handleMessage (content, message) {
    try {
      lastMessageTime = Date.now()
      messagesProcessed++

      const publishTime = new Date(message.properties.timestamp)
      const delayHeader = message.properties.headers['x-delay']
      const expectedDelay = delayHeader ? delayHeader / 1000 : 0
      const actualDelay = (Date.now() - publishTime.getTime()) / 1000

      console.log('\n📥 Message received:')
      console.log(`   ID: ${content.id}`)
      console.log(`   Type: ${content.type}`)
      console.log(`   Text: ${content.text}`)
      console.log('\n⏱️  Timing information:')
      console.log(`   Published at: ${publishTime.toISOString()}`)
      console.log(`   Received at: ${new Date().toISOString()}`)
      console.log(`   Configured delay: ${expectedDelay}s`)
      console.log(`   Actual delay: ${actualDelay.toFixed(1)}s`)

      if (Math.abs(actualDelay - expectedDelay) > 1) {
        console.log(`   ⚠️  Significant difference in delay: ${Math.abs(actualDelay - expectedDelay).toFixed(1)}s`)
      }

      // Simulates processing
      await new Promise(resolve => setTimeout(resolve, 500))

      console.log('\n✅ Message processed successfully!')
      console.log(`   Total processed so far: ${messagesProcessed}`)

      return true
    } catch (error) {
      console.error('❌ Error processing message:', error.message)
      return false
    }
  }

  function showStatus () {
    if (messagesProcessed > 0) {
      console.log('\n📊 Current status:')
      console.log(`   Messages processed: ${messagesProcessed}`)
      if (lastMessageTime) {
        console.log(`   Last message: ${formatTimeDifference(lastMessageTime)} ago`)
      }
      console.log('   Consumer remains active waiting for messages...\n')
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')

    if (messagesProcessed > 0) {
      console.log('\n📊 Final statistics:')
      console.log(`   Total messages processed: ${messagesProcessed}`)
      if (lastMessageTime) {
        console.log(`   Last message processed: ${formatTimeDifference(lastMessageTime)} ago`)
      }
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
        showStatus()
      }
    })

    console.log(`\n🔄 Setting up consumer for queue: ${QUEUE_NAME}`)

    await rabbitMQ.subscribe(QUEUE_NAME, handleMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'consumer-delay-' + Date.now()
    })

    console.log('✅ Consumer set up successfully')
    console.log('\n⚡ Starting delayed message consumption')
    console.log('   Waiting for messages to arrive after their respective delays')
    console.log('   Consumer will show the exact delay time of each message')
    console.log('   Status will be updated every minute')
    console.log('   Press Ctrl+C to stop\n')

    // Periodic status every minute
    const statusInterval = setInterval(() => {
      if (!shutdownInProgress) {
        showStatus()
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
