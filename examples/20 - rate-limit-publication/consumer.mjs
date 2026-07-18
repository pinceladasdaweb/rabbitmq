import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-rate-limit-consumption',
    exchange: {
      name: 'rate-limit-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let messagesProcessed = 0
  const QUEUE_NAME = 'rate-limit-queue'
  let processStartTime = null
  const messagesByType = {
    normal: 0,
    burst: 0,
    afterLimit: 0
  }

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()

      console.log('\n🔧 Setting up infrastructure...')

      console.log(`   Creating queue: ${QUEUE_NAME}`)
      await channel.assertQueue(QUEUE_NAME, {
        durable: true
      })

      console.log('   Binding queue to exchange...')
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'rate-limit-route')

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function handleMessage (content, message) {
    try {
      if (!processStartTime) {
        processStartTime = Date.now()
        console.log('\n🚀 Starting message processing...\n')
      }

      messagesProcessed++

      console.log('\n📥 Message received:')
      console.log(`   ID: ${content.id}`)
      console.log(`   Text: ${content.text}`)
      console.log(`   Type: ${content.type}`)

      // Publication info
      if (content.publishInfo) {
        console.log('\nℹ️ Publication info:')
        console.log(`   Attempt number: ${content.publishInfo.attempt}`)
        console.log(`   Timestamp: ${new Date(content.publishInfo.timestamp).toISOString()}`)
      }

      // Updates counters per type
      messagesByType[content.type] = (messagesByType[content.type] || 0) + 1

      // Simulates processing
      await new Promise(resolve => setTimeout(resolve, 500))

      console.log('\n✅ Message processed successfully!')
      console.log('📊 Progress:')
      console.log(`   Total processed: ${messagesProcessed}`)
      console.log(`   Normal: ${messagesByType.normal}`)
      console.log(`   Burst: ${messagesByType.burst}`)
      console.log(`   Post-limit: ${messagesByType.afterLimit}`)

      return true
    } catch (error) {
      console.error('❌ Error processing message:', error.message)
      return false
    }
  }

  function showStatus () {
    const runningTime = processStartTime ? (Date.now() - processStartTime) / 1000 : 0

    console.log('\n📊 Current status:')
    console.log(`   Messages processed: ${messagesProcessed}`)
    console.log(`   Normal messages: ${messagesByType.normal}`)
    console.log(`   Burst messages: ${messagesByType.burst}`)
    console.log(`   Post-limit messages: ${messagesByType.afterLimit}`)
    console.log(`   Running time: ${runningTime.toFixed(1)}s`)
    if (messagesProcessed > 0) {
      console.log(`   Average: ${(messagesProcessed / runningTime).toFixed(2)} msgs/s`)
    }
    console.log('   Consumer remains active waiting for messages...\n')
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')

    if (messagesProcessed > 0) {
      const runningTime = (Date.now() - processStartTime) / 1000
      console.log('\n📊 Final statistics:')
      console.log(`   Total messages processed: ${messagesProcessed}`)
      console.log(`   Normal messages: ${messagesByType.normal}`)
      console.log(`   Burst messages: ${messagesByType.burst}`)
      console.log(`   Post-limit messages: ${messagesByType.afterLimit}`)
      console.log(`   Total running time: ${runningTime.toFixed(1)}s`)
      console.log(`   Processing average: ${(messagesProcessed / runningTime).toFixed(2)} msgs/s`)
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

    await setupInfrastructure()

    console.log(`\n🔄 Setting up consumer for queue: ${QUEUE_NAME}`)

    await rabbitMQ.subscribe(QUEUE_NAME, handleMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'consumer-rate-limit-' + Date.now()
    })

    console.log('✅ Consumer set up successfully')
    console.log('\n⚡ Starting message consumption')
    console.log('   Consumer will show detailed info for every message')
    console.log('   Status is refreshed every minute')
    console.log('   Press Ctrl+C to exit\n')

    // Periodic status
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
