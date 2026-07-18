import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-high-volume-consumption',
    exchange: {
      name: 'example-exchange',
      type: 'direct',
      options: {
        durable: true
      }
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let messagesProcessed = 0
  let startTime = null
  let lastStatusUpdate = 0
  let lastMessageTime = null
  const STATUS_INTERVAL = 100 // 100ms
  const IDLE_NOTIFICATION_TIMEOUT = 5000 // 5 seconds without messages = idle notification

  function updateStatus (force = false) {
    const now = Date.now()
    if (force || now - lastStatusUpdate >= STATUS_INTERVAL) {
      const elapsedSeconds = (now - startTime) / 1000
      const throughput = messagesProcessed / elapsedSeconds

      // Moves the cursor to the beginning of the line and clears it
      // TTY-only APIs: guard them so the example also works with piped output
      if (process.stdout.isTTY) {
        process.stdout.clearLine()
        process.stdout.cursorTo(0)
      }

      // If idle for more than 5 seconds, shows waiting status
      if (lastMessageTime && (now - lastMessageTime > IDLE_NOTIFICATION_TIMEOUT)) {
        process.stdout.write(
          `⌛ Waiting for messages... | Processed so far: ${messagesProcessed} | ` +
          `Last message ${((now - lastMessageTime) / 1000).toFixed(1)}s ago`
        )
      } else {
        // Normal processing status
        process.stdout.write(
          `⚡ Processed: ${messagesProcessed.toString().padStart(6)} | ` +
          `Time: ${elapsedSeconds.toFixed(1)}s | ` +
          `Throughput: ${throughput.toFixed(2)} msgs/s`
        )
      }

      lastStatusUpdate = now
    }
  }

  async function handleMessage (content, message) {
    try {
      if (!startTime) {
        startTime = Date.now()
        await new Promise(resolve => setTimeout(resolve, 100))
        console.log('\n🚀 Starting message processing...\n')
      }

      messagesProcessed++
      lastMessageTime = Date.now()
      updateStatus()

      return true
    } catch (error) {
      console.error('\n❌ Error processing message:', error.message)
      return false
    }
  }

  function showStats () {
    const now = Date.now()
    const totalSeconds = (now - startTime) / 1000
    const throughput = messagesProcessed / totalSeconds

    console.log('\n📊 Current statistics:')
    console.log(`   Total messages processed: ${messagesProcessed}`)
    console.log(`   Running time: ${totalSeconds.toFixed(2)}s`)
    console.log(`   Average throughput: ${throughput.toFixed(2)} msgs/s`)
    console.log('   Consumer remains active and waiting for new messages...\n')
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n\n👋 Starting graceful shutdown...')

    // Shows final statistics
    const endTime = Date.now()
    const totalSeconds = (endTime - startTime) / 1000
    const finalThroughput = messagesProcessed / totalSeconds

    console.log('\n📊 Final statistics:')
    console.log(`   Total messages processed: ${messagesProcessed}`)
    console.log(`   Total execution time: ${totalSeconds.toFixed(2)}s`)
    console.log(`   Average throughput: ${finalThroughput.toFixed(2)} msgs/s`)

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
        showStats()
      }
    })

    const queueName = 'example-queue'
    console.log(`\n🔄 Setting up consumer for queue: ${queueName}`)

    await rabbitMQ.subscribe(queueName, handleMessage, {
      prefetchCount: 100,
      noAck: false,
      consumerTag: 'high-volume-consumer-' + Date.now()
    })

    // Initialization
    startTime = Date.now()
    lastMessageTime = Date.now()

    console.log('✅ Consumer set up successfully')
    console.log('\n⚡ Starting high-volume consumption')
    console.log('   prefetchCount: 100 messages')
    console.log('   Status will be updated constantly')
    console.log('   Consumer will remain active waiting for new messages')
    console.log('   Press Ctrl+C to exit\n')

    // Status update
    const statusInterval = setInterval(() => {
      if (!shutdownInProgress) {
        updateStatus()
      }
    }, STATUS_INTERVAL)

    // Shows statistics every minute
    const statsInterval = setInterval(() => {
      if (!shutdownInProgress) {
        showStats()
      }
    }, 60000)

    // Clears intervals on exit
    process.on('exit', () => {
      clearInterval(statusInterval)
      clearInterval(statsInterval)
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
