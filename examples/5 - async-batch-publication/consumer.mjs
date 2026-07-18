import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-async-batch-consumption',
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
  let batchesProcessed = 0
  let startTime = null
  let lastStatusUpdate = 0
  let lastMessageTime = null
  let lastThroughput = 0
  const STATUS_INTERVAL = 100 // 100ms
  const IDLE_NOTIFICATION_TIMEOUT = 5000 // 5 seconds without messages = idle notification

  function updateStatus (force = false) {
    const now = Date.now()
    if (force || now - lastStatusUpdate >= STATUS_INTERVAL) {
      // Moves the cursor and clears the line
      // TTY-only APIs: guard them so the example also works with piped output
      if (process.stdout.isTTY) {
        process.stdout.clearLine()
        process.stdout.cursorTo(0)
      }

      // If idle for more than 5 seconds, shows waiting status
      if (lastMessageTime && (now - lastMessageTime > IDLE_NOTIFICATION_TIMEOUT)) {
        process.stdout.write(
          `⌛ Waiting for messages... | Last batch processed ${((now - lastMessageTime) / 1000).toFixed(1)}s ago | ` +
          `Total: ${messagesProcessed} messages in ${batchesProcessed} batches`
        )
      } else {
        const elapsedSeconds = (now - startTime) / 1000
        const currentThroughput = messagesProcessed / elapsedSeconds
        lastThroughput = currentThroughput

        process.stdout.write(
          `⚡ Processed: ${batchesProcessed} batches | ` +
          `${messagesProcessed} messages | ` +
          `Time: ${elapsedSeconds.toFixed(1)}s | ` +
          `Throughput: ${currentThroughput.toFixed(2)} msgs/s`
        )
      }

      lastStatusUpdate = now
    }
  }

  function showDetailedStats () {
    const now = Date.now()
    const elapsedSeconds = (now - startTime) / 1000
    const avgThroughput = messagesProcessed / elapsedSeconds

    console.log('\n📊 Detailed statistics:')
    console.log(`   Batches processed: ${batchesProcessed}`)
    console.log(`   Messages processed: ${messagesProcessed}`)
    console.log(`   Running time: ${elapsedSeconds.toFixed(2)}s`)
    console.log(`   Current throughput: ${lastThroughput.toFixed(2)} msgs/s`)
    console.log(`   Average throughput: ${avgThroughput.toFixed(2)} msgs/s`)
    console.log('   Consumer remains active waiting for messages...\n')
  }

  async function handleMessage (content, message) {
    try {
      if (!startTime) {
        startTime = Date.now()
        console.log('\n🚀 Starting message processing...\n')
      }

      messagesProcessed++

      // Counts batches every 1000 messages
      if (messagesProcessed % 1000 === 0) {
        batchesProcessed++
        lastMessageTime = Date.now()
      }

      // Updates status
      updateStatus()

      return true
    } catch (error) {
      console.error('\n❌ Error processing message:', error.message)
      return false
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n\n👋 Starting graceful shutdown...')

    const endTime = Date.now()
    const totalSeconds = (endTime - startTime) / 1000
    const finalThroughput = messagesProcessed / totalSeconds

    console.log('\n📊 Final statistics:')
    console.log(`   Total batches processed: ${batchesProcessed}`)
    console.log(`   Total messages processed: ${messagesProcessed}`)
    console.log(`   Total execution time: ${totalSeconds.toFixed(2)}s`)
    console.log(`   Final average throughput: ${finalThroughput.toFixed(2)} msgs/s`)

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

    rabbitMQ.on('reconnected', async () => {
      if (!shutdownInProgress) {
        console.log('\n🔄 Reconnected to RabbitMQ')
        // Shows statistics after reconnection
        showDetailedStats()
      }
    })

    await new Promise(resolve => setTimeout(resolve, 1000))

    // 3. Consumer setup
    const queueName = 'example-queue'
    console.log(`\n🔄 Setting up consumer for queue: ${queueName}`)

    await rabbitMQ.subscribe(queueName, handleMessage, {
      prefetchCount: 1000, // Increased to match the publication batch size
      noAck: false,
      consumerTag: 'async-batch-consumer-' + Date.now()
    })

    console.log('✅ Consumer set up successfully')
    console.log('\n⚡ Starting high-volume consumption (batch mode)')
    console.log('   prefetchCount: 1000 messages')
    console.log('   Status will be updated constantly')
    console.log('   Detailed statistics every minute')
    console.log('   Consumer will remain active waiting for new messages')
    console.log('   Press Ctrl+C to exit\n')

    // 4. Periodic statistics
    const statsInterval = setInterval(() => {
      if (!shutdownInProgress && startTime) {
        showDetailedStats()
      }
    }, 60000)

    // 5. Status update
    const statusInterval = setInterval(() => {
      if (!shutdownInProgress && startTime) {
        updateStatus()
      }
    }, STATUS_INTERVAL)

    // 6. Cleanup on exit
    process.on('exit', () => {
      clearInterval(statsInterval)
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
