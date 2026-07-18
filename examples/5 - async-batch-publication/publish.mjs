import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-async-batch-publication',
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
  let batchesPublished = 0
  let totalMessagesPublished = 0
  let startTime = null
  let lastStatusUpdate = 0
  const STATUS_INTERVAL = 100 // 100ms

  async function publishBatchAsync (messages, batchSize = 1000) {
    const batches = []
    for (let i = 0; i < messages.length; i += batchSize) {
      batches.push(messages.slice(i, i + batchSize))
    }

    console.log(`\n📦 Starting publication of ${messages.length} messages in ${batches.length} batches`)
    console.log(`   Batch size: ${batchSize} messages\n`)

    startTime = Date.now()

    try {
      for (const batch of batches) {
        await rabbitMQ.publishAsyncBatch('test-route', batch, {
          persistent: true,
          timestamp: new Date().getTime(),
          contentType: 'application/json'
        })

        batchesPublished++
        totalMessagesPublished += batch.length
        updateStatus()

        // Small delay between batches to avoid overloading
        await new Promise(resolve => setTimeout(resolve, 50))
      }
      return true
    } catch (error) {
      console.error('\n❌ Error while publishing batch:', error.message)
      return false
    }
  }

  function updateStatus () {
    const now = Date.now()
    if (now - lastStatusUpdate >= STATUS_INTERVAL) {
      const elapsedSeconds = (now - startTime) / 1000
      const throughput = totalMessagesPublished / elapsedSeconds

      // TTY-only APIs: guard them so the example also works with piped output
      if (process.stdout.isTTY) {
        process.stdout.clearLine()
        process.stdout.cursorTo(0)
      }

      process.stdout.write(
        `⚡ Progress: ${batchesPublished} batches | ` +
        `${totalMessagesPublished} messages | ` +
        `Time: ${elapsedSeconds.toFixed(1)}s | ` +
        `Throughput: ${throughput.toFixed(2)} msgs/s`
      )

      lastStatusUpdate = now
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n\n👋 Starting graceful shutdown...')

    if (startTime) {
      const endTime = Date.now()
      const totalSeconds = (endTime - startTime) / 1000
      const finalThroughput = totalMessagesPublished / totalSeconds

      console.log('\n📊 Final statistics:')
      console.log(`   Batches published: ${batchesPublished}`)
      console.log(`   Total messages: ${totalMessagesPublished}`)
      console.log(`   Total execution time: ${totalSeconds.toFixed(2)}s`)
      console.log(`   Average throughput: ${finalThroughput.toFixed(2)} msgs/s`)
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
    // 1. Establishes the connection
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully\n')

    // 2. Waits a moment to make sure the connection is stable
    await new Promise(resolve => setTimeout(resolve, 1000))

    // 3. Prepares the messages
    const totalMessages = 100000 // 100k messages
    console.log(`🔄 Preparing ${totalMessages} messages for publication...`)

    const messages = Array.from({ length: totalMessages }, (_, i) => ({
      id: i + 1,
      text: `Async batch message ${i + 1}`,
      timestamp: new Date(),
      data: {
        batch: true,
        sequence: i + 1,
        info: 'Sample async batch message'
      }
    }))

    console.log('✅ Messages prepared')
    console.log('\n⚡ Starting async batch publication')
    console.log('   This operation does not wait for broker confirmation')
    console.log('   Messages will be published in batches of 1000\n')

    // 4. Publishes the messages
    const result = await publishBatchAsync(messages, 1000)

    if (result) {
      console.log('\n\n✨ Async batch publication completed!')
    }

    // 5. Waits a moment before shutting down
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
