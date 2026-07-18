import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-async-publication',
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
  let messagesPublished = 0
  let startTime = null

  async function publishMessageAsync (routingKey, message) {
    try {
      await rabbitMQ.publishAsync(routingKey, message, {
        persistent: true,
        messageId: new Date().getTime().toString(),
        timestamp: new Date().getTime(),
        contentType: 'application/json'
      })

      messagesPublished++

      // Calculates and shows metrics every 1000 messages
      if (messagesPublished % 1000 === 0) {
        const currentTime = Date.now()
        const elapsedSeconds = (currentTime - startTime) / 1000
        const messagesPerSecond = messagesPublished / elapsedSeconds

        console.log('\n📊 Partial metrics:')
        console.log(`   Messages published: ${messagesPublished}`)
        console.log(`   Elapsed time: ${elapsedSeconds.toFixed(2)}s`)
        console.log(`   Average: ${messagesPerSecond.toFixed(2)} msgs/s`)
      }

      return true
    } catch (error) {
      console.error('❌ Error publishing message:', error.message)
      return false
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    const endTime = Date.now()
    const totalSeconds = (endTime - startTime) / 1000
    const finalThroughput = messagesPublished / totalSeconds

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Final statistics:')
    console.log(`   Total messages published: ${messagesPublished}`)
    console.log(`   Total execution time: ${totalSeconds.toFixed(2)}s`)
    console.log(`   Average throughput: ${finalThroughput.toFixed(2)} msgs/s`)

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
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully\n')

    // Preparation for the throughput test
    const totalMessages = 10000 // Total messages for the test
    const messages = Array.from({ length: totalMessages }, (_, i) => ({
      id: i + 1,
      text: `Async message ${i + 1}`,
      timestamp: new Date(),
      data: {
        batch: false,
        sequence: i + 1,
        info: 'Sample async message'
      }
    }))

    console.log(`📝 Starting async publication of ${totalMessages} messages...`)
    console.log('   This operation does not wait for broker confirmation!\n')

    startTime = Date.now()

    // Publishes messages asynchronously
    for (const msg of messages) {
      await publishMessageAsync('test-route', msg)

      // Small delay to avoid overloading the console
      if (messagesPublished % 1000 === 0) {
        await new Promise(resolve => setTimeout(resolve, 100))
      }
    }

    const endTime = Date.now()
    const totalSeconds = (endTime - startTime) / 1000
    const throughput = messagesPublished / totalSeconds

    console.log('\n✨ Async publication completed!')
    console.log('📊 Final result:')
    console.log(`   Messages published: ${messagesPublished}`)
    console.log(`   Total time: ${totalSeconds.toFixed(2)}s`)
    console.log(`   Throughput: ${throughput.toFixed(2)} msgs/s`)

    // Waits a moment before shutting down
    await new Promise(resolve => setTimeout(resolve, 2000))
    await shutdown()
  } catch (error) {
    console.error('❌ Error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Fatal error:', error)
  process.exit(1)
})
