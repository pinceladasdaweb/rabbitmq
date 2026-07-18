import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-batch-publication',
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

  async function publishBatch (routingKey, messages, batchSize = 3) {
    try {
      // Splits the messages into batches of the specified size
      const batches = []
      for (let i = 0; i < messages.length; i += batchSize) {
        batches.push(messages.slice(i, i + batchSize))
      }

      console.log(`📦 Starting batch publication (${batches.length} batches)...`)

      // Publishes each batch
      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i]
        console.log(`\n📨 Publishing batch ${i + 1}/${batches.length} (${batch.length} messages)...`)

        await rabbitMQ.publishBatch(routingKey, batch, {
          persistent: true,
          timestamp: new Date().getTime(),
          contentType: 'application/json'
        })

        console.log('✅ Batch published and confirmed successfully!')
        console.log('📝 Batch messages:')
        batch.forEach(msg => console.log(`   - ID: ${msg.id}, Text: ${msg.text}`))
      }

      return true
    } catch (error) {
      console.error('❌ Error publishing batch:', error.message)
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
    // 1. Establishes the connection that will be reused
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully\n')

    // 2. Waits a moment to make sure everything is ready
    await new Promise(resolve => setTimeout(resolve, 1000))

    // 3. Prepares a larger set of messages to demonstrate batching
    const messages = Array.from({ length: 10 }, (_, i) => ({
      id: i + 1,
      text: `Test message ${i + 1}`,
      timestamp: new Date(),
      data: {
        batch: true,
        sequence: i + 1,
        info: 'Sample batch message'
      }
    }))

    console.log(`📝 Preparing to publish ${messages.length} messages in batches...\n`)

    // 4. Publishes the messages in batches
    const result = await publishBatch('test-route', messages, 3) // Batches of 3 messages

    if (result) {
      console.log('\n✨ All batches were published successfully!')
      console.log(`📊 Total messages published: ${messages.length}`)
    }

    console.log('\nℹ️  The connection remains open for more publications')
    console.log('ℹ️  Press Ctrl+C to exit and close the connection\n')

    // 5. Keeps the process and the connection alive for more publications
    await new Promise(resolve => setTimeout(resolve, 30000))

    // 6. If we got this far, closes the connection
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
