import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-continuous-batch-consumption',
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
  let processingStartTime = null
  let statusInterval = null

  async function handleMessage (content, message) {
    try {
      if (!processingStartTime) {
        processingStartTime = Date.now()
        console.log('\n🔄 Starting new processing cycle...')
      }

      console.log('\n📥 Processing message:')
      console.log('   ID:', content.id)
      console.log('   Text:', content.text)
      console.log('   Timestamp:', new Date(content.timestamp).toISOString())
      console.log('   Consumer Tag:', message.fields.consumerTag)
      console.log('   Delivery Tag:', message.fields.deliveryTag)

      // Simulates processing
      await new Promise(resolve => setTimeout(resolve, 500))

      messagesProcessed++

      // Shows statistics every 3 processed messages
      if (messagesProcessed % 3 === 0) {
        batchesProcessed++
        const currentTime = Date.now()
        const elapsedTime = currentTime - processingStartTime

        console.log('\n📊 Batch statistics:')
        console.log(`   Batch #${batchesProcessed} processed`)
        console.log('   Messages in this batch: 3')
        console.log(`   Processing time: ${elapsedTime}ms`)
        console.log(`   Average per message: ${(elapsedTime / 3).toFixed(2)}ms`)
        console.log(`   Total messages processed: ${messagesProcessed}`)
        console.log(`   Total batches processed: ${batchesProcessed}`)

        processingStartTime = null
      }

      return true
    } catch (error) {
      console.error('❌ Error processing message:', error.message)
      return false
    }
  }

  function showStatus () {
    console.log('\n📊 Current consumer status:')
    console.log(`   Messages processed: ${messagesProcessed}`)
    console.log(`   Complete batches: ${batchesProcessed}`)
    console.log(`   Messages in current batch: ${messagesProcessed % 3}`)
    console.log('   Consumer is active and waiting for messages...\n')
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Final statistics:')
    console.log(`   Total batches processed: ${batchesProcessed}`)
    console.log(`   Total messages processed: ${messagesProcessed}`)

    try {
      if (statusInterval) {
        clearInterval(statusInterval)
      }

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
    console.log('✅ Connection established successfully')

    rabbitMQ.on('disconnected', () => {
      if (!shutdownInProgress) {
        console.log('❌ Disconnected from RabbitMQ')
      }
    })

    rabbitMQ.on('reconnected', () => {
      if (!shutdownInProgress) {
        console.log('🔄 Reconnected to RabbitMQ')
        showStatus() // Shows status after reconnection
      }
    })

    await new Promise(resolve => setTimeout(resolve, 1000))

    const queueName = 'example-queue'
    console.log(`\n🔄 Setting up consumer for queue: ${queueName}`)

    await rabbitMQ.subscribe(queueName, handleMessage, {
      prefetchCount: 3, // Processes 3 messages at a time
      noAck: false, // Requires explicit acknowledgment
      consumerTag: 'example-continuous-batch-consumer-' + Date.now()
    })

    console.log('✅ Consumer set up successfully')
    console.log('\n👂 Consumer active and waiting for messages...')
    console.log('   Processing messages in batches of 3')
    console.log('   Press Ctrl+C to exit\n')

    // Shows status every minute
    statusInterval = setInterval(() => {
      if (!shutdownInProgress) {
        showStatus()
      }
    }, 60000)
  } catch (error) {
    console.error('❌ Fatal error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
