import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-compression-consumption',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    useCompression: true,
    compressionThreshold: 1000
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let messagesProcessed = 0
  let compressedMessagesReceived = 0
  const QUEUE_NAME = 'compression-queue'

  function formatBytes (bytes) {
    if (bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
  }

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()

      console.log('\n🔧 Setting up infrastructure...')

      // Creates the queue
      console.log(`   Creating queue: ${QUEUE_NAME}`)
      await channel.assertQueue(QUEUE_NAME, {
        durable: true
      })

      // Binds it to the exchange
      console.log('   Binding queue to exchange...')
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'compression-route')

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function handleMessage (content, message) {
    try {
      if (messagesProcessed === 0) {
        console.log('\n🚀 Starting message processing...\n')
      }

      messagesProcessed++

      // Checks whether the message was compressed
      const wasCompressed = message.properties.headers && message.properties.headers['x-compressed']
      if (wasCompressed) {
        compressedMessagesReceived++
      }

      // Calculates the original message size
      const messageSize = Buffer.from(JSON.stringify(content)).length

      console.log('\n📥 Message received:')
      console.log(`   ID: ${content.id}`)
      console.log(`   Type: ${content.data.type}`)
      console.log(`   Size: ${formatBytes(messageSize)}`)
      console.log('\n🔍 Compression info:')
      console.log(`   Compressed: ${wasCompressed ? 'Yes' : 'No'}`)
      console.log(`   Larger than threshold (${formatBytes(rabbitConfig.compressionThreshold)}): ${messageSize > rabbitConfig.compressionThreshold ? 'Yes' : 'No'}`)

      // Simulates some processing
      await new Promise(resolve => setTimeout(resolve, 500))

      console.log('\n✅ Message processed successfully!')
      console.log('📊 Statistics:')
      console.log(`   Total processed: ${messagesProcessed}`)
      console.log(`   Compressed: ${compressedMessagesReceived}`)
      console.log(`   Not compressed: ${messagesProcessed - compressedMessagesReceived}`)

      return true
    } catch (error) {
      console.error('❌ Error processing message:', error.message)
      return false
    }
  }

  function showStatus () {
    console.log('\n📊 Current status:')
    console.log(`   Messages processed: ${messagesProcessed}`)
    console.log(`   Compressed messages: ${compressedMessagesReceived}`)
    if (messagesProcessed > 0) {
      console.log(`   Compression rate: ${((compressedMessagesReceived / messagesProcessed) * 100).toFixed(1)}%`)
    }
    console.log('   Consumer remains active waiting for messages...\n')
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')

    if (messagesProcessed > 0) {
      console.log('\n📊 Final statistics:')
      console.log(`   Total messages processed: ${messagesProcessed}`)
      console.log(`   Compressed messages: ${compressedMessagesReceived}`)
      console.log(`   Uncompressed messages: ${messagesProcessed - compressedMessagesReceived}`)
      console.log(`   Compression rate: ${((compressedMessagesReceived / messagesProcessed) * 100).toFixed(1)}%`)
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

    // Sets up the infrastructure before consuming
    await setupInfrastructure()

    // Sets up the consumer
    console.log(`\n🔄 Setting up consumer for queue: ${QUEUE_NAME}`)

    await rabbitMQ.subscribe(QUEUE_NAME, handleMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'consumer-compression-' + Date.now()
    })

    console.log('✅ Consumer set up successfully')
    console.log('\n⚡ Starting message consumption')
    console.log(`   Compression threshold: ${formatBytes(rabbitConfig.compressionThreshold)}`)
    console.log('   Decompression happens automatically')
    console.log('   Consumer will remain active waiting for messages')
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
