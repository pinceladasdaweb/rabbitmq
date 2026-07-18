import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-compression-publication',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    channelPoolSize: 5,
    useCompression: true, // Enables compression
    compressionThreshold: 1000 // Compresses messages larger than 1000 bytes
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false

  // Helper to generate messages of different sizes
  function generateMessage (id, size) {
    return {
      id,
      timestamp: new Date(),
      text: 'Test message',
      data: {
        type: size <= 1000 ? 'SMALL' : 'LARGE',
        payload: Array(size).fill('X').join('') // Generates a payload of the given size
      }
    }
  }

  async function publishMessage (message) {
    try {
      const messageSize = Buffer.from(JSON.stringify(message)).length

      console.log(`\n📨 Publishing message ${message.id}...`)
      console.log(`   Size: ${messageSize} bytes`)
      console.log(`   Type: ${message.data.type}`)
      console.log(`   Compression: ${messageSize > rabbitConfig.compressionThreshold ? 'Yes' : 'No'}`)

      await rabbitMQ.publish('compression-route', message, {
        persistent: true,
        messageId: new Date().getTime().toString(),
        timestamp: new Date().getTime(),
        contentType: 'application/json'
      })

      console.log('✅ Message published successfully!')
      return true
    } catch (error) {
      console.error('❌ Error publishing message:', error.message)
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
    // 1. Establishes the connection
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully')

    // 2. Waits for the connection to stabilize
    await new Promise(resolve => setTimeout(resolve, 1000))

    // 3. Prepares messages of different sizes
    console.log('\n📝 Preparing messages for the compression test...')

    const messages = [
      generateMessage(1, 500), // Small (will not be compressed)
      generateMessage(2, 2000), // Large (will be compressed)
      generateMessage(3, 750), // Small (will not be compressed)
      generateMessage(4, 5000) // Large (will be compressed)
    ]

    console.log('✅ Messages prepared')
    console.log(`   Compression threshold: ${rabbitConfig.compressionThreshold} bytes`)
    console.log('   Messages larger than the threshold will be compressed')

    // 4. Publishes the messages
    for (const msg of messages) {
      await publishMessage(msg)
      // Small delay between publications for better visualization
      await new Promise(resolve => setTimeout(resolve, 1000))
    }

    // 5. Shows the summary
    console.log('\n📊 Publication summary:')
    console.log(`   Total messages: ${messages.length}`)
    console.log(`   Small messages: ${messages.filter(m => Buffer.from(JSON.stringify(m)).length <= rabbitConfig.compressionThreshold).length}`)
    console.log(`   Large messages (compressed): ${messages.filter(m => Buffer.from(JSON.stringify(m)).length > rabbitConfig.compressionThreshold).length}`)

    console.log('\n✨ Selective compression demonstration completed!')

    // 6. Wraps up
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
