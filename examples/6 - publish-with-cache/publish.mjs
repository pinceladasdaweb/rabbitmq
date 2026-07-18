import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-cache-publication',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    useCache: true, // Enables the cache
    cacheTTL: 30, // TTL of 30 seconds
    cacheCheckPeriod: 60, // Checks for expired cache every 60 seconds
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false

  // Simulates an expensive function that generates a message
  async function generateExpensiveMessage (id) {
    console.log(`\n🔄 Generating message ${id} (simulating expensive processing)...`)

    // Simulates heavy processing
    await new Promise(resolve => setTimeout(resolve, 2000))

    return {
      id,
      text: `Expensive message ${id}`,
      timestamp: new Date(),
      data: {
        cached: true,
        processedAt: new Date(),
        complexData: Array(1000).fill(Math.random()) // Simulates complex data
      }
    }
  }

  async function publishWithCache (id) {
    try {
      console.log(`\n📨 Publishing message ${id} (checking cache)...`)

      // Uses the publishWithCache method which will:
      // 1. Check if the message is cached
      // 2. If not, generate the message and cache it
      // 3. Publish the message
      const message = await rabbitMQ.publishWithCache(
        'test-route',
        // Generator function that is only called when there is no cache
        () => generateExpensiveMessage(id),
        {
          // Publish options
          persistent: true,
          messageId: new Date().getTime().toString(),
          timestamp: new Date().getTime(),
          contentType: 'application/json',
          // Cache options
          cacheTTL: 30 // Message-specific TTL (30 seconds)
        }
      )

      console.log('✅ Message published and cached successfully!')
      return message
    } catch (error) {
      console.error('❌ Error publishing message:', error.message)
      return null
    }
  }

  async function checkCache (id) {
    try {
      const cachedMessage = await rabbitMQ.getFromCache('test-route')
      if (cachedMessage) {
        console.log(`\n📦 Message ${id} found in cache:`)
        console.log(JSON.stringify(cachedMessage, null, 2))
      } else {
        console.log(`\n❌ Message ${id} not found in cache`)
      }
    } catch (error) {
      console.error('❌ Error checking cache:', error.message)
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')

    try {
      // Clears the cache before disconnecting
      rabbitMQ.clearCache()
      console.log('🧹 Cache cleared')

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
    console.log('✅ Connection established successfully\n')

    // 2. Ensures the queue exists and is bound to the exchange
    const channel = await rabbitMQ.getChannel()
    await channel.assertQueue('example-queue', { durable: true })
    await channel.bindQueue('example-queue', rabbitConfig.exchange.name, 'test-route')

    // 3. Cache usage demonstration
    console.log('🚀 Starting cached publication demonstration...')

    // First publication - No cache yet
    console.log('\n📝 Test 1: First publication (no cache)')
    await checkCache(1)
    await publishWithCache(1)
    await checkCache(1)

    // Second publication - Will use cache
    console.log('\n📝 Test 2: Second publication (should use cache)')
    await publishWithCache(1)

    // Waits for cache expiration (31 seconds)
    console.log('\n⏳ Waiting for cache expiration (31 seconds)...')
    await new Promise(resolve => setTimeout(resolve, 31000))

    // Third publication - Cache expired
    console.log('\n📝 Test 3: Third publication (cache expired)')
    await checkCache(1)
    await publishWithCache(1)

    console.log('\n✨ Demonstration completed!')

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
