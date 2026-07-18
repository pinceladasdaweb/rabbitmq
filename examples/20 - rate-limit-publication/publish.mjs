import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-rate-limit',
    exchange: {
      name: 'rate-limit-exchange',
      type: 'direct'
    },
    rateLimiter: {
      windowMs: 10000, // 10 seconds
      maxRequests: 5, // 5 requests per window
      strategy: 'token-bucket',
      burstable: true,
      burstLimit: 8 // Allows bursts of up to 8 messages
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let totalAttempts = 0
  let successfulPublishes = 0
  let rateLimitedPublishes = 0
  let failedPublishes = 0
  const QUEUE_NAME = 'rate-limit-queue'
  const ROUTING_KEY = 'rate-limit-route'

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()

      console.log('\n🔧 Setting up infrastructure...')

      console.log(`   Creating queue: ${QUEUE_NAME}`)
      await channel.assertQueue(QUEUE_NAME, {
        durable: true
      })

      console.log('   Binding queue to exchange...')
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, ROUTING_KEY)

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function publishWithRateLimit (message, delayMs = 0) {
    totalAttempts++

    try {
      console.log(`\n📨 Publish attempt #${totalAttempts}`)

      if (delayMs > 0) {
        console.log(`   Waiting ${delayMs}ms before publishing...`)
        await new Promise(resolve => setTimeout(resolve, delayMs))
      }

      // Tries to publish under the rate limit
      await rabbitMQ.publish(ROUTING_KEY, {
        ...message,
        publishInfo: {
          attempt: totalAttempts,
          timestamp: new Date()
        }
      }, {
        persistent: true,
        messageId: `msg-${totalAttempts}-${Date.now()}`,
        timestamp: Date.now()
      })

      successfulPublishes++
      console.log('✅ Message published successfully!')

      // Fetches and displays the rate limit status
      const status = rabbitMQ.getRateLimitStatus(ROUTING_KEY)
      console.log('\n📊 Rate Limit status:')
      console.log(`   Remaining tokens: ${status.remainingTokens}`)
      console.log(`   Strategy: ${status.strategy}`)

      return true
    } catch (error) {
      if (error.code === 'RATE_LIMIT_EXCEEDED') {
        rateLimitedPublishes++
        console.log('⚠️ Rate limit exceeded!')
        console.log(`   Window remaining: ${error.status.windowMs}ms`)
        console.log(`   Remaining tokens: ${error.status.remainingTokens}`)
      } else {
        failedPublishes++
        console.error('❌ Error publishing:', error.message)
      }
      return false
    }
  }

  function showStats () {
    console.log('\n📊 Publication statistics:')
    console.log(`   Total attempts: ${totalAttempts}`)
    console.log(`   Successful publishes: ${successfulPublishes}`)
    console.log(`   Rate-limited publishes: ${rateLimitedPublishes}`)
    console.log(`   Failed publishes: ${failedPublishes}`)
    console.log(`   Success rate: ${((successfulPublishes / totalAttempts) * 100).toFixed(1)}%`)
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    showStats()

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

    await setupInfrastructure()

    console.log('\n⚡ Rate Limit configuration:')
    console.log('   Time window: 10 seconds')
    console.log('   Max requests: 5 per window')
    console.log('   Strategy: token bucket')
    console.log('   Burst: enabled (max: 8)')

    console.log('\n🚀 Starting rate limit demonstration...')

    // Case 1: Publishing within the limit
    console.log('\n📝 Case 1: Publishing within the limit (3 messages)')
    for (let i = 1; i <= 3; i++) {
      await publishWithRateLimit({
        id: i,
        text: `Normal message ${i}`,
        type: 'normal'
      }, 500) // 500ms between publishes
    }

    // Case 2: Message burst
    console.log('\n📝 Case 2: Message burst (6 rapid messages)')
    for (let i = 1; i <= 6; i++) {
      await publishWithRateLimit({
        id: i,
        text: `Burst message ${i}`,
        type: 'burst'
      }, 100) // 100ms between publishes
    }

    await new Promise(resolve => setTimeout(resolve, 5000)) // Waits 5s

    // Case 3: Attempts after the limit is exceeded
    console.log('\n📝 Case 3: Attempts after the limit is exceeded')
    for (let i = 1; i <= 3; i++) {
      await publishWithRateLimit({
        id: i,
        text: `Post-limit message ${i}`,
        type: 'after-limit'
      }, 200)
    }

    console.log('\n✨ Rate limit demonstration completed!')
    await shutdown()
  } catch (error) {
    console.error('❌ Fatal error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
