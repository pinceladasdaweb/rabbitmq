import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-retry-publish',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let totalAttempts = 0
  let successfulPublishes = 0
  let failedPublishes = 0
  let totalRetries = 0
  const QUEUE_NAME = 'retry-queue'
  const ROUTING_KEY = 'retry-route'

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()

      console.log('\n🔧 Setting up infrastructure...')

      // Create the queue
      console.log(`   Creating queue: ${QUEUE_NAME}`)
      await channel.assertQueue(QUEUE_NAME, {
        durable: true
      })

      // Bind the queue to the exchange
      console.log('   Binding queue to exchange...')
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, ROUTING_KEY)

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function publishWithRetryMonitoring (message, shouldFailTimes = 0) {
    let attemptCount = 0

    try {
      totalAttempts++
      console.log(`\n📨 Starting publish #${totalAttempts}`)
      console.log(`   Configured to fail ${shouldFailTimes} times`)

      // Publish with retry
      await rabbitMQ.publish(ROUTING_KEY, {
        ...message,
        _retryInfo: {
          shouldFailTimes,
          currentAttempt: ++attemptCount
        }
      }, {
        persistent: true,
        messageId: new Date().getTime().toString(),
        timestamp: new Date().getTime(),
        maxRetries: 3, // Maximum number of attempts
        retryDelay: 2000 // Delay between attempts (2 seconds)
      })

      successfulPublishes++
      console.log('✅ Message published successfully!')
      return true
    } catch (error) {
      failedPublishes++
      totalRetries += attemptCount - 1
      console.error('❌ Publish error:', error.message)
      console.log(`   Attempts made: ${attemptCount}`)
      return false
    }
  }

  function showStats () {
    console.log('\n📊 Statistics:')
    console.log(`   Total publishes attempted: ${totalAttempts}`)
    console.log(`   Successful publishes: ${successfulPublishes}`)
    console.log(`   Failed publishes: ${failedPublishes}`)
    console.log(`   Total retries: ${totalRetries}`)
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
    // 1. Establish the connection
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully')

    // 2. Set up the infrastructure
    await setupInfrastructure()

    // 3. Retry configuration
    console.log('\n⚡ Retry mechanism configured:')
    console.log('   Maximum attempts: 3')
    console.log('   Delay between attempts: 2 seconds')

    // 4. Retry demonstration
    console.log('\n🚀 Starting retry demonstration...')

    // Case 1: Publish without failures
    console.log('\n📝 Case 1: Publish without failures')
    await publishWithRetryMonitoring({
      id: 1,
      text: 'Message without failures',
      timestamp: new Date()
    }, 0)

    // Small delay between tests
    await new Promise(resolve => setTimeout(resolve, 1000))

    // Case 2: Publish with one failure (should succeed on retry)
    console.log('\n📝 Case 2: Publish with one failure')
    await publishWithRetryMonitoring({
      id: 2,
      text: 'Message with one failure',
      timestamp: new Date()
    }, 1)

    await new Promise(resolve => setTimeout(resolve, 1000))

    // Case 3: Publish with two failures (should succeed on the third retry)
    console.log('\n📝 Case 3: Publish with two failures')
    await publishWithRetryMonitoring({
      id: 3,
      text: 'Message with two failures',
      timestamp: new Date()
    }, 2)

    await new Promise(resolve => setTimeout(resolve, 1000))

    // Case 4: Publish with failures exceeding the limit (should fail)
    console.log('\n📝 Case 4: Publish exceeding the retry limit')
    await publishWithRetryMonitoring({
      id: 4,
      text: 'Message with too many failures',
      timestamp: new Date()
    }, 4)

    console.log('\n✨ Retry demonstration completed!')
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
