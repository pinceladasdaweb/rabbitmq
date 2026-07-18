import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-optimized-prefetch-consumer'
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let processStartTime = null
  const QUEUE_NAME = 'optimized-queue'
  let messagesProcessed = 0
  let totalProcessingTime = 0

  const prefetchOptions = {
    initialPrefetch: 10,
    maxPrefetch: 100,
    minPrefetch: 1,
    optimizationInterval: 1000,
    increaseFactor: 1.5,
    decreaseFactor: 0.75
  }

  async function handleMessage (content) {
    if (!processStartTime) {
      processStartTime = Date.now()
    }

    messagesProcessed++
    const startTime = Date.now()

    console.log('\n📥 Processing message:')
    console.log(`   ID: ${content.id}`)
    console.log(`   Priority: ${content.priority}`)
    console.log(`   Expected time: ${content.expectedProcessingTime.toFixed(0)}ms`)

    // Simulates processing using the message's expected time
    await new Promise(resolve => setTimeout(resolve, content.expectedProcessingTime))

    const processingTime = Date.now() - startTime
    totalProcessingTime += processingTime

    console.log('\n✅ Message processed:')
    console.log(`   Actual time: ${processingTime}ms`)
    console.log(`   Total processed: ${messagesProcessed}`)
    console.log(`   Average time: ${(totalProcessingTime / messagesProcessed).toFixed(1)}ms`)
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')

    if (messagesProcessed > 0) {
      const runningTime = (Date.now() - processStartTime) / 1000
      console.log('\n📊 Final statistics:')
      console.log(`   Total messages processed: ${messagesProcessed}`)
      console.log(`   Total running time: ${runningTime.toFixed(1)}s`)
      console.log(`   Processing average: ${(messagesProcessed / runningTime).toFixed(2)} msgs/s`)
      console.log(`   Average time per message: ${(totalProcessingTime / messagesProcessed).toFixed(1)}ms`)
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

    console.log(`\n🔄 Setting up optimized consumer for queue: ${QUEUE_NAME}`)
    console.log('\n⚙️  Optimization settings:')
    console.log(`   Initial prefetch: ${prefetchOptions.initialPrefetch}`)
    console.log(`   Maximum prefetch: ${prefetchOptions.maxPrefetch}`)
    console.log(`   Minimum prefetch: ${prefetchOptions.minPrefetch}`)
    console.log(`   Optimization interval: ${prefetchOptions.optimizationInterval}ms`)
    console.log(`   Increase factor: ${prefetchOptions.increaseFactor}x`)
    console.log(`   Decrease factor: ${prefetchOptions.decreaseFactor}x`)

    await rabbitMQ.subscribeWithOptimizedPrefetch(QUEUE_NAME, handleMessage, prefetchOptions)

    console.log('\n✅ Consumer set up successfully')
    console.log('\n⚡ Waiting for messages...')
    console.log('   Press Ctrl+C to exit\n')
  } catch (error) {
    console.error('❌ Error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
