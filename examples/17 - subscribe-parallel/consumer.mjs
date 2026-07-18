import path, { dirname } from 'node:path'
import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'
import { fileURLToPath } from 'node:url'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-parallel-consumer'
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const processStartTime = null
  const QUEUE_NAME = 'parallel-queue'
  const messagesProcessed = 0
  const totalProcessingTime = 0

  // Options for parallel processing
  const parallelOptions = {
    workerCount: 4, // Number of workers (or use os.cpus().length)
    prefetch: 2, // Prefetch per worker
    noAck: false // Enables manual acknowledgments
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

    // New code to resolve the path correctly
    const __filename = fileURLToPath(import.meta.url)
    const __dirname = dirname(__filename)
    const processorPath = path.join(__dirname, 'message-processor.mjs')

    console.log(`\n🔄 Setting up parallel consumer for queue: ${QUEUE_NAME}`)
    console.log('\n⚙️  Settings:')
    console.log(`   Number of workers: ${parallelOptions.workerCount}`)
    console.log(`   Prefetch per worker: ${parallelOptions.prefetch}`)
    console.log(`   Total prefetch: ${parallelOptions.workerCount * parallelOptions.prefetch}`)

    await rabbitMQ.subscribeParallel(QUEUE_NAME, processorPath, parallelOptions)
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
