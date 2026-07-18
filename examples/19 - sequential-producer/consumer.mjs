import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-sequential-consumer'
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  const QUEUE_NAME = 'sequential-queue'
  let shutdownInProgress = false
  const startTime = Date.now()
  const sequenceStats = new Map() // Statistics per sequence

  function formatTime (ms) {
    return `${(ms / 1000).toFixed(1)}s`
  }

  function updateSequenceStats (message) {
    const { sequenceId, step } = message
    if (!sequenceStats.has(sequenceId)) {
      sequenceStats.set(sequenceId, {
        stepsCompleted: 0,
        startTime: Date.now(),
        steps: new Set()
      })
    }

    const stats = sequenceStats.get(sequenceId)
    stats.steps.add(step)
    stats.stepsCompleted = stats.steps.size

    if (step === 3) { // Last step
      stats.endTime = Date.now()
      stats.totalTime = stats.endTime - stats.startTime
    }
  }

  function showCurrentStatus () {
    console.log('\n📊 Current status:')
    for (const [seqId, stats] of sequenceStats.entries()) {
      console.log(`   Sequence ${seqId}:`)
      console.log(`      Steps completed: ${stats.stepsCompleted}/3`)
      if (stats.totalTime) {
        console.log(`      Total time: ${formatTime(stats.totalTime)}`)
      }
    }
  }

  async function handleMessage (content, message) {
    try {
      const messageId = message.properties.messageId
      const dependsOn = message.properties.headers?.['depends-on']

      console.log(`\n📥 Message received: ${messageId}`)
      console.log(`   Sequence: ${content.sequenceId}, Step: ${content.step}`)
      if (dependsOn) {
        console.log(`   Depends on: ${dependsOn}`)
      }

      // Simulates the processing
      console.log(`   ⏳ Processing (${content.processingTime}ms)...`)
      await new Promise(resolve => setTimeout(resolve, content.processingTime))

      // Updates statistics
      updateSequenceStats(content)

      console.log(`\n✅ Message processed: ${messageId}`)
      console.log(`   Text: ${content.text}`)
      showCurrentStatus()

      return true
    } catch (error) {
      console.error(`❌ Error processing message: ${error.message}`)
      return false
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Final statistics:')

    let totalMessages = 0
    let completedSequences = 0

    for (const [seqId, stats] of sequenceStats.entries()) {
      console.log(`\n   Sequence ${seqId}:`)
      console.log(`      Steps completed: ${stats.stepsCompleted}/3`)
      totalMessages += stats.stepsCompleted
      if (stats.totalTime) {
        console.log(`      Total time: ${formatTime(stats.totalTime)}`)
        completedSequences++
      }
    }

    const totalTime = Date.now() - startTime
    console.log('\n   Summary:')
    console.log(`      Completed sequences: ${completedSequences}`)
    console.log(`      Total messages: ${totalMessages}`)
    console.log(`      Total running time: ${formatTime(totalTime)}`)
    console.log(`      Average per sequence: ${formatTime(totalTime / completedSequences)}`)

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

    console.log(`\n🔄 Setting up sequential consumer for queue: ${QUEUE_NAME}`)
    await rabbitMQ.subscribeSequential(QUEUE_NAME, handleMessage, {
      prefetchCount: 1
    })

    console.log('\n⚡ Consumer started')
    console.log('   Waiting for messages...')
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
