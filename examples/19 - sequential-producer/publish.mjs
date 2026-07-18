import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-sequential-producer',
    exchange: {
      name: 'sequential-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let messagesPublished = 0
  const QUEUE_NAME = 'sequential-queue'
  const TOTAL_SEQUENCES = 5 // Number of sequences
  const STEPS_PER_SEQUENCE = 3 // Steps per sequence

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
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'sequential-route')

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function publishSequentialMessages () {
    try {
      console.log('\n📨 Starting sequential message publication...')

      for (let seq = 1; seq <= TOTAL_SEQUENCES; seq++) {
        console.log(`\n🔄 Publishing sequence ${seq}:`)

        // Publishes the steps of each sequence
        for (let step = 1; step <= STEPS_PER_SEQUENCE; step++) {
          const messageId = `seq${seq}-step${step}`
          const dependsOn = step === 1 ? null : `seq${seq}-step${step - 1}`

          const message = {
            sequenceId: seq,
            step,
            text: `Sequence ${seq} message, step ${step}`,
            timestamp: Date.now(),
            processingTime: Math.floor(Math.random() * 1000) + 500 // 500-1500ms
          }

          const options = {
            messageId,
            headers: dependsOn ? { 'depends-on': dependsOn } : undefined,
            persistent: true
          }

          await rabbitMQ.publish('sequential-route', message, options)
          messagesPublished++

          console.log(`   ✓ Step ${step}${dependsOn ? ` (depends on: ${dependsOn})` : ''}`)

          // Small delay between messages of the same sequence
          await new Promise(resolve => setTimeout(resolve, 100))
        }

        // Longer delay between different sequences
        await new Promise(resolve => setTimeout(resolve, 500))
      }

      console.log('\n📊 Publication summary:')
      console.log(`   Sequences published: ${TOTAL_SEQUENCES}`)
      console.log(`   Steps per sequence: ${STEPS_PER_SEQUENCE}`)
      console.log(`   Total messages: ${messagesPublished}`)
      console.log('\n✅ All sequences were published successfully!')
    } catch (error) {
      console.error('❌ Error publishing messages:', error.message)
      throw error
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Publication statistics:')
    console.log(`   Total messages published: ${messagesPublished}`)

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

    console.log('\n🚀 Starting sequential message publication...')
    await publishSequentialMessages()

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
