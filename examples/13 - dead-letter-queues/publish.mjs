import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-dlq-publish',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    deadLetterExchange: 'dlx' // Exchange for dead letters
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let messagesPublished = 0
  const QUEUE_NAME = 'main-queue'
  const DLQ_NAME = `${QUEUE_NAME}_dlq` // Naming convention for the DLQ

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()

      console.log('\n🔧 Setting up infrastructure...')

      // 1. Set up the Dead Letter Exchange
      console.log('   Setting up Dead Letter Exchange...')
      await channel.assertExchange(rabbitConfig.deadLetterExchange, 'direct', {
        durable: true
      })

      // 2. Create the DLQ queue
      console.log(`   Creating DLQ queue: ${DLQ_NAME}`)
      await channel.assertQueue(DLQ_NAME, {
        durable: true
      })

      // 3. Bind the DLQ to the DLX
      await channel.bindQueue(DLQ_NAME, rabbitConfig.deadLetterExchange, DLQ_NAME)

      // 4. Create the main queue with DLQ configuration
      console.log(`   Creating main queue: ${QUEUE_NAME}`)
      await channel.assertQueue(QUEUE_NAME, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': rabbitConfig.deadLetterExchange,
          'x-dead-letter-routing-key': DLQ_NAME
        }
      })

      // 5. Bind the main queue to the main exchange
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'dlq-route')

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function publishMessage (message, shouldFail = false) {
    try {
      messagesPublished++
      console.log(`\n📨 Publishing message ${messagesPublished}...`)

      if (shouldFail) {
        message.forceError = true
        console.log('   ⚠️  Message marked to fail')
      }

      await rabbitMQ.publish('dlq-route', message, {
        persistent: true,
        messageId: new Date().getTime().toString(),
        timestamp: new Date().getTime(),
        headers: {
          'x-should-fail': shouldFail
        }
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
    console.log(`\n📊 Total messages published: ${messagesPublished}`)

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
    // 1. Establish the connection
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully')

    // 2. Set up the infrastructure
    await setupInfrastructure()

    // 3. Demonstration
    console.log('\n🚀 Starting Dead Letter Queue demonstration...')
    console.log('   Some messages will be marked to fail')
    console.log('   Messages that fail will go to the DLQ')
    console.log(`   DLQ: ${DLQ_NAME}\n`)

    // Publish normal messages
    await publishMessage({
      id: 1,
      type: 'NORMAL',
      text: 'Message that should be processed normally',
      timestamp: new Date()
    })

    await publishMessage({
      id: 2,
      type: 'NORMAL',
      text: 'Another normal message',
      timestamp: new Date()
    })

    // Publish messages that should fail
    await publishMessage({
      id: 3,
      type: 'FAIL',
      text: 'Message that should fail and go to the DLQ',
      timestamp: new Date()
    }, true)

    await publishMessage({
      id: 4,
      type: 'FAIL',
      text: 'Another message that should fail',
      timestamp: new Date()
    }, true)

    // One more normal message
    await publishMessage({
      id: 5,
      type: 'NORMAL',
      text: 'Last normal message',
      timestamp: new Date()
    })

    console.log('\n✨ Demonstration completed!')
    console.log('   Messages published: 5')
    console.log('   Messages that should fail: 2')
    console.log('   Wait for the consumer to process the messages')
    console.log('   Messages that fail will show up in the DLQ')

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
