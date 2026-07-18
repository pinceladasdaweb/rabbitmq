import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-dlq-consumer',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    deadLetterExchange: 'dlx',
    channelPoolSize: 5 // Reduced to 5 channels in the pool
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const messagesProcessed = {
    main: 0,
    dlq: 0
  }
  const QUEUE_NAME = 'main-queue'
  const DLQ_NAME = `${QUEUE_NAME}_dlq`

  async function setupInfrastructure () {
    try {
      console.log('\n🔧 Setting up infrastructure...')

      // Use a single channel for the whole setup
      const channel = await rabbitMQ.getChannel()
      console.log('   Channel acquired for setup')

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

  async function handleMainMessage (content, message) {
    try {
      console.log('\n📥 Processing message on the main queue:')
      console.log(`   ID: ${content.id}`)
      console.log(`   Type: ${content.type}`)
      console.log(`   Text: ${content.text}`)

      const shouldFail = message.properties.headers['x-should-fail']

      if (shouldFail) {
        console.log('   ⚠️  Message marked to fail')
        console.log('   ↪️  It will be moved to the DLQ')
        throw new Error('Forced failure for DLQ testing')
      }

      await new Promise(resolve => setTimeout(resolve, 500))

      messagesProcessed.main++
      console.log('✅ Message processed successfully!')
      showStats()

      return true
    } catch (error) {
      console.error('❌ Error processing message:', error.message)
      return false
    }
  }

  async function handleDLQMessage (content, message) {
    try {
      console.log('\n💀 Message received on the DLQ:')
      console.log(`   ID: ${content.id}`)
      console.log(`   Type: ${content.type}`)
      console.log(`   Text: ${content.text}`)

      const deathInfo = message.properties.headers['x-death']
      if (deathInfo && deathInfo[0]) {
        console.log('\n⚰️  Failure information:')
        console.log(`   Queue: ${deathInfo[0].queue}`)
        console.log(`   Reason: ${deathInfo[0].reason}`)
        console.log(`   Time: ${new Date(deathInfo[0].time).toISOString()}`)
      }

      messagesProcessed.dlq++
      console.log('✅ Dead letter recorded')
      showStats()

      return true
    } catch (error) {
      console.error('❌ Error processing dead letter:', error.message)
      return false
    }
  }

  function showStats () {
    const total = messagesProcessed.main + messagesProcessed.dlq
    if (total > 0) {
      console.log('\n📊 Statistics:')
      console.log(`   Messages processed successfully: ${messagesProcessed.main}`)
      console.log(`   Messages in the DLQ: ${messagesProcessed.dlq}`)
      console.log(`   Total messages: ${total}`)
      console.log('   Consumers are still active...\n')
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')

    const total = messagesProcessed.main + messagesProcessed.dlq
    if (total > 0) {
      console.log('\n📊 Final statistics:')
      console.log(`   Messages processed successfully: ${messagesProcessed.main}`)
      console.log(`   Messages in the DLQ: ${messagesProcessed.dlq}`)
      console.log(`   Total: ${total}`)
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
        showStats()
      }
    })

    // Set up the whole infrastructure with a single channel
    await setupInfrastructure()

    // Set up the consumers
    console.log('\n🔄 Setting up consumers...')

    console.log(`   Main queue consumer: ${QUEUE_NAME}`)
    await rabbitMQ.subscribe(QUEUE_NAME, handleMainMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'consumer-main-' + Date.now()
    })

    console.log(`   DLQ consumer: ${DLQ_NAME}`)
    await rabbitMQ.subscribe(DLQ_NAME, handleDLQMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'consumer-dlq-' + Date.now()
    })

    console.log('\n✅ Consumers set up successfully')
    console.log('\n⚡ Starting message consumption')
    console.log('   Messages marked to fail will go to the DLQ')
    console.log('   Statistics will be shown for each processed message')
    console.log('   Press Ctrl+C to exit\n')
  } catch (error) {
    console.error('❌ Fatal error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
