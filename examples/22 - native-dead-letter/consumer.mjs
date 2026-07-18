import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

// Consumes the 'orders' queue demonstrating the three paths to the DLQ:
//
//   1. Automatic nack: the callback throws → the lib nacks without requeue
//      and the broker routes the message to 'orders_dlq' via the DLX.
//   2. moveToDeadLetter(): manual quarantine — copies the message to the DLQ
//      with tracking headers (x-death-reason etc.) and the original is ack'ed.
//   3. processDeadLetterQueue(): consumes the DLQ for inspection/reprocessing.

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-native-dlq-consumer',
    exchange: {
      name: 'native-dlq-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const QUEUE_NAME = 'orders'

  const stats = {
    processed: 0,
    rejected: 0,
    quarantined: 0,
    deadLettersInspected: 0
  }

  async function setupInfrastructure () {
    console.log('\n🔧 Ensuring infrastructure...')

    await rabbitMQ.setupDeadLetterExchange()
    await rabbitMQ.createQueue(QUEUE_NAME)

    const channel = await rabbitMQ.getChannel()
    await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, QUEUE_NAME)

    console.log('✅ Infrastructure ready')
  }

  async function startMainConsumer () {
    console.log(`\n👂 Consuming '${QUEUE_NAME}'...`)

    await rabbitMQ.subscribe(QUEUE_NAME, async (content, message) => {
      console.log(`\n📩 Order ${content.id}: ${content.item}`)

      if (content.quarantine) {
        // Manual DLQ: copies to 'orders_dlq' with the reason in the headers;
        // on return, the lib acks the original and it leaves the main queue.
        await rabbitMQ.moveToDeadLetter(message, 'Manual quarantine: suspicious order')

        stats.quarantined++
        console.log('   🔒 Manually moved to the DLQ (quarantine)')

        return
      }

      if (content.shouldFail) {
        stats.rejected++
        console.log('   💥 Processing will fail → nack → automatic DLQ')

        throw new Error(`Failed to process order ${content.id}`)
      }

      stats.processed++
      console.log('   ✅ Processed successfully')
    })
  }

  async function startDeadLetterProcessor () {
    console.log(`\n🕵️  Processing the DLQ '${QUEUE_NAME}_dlq'...`)

    await rabbitMQ.processDeadLetterQueue(QUEUE_NAME, async (content) => {
      stats.deadLettersInspected++

      console.log(`\n☠️  Message in the DLQ: order ${content.id} (${content.item})`)
      console.log('   Inspection/alerting/reprocessing logic would go here')
    })
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Stats:')
    console.log(`   Processed successfully: ${stats.processed}`)
    console.log(`   Rejected (nack → DLQ): ${stats.rejected}`)
    console.log(`   Quarantined (moveToDeadLetter): ${stats.quarantined}`)
    console.log(`   Inspected in the DLQ: ${stats.deadLettersInspected}`)

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
    console.log('📡 Connecting to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully')

    await setupInfrastructure()
    await startMainConsumer()
    await startDeadLetterProcessor()

    console.log('\nℹ️  Waiting for messages... Press CTRL+C to exit')
  } catch (error) {
    console.error('❌ Error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
