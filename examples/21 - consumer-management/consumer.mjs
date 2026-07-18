import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

// Demonstrates consumer lifecycle management:
//
//   1. unsubscribe(consumerTag): cancels a consumer without disconnecting —
//      the remaining messages stay in the queue.
//   2. Consumer events: consumerCancelled (broker cancelled it, e.g. queue
//      deleted), consumerRecovered (successfully recreated) and consumerLost
//      (recovery attempts exhausted).
//   3. enableGracefulShutdown(): the lib's native graceful shutdown.

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-consumer-management-consumer',
    exchange: {
      name: 'management-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  const QUEUE_NAME = 'management-queue'
  const TEMP_QUEUE = 'management-temp-queue'
  const STOP_AFTER = 5

  let processed = 0

  // Native graceful shutdown: CTRL+C disconnects and exits the process.
  rabbitMQ.enableGracefulShutdown()

  // Consumer lifecycle events
  rabbitMQ.on('consumerCancelled', ({ queueName }) => {
    console.log(`\n⚠️  consumerCancelled event: the broker cancelled the consumer of queue '${queueName}'`)
    console.log('   The lib will try to recreate it automatically...')
  })

  rabbitMQ.on('consumerRecovered', ({ queueName }) => {
    console.log(`\n♻️  consumerRecovered event: consumer of queue '${queueName}' was recreated`)
  })

  rabbitMQ.on('consumerLost', ({ queueName }) => {
    console.log(`\n💀 consumerLost event: consumer of queue '${queueName}' could not be recovered`)
    console.log('   (expected in this example: the queue was deleted on purpose)')
  })

  async function setupInfrastructure () {
    const channel = await rabbitMQ.getChannel()

    console.log('\n🔧 Setting up infrastructure...')

    await channel.assertQueue(QUEUE_NAME, { durable: true })
    await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'management-route')

    await channel.assertQueue(TEMP_QUEUE, { durable: false })

    console.log('✅ Infrastructure set up successfully')
  }

  async function demonstrateUnsubscribe () {
    console.log(`\n👂 Consuming '${QUEUE_NAME}' — will stop after ${STOP_AFTER} messages...`)

    const consumer = await rabbitMQ.subscribe(QUEUE_NAME, async (content) => {
      processed++
      console.log(`   📩 [${processed}] ${content.text}`)

      if (processed === STOP_AFTER) {
        // unsubscribe inside the callback itself: the current message is
        // finished and no other message is delivered to this consumer.
        const removed = await rabbitMQ.unsubscribe(consumer.consumerTag)

        console.log(`\n✂️  unsubscribe(${consumer.consumerTag}) → ${removed}`)
        console.log('   Consumption stopped; the remaining messages stay in the queue.')
      }
    }, { prefetchCount: 1 })
  }

  async function demonstrateBrokerCancel () {
    console.log(`\n👂 Consuming '${TEMP_QUEUE}' to demonstrate a broker-initiated cancel...`)

    await rabbitMQ.subscribe(TEMP_QUEUE, async () => {})

    await new Promise(resolve => setTimeout(resolve, 1000))

    console.log(`\n🗑️  Deleting queue '${TEMP_QUEUE}' to force the cancellation...`)

    const channel = await rabbitMQ.getChannel()
    await channel.deleteQueue(TEMP_QUEUE)
  }

  try {
    console.log('📡 Connecting to RabbitMQ...')
    await rabbitMQ.connect({ waitForConnection: true, timeout: 15000 })
    console.log('✅ Connection established successfully')

    await setupInfrastructure()
    await demonstrateUnsubscribe()
    await demonstrateBrokerCancel()

    console.log('\nℹ️  Waiting for events... Press CTRL+C to exit')
    console.log('   (graceful shutdown is handled by the lib itself)')
  } catch (error) {
    console.error('❌ Error:', error.message)

    await rabbitMQ.disconnect()
    process.exit(1)
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
