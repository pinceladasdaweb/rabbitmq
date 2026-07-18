import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-consumer-management-producer',
    exchange: {
      name: 'management-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  const QUEUE_NAME = 'management-queue'
  const ROUTING_KEY = 'management-route'
  const TOTAL_MESSAGES = 10

  // The lib's native graceful shutdown: registers SIGINT/SIGTERM,
  // disconnects and exits the process — no manual boilerplate.
  rabbitMQ.enableGracefulShutdown()

  async function setupInfrastructure () {
    const channel = await rabbitMQ.getChannel()

    console.log('\n🔧 Setting up infrastructure...')

    await channel.assertQueue(QUEUE_NAME, { durable: true })
    await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, ROUTING_KEY)

    console.log('✅ Infrastructure set up successfully')
  }

  try {
    console.log('📡 Connecting to RabbitMQ...')

    // waitForConnection: if every endpoint fails right now, wait for the
    // background reconnection instead of returning null (15s cap).
    await rabbitMQ.connect({ waitForConnection: true, timeout: 15000 })
    console.log('✅ Connection established successfully')

    await setupInfrastructure()

    console.log(`\n🚀 Publishing ${TOTAL_MESSAGES} messages...`)

    for (let i = 1; i <= TOTAL_MESSAGES; i++) {
      await rabbitMQ.publish(ROUTING_KEY, {
        id: i,
        text: `Message ${i} of ${TOTAL_MESSAGES}`,
        timestamp: Date.now()
      })

      console.log(`   ✓ Message ${i} published`)

      await new Promise(resolve => setTimeout(resolve, 200))
    }

    console.log('\n✅ All messages have been published!')

    await rabbitMQ.disconnect()
    console.log('👋 Disconnected')

    process.exit(0)
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
