import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'producer',
    exchange: {
      name: 'balance-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  const QUEUE_NAME = 'balanced-queue'
  const TOTAL_MESSAGES = 100

  try {
    await rabbitMQ.connect()
    console.log('✅ Producer connected')

    const channel = await rabbitMQ.getChannel()

    await channel.assertExchange(rabbitConfig.exchange.name, rabbitConfig.exchange.type, { durable: true })
    await channel.assertQueue(QUEUE_NAME, { durable: true })
    await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'balanced-route')

    console.log(`\n📨 Publishing ${TOTAL_MESSAGES} messages...`)

    for (let i = 1; i <= TOTAL_MESSAGES; i++) {
      const message = {
        id: i,
        content: `Message ${i}`,
        timestamp: Date.now()
      }

      await rabbitMQ.publish('balanced-route', message)
      console.log(`   ✓ Message ${i} published`)

      await new Promise(resolve => setTimeout(resolve, 100))
    }

    console.log('\n✨ All messages published')
    await rabbitMQ.disconnect()
    process.exit(0)
  } catch (error) {
    console.error('❌ Error:', error)
    process.exit(1)
  }
}

main()
