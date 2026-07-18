import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'consumer-1',
    exchange: {
      name: 'balance-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  const QUEUE_NAME = 'balanced-queue'
  let messagesProcessed = 0
  const startTime = Date.now()

  function formatUptime () {
    const diff = Date.now() - startTime
    const seconds = Math.floor(diff / 1000)
    const minutes = Math.floor(seconds / 60)
    return `${minutes}m ${seconds % 60}s`
  }

  async function handleMessage (content) {
    // Simulates slower processing
    await new Promise(resolve => setTimeout(resolve, 500))

    messagesProcessed++
    console.log('\n📥 Consumer 1 received a message:')
    console.log(`   ID: ${content.id}`)
    console.log(`   Content: ${content.content}`)
    console.log(`   Uptime: ${formatUptime()}`)
    console.log(`   Total processed: ${messagesProcessed}`)
  }

  async function setupInfrastructure () {
    const channel = await rabbitMQ.getChannel()

    // Sets up the exchange and the queue
    await channel.assertExchange(rabbitConfig.exchange.name, rabbitConfig.exchange.type, { durable: true })
    await channel.assertQueue(QUEUE_NAME, { durable: true })
    await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'balanced-route')
  }

  try {
    await rabbitMQ.connect()
    console.log('✅ Consumer 1 connected')

    await setupInfrastructure()

    console.log(`\n🔄 Starting consumption on queue: ${QUEUE_NAME}`)
    await rabbitMQ.subscribe(QUEUE_NAME, handleMessage, {
      prefetchCount: 1, // Important: ensures fair distribution
      noAck: false // Enables manual acknowledgments
    })

    console.log('\n⚡ Consumer 1 waiting for messages...\n')

    // Keeps the process running
    process.on('SIGINT', async () => {
      console.log('\n🛑 Shutting down consumer 1...')
      await rabbitMQ.disconnect()
      process.exit(0)
    })
  } catch (error) {
    console.error('❌ Error:', error)
    process.exit(1)
  }
}

main()
