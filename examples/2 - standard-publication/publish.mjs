import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-standard-publication',
    exchange: {
      name: 'example-exchange',
      type: 'direct',
      options: {
        durable: true
      }
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false

  async function publishMessage (routingKey, message) {
    try {
      console.log('📨 Publishing message...')
      await rabbitMQ.publish(routingKey, message, {
        persistent: true,
        messageId: new Date().getTime().toString(),
        timestamp: new Date().getTime(),
        contentType: 'application/json'
      })
      console.log('✅ Message published and confirmed successfully!')
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
    // 1. Establishes the connection that will be reused
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully\n')

    // 2. Ensures the queue exists and is bound to the exchange
    const channel = await rabbitMQ.getChannel()
    await channel.assertQueue('example-queue', { durable: true })
    await channel.bindQueue('example-queue', rabbitConfig.exchange.name, 'test-route')

    // 3. Waits a moment to make sure everything is ready
    await new Promise(resolve => setTimeout(resolve, 1000))

    // 3. Sample messages for publication
    const messages = [
      { id: 1, text: 'First test message', timestamp: new Date() },
      { id: 2, text: 'Second test message', timestamp: new Date() },
      { id: 3, text: 'Third test message', timestamp: new Date() }
    ]

    console.log('📝 Starting message publication using the same connection...\n')

    // 4. Publishes all messages reusing the same connection
    for (const msg of messages) {
      const result = await publishMessage('test-route', msg)
      if (result) {
        console.log(`📦 Message ${msg.id} published: ${JSON.stringify(msg)}\n`)
      }
      // Small delay between messages for better visualization
      await new Promise(resolve => setTimeout(resolve, 1000))
    }

    console.log('✨ All messages have been published!')
    console.log('ℹ️  The connection remains open for more publications')
    console.log('ℹ️  Press Ctrl+C to exit and close the connection\n')

    // 5. Keeps the process and the connection alive for more publications
    await new Promise(resolve => setTimeout(resolve, 30000))

    // 6. If we got this far, closes the connection
    await shutdown()
  } catch (error) {
    console.error('❌ Error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Fatal error:', error)
  process.exit(1)
})
