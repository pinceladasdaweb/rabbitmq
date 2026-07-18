import xml2js from 'xml2js'
import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-serializer-publish',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const messagesPublished = {
    xml: 0,
    pipe: 0,
    json: 0
  }

  async function setupQueues () {
    try {
      const channel = await rabbitMQ.getChannel()

      console.log('\n🔧 Setting up queues...')

      await channel.assertQueue('xml-messages', { durable: true })
      await channel.bindQueue('xml-messages', rabbitConfig.exchange.name, 'xml-route')

      await channel.assertQueue('pipe-messages', { durable: true })
      await channel.bindQueue('pipe-messages', rabbitConfig.exchange.name, 'pipe-route')

      await channel.assertQueue('json-messages', { durable: true })
      await channel.bindQueue('json-messages', rabbitConfig.exchange.name, 'json-route')

      console.log('✅ Queues set up successfully')
    } catch (error) {
      console.error('❌ Error setting up queues:', error.message)
      throw error
    }
  }

  async function publishMessage (message, type) {
    try {
      console.log(`\n📨 Publishing message (${type})...`)
      console.log('   Original data:', JSON.stringify(message, null, 2))

      switch (type) {
        case 'xml': {
          const builder = new xml2js.Builder({
            rootName: 'message',
            xmldec: { version: '1.0', encoding: 'UTF-8' },
            renderOpts: { pretty: true }
          })
          const xmlData = builder.buildObject(message)

          console.log('\n📝 Generated XML:')
          console.log(xmlData)

          await rabbitMQ.publish('xml-route', xmlData, {
            contentType: 'application/xml'
          })
          messagesPublished.xml++
          break
        }

        case 'pipe': {
          const pipeData = `${message.id}|${message.type}|${message.text}|${new Date(message.timestamp).toISOString()}`

          console.log('\n📝 Generated pipe-separated format:')
          console.log(pipeData)

          await rabbitMQ.publish('pipe-route', pipeData, {
            contentType: 'text/pipe-separated'
          })
          messagesPublished.pipe++
          break
        }

        case 'json': {
          console.log('\n📝 JSON to be sent:')
          console.log(JSON.stringify(message, null, 2))

          await rabbitMQ.publish('json-route', message, {
            contentType: 'application/json'
          })
          messagesPublished.json++
          break
        }
      }

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
    console.log('\n📊 Publishing statistics:')
    console.log(`   XML messages: ${messagesPublished.xml}`)
    console.log(`   Pipe messages: ${messagesPublished.pipe}`)
    console.log(`   JSON messages: ${messagesPublished.json}`)
    console.log(`   Total: ${messagesPublished.xml + messagesPublished.pipe + messagesPublished.json}`)

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

    await setupQueues()

    const baseMessage = {
      id: 1,
      type: 'TEST',
      text: 'Test message with custom serialization',
      timestamp: new Date(),
      metadata: {
        source: 'example-serialization',
        version: '1.0'
      }
    }

    console.log('\n🚀 Starting serialization demonstration...')

    // Publish as XML
    await publishMessage(baseMessage, 'xml')
    await new Promise(resolve => setTimeout(resolve, 1000))

    // Publish in pipe-separated format
    await publishMessage(baseMessage, 'pipe')
    await new Promise(resolve => setTimeout(resolve, 1000))

    // Publish as JSON for comparison
    await publishMessage(baseMessage, 'json')

    console.log('\n✨ Demonstration completed!')

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
