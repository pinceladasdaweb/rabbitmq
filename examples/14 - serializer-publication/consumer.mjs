import xml2js from 'xml2js'
import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-serializer-consumption',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const messagesProcessed = {
    xml: 0,
    pipe: 0,
    json: 0
  }

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()

      console.log('\n🔧 Setting up infrastructure...')

      await channel.assertQueue('xml-messages', { durable: true })
      await channel.bindQueue('xml-messages', rabbitConfig.exchange.name, 'xml-route')

      await channel.assertQueue('pipe-messages', { durable: true })
      await channel.bindQueue('pipe-messages', rabbitConfig.exchange.name, 'pipe-route')

      await channel.assertQueue('json-messages', { durable: true })
      await channel.bindQueue('json-messages', rabbitConfig.exchange.name, 'json-route')

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function handleXmlMessage (content, message) {
    try {
      console.log('\n📥 Processing message from the XML queue:')
      console.log('   XML received:', content.toString())

      const parser = new xml2js.Parser({
        explicitArray: false,
        explicitRoot: false,
        mergeAttrs: true
      })

      const data = await parser.parseStringPromise(content.toString())
      console.log('   Object after parsing:', JSON.stringify(data, null, 2))

      // Extract the data from the message object
      const messageData = data.message || data

      console.log('\n   Extracted data:')
      console.log('   ID:', messageData.id)
      console.log('   Type:', messageData.type)
      console.log('   Text:', messageData.text)
      console.log('   Timestamp:', messageData.timestamp)
      if (messageData.metadata) {
        console.log('   Metadata:', JSON.stringify(messageData.metadata))
      }

      console.log('   Content-Type:', message.properties.contentType)

      messagesProcessed.xml++
      showStats()
      return true
    } catch (error) {
      console.error('❌ Error processing XML message:', error.message)
      console.error('   Received content:', content.toString())
      return false
    }
  }

  async function handlePipeMessage (content, message) {
    try {
      console.log('\n📥 Processing message from the PIPE queue:')
      const pipeString = content.toString()
      console.log('   Received string:', pipeString)

      const [id, type, text, timestamp] = pipeString.split('|')
      const data = {
        id: parseInt(id),
        type,
        text,
        timestamp: new Date(timestamp)
      }

      console.log('   Extracted data:')
      console.log('   ID:', data.id)
      console.log('   Type:', data.type)
      console.log('   Text:', data.text)
      console.log('   Timestamp:', data.timestamp)

      console.log('   Content-Type:', message.properties.contentType)

      messagesProcessed.pipe++
      showStats()
      return true
    } catch (error) {
      console.error('❌ Error processing PIPE message:', error.message)
      return false
    }
  }

  async function handleJsonMessage (content, message) {
    try {
      console.log('\n📥 Processing message from the JSON queue:')
      console.log('   Received data:')
      console.log('   ID:', content.id)
      console.log('   Type:', content.type)
      console.log('   Text:', content.text)
      console.log('   Timestamp:', content.timestamp)
      if (content.metadata) {
        console.log('   Metadata:', JSON.stringify(content.metadata))
      }

      console.log('   Content-Type:', message.properties.contentType)

      messagesProcessed.json++
      showStats()
      return true
    } catch (error) {
      console.error('❌ Error processing JSON message:', error.message)
      return false
    }
  }

  function showStats () {
    const total = messagesProcessed.xml + messagesProcessed.pipe + messagesProcessed.json
    console.log('\n📊 Statistics:')
    console.log(`   Messages from the XML queue: ${messagesProcessed.xml}`)
    console.log(`   Messages from the PIPE queue: ${messagesProcessed.pipe}`)
    console.log(`   Messages from the JSON queue: ${messagesProcessed.json}`)
    console.log(`   Total processed: ${total}`)
    console.log('   Consumers remain active...\n')
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    showStats()

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

    await setupInfrastructure()

    console.log('\n🔄 Setting up consumers...')

    await rabbitMQ.subscribe('xml-messages', handleXmlMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'consumer-xml-' + Date.now()
    })

    await rabbitMQ.subscribe('pipe-messages', handlePipeMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'consumer-pipe-' + Date.now()
    })

    await rabbitMQ.subscribe('json-messages', handleJsonMessage, {
      prefetchCount: 1,
      noAck: false,
      consumerTag: 'consumer-json-' + Date.now()
    })

    console.log('\n✅ Consumers set up successfully')
    console.log('\n⚡ Starting message consumption')
    console.log('   Processing messages from different queues')
    console.log('   Statistics refreshed on every message')
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
