import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-routing-publication',
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const messagesPublished = {
    direct: 0,
    topic: 0,
    fanout: 0,
    headers: 0
  }

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()
      console.log('\n🔧 Setting up infrastructure...')

      // 1. Direct Exchange
      console.log('\n📬 Setting up Direct Exchange...')
      await channel.assertExchange('direct-exchange', 'direct', { durable: true })
      await channel.assertQueue('direct.error', { durable: true })
      await channel.assertQueue('direct.warning', { durable: true })
      await channel.assertQueue('direct.info', { durable: true })
      await channel.bindQueue('direct.error', 'direct-exchange', 'error')
      await channel.bindQueue('direct.warning', 'direct-exchange', 'warning')
      await channel.bindQueue('direct.info', 'direct-exchange', 'info')
      console.log('   ✓ Direct Exchange configured')

      // 2. Topic Exchange
      console.log('\n📬 Setting up Topic Exchange...')
      await channel.assertExchange('topic-exchange', 'topic', { durable: true })
      await channel.assertQueue('topic.system.*', { durable: true })
      await channel.assertQueue('topic.user.#', { durable: true })
      await channel.assertQueue('topic.all', { durable: true })
      await channel.bindQueue('topic.system.*', 'topic-exchange', 'system.*')
      await channel.bindQueue('topic.user.#', 'topic-exchange', 'user.#')
      await channel.bindQueue('topic.all', 'topic-exchange', '#')
      console.log('   ✓ Topic Exchange configured')

      // 3. Fanout Exchange
      console.log('\n📬 Setting up Fanout Exchange...')
      await channel.assertExchange('fanout-exchange', 'fanout', { durable: true })
      await channel.assertQueue('fanout.queue1', { durable: true })
      await channel.assertQueue('fanout.queue2', { durable: true })
      await channel.assertQueue('fanout.queue3', { durable: true })
      await channel.bindQueue('fanout.queue1', 'fanout-exchange', '')
      await channel.bindQueue('fanout.queue2', 'fanout-exchange', '')
      await channel.bindQueue('fanout.queue3', 'fanout-exchange', '')
      console.log('   ✓ Fanout Exchange configured')

      // 4. Headers Exchange
      console.log('\n📬 Setting up Headers Exchange...')
      await channel.assertExchange('headers-exchange', 'headers', { durable: true })

      // Queue that accepts messages with format=json and type=logs
      await channel.assertQueue('headers.logs.json', { durable: true })
      await channel.bindQueue('headers.logs.json', 'headers-exchange', '', {
        'x-match': 'all', // requires matching on every header
        format: 'json',
        type: 'logs'
      })

      // Queue that accepts any message with priority=high
      await channel.assertQueue('headers.priority.high', { durable: true })
      await channel.bindQueue('headers.priority.high', 'headers-exchange', '', {
        'x-match': 'any', // requires matching on at least one header
        priority: 'high',
        urgent: 'true'
      })

      // Queue that accepts error messages in any format
      await channel.assertQueue('headers.errors', { durable: true })
      await channel.bindQueue('headers.errors', 'headers-exchange', '', {
        'x-match': 'any',
        type: 'error',
        severity: 'high'
      })

      console.log('   ✓ Headers Exchange configured')
      console.log('\n✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function publishMessages () {
    try {
      // 1. Publishing to the Direct Exchange
      console.log('\n📨 Publishing messages to the Direct Exchange...')
      rabbitMQ.setExchange('direct-exchange', 'direct')
      const directMessages = [
        { level: 'error', content: 'Critical system error' },
        { level: 'warning', content: 'Low memory alert' },
        { level: 'info', content: 'System started successfully' }
      ]
      for (const msg of directMessages) {
        await rabbitMQ.publish(msg.level, msg.content)
        console.log(`   ✓ Message sent to routing key: ${msg.level}`)
        messagesPublished.direct++
      }

      // 2. Publishing to the Topic Exchange
      console.log('\n📨 Publishing messages to the Topic Exchange...')
      rabbitMQ.setExchange('topic-exchange', 'topic')
      const topicMessages = [
        { route: 'system.error', content: 'Operating system error' },
        { route: 'system.status', content: 'System status: OK' },
        { route: 'user.login.failed', content: 'User login failed' },
        { route: 'user.profile.updated', content: 'Profile updated' }
      ]
      for (const msg of topicMessages) {
        await rabbitMQ.publish(msg.route, msg.content)
        console.log(`   ✓ Message sent to routing key: ${msg.route}`)
        messagesPublished.topic++
      }

      // 3. Publishing to the Fanout Exchange
      console.log('\n📨 Publishing messages to the Fanout Exchange...')
      rabbitMQ.setExchange('fanout-exchange', 'fanout')
      const fanoutMessages = [
        { id: 1, content: 'Broadcast message 1' },
        { id: 2, content: 'Broadcast message 2' }
      ]
      for (const msg of fanoutMessages) {
        await rabbitMQ.publish('', msg.content)
        console.log(`   ✓ Broadcast message sent (ID: ${msg.id})`)
        messagesPublished.fanout++
      }

      // 4. Publishing to the Headers Exchange
      console.log('\n📨 Publishing messages to the Headers Exchange...')
      rabbitMQ.setExchange('headers-exchange', 'headers')

      const headersMessages = [
        {
          content: '{"level": "error", "message": "System crash"}',
          headers: {
            format: 'json',
            type: 'logs'
          }
        },
        {
          content: 'Critical security alert!',
          headers: {
            priority: 'high',
            urgent: 'true'
          }
        },
        {
          content: 'Database connection error',
          headers: {
            type: 'error',
            severity: 'high'
          }
        }
      ]

      for (const msg of headersMessages) {
        await rabbitMQ.publish('', msg.content, { headers: msg.headers })
        console.log('   ✓ Message sent with headers:', msg.headers)
        messagesPublished.headers++
      }

      console.log('\n✅ All messages were published successfully!')
    } catch (error) {
      console.error('❌ Error publishing messages:', error.message)
      throw error
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Publication statistics:')
    console.log(`   Direct messages: ${messagesPublished.direct}`)
    console.log(`   Topic messages: ${messagesPublished.topic}`)
    console.log(`   Fanout messages: ${messagesPublished.fanout}`)
    console.log(`   Headers messages: ${messagesPublished.headers}`)
    console.log(`   Total: ${Object.values(messagesPublished).reduce((a, b) => a + b, 0)}`)

    try {
      await rabbitMQ.disconnect()
      console.log('\n✅ Connection closed successfully')
      setTimeout(() => process.exit(0), 100)
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
    await setupInfrastructure()
    console.log('\n🚀 Starting routing demonstration...')
    await publishMessages()
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
