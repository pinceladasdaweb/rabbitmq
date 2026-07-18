import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'
import colors from '@colors/colors/safe.js'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-routing-consumption',
    channelPoolSize: 5
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let processStartTime = null
  const messagesByType = {
    direct: 0,
    topic: 0,
    fanout: 0,
    headers: 0,
    total: 0
  }

  function showStatus () {
    const runningTime = processStartTime ? (Date.now() - processStartTime) / 1000 : 0

    console.log(colors.yellow('\n📊 Current status:'))
    console.log(colors.red(`   Direct messages: ${messagesByType.direct}`))
    console.log(colors.green(`   Topic messages: ${messagesByType.topic}`))
    console.log(colors.blue(`   Fanout messages: ${messagesByType.fanout}`))
    console.log(colors.magenta(`   Headers messages: ${messagesByType.headers}`))
    console.log(colors.yellow(`   Total processed: ${messagesByType.total}`))
    console.log(colors.yellow(`   Running time: ${runningTime.toFixed(1)}s`))
    if (messagesByType.total > 0) {
      console.log(colors.yellow(`   Average: ${(messagesByType.total / runningTime).toFixed(2)} msgs/s`))
    }
    console.log(colors.yellow('   Consumer remains active waiting for messages...\n'))
  }

  async function handleDirectMessage (content, message, queueName) {
    if (!processStartTime) {
      processStartTime = Date.now()
    }

    messagesByType.direct++
    messagesByType.total++

    console.log(colors.red('\n📥 Direct message received:'))
    console.log(colors.red(`   Queue: ${queueName}`))
    console.log(colors.red(`   Content: ${content}`))
    console.log(colors.red(`   Timestamp: ${new Date().toISOString()}`))
  }

  async function handleTopicMessage (content, message, queueName) {
    if (!processStartTime) {
      processStartTime = Date.now()
    }

    messagesByType.topic++
    messagesByType.total++

    console.log(colors.green('\n📥 Topic message received:'))
    console.log(colors.green(`   Queue: ${queueName}`))
    console.log(colors.green(`   Content: ${content}`))
    console.log(colors.green(`   Timestamp: ${new Date().toISOString()}`))
  }

  async function handleFanoutMessage (content, message, queueName) {
    if (!processStartTime) {
      processStartTime = Date.now()
    }

    messagesByType.fanout++
    messagesByType.total++

    console.log(colors.blue('\n📥 Fanout message received:'))
    console.log(colors.blue(`   Queue: ${queueName}`))
    console.log(colors.blue(`   Content: ${content}`))
    console.log(colors.blue(`   Timestamp: ${new Date().toISOString()}`))
  }

  async function handleHeadersMessage (content, message, queueName) {
    if (!processStartTime) {
      processStartTime = Date.now()
    }

    messagesByType.headers++
    messagesByType.total++

    console.log(colors.magenta('\n📥 Headers message received:'))
    console.log(colors.magenta(`   Queue: ${queueName}`))
    console.log(colors.magenta(`   Content: ${content}`))
    console.log(colors.magenta('   Headers:'))

    if (message && message.properties && message.properties.headers) {
      Object.entries(message.properties.headers).forEach(([key, value]) => {
        console.log(colors.magenta(`      ${key}: ${value}`))
      })
    } else {
      console.log(colors.magenta('      No headers defined'))
    }

    console.log(colors.magenta(`   Timestamp: ${new Date().toISOString()}`))
  }

  async function setupConsumers () {
    try {
      console.log(colors.yellow('\n🔧 Setting up consumers...'))

      // 1. Direct Exchange consumers
      console.log(colors.red('\n📥 Setting up Direct Exchange consumers...'))
      await Promise.all([
        rabbitMQ.subscribe('direct.error', message => handleDirectMessage(message, message.properties, 'direct.error')),
        rabbitMQ.subscribe('direct.warning', message => handleDirectMessage(message, message.properties, 'direct.warning')),
        rabbitMQ.subscribe('direct.info', message => handleDirectMessage(message, message.properties, 'direct.info'))
      ])
      console.log(colors.red('   ✓ Direct Exchange consumers configured'))

      // 2. Topic Exchange consumers
      console.log(colors.green('\n📥 Setting up Topic Exchange consumers...'))
      await Promise.all([
        rabbitMQ.subscribe('topic.system.*', message => handleTopicMessage(message, message.properties, 'topic.system.*')),
        rabbitMQ.subscribe('topic.user.#', message => handleTopicMessage(message, message.properties, 'topic.user.#')),
        rabbitMQ.subscribe('topic.all', message => handleTopicMessage(message, message.properties, 'topic.all'))
      ])
      console.log(colors.green('   ✓ Topic Exchange consumers configured'))

      // 3. Fanout Exchange consumers
      console.log(colors.blue('\n📥 Setting up Fanout Exchange consumers...'))
      await Promise.all([
        rabbitMQ.subscribe('fanout.queue1', message => handleFanoutMessage(message, message.properties, 'fanout.queue1')),
        rabbitMQ.subscribe('fanout.queue2', message => handleFanoutMessage(message, message.properties, 'fanout.queue2')),
        rabbitMQ.subscribe('fanout.queue3', message => handleFanoutMessage(message, message.properties, 'fanout.queue3'))
      ])
      console.log(colors.blue('   ✓ Fanout Exchange consumers configured'))

      // 4. Headers Exchange consumers
      console.log(colors.magenta('\n📥 Setting up Headers Exchange consumers...'))
      await Promise.all([
        rabbitMQ.subscribe('headers.logs.json', message => handleHeadersMessage(message, message.properties, 'headers.logs.json')),
        rabbitMQ.subscribe('headers.priority.high', message => handleHeadersMessage(message, message.properties, 'headers.priority.high')),
        rabbitMQ.subscribe('headers.errors', message => handleHeadersMessage(message, message.properties, 'headers.errors'))
      ])
      console.log(colors.magenta('   ✓ Headers Exchange consumers configured'))

      console.log(colors.yellow('\n✅ All consumers configured and active'))
      console.log(colors.yellow('\n⚡ Starting message consumption'))
      console.log(colors.yellow('   Consumer will show detailed info per message type'))
      console.log(colors.yellow('   Status is refreshed every minute'))
      console.log(colors.yellow('   Press Ctrl+C to exit\n'))
    } catch (error) {
      console.error(colors.red(`❌ Error setting up consumers: ${error.message}`))
      throw error
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log(colors.yellow('\n👋 Starting graceful shutdown...'))

    if (messagesByType.total > 0) {
      const runningTime = (Date.now() - processStartTime) / 1000
      console.log(colors.yellow('\n📊 Final statistics:'))
      console.log(colors.red(`   Direct messages: ${messagesByType.direct}`))
      console.log(colors.green(`   Topic messages: ${messagesByType.topic}`))
      console.log(colors.blue(`   Fanout messages: ${messagesByType.fanout}`))
      console.log(colors.magenta(`   Headers messages: ${messagesByType.headers}`))
      console.log(colors.yellow(`   Total messages: ${messagesByType.total}`))
      console.log(colors.yellow(`   Total running time: ${runningTime.toFixed(1)}s`))
      console.log(colors.yellow(`   Processing average: ${(messagesByType.total / runningTime).toFixed(2)} msgs/s`))
    }

    try {
      await rabbitMQ.disconnect()
      console.log(colors.yellow('\n✅ Connection closed successfully'))

      setTimeout(() => {
        process.exit(0)
      }, 100)
    } catch (error) {
      console.error(colors.red(`❌ Error during shutdown: ${error.message}`))
      process.exit(1)
    }
  }

  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)

  try {
    console.log(colors.yellow('📡 Establishing connection to RabbitMQ...'))
    await rabbitMQ.connect()
    console.log(colors.yellow('✅ Connection established successfully'))

    rabbitMQ.on('disconnected', () => {
      if (!shutdownInProgress) {
        console.log(colors.red('\n❌ Disconnected from RabbitMQ'))
      }
    })

    rabbitMQ.on('reconnected', () => {
      if (!shutdownInProgress) {
        console.log(colors.yellow('\n🔄 Reconnected to RabbitMQ'))
        showStatus()
      }
    })

    // Sets up the consumers
    await setupConsumers()

    // Periodic status
    const statusInterval = setInterval(() => {
      if (!shutdownInProgress && messagesByType.total > 0) {
        showStatus()
      }
    }, 60000)

    // Cleanup on exit
    process.on('exit', () => {
      clearInterval(statusInterval)
    })
  } catch (error) {
    console.error(colors.red(`❌ Error: ${error.message}`))
    await shutdown()
  }
}

main().catch(error => {
  console.error(colors.red(`🔥 Unhandled error: ${error}`))
  process.exit(1)
})
