import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-connection',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    reconnectInterval: 1000, // Starts trying to reconnect every 1 second
    maxReconnectInterval: 5000, // Maximum of 5 seconds between attempts
    maxReconnectAttempts: 3, // Total attempts before giving up
    logger: {
      info: (msg) => {
        if (shutdownInProgress && (msg.includes('Reconnecting') || msg.includes('closed'))) {
          return
        }
        console.log(`[INFO] ${new Date().toISOString()} ${msg}`)
      },
      error: (msg) => {
        if (shutdownInProgress && (msg.includes('closed') || msg.includes('Connection'))) {
          return
        }
        console.error(`[ERROR] ${new Date().toISOString()} ${msg}`)
      },
      warn: (msg) => console.warn(`[WARN] ${new Date().toISOString()} ${msg}`)
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const startTime = Date.now()

  function getUptime () {
    const diff = Date.now() - startTime
    const seconds = Math.floor(diff / 1000)
    const minutes = Math.floor(seconds / 60)
    return `${minutes}m ${seconds % 60}s`
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log(`   Total execution time: ${getUptime()}`)

    try {
      await rabbitMQ.disconnect()
      console.log('✅ Gracefully disconnected from RabbitMQ')

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
    console.log('\n🚀 Starting connection/reconnection test...')
    console.log('   Settings:')
    console.log(`   - Initial interval: ${rabbitConfig.reconnectInterval}ms`)
    console.log(`   - Maximum interval: ${rabbitConfig.maxReconnectInterval}ms`)
    console.log(`   - Maximum attempts: ${rabbitConfig.maxReconnectAttempts}\n`)

    // Connection events
    rabbitMQ.on('connected', () => {
      const status = rabbitMQ.getClusterStatus()
      console.log('\n📬 Successfully connected to RabbitMQ!')
      console.log(`   Uptime: ${getUptime()}`)
      console.log('   Cluster status:', status)
    })

    rabbitMQ.on('disconnected', () => {
      if (!shutdownInProgress) {
        console.log('\n❌ Disconnected from RabbitMQ')
        console.log(`   Uptime: ${getUptime()}`)
        console.log('   Starting reconnection attempts...')
      }
    })

    rabbitMQ.on('reconnected', () => {
      if (!shutdownInProgress) {
        console.log('\n🔄 Reconnected to RabbitMQ')
        console.log(`   Uptime: ${getUptime()}`)

        const status = rabbitMQ.getClusterStatus()
        console.log('   Current status:', status)
      }
    })

    rabbitMQ.on('reconnectFailed', () => {
      if (!shutdownInProgress) {
        console.log('\n💔 All reconnection attempts failed')
        console.log(`   Uptime: ${getUptime()}`)
        console.log(`   All ${rabbitConfig.maxReconnectAttempts} attempts failed`)
        process.exit(1)
      }
    })

    console.log('📡 Attempting to establish connection...')
    await rabbitMQ.connect()

    console.log('\n📋 Instructions:')
    console.log('   1. Keep the application running')
    console.log('   2. Take down RabbitMQ to test reconnection')
    console.log(`   3. Watch the ${rabbitConfig.maxReconnectAttempts} reconnection attempts`)
    console.log('   4. The application will exit after all attempts fail')
    console.log('   5. Press CTRL+C to shut down gracefully\n')

    // Keeps the application running
    await new Promise(resolve => {
      setTimeout(() => {
        console.log('\n⏱️  Maximum execution time reached')
        shutdown()
      }, 600000) // 10 minutes
    })
  } catch (error) {
    console.error('\n❌ Error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Fatal error:', error)
  process.exit(1)
})
