import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-circuit-breaker',
    exchange: {
      name: 'example-exchange',
      type: 'direct'
    },
    circuitBreaker: {
      failureThreshold: 3, // Opens after 3 failures
      successThreshold: 2, // Closes after 2 successes
      timeout: 10000 // 10 second timeout
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  let totalAttempts = 0
  let successfulPublishes = 0
  let failedPublishes = 0

  // Event emitted on every circuit breaker state transition
  // (CLOSED → OPEN → HALF-OPEN → CLOSED), useful for observability.
  rabbitMQ.on('circuitBreakerStateChanged', (state) => {
    console.log(`\n🔔 Circuit breaker changed state: ${state}`)
    console.log(`   Details: ${JSON.stringify(rabbitMQ.getCircuitBreakerState())}`)
  })

  async function publishWithMonitoring (message) {
    totalAttempts++

    try {
      console.log(`\n📨 Publish attempt #${totalAttempts}`)
      console.log(`   Message: ${JSON.stringify(message)}`)

      // maxRetries: 1 so each failure counts immediately against the circuit breaker
      await rabbitMQ.publish('circuit-route', message, {
        persistent: true,
        messageId: new Date().getTime().toString(),
        timestamp: new Date().getTime(),
        maxRetries: 1
      })

      successfulPublishes++
      console.log('✅ Message published successfully!')
      return true
    } catch (error) {
      failedPublishes++

      if (error.code === 'CIRCUIT_OPEN') {
        console.error('⛔ Circuit breaker OPEN: publish rejected without touching the broker')
      } else {
        console.error('❌ Error publishing message:', error.message)
      }

      return false
    }
  }

  function showStats () {
    console.log('\n📊 Statistics:')
    console.log(`   Total attempts: ${totalAttempts}`)
    console.log(`   Successful publishes: ${successfulPublishes}`)
    console.log(`   Failures: ${failedPublishes}`)
    console.log(`   Success rate: ${((successfulPublishes / totalAttempts) * 100).toFixed(1)}%`)
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
    // 1. Establish the connection
    console.log('📡 Establishing connection to RabbitMQ...')
    await rabbitMQ.connect()
    console.log('✅ Connection established successfully')

    // 2. Initial circuit breaker configuration
    console.log('\n⚡ Circuit Breaker configured:')
    console.log(`   Failure threshold: ${rabbitConfig.circuitBreaker.failureThreshold}`)
    console.log(`   Success threshold: ${rabbitConfig.circuitBreaker.successThreshold}`)
    console.log(`   Timeout: ${rabbitConfig.circuitBreaker.timeout}ms`)

    // 3. Circuit breaker demonstration
    console.log('\n🚀 Starting circuit breaker demonstration...')

    // Phase 1: Successful publishes
    console.log('\n📝 Phase 1: Normal publishes')
    for (let i = 0; i < 2; i++) {
      await publishWithMonitoring({
        id: i + 1,
        text: `Test message ${i + 1}`,
        timestamp: new Date()
      })
      await new Promise(resolve => setTimeout(resolve, 1000))
    }

    // Phase 2: Forcing REAL failures to open the circuit breaker —
    // points to a nonexistent exchange, so the broker rejects the
    // publish and the failure is counted by the circuit breaker.
    console.log('\n📝 Phase 2: Forcing failures (nonexistent exchange)')
    rabbitMQ.setExchange('nonexistent-exchange')

    for (let i = 0; i < 3; i++) {
      await publishWithMonitoring({
        id: `fail-${i + 1}`,
        text: `Failing message ${i + 1}`,
        timestamp: new Date()
      })
      await new Promise(resolve => setTimeout(resolve, 1000))
    }

    // Phase 3: Attempt with the circuit breaker open — the publish is
    // rejected immediately, without even trying to talk to the broker.
    console.log('\n📝 Phase 3: Attempts with circuit breaker open')
    await publishWithMonitoring({
      id: 'after-failures',
      text: 'Attempt after failures',
      timestamp: new Date()
    })

    // Phase 4: Wait for the timeout, restore the valid exchange and try again
    console.log('\n⏳ Waiting for circuit breaker timeout...')
    await new Promise(resolve => setTimeout(resolve, rabbitConfig.circuitBreaker.timeout))

    rabbitMQ.setExchange(rabbitConfig.exchange.name, rabbitConfig.exchange.type)

    console.log('\n📝 Phase 4: Attempts after timeout (HALF-OPEN → CLOSED)')
    for (let i = 0; i < 3; i++) {
      await publishWithMonitoring({
        id: `recovery-${i + 1}`,
        text: `Recovery message ${i + 1}`,
        timestamp: new Date()
      })
      await new Promise(resolve => setTimeout(resolve, 1000))
    }

    console.log('\n✨ Circuit breaker demonstration completed!')
    await shutdown()
  } catch (error) {
    console.error('❌ Fatal error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
