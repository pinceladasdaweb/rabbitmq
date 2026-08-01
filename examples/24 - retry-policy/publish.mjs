// Publishes one message to each of the three queues set up by consumer.mjs.
//
// The payloads are identical — the difference in what happens to them comes
// entirely from each subscription's `retryPolicy`.
//
// Start `node consumer.mjs` first, then run this.

import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-retry-policy-publish',
    exchange: {
      name: 'example-retry-exchange',
      type: 'direct'
    },
    deadLetterExchange: 'dlx'
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false

  const TARGETS = [
    { queue: 'retry-none-queue', id: 'msg-none', describes: "retryPolicy: 'none' — dies on the first failure" },
    { queue: 'retry-once-queue', id: 'msg-once', describes: "retryPolicy: 'once' — retried once, then dies" },
    { queue: 'retry-opt-out-queue', id: 'msg-opt-out', describes: "retryPolicy: 'once', declined by error.retryable = false" }
  ]

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

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

    console.log('\n🚀 Publishing one failing message per policy...\n')

    for (const target of TARGETS) {
      // The routing key matches the queue name: consumer.mjs binds each queue
      // to the exchange under its own name.
      await rabbitMQ.publish(target.queue, {
        id: target.id,
        text: 'This payload always fails — watch how each policy handles it'
      }, {
        persistent: true,
        messageId: target.id
      })

      console.log(`📨 ${target.id} -> ${target.queue}`)
      console.log(`   ${target.describes}`)
    }

    console.log('\n✨ Published. Watch the consumer terminal:')
    console.log("   'once' is the only one that shows a second attempt")
    console.log('   (its redelivery arrives with redelivered: true)')

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
