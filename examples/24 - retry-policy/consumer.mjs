// Demonstrates the `retryPolicy` subscribe option side by side.
//
// Three consumers process the exact same failing payloads, differing only in
// their failure policy:
//
//   retry-none-queue  retryPolicy: 'none' (the default)  -> DLQ on first failure
//   retry-once-queue  retryPolicy: 'once'                -> one retry, then DLQ
//   retry-opt-out     retryPolicy: 'once' + retryable    -> handler declines the retry
//
// Each queue has its own DLQ consumer, so you can watch where every message
// ends up and how many attempts it took to get there.
//
// Run the consumer first, then `node publish.mjs` in another terminal.

import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-retry-policy-consumer',
    exchange: {
      name: 'example-retry-exchange',
      type: 'direct'
    },
    deadLetterExchange: 'dlx'
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false

  const QUEUES = {
    none: 'retry-none-queue',
    once: 'retry-once-queue',
    optOut: 'retry-opt-out-queue'
  }

  // Attempts per message id, so the log can show that 'once' really delivers
  // a second time while 'none' never does.
  const attempts = new Map()
  const deadLettered = []

  async function setupInfrastructure () {
    console.log('\n🔧 Setting up infrastructure...')

    const channel = await rabbitMQ.getChannel()

    for (const [label, queueName] of Object.entries(QUEUES)) {
      // createQueue also declares `${queueName}_dlq` and binds it to the DLX,
      // which is what makes a nack without requeue land somewhere visible.
      await rabbitMQ.createQueue(queueName)
      await channel.bindQueue(queueName, rabbitConfig.exchange.name, queueName)

      console.log(`   ${label.padEnd(6)} -> ${queueName} (+ ${queueName}_dlq)`)
    }

    console.log('✅ Infrastructure set up successfully')
  }

  // The same handler for all three queues: it always fails. The only thing
  // that changes between subscriptions is the policy.
  function createFailingHandler (label, { permanent = false } = {}) {
    return async (content, message) => {
      const attempt = (attempts.get(content.id) || 0) + 1

      attempts.set(content.id, attempt)

      console.log(`\n📥 [${label}] message ${content.id} — attempt ${attempt}`)
      console.log(`   redelivered: ${message.fields.redelivered}`)

      const error = new Error(`Simulated failure on ${label}`)

      if (permanent) {
        // Tells the policy this failure will never succeed: skip the retry
        // even though the subscription allows one.
        error.retryable = false
        console.log('   ⛔ error.retryable = false — declining the retry')
      }

      throw error
    }
  }

  function createDlqHandler (label) {
    return async (content, message) => {
      const death = message.properties.headers?.['x-death']?.[0]
      const total = attempts.get(content.id) || 0

      deadLettered.push({ label, id: content.id, attempts: total })

      console.log(`\n💀 [${label}] message ${content.id} reached the DLQ`)
      console.log(`   delivery attempts before dying: ${total}`)

      if (death) {
        console.log(`   dead-lettered from: ${death.queue} (${death.reason})`)
      }

      showSummary()
    }
  }

  function showSummary () {
    console.log('\n📊 Dead-lettered so far:')

    for (const entry of deadLettered) {
      const plural = entry.attempts === 1 ? 'attempt' : 'attempts'

      console.log(`   ${entry.label.padEnd(20)} ${entry.id} after ${entry.attempts} ${plural}`)
    }

    console.log('')
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    showSummary()

    try {
      await rabbitMQ.disconnect()
      console.log('✅ Connection closed successfully')

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

    console.log('\n🔄 Setting up consumers...')

    // 1. Default policy: a failure goes straight to the DLQ, no second chance.
    await rabbitMQ.subscribe(QUEUES.none, createFailingHandler("policy 'none'"), {
      prefetchCount: 1
    })
    console.log(`   ${QUEUES.none} — retryPolicy: 'none' (default)`)

    // 2. One retry: the first delivery is requeued, the redelivery is
    //    dead-lettered. Two attempts total — never a hot loop.
    await rabbitMQ.subscribe(QUEUES.once, createFailingHandler("policy 'once'"), {
      prefetchCount: 1,
      retryPolicy: 'once'
    })
    console.log(`   ${QUEUES.once} — retryPolicy: 'once'`)

    // 3. Same 'once' subscription, but the handler marks the failure as
    //    permanent, so the retry is skipped. The policy is a ceiling: the
    //    handler can decline a retry, never demand one.
    await rabbitMQ.subscribe(QUEUES.optOut, createFailingHandler('retryable false', { permanent: true }), {
      prefetchCount: 1,
      retryPolicy: 'once'
    })
    console.log(`   ${QUEUES.optOut} — retryPolicy: 'once' + error.retryable = false`)

    for (const [label, queueName] of Object.entries(QUEUES)) {
      await rabbitMQ.subscribe(`${queueName}_dlq`, createDlqHandler(label), { prefetchCount: 1 })
    }

    console.log('\n✅ Consumers ready')
    console.log('\n⚡ Expected outcome once you run publish.mjs:')
    console.log("   'none'          -> 1 attempt,  then DLQ")
    console.log("   'once'          -> 2 attempts, then DLQ")
    console.log('   retryable false -> 1 attempt,  then DLQ')
    console.log('\n   Press Ctrl+C to exit\n')
  } catch (error) {
    console.error('❌ Fatal error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
