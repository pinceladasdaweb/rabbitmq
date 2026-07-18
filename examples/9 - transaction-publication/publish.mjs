import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

// The AMQP protocol has transactions (tx.select/tx.commit), but amqplib does
// not expose them on purpose: publisher confirms cover the same use cases with
// much better performance. This example demonstrates transactional publishing
// in the practical sense — "all-or-nothing":
//
//   1. All messages of a business transaction are published as a batch
//      (publishBatch) and confirmed by the broker at once.
//   2. If any confirmation fails, the whole batch is retried.
//   3. Once retries are exhausted, a compensation (rollback) event is
//      published so consumers can undo what was done.

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-transaction-producer',
    exchange: {
      name: 'transaction-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const QUEUE_NAME = 'transaction-queue'
  const ROUTING_KEY = 'transaction-route'
  const TOTAL_TRANSACTIONS = 3

  const stats = {
    committed: 0,
    rolledBack: 0,
    messagesPublished: 0
  }

  async function setupInfrastructure () {
    try {
      const channel = await rabbitMQ.getChannel()
      console.log('\n🔧 Setting up infrastructure...')

      console.log(`   Creating queue: ${QUEUE_NAME}`)
      await channel.assertQueue(QUEUE_NAME, {
        durable: true
      })

      console.log('   Binding queue to exchange...')
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, ROUTING_KEY)

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  // A "business transaction": events that only make sense together.
  function buildTransactionMessages (transactionId) {
    const orderId = `order-${transactionId}`

    return [
      {
        event: 'order.created',
        transactionId,
        orderId,
        items: [{ sku: 'SKU-123', quantity: 2 }],
        timestamp: Date.now()
      },
      {
        event: 'stock.reserved',
        transactionId,
        orderId,
        reservation: { sku: 'SKU-123', quantity: 2 },
        timestamp: Date.now()
      },
      {
        event: 'payment.requested',
        transactionId,
        orderId,
        amount: 199.9,
        currency: 'BRL',
        timestamp: Date.now()
      }
    ]
  }

  async function publishCompensation (transactionId, reason) {
    try {
      await rabbitMQ.publish(ROUTING_KEY, {
        event: 'transaction.rolled-back',
        transactionId,
        reason,
        timestamp: Date.now()
      }, {
        persistent: true,
        headers: {
          'x-transaction-id': transactionId,
          'x-compensation': true
        }
      })

      console.log(`   ↩️  Compensation published for transaction ${transactionId}`)
    } catch (compensationError) {
      // If even the compensation cannot be published, it is a case for manual
      // intervention — in production, record it in durable storage and alert.
      console.error(`   🚨 Failed to publish compensation for ${transactionId}:`, compensationError.message)
    }
  }

  async function publishTransaction (transactionId) {
    const messages = buildTransactionMessages(transactionId)

    console.log(`\n🔄 Transaction ${transactionId}: publishing ${messages.length} messages as a batch...`)

    try {
      // publishBatch publishes everything on the same channel and only resolves
      // when the broker confirms every message in the batch. Any failure
      // triggers a retry of the whole batch (maxRetries), preserving the
      // all-or-nothing semantics.
      await rabbitMQ.publishBatch(ROUTING_KEY, messages, {
        persistent: true,
        maxRetries: 3,
        retryDelay: 500,
        headers: {
          'x-transaction-id': transactionId
        }
      })

      stats.committed++
      stats.messagesPublished += messages.length

      console.log(`   ✅ Transaction ${transactionId} confirmed by the broker (commit)`)

      for (const message of messages) {
        console.log(`      ✓ ${message.event}`)
      }
    } catch (error) {
      stats.rolledBack++

      console.error(`   ❌ Transaction ${transactionId} failed after all retries: ${error.message}`)
      await publishCompensation(transactionId, error.message)
    }
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Publication statistics:')
    console.log(`   Transactions committed: ${stats.committed}`)
    console.log(`   Transactions rolled back: ${stats.rolledBack}`)
    console.log(`   Total messages published: ${stats.messagesPublished}`)

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

    await setupInfrastructure()

    console.log('\n🚀 Starting transactional publication...')

    for (let i = 1; i <= TOTAL_TRANSACTIONS; i++) {
      await publishTransaction(`tx-${Date.now()}-${i}`)

      // Small delay between transactions to make the logs easier to read
      await new Promise(resolve => setTimeout(resolve, 300))
    }

    console.log('\n✅ All transactions have been processed!')

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
