import RabbitMQ from '../../src/index.js'
import { baseConfig } from '../config.mjs'

// Consumer for the transactional publication example.
//
// Messages arrive with the 'x-transaction-id' header and are grouped by
// transaction. When all expected events of a transaction have arrived, it is
// considered complete (commit). If a compensation event arrives
// (header 'x-compensation'), the transaction is marked as rolled back and the
// effects already applied must be undone.

async function main () {
  const rabbitConfig = {
    ...baseConfig,
    connectionName: 'example-transaction-consumer',
    exchange: {
      name: 'transaction-exchange',
      type: 'direct'
    }
  }

  const rabbitMQ = new RabbitMQ(rabbitConfig)
  let shutdownInProgress = false
  const QUEUE_NAME = 'transaction-queue'
  const EXPECTED_EVENTS = ['order.created', 'stock.reserved', 'payment.requested']

  // State of in-flight transactions: transactionId -> { events, startedAt }
  const transactions = new Map()

  const stats = {
    messagesProcessed: 0,
    transactionsCompleted: 0,
    transactionsRolledBack: 0
  }

  function getTransaction (transactionId) {
    if (!transactions.has(transactionId)) {
      transactions.set(transactionId, {
        events: [],
        startedAt: Date.now()
      })
    }

    return transactions.get(transactionId)
  }

  function handleCompensation (transactionId, content) {
    const transaction = transactions.get(transactionId)
    const appliedEvents = transaction ? transaction.events.map(event => event.event) : []

    stats.transactionsRolledBack++
    transactions.delete(transactionId)

    console.log(`\n↩️  Transaction ${transactionId} ROLLED BACK`)
    console.log(`   Reason: ${content.reason}`)

    if (appliedEvents.length > 0) {
      console.log(`   Undoing effects already applied: ${appliedEvents.join(', ')}`)
      // The real compensation logic would go here: release stock,
      // cancel the charge, mark the order as cancelled, etc.
    } else {
      console.log('   No effects applied, nothing to undo')
    }
  }

  function handleTransactionEvent (transactionId, content) {
    const transaction = getTransaction(transactionId)

    transaction.events.push(content)

    console.log(`\n📩 Event received: ${content.event}`)
    console.log(`   Transaction: ${transactionId}`)
    console.log(`   Progress: ${transaction.events.length}/${EXPECTED_EVENTS.length} events`)

    const receivedEvents = transaction.events.map(event => event.event)
    const isComplete = EXPECTED_EVENTS.every(event => receivedEvents.includes(event))

    if (isComplete) {
      const duration = Date.now() - transaction.startedAt

      stats.transactionsCompleted++
      transactions.delete(transactionId)

      console.log(`\n🎉 Transaction ${transactionId} COMPLETE in ${duration}ms`)
      console.log(`   Events processed: ${receivedEvents.join(', ')}`)
    }
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
      await channel.bindQueue(QUEUE_NAME, rabbitConfig.exchange.name, 'transaction-route')

      console.log('✅ Infrastructure set up successfully')
    } catch (error) {
      console.error('❌ Error setting up infrastructure:', error.message)
      throw error
    }
  }

  async function startConsumer () {
    await rabbitMQ.subscribe(QUEUE_NAME, async (content, message) => {
      const headers = message.properties.headers || {}
      const transactionId = headers['x-transaction-id'] || content.transactionId

      stats.messagesProcessed++

      if (!transactionId) {
        console.warn('⚠️  Message without x-transaction-id, skipping grouping:', content.event)

        return
      }

      if (headers['x-compensation']) {
        handleCompensation(transactionId, content)

        return
      }

      handleTransactionEvent(transactionId, content)
    })

    console.log(`\n👂 Waiting for messages on queue: ${QUEUE_NAME}`)
    console.log('   Press CTRL+C to stop\n')
  }

  async function shutdown () {
    if (shutdownInProgress) return
    shutdownInProgress = true

    console.log('\n👋 Starting graceful shutdown...')
    console.log('\n📊 Consumption statistics:')
    console.log(`   Messages processed: ${stats.messagesProcessed}`)
    console.log(`   Transactions completed: ${stats.transactionsCompleted}`)
    console.log(`   Transactions rolled back: ${stats.transactionsRolledBack}`)

    if (transactions.size > 0) {
      console.log(`   ⚠️  Incomplete transactions: ${transactions.size}`)

      for (const [transactionId, transaction] of transactions.entries()) {
        console.log(`      - ${transactionId}: ${transaction.events.length}/${EXPECTED_EVENTS.length} events`)
      }
    }

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
    await startConsumer()
  } catch (error) {
    console.error('❌ Error:', error.message)
    await shutdown()
  }
}

main().catch(error => {
  console.error('🔥 Unhandled error:', error)
  process.exit(1)
})
