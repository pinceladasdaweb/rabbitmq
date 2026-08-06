class Topology {
  constructor (context) {
    this.logger = context.logger
    this.getChannel = context.getChannel
    this.getExchange = context.getExchange
    this.deadLetterExchange = context.deadLetterExchange
    this.delayExchange = context.delayExchange
    this.getQueueNameByConsumerTag = context.getQueueNameByConsumerTag || (() => null)
  }

  async ensureExchange () {
    const exchange = this.getExchange()

    if (!exchange.name) return

    try {
      const channel = await this.getChannel()

      await channel.assertExchange(
        exchange.name,
        exchange.type || 'direct',
        exchange.options || { durable: true }
      )

      // Stryker disable next-line StringLiteral: log phrasing is not contract
      this.logger.info(`Exchange ${exchange.name} configured successfully`)
    } catch (error) {
      // Stryker disable next-line StringLiteral: log phrasing is not contract
      this.logger.error(`Failed to setup exchange: ${error.message}`)

      throw error
    }
  }

  async setupDeadLetterExchange () {
    const channel = await this.getChannel()

    await channel.assertExchange(this.deadLetterExchange, 'direct', { durable: true })

    // Stryker disable next-line StringLiteral: log phrasing is not contract
    this.logger.info(`Dead letter exchange '${this.deadLetterExchange}' setup completed`)
  }

  async createQueue (queueName, options = {}) {
    const channel = await this.getChannel()
    const deadLetterQueue = `${queueName}_dlq`

    const queueOptions = {
      durable: true,
      ...options,
      arguments: {
        'x-dead-letter-exchange': this.deadLetterExchange,
        'x-dead-letter-routing-key': deadLetterQueue,
        ...(options.maxPriority ? { 'x-max-priority': options.maxPriority } : {}),
        ...options.arguments
      }
    }

    try {
      await channel.assertQueue(queueName, queueOptions)
      await channel.assertQueue(deadLetterQueue, { durable: true })
      await channel.bindQueue(deadLetterQueue, this.deadLetterExchange, deadLetterQueue)

      // Stryker disable next-line StringLiteral: log phrasing is not contract
      this.logger.info(`Queue '${queueName}' and its dead letter queue '${deadLetterQueue}' created successfully`)
    } catch (error) {
      // Stryker disable next-line StringLiteral: log phrasing is not contract
      this.logger.error(`Failed to create queue '${queueName}' with dead letter queue: ${error.message}`)

      throw error
    }
  }

  async moveToDeadLetter (message, reason = 'Manually moved to DLQ') {
    // Prefer the actual source queue (resolved through the delivering
    // consumerTag); fall back to the routing-key convention for messages
    // that were not consumed through this instance.
    const sourceQueue = this.getQueueNameByConsumerTag(message.fields?.consumerTag)
    const deadLetterQueue = `${sourceQueue || message.fields.routingKey}_dlq`
    const channel = await this.getChannel()

    const properties = {
      ...message.properties,
      persistent: true,
      // mandatory makes the broker return unroutable messages instead of
      // silently dropping them when the DLQ binding does not exist.
      mandatory: true,
      headers: {
        ...message.properties.headers,
        'x-death-reason': reason,
        'x-death-time': new Date().toISOString(),
        'x-original-exchange': message.fields.exchange,
        'x-original-routing-key': message.fields.routingKey
      }
    }

    await new Promise((resolve, reject) => {
      let returned = false

      const onReturn = (returnedMessage) => {
        if (returnedMessage.fields.routingKey === deadLetterQueue) {
          returned = true
        }
      }

      channel.on('return', onReturn)

      channel.publish(this.deadLetterExchange, deadLetterQueue, message.content, properties, (err) => {
        // basic.return (if any) arrives before the confirm ack; give the
        // event loop one turn so onReturn has definitely been processed.
        setImmediate(() => {
          channel.off('return', onReturn)

          if (err) {
            reject(new Error(`Failed to move message to dead letter queue: ${err.message}`))
          } else if (returned) {
            reject(new Error(`Dead letter queue routing key '${deadLetterQueue}' has no binding on exchange '${this.deadLetterExchange}' — message was returned`))
          } else {
            resolve()
          }
        })
      })
    })

    // Stryker disable next-line StringLiteral: log phrasing is not contract
    this.logger.info(`Message moved to dead letter queue: ${reason}`)
  }

  async setupDelayExchange (options = {}) {
    const channel = await this.getChannel()

    await channel.assertExchange(this.delayExchange, 'x-delayed-message', {
      durable: true,
      arguments: { 'x-delayed-type': options.type || this.getExchange().type || 'direct' },
      ...options.exchangeOptions
    })

    // Stryker disable next-line StringLiteral: log phrasing is not contract
    this.logger.info(`Delay exchange '${this.delayExchange}' setup completed`)
  }

  async setupDelayPlugin () {
    try {
      const isEnabled = await this.isDelayPluginEnabled()

      if (!isEnabled) {
        throw new Error('Delay plugin is not enabled on the RabbitMQ server')
      }

      // Stryker disable next-line StringLiteral: log phrasing is not contract
      this.logger.info('Delay plugin is enabled and ready to use')
    } catch (error) {
      // Stryker disable next-line StringLiteral: log phrasing is not contract
      this.logger.error(`Failed to setup delay plugin: ${error.message}`)

      throw error
    }
  }

  async isDelayPluginEnabled () {
    try {
      const channel = await this.getChannel()

      await channel.assertExchange('test.delay', 'x-delayed-message', {
        durable: false,
        arguments: { 'x-delayed-type': 'direct' }
      })

      await channel.deleteExchange('test.delay')

      return true
    } catch (error) {
      // RabbitMQ <4: "NOT_FOUND - exchange type"; RabbitMQ 4+:
      // "PRECONDITION_FAILED - unknown exchange type 'x-delayed-message'"
      if (error.message.includes('exchange type')) {
        return false
      }

      throw error
    }
  }
}

export { Topology }
export default Topology
