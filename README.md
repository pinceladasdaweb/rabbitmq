# RabbitMQ

A robust and elegant abstraction for RabbitMQ in Node.js, featuring advanced capabilities such as circuit breaker, intelligent retries, automatic compression, and parallel processing.

## Features

- **Robust Connection Management**: Smart handling of connections with automatic reconnection, backoff strategy, and cluster support.
- **Advanced Channel Management**: Efficient channel pooling with dedicated channels for critical operations.
- **Circuit Breaker Pattern**: Built-in circuit breaker to prevent cascading failures and manage system overload.
- **Rate Limiter**: Flexible rate limiting strategies with token bucket, leaky bucket, fixed window and sliding window implementations.
- **Intelligent Retry Mechanism**: Configurable retry strategies with exponential backoff and customizable attempts.
- **Message Compression**: Automatic compression for large messages with configurable thresholds.
- **Dead Letter Exchange**: Built-in DLQ support for failed message handling and reprocessing capabilities.
- **Request/Response (RPC)**: Distributed RPC over RabbitMQ's direct reply-to — no reply queues to declare, no correlation bookkeeping, timeouts that never hang.
- **Parallel Processing**: Multi-threading support through worker threads for CPU-intensive tasks.
- **Delayed Message Support**: Native integration with RabbitMQ delayed message exchange plugin.
- **Flexible Consumer Patterns**: Support for different consumption patterns including optimized prefetch and parallel processing.
- **Caching Mechanism**: Optional message caching with TTL support for improved performance.

## Advantages

- **Enhanced Reliability**: Sophisticated connection and channel management ensures application stability during RabbitMQ unavailability.
- **Developer-Friendly**: Clean and intuitive API design makes complex RabbitMQ operations straightforward.
- **Production-Ready**: Built-in support for common enterprise patterns and failure scenarios.
- **Performance Optimized**: Smart resource management with features like channel pooling and message compression.
- **Flexible Configuration**: Highly configurable with sensible defaults for quick start and fine-tuning capabilities.
- **Automatic Recovery**: Self-healing capabilities with intelligent reconnection and channel recovery.
- **Comprehensive Monitoring**: Built-in events and status reporting for better observability.
- **Type-Safe**: Written in JavaScript with proper error handling and parameter validation.
- **Memory Efficient**: Careful resource management with proper cleanup and memory optimization.
- **Scale-Ready**: Designed for high-throughput scenarios with parallel processing capabilities.

## Understanding RabbitMQ

RabbitMQ is a robust message broker that implements the Advanced Message Queuing Protocol (AMQP). It enables applications to communicate asynchronously by acting as an intermediary for messages, supporting multiple messaging patterns including point-to-point, publish/subscribe, and request/reply.

### What is RabbitMQ?

RabbitMQ acts as a post office for your applications. It accepts, stores, and forwards binary messages between producers (applications that send messages) and consumers (applications that process messages). This decoupling allows for more resilient and scalable systems.

### What are Exchanges?

An exchange is like a post office's sorting facility. When a producer sends a message to RabbitMQ, it sends it to an exchange. The exchange is responsible for routing messages to the appropriate queues based on routing rules called "bindings".

#### Types of Exchanges

1. **Direct Exchange**
   - Routes messages based on an exact match of the routing key
   - Perfect for direct, point-to-point communication
   - Example: Sending logs of a specific severity to their respective handlers

2. **Topic Exchange**
   - Routes messages based on routing key patterns using wildcards
   - Ideal for multicast routing and complex routing scenarios
   - Example: Routing messages based on categories like "users.*.deleted" or "order.#"

3. **Fanout Exchange**
   - Broadcasts messages to all bound queues
   - Ignores routing keys entirely
   - Perfect for broadcast messaging
   - Example: Broadcasting real-time updates to multiple subscribers

4. **Headers Exchange**
   - Routes based on message header attributes instead of routing keys
   - Allows for complex routing based on multiple criteria
   - Example: Routing based on message type, content-type, or other custom headers

### What are Queues?

Queues are where messages live until they're consumed. Think of them as buffers or mailboxes that hold messages in a first-in-first-out (FIFO) manner. They have several important characteristics and variations:

#### Types of Queues

1. **Standard Queues**
   - Basic FIFO queues
   - Messages are consumed in the order they arrive
   - Support multiple consumers (round-robin distribution)

2. **Priority Queues**
   - Messages can have priority levels
   - Higher priority messages are delivered first
   - Useful for handling urgent messages

3. **Dead Letter Queues (DLQ)**
   - Special queues for messages that can't be delivered
   - Used for handling failed processing attempts
   - Important for error handling and monitoring

4. **Delayed Queues**
   - Support message delivery with a delay
   - Messages become available after a specified time
   - Useful for scheduling and retries

### Queue Properties

- **Durability**: Queues can be durable (survive broker restarts) or transient
- **Auto-delete**: Queues can be automatically deleted when no longer needed
- **Exclusivity**: Queues can be exclusive to one connection
- **Arguments**: Custom queue parameters for features like message TTL, max length, etc.

### Message Flow Example

```plaintext
Producer -> Exchange -> [Routing] -> Queue -> Consumer
                          |
                     Binding Rules
```

This message flow ensures:
- Reliable delivery
- Message persistence when needed
- Flexible routing options
- Scalable processing

## Local Development

### Environment Variables

The project includes an `.env.example` file with all required environment variables. To set up your local environment:

1. Create your `.env` file from the example:

```bash
cp .env.example .env
```

2. Load the environment variables into your shell:

```bash
set -a; source .env; set +a
```

The `.env` file contains these default configurations:

```env
RMQ_USERNAME=admin
RMQ_PASSWORD=admin
RABBITMQ_AMQP_PORT=5672
RABBITMQ_ADMIN_PORT=15672
```

You can adjust these values in your `.env` file according to your needs.

### Running Tests

After cloning, enable the repo's git hooks once (lint and commit message checks):

```bash
npm run hooks
```

The published package declares **zero lifecycle scripts**, so installing it never triggers npm's `allow-scripts` approval prompt.

Unit tests run without any external dependency:

```bash
npm test
```

Integration tests exercise the library against a real broker — including a forced connection drop to verify automatic recovery of the channel pool and consumers. Start RabbitMQ first, then enable them via environment variable:

```bash
docker compose up -d
npm run test:integration
```

Both suites also run on every push and pull request via GitHub Actions (`.github/workflows/ci.yml`), with RabbitMQ provisioned as a service container.

A three-node cluster suite covers what a single broker cannot: it kills the container the client is attached to and checks that the client comes back on a surviving node with its consumers draining again.

```bash
docker compose -f docker-compose.cluster.yml up -d --wait
npm run test:cluster
```

It also answers the question the `{ attempts: N }` retry budget depends on — whether a quorum queue's `x-delivery-count` survives a leader failover. It does: the count keeps climbing across the node change, so a failover mid-retry does not silently restart a message's budget.

Mutation testing grades whether those tests actually assert anything — it breaks a line in `src/` and checks that a test notices:

```bash
npm run test:mutation
```

It runs [Stryker](https://stryker-mutator.io/) over the unit suite and writes an HTML report to `reports/mutation/index.html`. Deliberately not part of CI: it is slow, and it is a gate for the person writing the tests rather than a check on the branch. A surviving mutant is a missing assertion until proven to be an equivalent mutant — see [CONTRIBUTING.md](CONTRIBUTING.md).

### Installing Delayed Message Plugin

The RabbitMQ Delayed Message Plugin is required for using message scheduling features (`publishDelayed()`).

**Using the project's docker-compose (recommended):** nothing to do. The compose file mounts the plugin (`rabbitmq_delayed_message_exchange-4.2.0.ez`) and the `.docker/enabled_plugins` file into the container, so the plugin is active on every `docker compose up` — including fresh container recreations. In CI, the workflow installs the plugin on the service container before running the integration tests.

**On brokers you manage yourself**, install it manually:

1. Copy the plugin file to the container:
```bash
docker cp rabbitmq_delayed_message_exchange-4.2.0.ez rabbitmq:/plugins
```

2. Enable the plugin:
```bash
docker exec -it rabbitmq rabbitmq-plugins enable rabbitmq_delayed_message_exchange
```

The delayed message plugin allows you to:
- Schedule messages for future delivery
- Set precise delays for message processing
- Implement deferred tasks and scheduled jobs

You can verify the plugin installation through the RabbitMQ Management UI under the "Admin > Plugins" section.

### Common Issues

1. **Port conflicts**: If ports 5672 or 15672 are already in use, adjust the port mappings in your `.env` file.
2. **Permission denied**: Ensure your user has permissions to execute Docker commands.
3. **Container not starting**: Check logs with `docker-compose logs rabbitmq`

## Installation

### Package manager

Using npm:

```bash
npm install @pinceladasdaweb/rabbitmq
```

Using yarn:

```bash
yarn add @pinceladasdaweb/rabbitmq
```

Using pnpm:

```bash
pnpm add @pinceladasdaweb/rabbitmq
```

Once the package is installed, you can import the library using `import` or `require` approach:

```bash
import RabbitMQ from '@pinceladasdaweb/rabbitmq'
```

Or if you use require for importing:

```bash
const { RabbitMQ } = require('@pinceladasdaweb/rabbitmq')
```

## Constructor Options

The `RabbitMQ` constructor accepts an options object with the following parameters:

### Connection Options

- **username** `{string}` *(required)*: RabbitMQ server username.
  - Example: `'admin'`
- **password** `{string}` *(required)*: RabbitMQ server password.
  - Example: `'admin'`
- **endpoints** `{array}` *(required)*: Array of RabbitMQ server hostnames.
  - Example: `['localhost:5672']`
- **protocol** `{string}`: Connection protocol (`'amqp'` or `'amqps'` for TLS).
  - Default: `'amqp'`
  - Example: `'amqps'`
- **vhost** `{string}`: Virtual host to connect to.
  - Default: server default (`/`)
  - Example: `'my-vhost'`
- **connectionName** `{string}`: Identifier for the connection.
  - Default: `'default_connection'`
  - Example: `'my-app-connection'`

### Reconnection Options

- **reconnectInterval** `{number}`: Initial interval between reconnection attempts in milliseconds.
  - Default: `1000`
  - Example: `2000`
- **maxReconnectInterval** `{number}`: Maximum interval between reconnection attempts in milliseconds.
  - Default: `15000`
  - Example: `30000`
- **maxReconnectAttempts** `{number}`: Maximum number of reconnection attempts. Pass `0` to disable automatic reconnection entirely.
  - Default: `Infinity`
  - Example: `10`

### Exchange Options

- **exchange** `{object}`: Configuration for the default exchange.
  - Properties:
    - **name** `{string}`: Exchange name.
      - Example: `'my-exchange'`
    - **type** `{string}`: Exchange type ('direct', 'topic', 'fanout', 'headers').
      - Example: `'direct'`
    - **options** `{object}`: Additional exchange options.
      - Example: `{ durable: true }`
- **deadLetterExchange** `{string}`: Name of the dead letter exchange used by `createQueue()` and `moveToDeadLetter()`.
  - Default: `'dlx'`
- **delayExchange** `{string}`: Name of the delay exchange used by `setupDelayExchange()` and `publishDelayed()`.
  - Default: `'delayed'`

### Channel Options

- **channelPoolSize** `{number}`: Number of channels to maintain in the pool.
  - Default: `10`
  - Example: `5`
- **prefetchCount** `{number}`: Number of messages to prefetch.
  - Default: `10`
  - Example: `1`
- **channelRecoveryInterval** `{number}`: Base backoff in milliseconds between attempts to recreate a pool channel the broker killed (attempt N waits N × this value, 5 attempts before the slot is dropped from rotation).
  - Default: `500`
  - Example: `100`
- **consumerRecoveryInterval** `{number}`: Base backoff in milliseconds between attempts to recover a consumer cancelled by the broker (attempt N waits N × this value, up to 3 attempts before `consumerLost` is emitted).
- **consumerDrainTimeout** `{number}`: How long `unsubscribe()` waits for handlers still processing a delivery before closing the consumer's dedicated channel anyway (default `30000`). The channel must outlive in-flight handlers or their late acks die and the broker redelivers work that succeeded.
  - Default: `1000`
  - Example: `500`

### Message Options

- **useCompression** `{boolean}`: Enable message compression.
  - Default: `false`
  - Example: `true`
- **compressionThreshold** `{number}`: Minimum message size in bytes for compression. `0` compresses every message.
  - Default: `1000`
  - Example: `2048`
- **serializer** `{function}`: Custom function for message serialization.
  - Default: `JSON.stringify`
- **deserializer** `{function}`: Custom function for message deserialization.
  - Default: `JSON.parse`

### Cache Options

- **useCache** `{boolean}`: Enable message caching.
  - Default: `false`
- **cacheTTL** `{number}`: Time to live for cached messages in seconds. `0` means entries never expire.
  - Default: `60`
  - Example: `120`
- **cacheCheckPeriod** `{number}`: Cache cleanup interval in seconds.
  - Default: `120`
  - Example: `240`

### Rate Limiter Options

- **rateLimiter** `{object}`: Rate limiter configuration.
  - Properties:
    - **windowMs** `{number}`: Time window in milliseconds.
      - Default: `60000`
    - **maxRequests** `{number}`: Maximum requests in window.
      - Default: `100`
    - **strategy** `{string}`: Rate limiting strategy ('token-bucket', 'leaky-bucket', 'fixed-window', 'sliding-window').
      - Default: `'token-bucket'`
      - All strategies track limits **per rate-limit key** (the routing key by default, or `options.rateLimitKey`).
      - An unknown strategy name throws at construction time, so a typo can never become an unlimited limiter.
      - `'leaky-bucket'` smooths bursts instead of rejecting: accepted requests are delayed proportionally to the queue occupancy of that key, and only rejected once the occupancy exceeds `queueLimit`.
    - **queueLimit** `{number}`: Maximum queue occupancy for the leaky-bucket strategy.
      - Default: `1000`
    - **burstable** `{boolean}`: Allow request bursting.
      - Default: `false`
    - **burstLimit** `{number}`: Maximum burst size.
      - Default: `maxRequests * 1.5`

### Circuit Breaker Options

- **circuitBreaker** `{object}`: Circuit breaker configuration.
  - Properties:
    - **failureThreshold** `{number}`: Number of failures before opening.
      - Default: `5`
    - **successThreshold** `{number}`: Successes needed to close.
      - Default: `2`
    - **timeout** `{number}`: Time in milliseconds before retry.
      - Default: `60000`

### Logging Options

- **logger** `{object}`: Custom logger implementation. **Recommended for production** — inject your application's logger so the library writes to the same stream, format and destinations as the rest of your app.
  - Required methods: **error**, **warn**, **info**. Optional: **debug** (used for hot-path messages such as per-message publish confirmations).
  - Any pino, winston or bunyan instance satisfies this interface directly:
    ```javascript
    import pino from 'pino'

    const rabbitMQ = new RabbitMQ({
      // ...
      logger: pino({ name: 'rabbitmq' })
    })
    ```
- **Default behavior (no logger injected)**: the library ships a minimal, dependency-free console logger — timestamped, leveled (`error`/`warn`/`info`/`debug`) and controlled by the `LOG_LEVEL` environment variable (default: `info`). It keeps the out-of-the-box experience visible without pulling any logging dependency into your project.
  ```text
  2026-07-18T14:02:11.407Z [info] Successfully connected to RabbitMQ cluster node: localhost:5672
  ```

### Usage Example

```javascript
const rabbitMQ = new RabbitMQ({
  username: 'admin',
  password: 'admin',
  endpoints: ['localhost:5672'],
  connectionName: 'my-app',
  exchange: {
    name: 'main-exchange',
    type: 'direct'
  },
  channelPoolSize: 5,
  useCompression: true,
  rateLimiter: {
    windowMs: 60000,
    maxRequests: 1000
  }
})
```

### Publish and Consumer example

#### Basic Connection

```javascript
const { RabbitMQ } = require('@pinceladasdaweb/rabbitmq')

const rabbitMQ = new RabbitMQ({
  username: 'admin',
  password: 'admin',
  endpoints: ['localhost:5672'],
  exchange: {
    name: 'my-exchange',
    type: 'direct'
  }
})

await rabbitMQ.connect()
```

#### Simple Publisher

```javascript
const publishMessage = async () => {
  try {
    const message = {
      id: 1,
      content: 'Hello World',
      timestamp: Date.now()
    }

    await rabbitMQ.publish('my-route', message)
    console.log('Message published successfully')
  } catch (error) {
    console.error('Error publishing message:', error.message)
  } finally {
    await rabbitMQ.disconnect()
  }
}
```

#### Simple Consumer

```javascript
const startConsumer = async () => {
  try {
    const queueName = 'my-queue'
    
    // Get a channel and set up the queue
    const channel = await rabbitMQ.getChannel()
    await channel.assertQueue(queueName, { durable: true })
    await channel.bindQueue(queueName, 'my-exchange', 'my-route')

    // Start consuming messages
    await rabbitMQ.subscribe(queueName, async (content, message) => {
      try {
        console.log('Processing message:', content)
        // Process your message here
        
        // Explicitly acknowledge successful processing
        await rabbitMQ.acknowledgeMessage(message)
        console.log('Message processed successfully')
      } catch (error) {
        // Explicitly reject failed messages
        // requeue: true - message will be requeued
        // requeue: false - message will be discarded or sent to DLQ if configured
        await rabbitMQ.negativeAcknowledgeMessage(message, { requeue: false })
        console.error('Failed to process message:', error)
      }
    }, {
      noAck: false  // Enable manual acknowledgment
    })

    console.log('Consumer started')
  } catch (error) {
    console.error('Error starting consumer:', error.message)
  }
}
```

### Publish and Consumer example with DLQ

#### Basic Connection

```javascript
const { RabbitMQ } = require('@pinceladasdaweb/rabbitmq')

const rabbitMQ = new RabbitMQ({
  username: 'admin',
  password: 'admin',
  endpoints: ['localhost:5672'],
  exchange: {
    name: 'my-exchange',
    type: 'direct'
  },
  deadLetterExchange: 'dlx', // Configure DLX name
  channelPoolSize: 5
})

// Setup connection events
rabbitMQ.on('connected', () => {
  const status = rabbitMQ.getClusterStatus()
  console.log('📬 Connected successfully to RabbitMQ!')
  console.log('Cluster status:', status)
})

rabbitMQ.on('disconnected', () => {
  console.log('❌ Disconnected from RabbitMQ')
})

rabbitMQ.on('reconnected', () => {
  console.log('🔄 Reconnected to RabbitMQ')
})

rabbitMQ.on('reconnectFailed', () => {
  console.log('💔 Failed to reconnect after all attempts')
})

await rabbitMQ.connect()
```

#### Publisher with DLQ

```javascript
const publish = async () => {
  try {
    // Setup DLX
    await rabbitMQ.setupDeadLetterExchange()

    // Create queue with DLQ configuration
    const queueName = 'my-queue'

    await rabbitMQ.createQueue(queueName, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': 'dlx',
        'x-dead-letter-routing-key': `${queueName}_dlq`
      }
    })

    // Publish message
    const message = {
      id: 1,
      content: 'Test message',
      timestamp: Date.now()
    }

    await rabbitMQ.publish('my-route', message)
    console.log('Message published successfully')
    
    await rabbitMQ.disconnect()
  } catch (error) {
    console.error('Error:', error.message)
    await rabbitMQ.disconnect()
  }
}
```

#### Consumer with DLQ

```javascript
const startConsumer = async () => {
  try {
    const queueName = 'my-queue'
    const maxRetries = 3
    
    // Get a channel and set up the queue
    const channel = await rabbitMQ.getChannel()

    await rabbitMQ.subscribe(queueName, async (content, message) => {
      try {
        console.log('Processing message:', content)
        
        // Simulate processing that might fail
        if (Math.random() < 0.5) {
          throw new Error('Processing failed')
        }
        
        await rabbitMQ.acknowledgeMessage(message)
        console.log('Message processed successfully')
      } catch (error) {
        const retryCount = (message.properties.headers['x-retry-count'] || 0) + 1
        
        if (retryCount <= maxRetries) {
          // Add retry count and requeue
          message.properties.headers['x-retry-count'] = retryCount
          await rabbitMQ.negativeAcknowledgeMessage(message, { requeue: true })
          console.log(`Retry attempt ${retryCount}/${maxRetries}`)
        } else {
          // Move to DLQ after max retries
          await rabbitMQ.moveToDeadLetter(
            message,
            `Failed after ${maxRetries} attempts: ${error.message}`
          )
          console.log('Message moved to DLQ')
        }
      }
    }, {
      noAck: false
    })

    console.log('Consumer started')
  } catch (error) {
    console.error('Error:', error.message)
    await rabbitMQ.disconnect()
  }
}
```

These examples demonstrate:
- DLQ configuration and usage
- Connection event handling
- Retry mechanism with max attempts
- Error handling and message flow control

These examples show the minimum setup required to start working with the library. For more advanced features, the [examples](examples) folder contains a runnable demo for every capability.

All examples share their connection settings through [examples/config.mjs](examples/config.mjs), which reads credentials and endpoint from environment variables (`RABBITMQ_USER`/`RMQ_USERNAME`, `RABBITMQ_PASS`/`RMQ_PASSWORD`, `RABBITMQ_ENDPOINT`) and falls back to the docker-compose defaults (`admin`/`admin` @ `localhost:5672`) — so each example only declares what is specific to it (`connectionName`, `exchange`, features):

| # | Example | What it demonstrates |
|---|---------|----------------------|
| 1 | [connection](examples/1%20-%20connection) | Connection lifecycle, reconnection with backoff, connection events |
| 2 | [standard-publication](examples/2%20-%20standard-publication) | Basic publish with confirms and a simple consumer |
| 3 | [batch-publication](examples/3%20-%20batch-publication) | Publishing multiple messages at once with `publishBatch()` |
| 4 | [async-publication](examples/4%20-%20async-publication) | Fire-and-forget publishing with `publishAsync()` |
| 5 | [async-batch-publication](examples/5%20-%20async-batch-publication) | Fire-and-forget batches with `publishAsyncBatch()` |
| 6 | [publish-with-cache](examples/6%20-%20publish-with-cache) | Publish deduplication with `publishWithCache()` and TTL |
| 7 | [priority-publication](examples/7%20-%20priority-publication) | Priority queues and message priority |
| 8 | [delay-publication](examples/8%20-%20delay-publication) | Scheduled delivery with `setupDelayExchange()` + `publishDelayed()` |
| 9 | [transaction-publication](examples/9%20-%20transaction-publication) | All-or-nothing batches with confirms, retry and compensation events |
| 10 | [selective-compression-publication](examples/10%20-%20selective-compression-publication) | Automatic gzip compression above a size threshold |
| 11 | [circuit-breaker-publication](examples/11%20-%20circuit-breaker-publication) | Circuit breaker states and the `circuitBreakerStateChanged` event |
| 12 | [retry-publication](examples/12%20-%20retry-publication) | Retry with exponential backoff (`maxRetries`/`retryDelay`) |
| 13 | [dead-letter-queues](examples/13%20-%20dead-letter-queues) | DLQ topology built manually with raw channels |
| 14 | [serializer-publication](examples/14%20-%20serializer-publication) | Custom serializer/deserializer (XML via xml2js) |
| 15 | [flexible-routing](examples/15%20-%20flexible-routing) | Topic exchanges and routing patterns |
| 16 | [optimized-prefetch](examples/16%20-%20optimized-prefetch) | Adaptive prefetch with `subscribeWithOptimizedPrefetch()` |
| 17 | [subscribe-parallel](examples/17%20-%20subscribe-parallel) | CPU-bound processing on worker threads with `subscribeParallel()` |
| 18 | [round-robin-consumers](examples/18%20-%20round-robin-consumers) | Competing consumers on the same queue |
| 19 | [sequential-producer](examples/19%20-%20sequential-producer) | Ordered processing with `subscribeSequential()` and `depends-on` |
| 20 | [rate-limit-publication](examples/20%20-%20rate-limit-publication) | Rate limiting strategies, status and events |
| 21 | [consumer-management](examples/21%20-%20consumer-management) | `unsubscribe()`, consumer lifecycle events, `enableGracefulShutdown()`, `connect({ waitForConnection })` |
| 22 | [native-dead-letter](examples/22%20-%20native-dead-letter) | Built-in DLQ support: `createQueue()`, `moveToDeadLetter()`, `processDeadLetterQueue()` |
| 23 | [request-response](examples/23%20-%20request-response) | Distributed RPC over direct reply-to: `request()`, `respond()`, timeouts and error envelopes |
| 24 | [retry-policy](examples/24%20-%20retry-policy) | `retryPolicy: 'none'` vs `'once'` side by side, plus the `error.retryable = false` opt-out |

## Available Methods

### Connection Management

- **connect(options?)** `{Promise<Object|null>}`
  - Establishes connection with RabbitMQ server.
  - Initializes channel pool and ensures exchange setup.
  - If every endpoint fails, reconnection keeps running in the background. By default `connect()` returns `null` in that case; pass `waitForConnection: true` to make the promise resolve only when the connection is finally established (or reject when reconnection gives up / the optional `timeout` in ms expires).
  - Example:
    ```javascript
    await rabbitMQ.connect()

    // Or block until connected, with a 30s cap:
    await rabbitMQ.connect({ waitForConnection: true, timeout: 30000 })
    ```

- **disconnect()** `{Promise<void>}`
  - Gracefully closes all channels and connection.
  - Terminates parallel processing workers and cleans up resources, timers and event listeners.
  - Example:
    ```javascript
    await rabbitMQ.disconnect()
    ```

- **enableGracefulShutdown(options?)** `{void}`
  - Opt-in helper that registers `SIGINT`/`SIGTERM` handlers which call `disconnect()` and exit the process.
  - The library never registers process-level handlers on its own — your application stays in control of its lifecycle. Call this method (or handle signals yourself) if you want automatic shutdown.
  - Parameters:
    - **options.signals** `{Array<string>}`: Signals to handle. Default: `['SIGINT', 'SIGTERM']`
    - **options.exitProcess** `{boolean}`: Whether to call `process.exit()` after disconnecting. Default: `true`
  - Example:
    ```javascript
    rabbitMQ.enableGracefulShutdown()
    ```

- **getClusterStatus()** `{Object}`
  - Returns current cluster connection status.
  - Returns:
    ```javascript
    {
      connectedTo: 'localhost:5672',
      allEndpoints: ['localhost:5672'],
      connectionState: 'connected'
    }
    ```

### Message Publishing

- **publish(routingKey, message, options?)** `{Promise<void>}`
  - Publishes a message to the configured exchange.
  - Parameters:
    - **routingKey** `{string}`: Routing key for message delivery
    - **message** `{any}`: Message content
    - **options** `{Object}`: Optional publishing options
  - Example:
    ```javascript
    await rabbitMQ.publish('user.created', { id: 1, name: 'John' })
    ```

- **publishBatch(routingKey, messages, options?)** `{Promise<void>}`
  - Publishes multiple messages in a single operation.
  - Parameters:
    - **routingKey** `{string}`: Routing key for messages
    - **messages** `{Array<any>}`: Array of messages
    - **options** `{Object}`: Optional publishing options
  - Example:
    ```javascript
    await rabbitMQ.publishBatch('logs', [log1, log2, log3])
    ```

- **publishAsync(routingKey, message, options?)** `{Promise<void>}`
  - Publishes message asynchronously without waiting for confirmation.
  - Uses same parameters as publish().
  - Example:
    ```javascript
    await rabbitMQ.publishAsync('metrics', metricData)
    ```

- **publishAsyncBatch(routingKey, messages, options?)** `{Promise<void>}`
  - Publishes multiple messages asynchronously without waiting for confirmation.
  - Parameters:
    - **routingKey** `{string}`: Routing key for messages
    - **messages** `{Array<any>}`: Array of messages to publish
    - **options** `{Object}`: Optional publishing options
  - Example:
    ```javascript
    const messages = [message1, message2, message3]
    await rabbitMQ.publishAsyncBatch('logs', messages, {
      headers: { 'x-async-batch': true }
    })
    ```

- **publishWithCache(routingKey, messageGenerator, options?)** `{Promise<any>}`
  - Publish deduplication: on cache **miss** it generates, publishes and caches the message; on cache **hit** it returns the cached message **without publishing anything**.
  - Use it when repeated calls within the TTL should result in a single publication (e.g. periodic snapshots). It is not a read cache for consumers.
  - Requires `useCache: true` in the constructor. Use `invalidateCache(routingKey)` to force the next call to publish again.
  - Parameters:
    - **routingKey** `{string}`: Routing key
    - **messageGenerator** `{Function|any}`: Message or function to generate the message (only invoked on cache miss)
    - **options** `{Object}`: Publishing and cache options (`cacheTTL` overrides the default TTL)
  - Example:
    ```javascript
    await rabbitMQ.publishWithCache('my-route', 
      () => generateMessage(),
      { cacheTTL: 60 }
    )
    ```

- **publishDelayed(routingKey, message, delayMs, options?)** `{Promise<void>}`
  - Publishes a message that the broker only routes after `delayMs` milliseconds, using the delay exchange (see `setupDelayExchange()`).
  - Requires the `rabbitmq_delayed_message_exchange` plugin on the broker (see [Installing Delayed Message Plugin](#installing-delayed-message-plugin)) and queues bound to the delay exchange.
  - Parameters:
    - **routingKey** `{string}`: Routing key
    - **message** `{any}`: Message content
    - **delayMs** `{number}`: Delay in milliseconds (non-negative)
    - **options** `{Object}`: Optional publishing options
  - Example:
    ```javascript
    await rabbitMQ.setupDelayExchange()

    const channel = await rabbitMQ.getChannel()
    await channel.bindQueue('my-queue', 'delayed', 'my-route')

    await rabbitMQ.publishDelayed('my-route', { remind: true }, 5000)
    ```

### Message Consumption

#### Unroutable publishes (`mandatory`)

A message published to a routing key with **no queue bound to it** is discarded by the broker — and the publisher confirm still arrives, so the publish resolves and the caller is told everything went fine. Nothing is logged, because nothing failed as far as AMQP is concerned.

Pass `mandatory: true` and the broker hands the message back instead; the publish then rejects with `code: 'UNROUTABLE'`:

```javascript
try {
  await rabbitMQ.publish('orders.typo', order, { mandatory: true })
} catch (error) {
  if (error.code === 'UNROUTABLE') {
    // The routing key reaches no queue: a missing binding or a typo,
    // not a broker failure.
  }
}
```

Available on `publish`, `publishBatch` and `publishDelayed`. RPC requests already publish `mandatory` and surface the same condition as `RPC_UNROUTABLE`. The fire-and-forget methods (`publishAsync`, `publishAsyncBatch`) cannot see the broker's return — they **reject** `mandatory: true` at the call site instead of silently ignoring it; use a confirmed method when routability matters.

#### Failure policy (`retryPolicy`)

Every subscribe method accepts `retryPolicy`, which decides what happens to a message whose processing threw:

| Value | Behaviour |
| --- | --- |
| `'none'` | Nack without requeue — the message goes straight to the DLQ. No retries. |
| `'once'` | A first delivery is requeued and retried. A delivery already marked `redelivered` is dead-lettered instead, so a permanently failing message can never hot-loop. |
| `{ attempts: N }` | A real budget of **N deliveries**, counted by the broker through a quorum queue's `x-delivery-count`. |

##### Why `{ attempts: N }` needs a quorum queue

`'once'` reads the `redelivered` flag, and the broker sets that on **any** requeue — including one caused by a connection drop, which has nothing to do with your handler. So an infrastructure blip can spend the retry before the message ever fails.

Quorum queues carry `x-delivery-count`, a real counter of deliveries, and `{ attempts: N }` uses it: exactly N tries, no matter how many reconnections happen in between. On a classic queue the header does not exist, so the policy degrades to the `'once'` ceiling rather than looping forever on a budget the broker cannot track.

```javascript
await rabbitMQ.createQueue('payments', { arguments: { 'x-queue-type': 'quorum' } })

// Three real attempts, then the DLQ.
await rabbitMQ.subscribe('payments', handlePayment, { retryPolicy: { attempts: 3 } })
```

Defaults preserve each method's historical behaviour:

| Method | Default |
| --- | --- |
| `subscribe` | `'none'` |
| `subscribeWithOptimizedPrefetch` | `'none'` |
| `subscribeParallel` | `'none'` |
| `respond` (RPC) | `'none'` |
| `processDeadLetterQueue` | `'none'` |
| `subscribeSequential` | `'once'` |

```javascript
// Handler is idempotent — a transient failure is worth one retry.
await rabbitMQ.subscribe('orders', handleOrder, { retryPolicy: 'once' })

// Ordering matters more than the retry.
await rabbitMQ.subscribeSequential('steps', handleStep, { retryPolicy: 'none' })
```

A handler can decline the retry per message by marking the error:

```javascript
await rabbitMQ.subscribe('orders', async (message) => {
  if (!isValid(message)) {
    const error = new Error('Malformed payload')
    error.retryable = false // permanent: skip the retry, dead-letter it now

    throw error
  }

  await processOrder(message)
}, { retryPolicy: 'once' })
```

The policy is a **ceiling**: `error.retryable = false` opts out of a retry, but `retryable = true` does not create one under `'none'`. An unrecognized value throws at subscribe time rather than silently falling back.

Things worth knowing before choosing `'once'`:

- **The retry is immediate, with no backoff.** If the failure is a dependency that is down for a few seconds, the redelivery happens within milliseconds and fails again — reaching the DLQ anyway. The retry helps with instantaneous blips, not outages.
- **A requeue means the message may be processed twice.** Only choose `'once'` when the handler is idempotent, or when a partially applied side effect can safely be reapplied.
- **The broker sets `redelivered` on *any* requeue, not just ours.** An unacked message returned to the queue after a connection drop arrives already marked, so an infrastructure event can consume the retry budget before the handler ever fails. AMQP has no redelivery counter on classic queues — use a [quorum queue](https://www.rabbitmq.com/docs/quorum-queues)'s `x-delivery-count` if you need a true attempt budget.
- **On `subscribeSequential`, a retry can break ordering.** The requeued message returns to the queue while later messages keep being processed.
- **On `respond` (RPC), a retry re-runs the responder.** The staleness guard drops requests past their deadline, but a fast retry within the deadline executes the handler a second time.
- **Decode failures follow the policy too.** They are deterministic, so under `'once'` an undecodable message costs one pointless redelivery before the DLQ.
- **`noAck: true` makes the policy moot.** Nothing is acknowledged, so there is no nack to requeue — the broker considers the message delivered the moment it leaves the queue.

See [`examples/24 - retry-policy`](examples/24%20-%20retry-policy) for a runnable demo that puts both policies and the opt-out side by side on the same failing payload.

- **subscribe(queueName, callback, options?)** `{Promise<Object>}`
  - Basic message consumption with automatic acknowledgment.
  - Parameters:
    - **queueName** `{string}`: Queue to consume from
    - **callback** `{Function}`: Message handling function
    - **options** `{Object}`: Consumption options, including:
      - **retryPolicy** `{'none'|'once'}`: Failure policy (default: `'none'`) — see [Failure policy](#failure-policy-retrypolicy)
  - Returns the consumer object — keep its `consumerTag` if you plan to call `unsubscribe()` later.
  - Example:
    ```javascript
    await rabbitMQ.subscribe('my-queue', async (message) => {
      console.log(message)
    })
    ```

- **unsubscribe(consumerTag)** `{Promise<boolean>}`
  - Cancels an active consumer without disconnecting. Stops delivery, releases the consumer's resources (worker threads for `subscribeParallel`, internal state for `subscribeSequential`) and removes it from automatic recreation on reconnect.
  - Returns `true` when a consumer with that tag was found and cancelled, `false` otherwise.
  - Example:
    ```javascript
    const consumer = await rabbitMQ.subscribe('my-queue', handler)

    // later...
    await rabbitMQ.unsubscribe(consumer.consumerTag)
    ```

- **subscribeWithOptimizedPrefetch(queueName, callback, options?)** `{Promise<Object>}`
  - Advanced consumption with automatic prefetch optimization.
  - Adjusts prefetch count based on processing performance.
  - Parameters:
    - **queueName** `{string}`: Queue to consume from
    - **callback** `{Function}`: Message handling function
    - **options** `{Object}`: Additional options including:
      - **initialPrefetch** `{number}`: Starting prefetch count (default: 10)
      - **maxPrefetch** `{number}`: Maximum prefetch limit (default: 1000)
      - **optimizationInterval** `{number}`: Adjustment interval in ms
  - Example:
    ```javascript
    await rabbitMQ.subscribeWithOptimizedPrefetch('heavy-queue', 
      async (message) => {
        await processMessage(message)
      },
      { initialPrefetch: 5, maxPrefetch: 100 }
    )
    ```

- **subscribeParallel(queueName, processorFile, options?)** `{Promise<Object>}`
  - Parallel message processing using worker threads.
  - Parameters:
    - **queueName** `{string}`: Queue to consume from
    - **processorFile** `{string}`: Path to worker processor file. File must export a message processor that:
      - Receives message via worker thread message event
      - Returns { success: true/false } to indicate processing result
      - Can access workerData.workerId for identification
    - **options** `{Object}`: Options including:
      - **workerCount** `{number}`: Number of workers (default: CPU cores)
      - **prefetch** `{number}`: Prefetch per worker
  - Example:
    ```javascript
    // processor.js
    const { parentPort, workerData } = require('worker_threads')
    parentPort.on('message', async (message) => {
      try {
        // Process message
        parentPort.postMessage({ success: true })
      } catch (error) {
        parentPort.postMessage({ success: false, error: error.message })
      }
    })

    // consumer.js
    await rabbitMQ.subscribeParallel('cpu-intensive-queue', 
      './processor.js',
      { workerCount: 4 }
    )
    ```

- **subscribeSequential(queueName, callback, options?)** `{Promise<Object>}`
  - Sequential message processing with dependency handling.
  - Ensures messages are processed in order based on dependencies.
  - Parameters:
    - **queueName** `{string}`: Queue to consume from
    - **callback** `{Function}`: Message handling function
    - **options** `{Object}`: Consumption options, including:
      - **retryPolicy** `{'none'|'once'}`: Failure policy (default: `'once'` — the only method that retries by default) — see [Failure policy](#failure-policy-retrypolicy)
      - **staleTimeout** `{number}`: How long a message may wait for its dependency before being settled under the retry policy (default: `30000`)
  - Message Dependency Format:
    - Messages must include messageId in properties
    - Dependencies specified in headers['depends-on']
  - Notes:
    - Messages are acknowledged only after they are actually processed. Messages parked waiting for a dependency stay unacknowledged.
    - This is the only subscribe method that defaults to `retryPolicy: 'once'`. The requeued message goes back to the queue while later ones keep being processed, so the retry can break the ordering this method exists to provide — pass `'none'` when order matters more than the retry.
    - Dependency reordering requires `prefetchCount` greater than `1` (default is `1`, which is strictly sequential). Use e.g. `{ prefetchCount: 5 }` so dependent messages can be buffered while their dependency is being processed.
  - Example:
    ```javascript
    // Publishing with dependencies
    await rabbitMQ.publish('route', message1, {
      messageId: 'msg-1'
    })
    await rabbitMQ.publish('route', message2, {
      messageId: 'msg-2',
      headers: {
        'depends-on': 'msg-1'  // Will process after msg-1
      }
    })

    // Consumer
    await rabbitMQ.subscribeSequential('sequential-queue', 
      async (message) => {
        await processInOrder(message)
      },
      { prefetchCount: 5 }
    )
    ```

### Request/Response (RPC)

Distributed request/response built on RabbitMQ's [direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to) (`amq.rabbitmq.reply-to`): the requester consumes a private pseudo-queue on a dedicated channel, so there are no reply queues to declare, nothing to clean up, and correlation is handled internally with `crypto.randomUUID()`.

- **request(routingKey, message, options?)** `{Promise<unknown>}`
  - Publishes a request through the configured exchange and resolves with whatever the responder's handler returned.
  - Goes through the same pipeline as every publish: validation, fail-fast connection probe, per-key rate limiting (`rpc:<routingKey>` by default) and the circuit breaker. One difference: `maxRetries` defaults to **1** (single publish attempt) instead of 3 — republishing a request whose confirm was lost can execute the responder twice. Opt into retries explicitly if that is acceptable.
  - By default requests are transient and carry a per-message TTL equal to the timeout (both overridable via `persistent`/`expiration`). Note: on queues configured with a dead letter exchange (e.g. created via `createQueue()`), requests that expire while queued are dead-lettered with reason `expired`, per AMQP semantics — filter for that if you reprocess the DLQ.
  - The returned promise **never hangs**. It settles on:
    - the correlated reply (resolve),
    - timeout — rejects with `error.code = 'RPC_TIMEOUT'`,
    - a responder error envelope — rejects with `error.code = 'RPC_RESPONDER_ERROR'`,
    - an unroutable request (nothing bound to the routing key) — requests are published with `mandatory` by default, so the broker's `basic.return` rejects immediately with `error.code = 'RPC_UNROUTABLE'` instead of burning the full timeout,
    - connection/channel loss — rejects with `error.code = 'RPC_CONNECTION_LOST'`. Direct reply-to routes are connection-scoped and cannot survive a reconnect, so in-flight requests fail fast and the caller decides whether to retry. The reply consumer is recreated lazily by the next `request()`.
  - Parameters:
    - **routingKey** `{string}`: Routing key for the request
    - **message** `{unknown}`: Request payload
    - **options** `{Object}`: All publish options, plus:
      - **timeout** `{number}`: Milliseconds to wait for the reply (default: 30000)
  - Example:
    ```javascript
    try {
      const user = await rabbitMQ.request('rpc.users.get', { id: 42 }, { timeout: 5000 })
      console.log(user.name)
    } catch (error) {
      if (error.code === 'RPC_TIMEOUT') {
        // nobody answered in time
      }
    }
    ```

- **respond(queueName, handler, options?)** `{Promise<Object>}`
  - Subscribes to the queue and publishes each handler's return value back to the requester's private reply route, correlating automatically. Returning `undefined` still settles the requester (as `null`).
  - Stale requests are dropped without running the handler: every request carries a deadline header, and a request that outlived its requester's timeout (e.g. sitting in the prefetch buffer) is acknowledged and discarded — its reply route would ignore the answer anyway. Assumes reasonably synchronized clocks (NTP).
  - Messages without a `replyTo` property are processed normally and logged — nothing to reply to.
  - Error handling follows the poison-message policy:
    - default: a handler crash nacks the request to the DLQ (no hot requeue loops) and the requester surfaces the failure through its timeout;
    - `{ replyOnError: true }`: the error is published back as a structured envelope and the requester rejects immediately with `RPC_RESPONDER_ERROR`.
  - `retryPolicy` defaults to `'none'` here and is best left alone: a retry re-runs the responder. The staleness guard drops requests past their deadline, but a redelivery that lands within the deadline executes the handler a second time.
  - A reply-transport failure after a **successful** handler never dead-letters the request (a DLQ replay would re-run committed side effects): the responder falls back to an error envelope (e.g. when the result is not serializable, the requester fails fast with `RPC_RESPONDER_ERROR`), and if even that cannot be published the request is acked and the requester's timeout takes over.
  - Responders are regular consumers: they are recreated automatically after a reconnection.
  - Parameters:
    - **queueName** `{string}`: Queue holding the requests (bind it to the exchange yourself)
    - **handler** `{Function}`: `(content, message) => result` — the return value is the reply
    - **options** `{Object}`: All subscribe options, plus:
      - **replyOnError** `{boolean}`: Send handler errors back to the requester (default: false)
  - Example:
    ```javascript
    await rabbitMQ.respond('rpc-users', async (content) => {
      return await getUser(content.id)
    }, { replyOnError: true })
    ```

See [`examples/23 - request-response`](examples/23%20-%20request-response) for a complete runnable requester/responder pair.

### Message Handling

- **acknowledgeMessage(message)** `{Promise<void>}`
  - Explicitly acknowledges a message.
  - Parameters:
    - **message** `{Object}`: Message to acknowledge
  - Example:
    ```javascript
    await rabbitMQ.acknowledgeMessage(message)
    ```

- **negativeAcknowledgeMessage(message, options?)** `{Promise<void>}`
  - Rejects a message with optional requeue.
  - Parameters:
    - **message** `{Object}`: Message to reject
    - **options** `{Object}`: Options including:
      - **requeue** `{boolean}`: Whether to requeue the message
  - Example:
    ```javascript
    await rabbitMQ.negativeAcknowledgeMessage(message, { requeue: true })
    ```

### Queue Management

- **createQueue(queueName, options?)** `{Promise<void>}`
  - Creates a queue with optional dead letter setup.
  - Parameters:
    - **queueName** `{string}`: Name of the queue
    - **options** `{Object}`: Queue options
  - Example:
    ```javascript
    await rabbitMQ.createQueue('my-queue', { 
      durable: true,
      maxPriority: 10 
    })
    ```

### Exchange Management

- **setExchange(name, type?, options?)** `{void}`
  - Switches which exchange subsequent publishes target. Synchronous — it only updates the in-memory configuration.
  - ⚠️ It does **not** assert the exchange on the broker: publishing to an exchange that does not exist fails with a channel-level 404. Make sure the exchange exists (assert it via `getChannel()` + `assertExchange`, or configure it in the constructor so `connect()` asserts it).
  - Parameters:
    - **name** `{string}`: Exchange name
    - **type** `{string}`: Exchange type
    - **options** `{Object}`: Exchange options
  - Example:
    ```javascript
    rabbitMQ.setExchange('new-exchange', 'topic', { durable: true })
    ```

### Dead Letter Management

- **setupDeadLetterExchange()** `{Promise<void>}`
  - Sets up the dead letter exchange configuration.
  - Example:
    ```javascript
    await rabbitMQ.setupDeadLetterExchange()
    ```

- **moveToDeadLetter(message, reason?)** `{Promise<void>}`
  - Copies a message to the dead letter queue with tracking headers (`x-death-reason`, `x-death-time`, `x-original-exchange`, `x-original-routing-key`). The original message must still be acknowledged (the automatic ack from `subscribe` covers this when the callback returns normally).
  - The target DLQ is resolved from the **source queue** of the delivering consumer (`<queueName>_dlq`, matching `createQueue()`), falling back to the routing-key convention (`<routingKey>_dlq`) for messages that were not consumed through this instance.
  - The message is published with `mandatory: true`: if the resolved DLQ routing has no binding on the DLX, the promise **rejects** instead of silently dropping the message. See [examples/22 - native-dead-letter](examples/22%20-%20native-dead-letter).
  - Parameters:
    - **message** `{Object}`: Message to move
    - **reason** `{string}`: Reason for moving to DLQ
  - Example:
    ```javascript
    await rabbitMQ.moveToDeadLetter(message, 'Message processing failed')
    ```

- **processDeadLetterQueue(originalQueueName, processor, options?)** `{Promise<void>}`
  - Processes messages from a dead letter queue (`${originalQueueName}_dlq`).
  - Parameters:
    - **originalQueueName** `{string}`: Original queue name
    - **processor** `{Function}`: Processing function
    - **options** `{Object}`: Consumer options, including **retryPolicy** — see [Failure policy](#failure-policy-retrypolicy)
  - A processor failure is logged and then settled under the subscription's `retryPolicy`, like any other consumer. Two things are worth knowing about what that means on a DLQ:
    - `createQueue()` declares the DLQ **without a dead letter exchange of its own**, so the default `'none'` discards a message its processor could not handle. Declare the DLQ yourself with an `x-dead-letter-exchange` if failures must be preserved.
    - `'once'` gives the processor a second attempt before the message is discarded — useful when the processor republishes to another system that can be briefly unavailable.
  - Example:
    ```javascript
    await rabbitMQ.processDeadLetterQueue('my-queue', async (message) => {
      // Process dead letter message
    })
    ```

### Cache Management

- **getFromCache(routingKey)** `{Promise<any>}`
  - Retrieves a message from cache.
  - Parameters:
    - **routingKey** `{string}`: Routing key of the cached message
  - Example:
    ```javascript
    const cachedMessage = await rabbitMQ.getFromCache('my-route')
    ```

- **invalidateCache(routingKey)** `{void}`
  - Invalidates cache for a specific routing key.
  - Parameters:
    - **routingKey** `{string}`: Routing key to invalidate
  - Example:
    ```javascript
    rabbitMQ.invalidateCache('my-route')
    ```

- **clearCache()** `{void}`
  - Clears all cached messages.
  - Example:
    ```javascript
    rabbitMQ.clearCache()
    ```

### Rate Limiter Management

- **getRateLimitStatus(key)** `{Object}`
  - Returns rate limit status for a key.
  - Example:
    ```javascript
    const status = rabbitMQ.getRateLimitStatus('my-route')
    ```

- **resetRateLimit(key)** `{void}`
  - Resets rate limit counters for a specific key.
  - Parameters:
    - **key** `{string}`: Rate limit key to reset
  - Example:
    ```javascript
    rabbitMQ.resetRateLimit('my-route')
    ```

- **blockRateLimit(key, duration)** `{Promise<void>}`
  - Blocks a key for a specified duration.
  - Parameters:
    - **key** `{string}`: Key to block
    - **duration** `{number}`: Duration in milliseconds
  - Example:
    ```javascript
    await rabbitMQ.blockRateLimit('my-route', 5000)
    ```

### System Configuration

- **setCompression(useCompression)** `{void}`
  - Enables/disables message compression.
  - Parameters:
    - **useCompression** `{boolean}`: Whether to use compression
  - Example:
    ```javascript
    rabbitMQ.setCompression(true)
    ```

- **setCompressionThreshold(threshold)** `{void}`
  - Sets minimum message size for compression.
  - Parameters:
    - **threshold** `{number}`: Size in bytes
  - Example:
    ```javascript
    rabbitMQ.setCompressionThreshold(1000)
    ```

### Delay Message Support

- **setupDelayPlugin()** `{Promise<void>}`
  - Sets up and verifies the delay plugin configuration.
  - Requires rabbitmq_delayed_message_exchange plugin to be installed on the RabbitMQ server.
  - Check plugin installation with: `rabbitmq-plugins list`
  - Enable plugin with: `rabbitmq-plugins enable rabbitmq_delayed_message_exchange`
  - Example:
    ```javascript
    try {
      await rabbitMQ.setupDelayPlugin()
      console.log('Delay plugin is ready')
    } catch (error) {
      console.error('Delay plugin not available:', error.message)
      // Handle case where plugin is not installed
    }
    ```

- **isDelayPluginEnabled()** `{Promise<boolean>}`
  - Checks if the delay plugin is enabled and available.
  - Example:
    ```javascript
    const enabled = await rabbitMQ.isDelayPluginEnabled()
    ```

- **setupDelayExchange(options?)** `{Promise<void>}`
  - Asserts the delay exchange (type `x-delayed-message`) used by `publishDelayed()`.
  - The exchange name comes from the `delayExchange` constructor option (default: `'delayed'`).
  - Parameters:
    - **options.type** `{string}`: Routing behavior after the delay (`'direct'`, `'topic'`, `'fanout'`, `'headers'`). Defaults to the configured exchange type.
    - **options.exchangeOptions** `{Object}`: Additional exchange options.
  - Example:
    ```javascript
    await rabbitMQ.setupDelayExchange()
    ```

### Events

The `RabbitMQ` instance is an `EventEmitter`. All emitted events:

| Event | Payload | When it fires |
|-------|---------|---------------|
| `connected` | — | Connection established (initial connect and every successful reconnect) |
| `disconnecting` | — | An explicit `disconnect()` began — unlike `disconnected`, this never fires on transient losses |
| `disconnected` | — | Connection lost or closed |
| `reconnected` | — | Reconnection finished **and** internal state restored (channel pool, exchange, consumers) |
| `reconnectFailed` | — | Reconnection gave up after `maxReconnectAttempts` |
| `reconnectError` | `Error` | Reconnected, but restoring internal state failed |
| `consumerCancelled` | `{ queueName, consumerTag }` | The broker cancelled a consumer (e.g. queue deleted); automatic recovery starts |
| `consumerRecovered` | `{ queueName, consumerTag }` | A broker-cancelled consumer was recreated successfully |
| `consumerLost` | `{ queueName }` | Recovery attempts were exhausted and the consumer was removed |
| `circuitBreakerStateChanged` | `'CLOSED' \| 'OPEN' \| 'HALF-OPEN'` | Every circuit breaker state transition |
| `rateLimited` | `{ key, strategy }` | A publish was rejected by the rate limiter |
| `rateBlocked` | `{ key, remainingTime }` | A publish hit a key blocked via `blockRateLimit()` |
| `messageProcessed` | `{ queue, messageId, consumerTag, durationMs }` | A consumed message was handled successfully (every subscribe variant) |
| `messageFailed` | `{ queue, messageId, consumerTag, durationMs, error, requeued }` | A consumed message failed — handler crash, decode failure or an expired `depends-on` dependency |

**A listener that throws never breaks the client.** Every event above is emitted
defensively: the listener's exception is logged as such (`A 'reconnected'
listener threw: …`) and the operation it was reporting on carries on. A crashing
`disconnected` listener cannot stop reconnection, a crashing `consumerCancelled`
listener cannot abort consumer recovery, a crashing `messageProcessed` listener
cannot turn a successful delivery into a nack, and a crashing `reconnected`
listener is not reported as `reconnectError` — the state really was restored.
Note this covers **synchronous** throws; an `async` listener's rejection belongs
to the listener, so `await` inside one needs its own `try/catch`.

Notes on the per-message events:

- `durationMs` measures the handler work. It is `undefined` on `messageFailed` when the message never ran — a sequential message that expired waiting for its `depends-on` dependency.
- `requeued` reports what actually happened to the delivery, not what the retry policy wanted: under `noAck` it is always `false`, because there was nothing left to requeue.
- A duplicate delivery of a message parked behind a dependency is acknowledged and dropped without an event — the original reports once, when it completes.

Example:

```javascript
rabbitMQ.on('consumerLost', ({ queueName }) => {
  alertOps(`Consumer for ${queueName} could not be recovered`)
})

rabbitMQ.on('circuitBreakerStateChanged', (state) => {
  metrics.gauge('rabbitmq.circuit_breaker', state === 'CLOSED' ? 0 : 1)
})

rabbitMQ.on('messageProcessed', ({ queue, durationMs }) => {
  metrics.histogram('rabbitmq.consume_duration_ms', durationMs, { queue })
})

rabbitMQ.on('messageFailed', ({ queue, requeued, error }) => {
  metrics.increment('rabbitmq.consume_failures', { queue, requeued })
})
```

See [examples/21 - consumer-management](examples/21%20-%20consumer-management) and [examples/11 - circuit-breaker-publication](examples/11%20-%20circuit-breaker-publication) for runnable demos.

### Monitoring and Control

- **getCircuitBreakerState()** `{Object}`
  - Returns current circuit breaker status.
  - Example:
    ```javascript
    const state = rabbitMQ.getCircuitBreakerState()
    ```

## Failure Modes

What the library does when things go wrong, and how each condition surfaces. Every row is pinned by tests — including integration tests against a real broker and a real three-node cluster.

### Connection

| Condition | What the library does | How it surfaces |
|-----------|----------------------|-----------------|
| Broker connection lost | Reconnects automatically with backoff, rotating through every configured endpoint. Once the dial succeeds, the channel pool, the exchange and **every consumer** are restored — atomically: if any step fails, the half-built state is torn down and the next cycle retries from scratch. Consumers survive outages of **any** length: reconnection owns their recovery, and per-consumer recovery never races it. | `disconnected`, then `connected` and `reconnected` |
| State restore fails after a successful dial | The half-restored pool is closed, nothing is left half-alive, and the reconnection cycle keeps running. | `reconnectError` |
| Reconnection exhausts `maxReconnectAttempts` | The client stops trying. Callers parked in `connect({ waitForConnection: true })` are rejected instead of hanging. | `reconnectFailed` |
| Cluster node fails over | Reconnection lands on another endpoint. In-flight unacked deliveries are requeued by the broker; a quorum queue's `x-delivery-count` survives the failover, so `{ attempts: N }` budgets stay honest. | `reconnected`, plus `getClusterStatus().connectedTo` |

See [Reconnection Options](#reconnection-options).

### Publishing

| Condition | What the library does | How it surfaces |
|-----------|----------------------|-----------------|
| Publish while disconnected | Fails fast — the connection probe runs before rate-limit tokens are consumed and outside the circuit breaker, so an outage neither drains quotas nor trips the breaker (reconnection already owns that failure). | The publish rejects with `Not connected to RabbitMQ` |
| Broker refuses / does not confirm | Retries up to `maxRetries` (default 3) with exponential backoff. `publishBatch` retries **only the unconfirmed messages** of the batch. | The publish rejects after the budget is spent |
| Repeated failures | The circuit breaker opens after `failureThreshold` consecutive failures and publishes fail fast until a probe succeeds. It resets to CLOSED on a successful reconnection — old failures say nothing about the new connection. | `circuitBreakerStateChanged` |
| Unroutable routing key | Silently dropped by AMQP — unless you pass `mandatory: true`, which makes the publish reject with `code: 'UNROUTABLE'`. | See [Unroutable publishes](#unroutable-publishes-mandatory) |
| Rate limit hit | The publish is rejected before reaching the broker, with `code: 'RATE_LIMIT_EXCEEDED'`. Retrying later succeeds. | `rateLimited` / `rateBlocked` |
| Rate limit cost above the limiter's capacity | Rejected immediately with `code: 'RATE_LIMIT_COST_UNSATISFIABLE'`. A `publishBatch` spends one unit per message, so a batch larger than the limit could never be admitted however long you wait — publish in smaller batches or raise the limit. | The publish rejects; no `rateLimited` event, because nothing was limited |

### Consuming

| Condition | What the library does | How it surfaces |
|-----------|----------------------|-----------------|
| Handler throws | The delivery is settled under the subscription's [`retryPolicy`](#failure-policy-retrypolicy) — `'none'` dead-letters, `'once'` retries a first delivery, `{ attempts: N }` spends a broker-counted budget. Decode failures follow the same rule. | `messageFailed` with the real `requeued` decision |
| Consumer channel dies | The consumer is recovered on a fresh channel, with backoff. Consumers never die silently — and a *connection*-level drop is told apart from a channel-level one, so it raises no consumer events at all (reconnection restores everything). | `consumerCancelled`, then `consumerRecovered` |
| `unsubscribe()` with a handler mid-message | The dedicated channel outlives every in-flight handler (bounded by `consumerDrainTimeout`, default 30s), so late acks land instead of dying with the channel and forcing redeliveries of work that succeeded. | A warn only if the grace period expires |
| Broker cancels the consumer (queue deleted) | Same recovery loop; when the attempts are exhausted the consumer is dropped **loudly**. | `consumerLost` |
| Worker thread dies (`subscribeParallel`) | Respawned up to `maxRespawns`; messages in flight on the dead worker fail and follow the retry policy. When the budget is gone, queued work is rejected instead of hanging. | `messageFailed` per message |
| Sequential dependency never arrives | After `staleTimeout` (default 30s) the parked message is settled under the retry policy — under `'once'` it goes back once (the dependency may still arrive), a redelivery is dead-lettered. | `messageFailed` with `durationMs: undefined` |
| Duplicate delivery of a parked message | Acked and dropped — the tracked original settles exactly once. | A single `messageProcessed` |

### RPC

| Condition | What the library does | How it surfaces |
|-----------|----------------------|-----------------|
| Responder is slow or gone | `request()` rejects after `timeout` (default 30s). The request is published with a matching `expiration`, so a request nobody will ever answer does not rot in the queue. | The request rejects |
| Responder queue does not exist | The request fails immediately instead of burning the caller's timeout. | Rejects with `code: 'RPC_UNROUTABLE'` |

### Process

| Condition | What the library does | How it surfaces |
|-----------|----------------------|-----------------|
| SIGINT / SIGTERM | With `enableGracefulShutdown()`, the client disconnects cleanly (and exits, unless `exitProcess: false`); closing the channels hands every unacked delivery back to its queue. | `disconnected` |
| Event-loop exit | Housekeeping timers (rate-limit sweeps, sequential cleanup, retry backoff sleeps) are unref'd and never hold the process open. A **pending reconnection does** — deliberately, so a worker that only consumes does not exit mid-outage as if it were done. | The process exits when the work is done |

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) has the setup, the commands and the rules a change has to meet — most of them are about the failure modes this library hides well, like a consumer that silently stops draining or a message settled on the wrong channel.

The short version:

- Small change: send a PR against `development`. Bigger change: open an issue first.
- A PR carries tests that fail without it, documentation, and an example when there is a choice for the user to make.
- Bugs and feature requests go through the [issue templates](https://github.com/pinceladasdaweb/rabbitmq/issues/new/choose). Security vulnerabilities do **not** — see [SECURITY.md](SECURITY.md).
- Participation is covered by the [Code of Conduct](CODE_OF_CONDUCT.md).

## Author
- Pedro Rogério - [Github](https://github.com/pinceladasdaweb)
