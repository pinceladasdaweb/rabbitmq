/// <reference types="node" />

import { EventEmitter } from 'node:events'

export interface LoggerLike {
  info: (message: string, ...args: unknown[]) => void
  warn: (message: string, ...args: unknown[]) => void
  error: (message: string, ...args: unknown[]) => void
  debug?: (message: string, ...args: unknown[]) => void
}

export interface ExchangeOptions {
  name: string
  type?: 'direct' | 'topic' | 'fanout' | 'headers'
  options?: Record<string, unknown>
}

export interface CircuitBreakerOptions {
  failureThreshold?: number
  successThreshold?: number
  timeout?: number
}

export interface CircuitBreakerState {
  state: 'CLOSED' | 'OPEN' | 'HALF-OPEN'
  failureCount: number
  successCount: number
  nextAttempt: number
}

export type RateLimiterStrategy = 'token-bucket' | 'leaky-bucket' | 'fixed-window' | 'sliding-window'

export interface RateLimiterOptions {
  windowMs?: number
  maxRequests?: number
  strategy?: RateLimiterStrategy
  burstable?: boolean
  burstLimit?: number
  queueLimit?: number
}

export interface RateLimitStatus {
  strategy: RateLimiterStrategy
  remainingTokens: number
  isBlocked: boolean
  windowMs: number
  maxRequests: number
  burstable: boolean
  currentTime: number
}

export interface RabbitMQOptions {
  username?: string
  password?: string
  protocol?: 'amqp' | 'amqps'
  vhost?: string
  endpoint?: string
  endpoints?: string[]
  connectionName?: string
  reconnectInterval?: number
  maxReconnectInterval?: number
  maxReconnectAttempts?: number
  exchange?: ExchangeOptions
  prefetchCount?: number
  channelPoolSize?: number
  /** Base backoff in ms between attempts to recreate a dead pool channel (attempt N waits N * this, 5 attempts before the slot is dropped). Default: 500. */
  channelRecoveryInterval?: number
  /** Base backoff in ms between attempts to recover a broker-cancelled consumer (attempt N waits N * this). Default: 1000. */
  consumerRecoveryInterval?: number
  /** How long unsubscribe() waits for in-flight handlers to finish before closing the consumer's dedicated channel anyway. Default: 30000. */
  consumerDrainTimeout?: number
  useCompression?: boolean
  compressionThreshold?: number
  serializer?: (message: unknown) => string
  deserializer?: (message: string) => unknown
  circuitBreaker?: CircuitBreakerOptions
  maxPriority?: number
  deadLetterExchange?: string
  delayExchange?: string
  useCache?: boolean
  cacheTTL?: number
  cacheCheckPeriod?: number
  cacheOptions?: Record<string, unknown>
  rateLimiter?: RateLimiterOptions
  logger?: LoggerLike
}

export interface PublishOptions {
  persistent?: boolean
  priority?: number
  messageId?: string
  headers?: Record<string, unknown>
  maxRetries?: number
  retryDelay?: number
  rateLimitKey?: string
  rateLimitCost?: number
  cacheTTL?: number
  /**
   * Ask the broker to hand the message back instead of dropping it when no
   * queue is bound for the routing key. Without it an unroutable publish is
   * discarded in silence and still confirmed, so the caller is told it
   * succeeded; with it, the publish rejects with `code: 'UNROUTABLE'`.
   *
   * Honored by `publish`, `publishBatch` and `publishDelayed`. The
   * fire-and-forget methods (`publishAsync`, `publishAsyncBatch`) cannot see
   * the broker's return and REJECT this option instead of silently ignoring
   * it.
   */
  mandatory?: boolean
  [key: string]: unknown
}

/** Values of `error.code` on errors rejected by the publish methods. */
export type PublishErrorCode = 'RATE_LIMIT_EXCEEDED' | 'UNROUTABLE'

/**
 * What happens to a message whose processing failed.
 *
 * - `'none'`: nack without requeue — the message goes straight to the DLQ.
 * - `'once'`: a first delivery is requeued and retried; a delivery already
 *   marked `redelivered`, or one whose error carries `retryable === false`,
 *   is dead-lettered instead. Never more than one retry, so a permanently
 *   failing message cannot hot-loop.
 * - `{ attempts: N }`: a real budget of N deliveries, counted by the broker
 *   through a quorum queue's `x-delivery-count`. Unlike `'once'` — which
 *   reads the `redelivered` flag any requeue sets, including one caused by a
 *   connection drop — this counts actual deliveries. On a classic queue the
 *   header does not exist and the policy degrades to the `'once'` ceiling
 *   rather than looping on a budget the broker cannot track.
 */
export type RetryPolicy = 'none' | 'once' | { attempts: number }

/**
 * Error shape a consumer handler can throw to opt out of the subscription's
 * retry. Setting `retryable = false` skips the retry under `'once'`; setting
 * it to `true` does not create one under `'none'`.
 */
export interface RetryableError extends Error {
  retryable?: boolean
}

export interface SubscribeOptions {
  noAck?: boolean
  prefetchCount?: number
  /**
   * Failure policy for this subscription. Defaults to `'none'` everywhere
   * except `subscribeSequential`, which defaults to `'once'`.
   *
   * The policy is a ceiling: a handler can decline a retry with
   * `error.retryable = false`, but cannot force one under `'none'`.
   * An unrecognized value throws at subscribe time.
   */
  retryPolicy?: RetryPolicy
  [key: string]: unknown
}

/** Values of `error.code` on errors rejected by request(). */
export type RpcErrorCode = 'RPC_TIMEOUT' | 'RPC_CONNECTION_LOST' | 'RPC_RESPONDER_ERROR' | 'RPC_UNROUTABLE'

export interface RequestOptions extends PublishOptions {
  /** Milliseconds to wait for the reply before rejecting with code 'RPC_TIMEOUT'. Default: 30000. */
  timeout?: number
  /**
   * Publish attempts for the request. Unlike other publish methods (default 3),
   * request() defaults to 1: republishing a request whose confirm was lost can
   * execute the responder twice. Opt in explicitly if that is acceptable.
   */
  maxRetries?: number
}

export interface RespondOptions extends SubscribeOptions {
  /**
   * When true, a handler crash is published back to the requester as a
   * structured error (the request rejects with code 'RPC_RESPONDER_ERROR').
   * When false (default), the request is nacked to the DLQ and the requester
   * surfaces the failure through its timeout.
   */
  replyOnError?: boolean
}

export interface OptimizedPrefetchOptions extends SubscribeOptions {
  initialPrefetch?: number
  maxPrefetch?: number
  minPrefetch?: number
  optimizationInterval?: number
  increaseFactor?: number
  decreaseFactor?: number
}

export interface ParallelSubscribeOptions extends SubscribeOptions {
  workerCount?: number
  prefetch?: number
  maxRespawns?: number
}

export interface ConnectOptions {
  waitForConnection?: boolean
  timeout?: number
}

export interface DelayExchangeOptions {
  type?: 'direct' | 'topic' | 'fanout' | 'headers'
  exchangeOptions?: Record<string, unknown>
}

export interface SequentialSubscribeOptions extends SubscribeOptions {
  staleTimeout?: number
  /**
   * Defaults to `'once'` here, unlike every other subscribe method. The
   * requeued message goes back to the queue while later ones keep being
   * processed, so the retry can break the ordering this method provides —
   * pass `'none'` when order matters more than the retry.
   */
  retryPolicy?: RetryPolicy
}

export interface ConsumeMessageFields {
  consumerTag: string
  deliveryTag: number
  redelivered: boolean
  exchange: string
  routingKey: string
}

export interface ConsumeMessageProperties {
  contentType?: string
  contentEncoding?: string
  headers?: Record<string, unknown>
  deliveryMode?: number
  priority?: number
  correlationId?: string
  replyTo?: string
  expiration?: string
  messageId?: string
  timestamp?: number
  type?: string
  userId?: string
  appId?: string
  [key: string]: unknown
}

export interface ConsumeMessage {
  content: Buffer
  fields: ConsumeMessageFields
  properties: ConsumeMessageProperties
}

export interface Consumer {
  consumerTag: string
}

export interface MessageProcessedEvent {
  queue: string
  messageId?: string
  consumerTag?: string
  durationMs?: number
}

export interface MessageFailedEvent {
  queue: string
  messageId?: string
  consumerTag?: string
  /**
   * Absent when the message never ran — e.g. a sequential message that
   * expired waiting for its `depends-on` dependency.
   */
  durationMs?: number
  error: unknown
  /**
   * What actually happened to the delivery. Always `false` under `noAck`,
   * whatever the retry policy says.
   */
  requeued: boolean
}

export interface ClusterStatus {
  connectedTo: string
  allEndpoints: string[]
  connectionState: 'disconnected' | 'connecting' | 'connected' | 'reconnecting' | 'disconnecting' | 'failed'
}

export interface GracefulShutdownOptions {
  signals?: NodeJS.Signals[]
  exitProcess?: boolean
}

export type MessageCallback = (content: unknown, message: ConsumeMessage) => void | Promise<void>

export type RpcHandler = (content: unknown, message: ConsumeMessage) => unknown | Promise<unknown>

export declare class RabbitMQ extends EventEmitter {
  constructor (options?: RabbitMQOptions)

  on (event: 'messageProcessed', listener: (event: MessageProcessedEvent) => void): this
  on (event: 'messageFailed', listener: (event: MessageFailedEvent) => void): this
  on (event: string | symbol, listener: (...args: any[]) => void): this

  once (event: 'messageProcessed', listener: (event: MessageProcessedEvent) => void): this
  once (event: 'messageFailed', listener: (event: MessageFailedEvent) => void): this
  once (event: string | symbol, listener: (...args: any[]) => void): this

  connect (options?: ConnectOptions): Promise<unknown | null>
  disconnect (): Promise<void>
  getChannel (): Promise<unknown>
  getClusterStatus (): ClusterStatus
  enableGracefulShutdown (options?: GracefulShutdownOptions): void
  /** @deprecated Renamed to `enableGracefulShutdown()`; this shim delegates and warns. */
  setupGracefulShutdown (options?: GracefulShutdownOptions): void

  setExchange (name: string, type?: 'direct' | 'topic' | 'fanout' | 'headers', options?: Record<string, unknown>): void

  publish (routingKey: string, message: unknown, options?: PublishOptions): Promise<void>
  publishBatch (routingKey: string, messages: unknown[], options?: PublishOptions): Promise<void>
  publishAsync (routingKey: string, message: unknown, options?: PublishOptions): Promise<void>
  publishAsyncBatch (routingKey: string, messages: unknown[], options?: PublishOptions): Promise<void>
  publishWithCache (routingKey: string, messageGenerator: unknown | (() => unknown | Promise<unknown>), options?: PublishOptions): Promise<unknown>
  publishDelayed (routingKey: string, message: unknown, delayMs: number, options?: PublishOptions): Promise<void>

  request (routingKey: string, message: unknown, options?: RequestOptions): Promise<unknown>
  respond (queueName: string, handler: RpcHandler, options?: RespondOptions): Promise<Consumer>

  subscribe (queueName: string, callback: MessageCallback, options?: SubscribeOptions): Promise<Consumer>
  subscribeWithOptimizedPrefetch (queueName: string, callback: MessageCallback, options?: OptimizedPrefetchOptions): Promise<Consumer>
  subscribeParallel (queueName: string, processorFile: string, options?: ParallelSubscribeOptions): Promise<Consumer>
  subscribeSequential (queueName: string, callback: MessageCallback, options?: SequentialSubscribeOptions): Promise<Consumer>
  unsubscribe (consumerTag: string): Promise<boolean>

  acknowledgeMessage (message: ConsumeMessage): Promise<void>
  negativeAcknowledgeMessage (message: ConsumeMessage, options?: { requeue?: boolean }): Promise<void>

  setCompression (useCompression: boolean): void
  setCompressionThreshold (threshold: number): void
  setSerializer (serializer: (message: unknown) => string): void
  setDeserializer (deserializer: (message: string) => unknown): void

  getCircuitBreakerState (): CircuitBreakerState

  setupDeadLetterExchange (): Promise<void>
  createQueue (queueName: string, options?: Record<string, unknown>): Promise<void>
  moveToDeadLetter (message: ConsumeMessage, reason?: string): Promise<void>
  processDeadLetterQueue (originalQueueName: string, processor: (content: unknown) => void | Promise<void>, options?: SubscribeOptions): Promise<Consumer>

  setupDelayPlugin (): Promise<void>
  setupDelayExchange (options?: DelayExchangeOptions): Promise<void>
  isDelayPluginEnabled (): Promise<boolean>

  getRateLimitStatus (key: string): RateLimitStatus
  resetRateLimit (key: string): void
  blockRateLimit (key: string, duration?: number): void

  getFromCache (routingKey: string): Promise<unknown>
  invalidateCache (routingKey: string): void
  clearCache (): void
}

export default RabbitMQ
