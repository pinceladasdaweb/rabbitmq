// Shared connection settings for every example.
//
// Values can be overridden through environment variables, matching the
// project's .env file (see .env.example):
//
//   RABBITMQ_USER / RMQ_USERNAME      broker username (default: admin)
//   RABBITMQ_PASS / RMQ_PASSWORD      broker password (default: admin)
//   RABBITMQ_ENDPOINT                 host:port (default: localhost:5672)
//
// Each example spreads this base and adds its own connectionName/exchange:
//
//   import { baseConfig } from '../config.mjs'
//
//   const rabbitConfig = {
//     ...baseConfig,
//     connectionName: 'my-example',
//     exchange: { name: 'my-exchange', type: 'direct' }
//   }

export const baseConfig = {
  username: process.env.RABBITMQ_USER || process.env.RMQ_USERNAME || 'admin',
  password: process.env.RABBITMQ_PASS || process.env.RMQ_PASSWORD || 'admin',
  endpoints: [process.env.RABBITMQ_ENDPOINT || 'localhost:5672']
}

export default baseConfig
