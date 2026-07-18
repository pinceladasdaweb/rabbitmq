import pino from 'pino'

const isDevelopment = process.env.NODE_ENV === 'development'

const baseConfig = {
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level: (level) => ({ level }),
    ...(!isDevelopment ? { bindings: (bindings) => ({ hostname: bindings.hostname }) } : {})
  },
  timestamp: () => `,"time":"${new Date(Date.now()).toISOString()}"`
}

let logger

try {
  logger = pino({
    ...baseConfig,
    ...(isDevelopment && {
      transport: {
        target: 'pino-pretty',
        options: {
          colorize: true,
          levelFirst: true,
          ignore: 'time,pid,hostname'
        }
      }
    })
  })
} catch (error) {
  logger = pino(baseConfig)
}

export default logger
