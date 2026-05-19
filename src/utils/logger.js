const isDev = process.env.NODE_ENV !== 'production';

const transportConfig = isDev
  ? { transport: { target: 'pino-pretty', options: { colorize: true } } }
  : {};

// For module-level or worker-process logging outside of a request context
export const loggerOptions = {
  level: process.env.LOG_LEVEL || 'info',
  ...transportConfig,
};

// Passed to the Fastify constructor — Fastify creates its own Pino instance internally
export const fastifyLoggerOptions = {
  level: process.env.LOG_LEVEL || 'info',
  ...transportConfig,
};