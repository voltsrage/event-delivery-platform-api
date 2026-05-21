const isDev   = process.env.NODE_ENV !== 'production';
const seqUrl  = process.env.SEQ_SERVER_URL || '';

// Build the pino transport config based on the runtime environment.
//
// Four cases:
//   prod + Seq   → structured JSON sent to Seq only
//   prod + no Seq → no transport (pino writes raw JSON to stdout)
//   dev  + Seq   → pretty-print locally AND send to Seq
//   dev  + no Seq → pretty-print only
function buildTransport() {
  if (seqUrl && isDev) {
    return {
      transport: {
        targets: [
          { target: 'pino-pretty', options: { colorize: true }, level: 'trace' },
          { target: 'pino-seq',    options: { serverUrl: seqUrl }, level: 'trace' },
        ],
      },
    };
  }
  if (seqUrl) {
    return { transport: { target: 'pino-seq', options: { serverUrl: seqUrl } } };
  }
  if (isDev) {
    return { transport: { target: 'pino-pretty', options: { colorize: true } } };
  }
  return {};
}

const transportConfig = buildTransport();

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