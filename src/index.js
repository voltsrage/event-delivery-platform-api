import 'dotenv/config';
import { buildApp } from './app.js';
import pino from 'pino';
import { loggerOptions } from './utils/logger.js';

const logger = pino(loggerOptions);
const PORT = parseInt(process.env.PORT || '3075', 10);

async function start() {
    const app = await buildApp();

    try
    {
        await app.listen({ port: PORT, host: '0.0.0.0'});
        logger.info({port: PORT}, 'Server Started');
    }
    catch (err)
    {
        logger.error(err, 'Failed to start server');
        process.exit(1)
    }
}

start();
