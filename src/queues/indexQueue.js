import { Queue } from "bullmq";
import {redisConnection} from './retryQueue.js';

export const indexQueue = new Queue('delivery-log-index', {
    connection: redisConnection,
    defaultJobOptions: {
        // Unlike the retry queue (where we manage attempts manually), indexing
        // is idempotent - BullMQ can safely retry it automatically it ES is down
        attempts: 5,
        backoff: {type: 'exponential', delay: 5_000},
        removeOnComplete: 50,
        removeOnFail: 100
    }
})