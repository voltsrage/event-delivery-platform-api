import { Queue } from "bullmq";

// BullMQ requires maxRetriesPerRequest: null - without it, IORedis's defaults
// timeout causes blocking commands to throw, which breaks the queue
export const redisConnection = {
    host: process.env.REDIS_HOST ?? 'localhost',
    port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
    maxRetriesPerRequest: null
};

export const retryQueue = new Queue('webhook-retry', {
    connection: redisConnection,
    defaultJobOptions: {
        // BullMQ-level retry attempts: 1. We manage retries ourselves - BullMQ
        // retrying a failed job processor would double-count attempts
        attempts: 1,
        removeOnComplete: 100, // keep last 100 completed jobs for debugging
        removeOnFail: 200 // keep last 200 failed jobs for inspection
    }
});