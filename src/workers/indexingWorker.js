import {fileURLToPath} from 'node:url';
import { Worker } from 'bullmq';
import { redisConnection } from '../queues/retryQueue.js';
import esClient from '../search/esClient.js';
import { DELIVERY_LOGS_INDEX,ensureDeliveryLogsIndex } from '../search/deliveryLogsIndex.js';

// Exported for Vitest - allows testing processIndexJob without starting the Worker loop.
export async function processIndexJob(job){
    const {attemptId, document} = job.data;

    // Using attemptId as the Elasticsearch document _id makes this call idempotent.
    // If BullMQ retries this job (ES was temporarily down), the second call overwrites
    // the first with identical data - no duplicate documents are created.
    await esClient.index({
        index: DELIVERY_LOGS_INDEX,
        id: attemptId,
        document: document,
        // refresh: false (default) - the document may not be visible to searches
        // immediately. This is acceptable; the delivery log is eventually consistent
    })
}

/** 
`concurrency: 20` is intentionally higher than the retry worker's 10. 
Elasticsearch indexing is a single HTTP PUT per job — I/O-bound and very fast.
High concurrency amortises the per-job overhead. 
If a backlog builds during an ES outage, the worker drains it quickly once ES recovers.
 */
export const worker = new Worker(
    'delivery-log-index',
    processIndexJob,
    {
        connection: redisConnection,
        concurrency: 20 // indexing is I/O bound, high concurrency is safe and efficient
    }
)

worker.on('failed',(job, err) => {
    console.error('[indexing-worker] job failed', { jobId: job?.id, error: err.message });
});

const isMain = process.argv[1] === fileURLToPath(import.meta.url);
if (isMain) {
    console.log('[indexing-worker] starting...');

    await ensureDeliveryLogsIndex();
    console.log('[indexing-worker] running');

    process.on('SIGTERM', async () => {
        console.log('[indexing-worker] SIGTERM — shutting down');
        await worker.close();
        process.exit(0);
    });
}