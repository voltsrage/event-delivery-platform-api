import {fileURLToPath} from 'node:url';
import kafka from '../kafka/client.js';
import { withTenant } from '../utils/withTenant.js';
import {deliverWebhook} from '../delivery/deliver.js';
import { createPendingAttempt, resolveAttempt } from '../services/deliveryAttemptService.js';

const CONSUMER_GROUP = 'webhook-delivery-worker';
const consumer = kafka.consumer({groupId: CONSUMER_GROUP});

// Exported so Vitest can call it directly without starting the Kafka consumer loop
export async function processEvent(event){
    const {eventId, tenantId, topicId, topicName, payload} = event;

    // Load all active subscriptions for this (tenant, topic) pairs
    // withTenant sets RLS context via SET LOCAL - the query is already tenant-scoped
    // at the database layer even though this is a background worker, not na API request.
    const subscriptions = await withTenant(tenantId, async (tx)=> {
        return tx.subscription.findMany({
            where: {topicId, enabled: true},
            select: {id: true, endpointUrl: true, secretRaw: true}
        });
    });

    // No active subscriptions - nothing to do. This is not an error; topics can
    // exist without subscriptions (e.g., all subscriptions deleted or disabled)
    if(subscriptions.length === 0) return;

    // Process every subscription before committing the Kafka offset (autoCommit: false)
    // If the worker crashes here, the offset is uncommitted -> the event is re-consumed
    // -> subscriptions already delivered get a duplicate (they deduplicate via X-Webhook-Event-Id).
    // This is at-least-once delivery - documented and accepted
    for(const sub of subscriptions){
        const attempt = await createPendingAttempt({
            tenantId,
            eventId,
            subscriptionId: sub.id,
            attemptNumber: 1
        });

        const result = await deliverWebhook({
            subscription: {endpoint: sub.endpointUrl, secretRaw: sub.secretRaw},
            event: {eventId, topicName, payload}
        });

        await resolveAttempt({
            tenantId,
            attemptId: attempt.id,
            success: result.success,
            httpStatus: result.httpStatus,
            responseBody: result.responseBody,
            durationMs: result.durationMs,
            nextRetryAt: null, // Phase 9 computes the backoff schedule here.
        });

        if(!result.success){
            // Phase 9: enqueue BullMQ retry job with computed delay.
            // Logging only in Phase 8.
            console.warn('[delivery] failed — retry scheduling deferred to Phase 9', {
                eventId,
                subscriptionId: sub.id,
                httpStatus:     result.httpStatus,
            });
        }
    }
}

export async function startDeliveryWorker() {
    await consumer.connect();
    await consumer.subscribe({
        topic: process.env.KAFKA_TOPIC ?? 'platform.events',
        fromBeginning: false,
    });

    await consumer.run({
        // autoCommit: false - we commit manually after ALL subscriptions for an event
        // are processed. Default autoCommit: true commits on a timer and could commit
        // an offset before processEvent finishes, breaking the at-least-once guarantee.
        autoCommit: false,
        eachMessage: async({ topic, partition, message}) => {
            const event = JSON.parse(message.value.toString());
            await processEvent(event);

            // Commit offset only after the full fan-out is complete
            // offset + 1 tells the broker: "I have processed up to and including this offset."
            await consumer.commitOffsets([{
                topic,
                partition,
                offset: (BigInt(message.offset) + 1n).toString(),
            }])
        }
    });
}

export async function stopDeliveryWorker() {
    await consumer.disconnect();
}

// Only run when executed directly (not when imported by Vitest)
const isMain = process.argv[1] === fileURLToPath(import.meta.url);

/*
The `isMain` guard is the ESM equivalent of `if (require.main === module)`. 
Without it, every `import` of this file from a test would immediately try to connect to Kafka — 
which fails in test environments with no broker.
*/
if(isMain){
    console.log('[delivery-worker] starting...');

    process.on('SIGTERM', async () => {
        console.log('[delivery-worker] SIGTERM - shutting down');
        await stopDeliveryWorker();
        process.exit(0);
    });

    startDeliveryWorker()
        .then(() => console.log('[delivery-worker] running - waiting for events'))
        .catch((err) => {console.error('[delivery-worker] fatal: ', err); process.exit(1);});
}