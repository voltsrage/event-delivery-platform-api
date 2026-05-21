import {fileURLToPath} from 'node:url';
import {Worker} from 'bullmq';
import { redisConnection, retryQueue } from '../queues/retryQueue.js';
import { withTenant } from '../utils/withTenant.js';
import { deliverWebhook } from '../delivery/deliver.js';
import * as deliveryService from '../services/deliveryAttemptService.js';
import { createDeadLetter } from '../services/deadLetterService.js';
import { computeRetryDelay, isLastAttempt } from '../utils/retrySchedule';

export async function processRetryJob(job){
    const { eventId, subscriptionId, tenantId, nextAttemptNumber } = job.data;

    // Reload event + subscription fom DB - not stored in the job payload
    // If the subscription was deleted or disabled between enqueue and now, drop the job
    const {event, subscription, topic} = await withTenant(tenantId, async(tx) =>{
        const [event, subscription] = await Promise.all([
            tx.event.findUnique({where: {id: eventId}}),
            tx.subscription.findUnique({
                where: {id: subscriptionId, enabled: true},
                select: {id: true, endpointUrl: true, secretRaw: true}
            })
        ]);
        const topic = event
            ? await tx.topic.findUnique({where: {id: event.topicId}})
            : null;

        return {event, subscription, topic};
    });

    if(!event || !subscription){
        // Event deleted or subscription disabled/deleted - nothing to deliver
        return;
    }

    const attempt = await deliveryService.createPendingAttempt({
        tenantId,
        eventId,
        subscriptionId,
        attemptNumber: nextAttemptNumber
    });

    const result = await deliverWebhook({
        subscription: {endpoint: subscription.endpointUrl, secretRaw: subscription.secretRaw},
        event: {eventId, topicName: topic?.name ?? 'unknown', payload: event.payload}
    });

    if(result.success) {
        await deliveryService.resolveAttempt({
            tenantId,
            attemptId: attempt.id,
            success: true,
            httpStatus: result.httpStatus,
            responseBody: result.responseBody,
            durationMs: result.durationMs,
            nextRetryAt: null,
        });

        const attemptedAt = new Date();
        
        await indexQueue.add('index-delivery-log', {
            attemptId: attempt.id,
            document: buildDeliveryLogDocument({
                tenantId,
                eventId,
                subscriptionId,
                topicName: topic?.name ?? 'unknown',
                endpoint: subscription.endpointUrl,
                status: 'success',
                httpStatus: result.httpStatus,
                attemptNumber: nextAttemptNumber,
                payload: event.payload,
                responseBody: result.responseBody,
                attemptedAt,
                nextRetryAt: null
            })
        });

        return;
    }

    // Delivery failed again
    if(isLastAttempt(nextAttemptNumber)){
        // All 5 attempts exhausted - move to dead letter.
        // deadLetterAttempt sets status = 'dead_lettered' on the attempt row.

        await deliveryService.deadLetterAttempt({
            tenantId,
            attemptId: attempt.id,
            httpStatus: result.httpStatus,
            responseBody: result.responseBody,
            durationMs: result.durationMs
        });

        const attemptedAt = new Date();
        
        await indexQueue.add('index-delivery-log', {
            attemptId: attempt.id,
            document: buildDeliveryLogDocument({
                tenantId,
                eventId,
                subscriptionId,
                topicName: topic?.name ?? 'unknown',
                endpoint: subscription.endpointUrl,
                status: 'dead_lettered',
                httpStatus: result.httpStatus,
                attemptNumber: nextAttemptNumber,
                payload: event.payload,
                responseBody: result.responseBody,
                attemptedAt,
                nextRetryAt: null
            })
        });

        await createDeadLetter({
            tenantId,
            eventId,
            subscriptionId,
            totalAttempts: nextAttemptNumber,
            lastError: result.responseBody
        });

        return;
    }
    
    // Schedule the next retry with the appropriate delay.
    const delay = computeRetryDelay(nextAttemptNumber);
    const nextRetryAt = new Date(Date.now() + delay);

    await deliveryService.resolveAttempt({
        tenantId,
        attemptId: attempt.id,
        success: false,
        httpStatus: result.httpStatus,
        responseBody: result.responseBody,
        durationMs: result.durationMs,
        nextRetryAt
    });

    const attemptedAt = new Date();
        
    await indexQueue.add('index-delivery-log', {
        attemptId: attempt.id,
        document: buildDeliveryLogDocument({
            tenantId,
            eventId,
            subscriptionId,
            topicName: topic?.name ?? 'unknown',
            endpoint: subscription.endpointUrl,
            status: 'failed',
            httpStatus: result.httpStatus,
            attemptNumber: nextAttemptNumber,
            payload: event.payload,
            responseBody: result.responseBody,
            attemptedAt,
            nextRetryAt
        })
    });

    await retryQueue.add(
        'retry',
        {
            eventId,
            subscriptionId,
            tenantId,
            nextAttemptNumber: nextAttemptNumber + 1
        },
        {delay}
    );
}

// Worker instance - exported so tests can close it cleanly
export const worker = new Worker('webhook-retry', processRetryJob, {
    connection: redisConnection,
    concurrency: 10 // process up to 10 retry jobs concurrently
});

worker.on('failed', (job, err) => {
    console.error('[retry-worker] job failed', { jobId: job?.id, error: err.message });
});

// Only run when executed directly
const isMain = process.argv[1] === fileURLToPath(import.meta.url);
if (isMain) {
    console.log('[retry-worker] started — concurrency=10');

    process.on('SIGTERM', async () => {
        console.log('[retry-worker] SIGTERM — shutting down');
        await worker.close();
        process.exit(0);
    });
}