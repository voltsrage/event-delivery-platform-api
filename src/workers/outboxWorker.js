import { fileURLToPath } from 'url';
import {workerPrisma} from '../db/workerClient.js';
import {publishToKafka} from '../kafka/producer.js';

const POLL_INTERVAL_MS = 1000;
const BATCH_SIZE = 100;

export async function pollOutbox() {
    await workerPrisma.$transaction(async (tx) => {
        // FOR UPDATE of e SKIP LOCKED
        // -Locks only events rows, not the joined topics row
        // - Two worker instances running simultaneously each get a distinct,
        //   non-overlapping batch - no coordination beyond the database lock manager.
        // - idx_events_outbox (partial index on published_to_kafka = FALSE) keeps
        //   this scan fast regardless of total historical event volume
        const events = await tx.$queryRaw`
            SELECT
                e.id,
                e.tenant_id as "tenantId",
                e.topic_id as "topicId",
                t.name as "topicName",
                e.event_type as "eventType",
                e.payload,
                e.created_at as "createdAt"
            FROM public.events e
            JOIN public.topics t on t.id = e.topic_id
            WHERE e.published_to_kafka = FALSE
            ORDER BY e.created_at ASC
            LIMIT ${BATCH_SIZE}
            FOR UPDATE OF e SKIP LOCKED
        `;

        for (const event of events){
            // If publishToKafka throws, the transaction rolls back.
            // published_to_kafka stays false and the event is retried on the next poll
            // This is the core guarantee of the outbox pattern: either the event reaches
            // Kafka AND is marked done, or neither happens

            await publishToKafka({
                eventId: event.id,
                tenantId: event.tenantId,
                topicId: event.topicId,
                topicName: event.topicName,
                eventType: event.eventType,
                payload: event.payload,
                publishedAt: new Date().toISOString()
            });

            await tx.$executeRaw`
                UPDATE public.events SET published_to_kafka = true WHERE id = ${event.id}
            `;
        }
    });
}

async function run() {
    console.log('[outbox-worker] started');
    while(true)
    {
        try {
            await pollOutbox();
        }
        catch(err){
            // Transient errors (network blip, lock contention) should not kill the worker.
            console.error('[outbox-worker] poll error:', err.message);
        }
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
    }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
    run().catch((err) => {
        console.error('[outbox-worker] fatal startup error:', err);
        process.exit(1);
    });
}