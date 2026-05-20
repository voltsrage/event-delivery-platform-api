import { withTenant } from "../utils/withTenant.js";
import { NotFoundError } from "../errors/AppError.js";

function toPublicEvent(event){
    return {
        id: event.id,
        topicId: event.topicId,
        eventType: event.eventType,
        payload: event.payload,
        // Echo idempotencyKey back so callers can correlate; omit when null
        ...(event.idempotencyKey && {idempotencyKey: event.idempotencyKey}),
        createdAt: event.createdAt
    };
}

export async function createEvent({tenantId, topicId, eventType, payload, idempotencyKey}){
    const event = await withTenant(tenantId, async (tx) => {
        // RLS scopes this to tenantId via SET LOCAL - no explicit where tenantId clause needed
        const topic = await tx.topic.findFirst({
            where: {id: topicId, deletedAt: null}
        });
        if(!topic) throw new NotFoundError('Topic not found.', 'TOPIC_NOT_FOUND');

        // If a Key is supplied and an event with that key already exists for this tenant.
        // return it without inserting. RLS ensures the lookup is already tenant-scoped.
        // The partial unique index(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL
        // is the hard constraint; this check is the clean early-exit path
        if(idempotencyKey){
            const existing = await tx.event.findFirst({where: {idempotencyKey}});
            if(existing) return existing;
        }

        return tx.event.create({
            data:{
                tenantId,
                topicId,
                eventType,
                payload,
                idempotencyKey: idempotencyKey ?? null,
                publishedToKafka: false
            }
        })
    });

    return toPublicEvent(event);
}