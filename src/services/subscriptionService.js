import { withTenant } from "../utils/withTenant.js";
import { generateSigningSecret } from "../utils/generateSigningSecret.js";
import {paginatedResponse} from '../utils/paginate.js';
import { NotFoundError, ConflictError, ValidationError } from "../errors/AppError.js";
import {prisma} from '../db/prisma.js';

function assertHttps(endpoint)
{
    if(!endpoint.startsWith('https://'))
    {
        throw new ValidationError(
            'Endpoint must use HTTPS',
            'ENDPOINT_MUST_BE_HTTPS',
        );
    }
}

function toPublicSubscription(sub, rawSecret = null){
    const out = {
        id: sub.id,
        topicId: sub.topicId,
        endpoint: sub.endpointUrl,
        isEnabled: sub.enabled,
        secretPrefix: sub.secretPrefix,
        createdAt: sub.createdAt,
        updatedAt: sub.updatedAt
    };

    // rawSecret is only present immediately after creation or rotation.
    // It is never read from the DB after that point.
    if(rawSecret !== null) out.secret = rawSecret;
    return out;
}

export async function createSubscription({tenantId, topicId, endpoint}){
    assertHttps(endpoint);

    const { rawSecret, secretHash, secretPrefix} = generateSigningSecret();

    const sub = await withTenant(tenantId, async (tx) => {
        // Quota check: read limit and current count inside the same transaction
        // to prevent TOCTOU race (two concurrent creates both passing the check)
        const tenant = await tx.tenant.findUnique({where: {id: tenantId}});
        const count = await tx.subscription.count();

        // RLS ensure count() only counts this tenant's subscriptions.
        if(count >= tenant.maxSubscriptions) {
            throw new ConflictError(
                `Subscription limit reached (${tenant.maxSubscriptions})`,
                'SUBSCRIPTION_LIMIT_REACHED'
            )
        }

        // Verify the topic exists and belongs to this tenant (not deleted)
        const topic = await tx.topic.findFirst({
            where: {id: topicId, deletedAt: null}
        });
        if(!topic) throw new NotFoundError('Topic not found', 'TOPIC_NOT_FOUND');

        return tx.subscription.create({
            data: {tenantId, topicId, endpointUrl: endpoint, secretHash, secretRaw: rawSecret, secretPrefix}
        });
    });

    return toPublicSubscription(sub, rawSecret);
}

export async function listSubscriptions({tenantId, topicId, page, pageSize, skip}){
    const [items, total] = await withTenant(tenantId, async(tx) => {
        // Verify topic exists for this tenant before listing its subscriptions
        const topic = await tx.topic.findFirst({
            where: {id: topicId, deleteAt: null}
        });
        if(!topic) throw new NotFoundError('Topic not found.', 'TOPIC_NOT_FOUND');

        return Promise.all([
            tx.subscription.findMany({
                where: {topicId},
                skip,
                take: pageSize,
                orderBy: {createdAt: 'asc'},
            }),
            tx.subscription.count({where: {topicId}})
        ]);
    });

    return paginatedResponse(items.map((s) => toPublicSubscription(s)), total, page, pageSize);
}

export async function getSubscriptionById({tenantId, subscriptionId}){
    const sub = await withTenant(tenantId, async (tx) => {
        return tx.subscription.findUnique({where: {id: subscriptionId}});
    });
    if(!sub) throw new NotFoundError('Subscription not found.', 'SUBSCRIPTION_NOT_FOUND');
    return toPublicSubscription(sub);
}

export async function updateSubscription({tenantId, subscriptionId, endpoint, isEnabled}){
    if(endpoint !== undefined) assertHttps(endpoint);

    const sub = await withTenant(tenantId, async (tx) => {
        const existing = await tx.subscription.findUnique({where: {id: subscriptionId}});
        if(!existing) throw new NotFoundError('Subscription not found.', 'SUBSCRIPTION_NOT_FOUND');

        return tx.subscription.update({
            where: {id: subscriptionId},
            data: {
                ...(endpoint !== undefined && {endpointUrl: endpoint}),
                ...(isEnabled !== undefined && {enabled: isEnabled})
            }
        });
    });

    return toPublicSubscription(sub);
}

export async function deleteSubscription({tenantId, subscriptionId})
{
    await withTenant(tenantId, async (tx) => {
        const existing = await tx.subscription.findUnique({where: {id: subscriptionId}});
        if(!existing) throw new NotFoundError('Subscription not found.', 'SUBSCRIPTION_NOT_FOUND');

        await tx.subscription.delete({where: {id: subscriptionId}});
    });
}

export async function rotateSecret({tenantId, subscriptionId}){
    const {rawSecret, secretHash, secretPrefix} = generateSigningSecret();

    const sub = await withTenant(tenantId, async (tx) => {
        const existing = await tx.subscription.findUnique({where: {id: subscriptionId}});
        if(!existing) throw new NotFoundError('Subscription not found.', 'SUBSCRIPTION_NOT_FOUND');

        return tx.subscription.update({
            where: {id: subscriptionId},
            data: {secretHash, secretRaw: rawSecret, secretPrefix}
        });
    });

    // Return the new raw secret once. The old secret is immediately invalidated -
    // any in-flight deliveries that signed with the old secret will fail verification
    // on the subscriber side until they update. This brief interruption window is
    // on accepted trade-off of single-active-secret rotation
    return toPublicSubscription(sub, rawSecret);
}