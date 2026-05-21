import { withTenant } from "../utils/withTenant.js";
import {deliverWebhook} from '../delivery/deliver.js';
import * as deliveryAttemptService from './deliveryAttemptService.js';
import { NotFoundError } from "../errors/AppError.js";
import { paginatedResponse } from "../utils/paginate.js";

export async function createDeadLetter({tenantId, eventId, subscriptionId, totalAttempts, lastError}){
    return withTenant(tenantId, async (tx) => {
        return tx.deadLetter.create({
            data: {
                tenantId,
                eventId,
                subscriptionId,
                totalAttempts,
                lastError: lastError ?? null,
                // resolvedAt is null until Phase 13 manual retry succeeds.
            }
        })
    })
}

function toPublicDeadLetter(dl){
    return {
        id: dl.id,
        eventId: dl.eventId,
        subscriptionId: dl.subscriptionId,
        totalAttempts: dl.totalAttempts,
        lastError: dl.lastError,
        resolvedAt: dl.resolvedAt,
        createdAt: dl.createdAt,
        eventType: dl.event?.eventType ?? null,
        endpointUrl: dl.subscription?.endpointUrl ?? null
    };
}

export async function listDeadLetters({tenantId, page, pageSize, skip}){
    const [items, total] = await withTenant(tenantId, async (tx) => {
        return Promise.all([
            tx.deadLetter.findMany({
                skip,
                take: pageSize,
                orderBy: {createdAt: 'desc'},
                include: {
                    event: {select : {eventType : true}},
                    subscription: {select : {endpointUrl: true}}
                }
            }),
            tx.deadLetter.count()
        ]);
    });

    return paginatedResponse(items.map(toPublicDeadLetter), total, page, pageSize);
}

export async function getDeadLetterById({tenantId, deadLetterId}){
    const result = await withTenant(tenantId, async (tx) => {
        const dl = await tx.deadLetter.findUnique(
            {
                where: {id: deadLetterId},
                include: {
                    event: {select: {id: true, eventType: true, payload: true, createdAt: true}},
                    subscription: {select: {id: true, endpointUrl: true}}
                }
            }
        );
        if(!dl) return null;
        
        const attempts = await tx.deliveryAttempt.findMany({
            where: {eventId: dl.eventId, subscriptionId: dl.subscriptionId},
            orderBy: {attemptNumber: 'asc'}
        });

        return {...dl, deliveryAttempts: attempts};
    });

    if(!result) throw new NotFoundError('Dead letter not found.', 'DEAD_LETTER_NOT_FOUND');

    return {
        ...toPublicDeadLetter(result),
        event: result.event,
        deliveryAttempts: result.deliveryAttempts
    };
}

export async function retryDeadLetter({tenantId, deadLetterId}){
    const {deadLetter, event, subscription, topic} = await withTenant(tenantId, async (tx) => {
        const dl = await tx.deadLetter.findUnique({where: {id: deadLetterId}});
        if(!dl) throw new NotFoundError('Dead letter not found', 'DEAD_LETTER_NOT_FOUND');

        const [event, subscription] = await Promise.all([
            tx.event.findUnique({where: {id: dl.eventId}}),
            tx.subscription.findUnique({
                where: {id: dl.subscriptionId},
                select: {id: true, endpointUrl: true, secretRaw: true}
            }),
        ]);

        const topic = event
            ? await tx.topic.findUnique({where: {id: event.topicId}})
            : null;
        
        return {deadLetter: dl, event, subscription, topic};
    });

    if(!event) throw new NotFoundError('Event not found', 'EVENT_NOT_FOUND');

    if(!subscription) throw NotFoundError("Subscription not found", 'SUBSCRIPTION_NOT_FOUND');

    const attempt = await deliveryAttemptService.createPendingAttempt({
        tenantId,
        eventId: deadLetter.eventId,
        subscriptionId: deadLetter.subscriptionId,
        attemptNumber: deadLetter.totalAttempts + 1
    });

    const result = await deliverWebhook({
        subscription: {endpoint: subscription.endpointUrl, secretRaw: subscription.secretRaw},
        event: {
            eventId: deadLetter.eventId,
            topicName: topic?.name ?? 'unknown',
            payload: event.payload
        }
    });

    if(result.success){
        // Resolve the attempt and mark the dead letter resolved in one transaction

        let resolvedAt;
        await withTenant(tenantId, async (tx) => {
            resolvedAt = new Date();
            await tx.deliveryAttempt.update({
                where: {id: attempt.id},
                data: {
                    status: 'success',
                    httpStatus: result.httpStatus,
                    responseBody: result.responseBody,
                    durationMs: result.durationMs,
                    nextRetryAt: null
                }
            });
            await tx.deadLetter.update({
                where: {id: deadLetterId},
                data: {resolvedAt}
            });
        });

        return {success: true, httpStatus: result.httpStatus, resolvedAt};
    }

    // Failed- record the attempt; resolvedAt stays null.
    await deliveryAttemptService.resolveAttempt({
        tenantId,
        attemptId: attempt.id,
        success: false,
        httpStatus: result.httpStatus,
        responseBody: result.responseBody,
        durationMs: result.durationMs,
        nextRetryAt: null
    });

    return {success: false, httpStatus: result.httpStatus, resolvedAt: null}
}
