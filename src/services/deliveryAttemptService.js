import {withTenant} from '../utils/withTenant.js';

export async function createPendingAttempt({tenantId, eventId, subscriptionId, attemptNumber}){
    return withTenant(tenantId, async (tx) => {
        return tx.deliveryAttempt.create({
            data: {
                tenantId,
                eventId,
                subscriptionId,
                attemptNumber,
                status: 'pending'
            }
        });
    });
}

export async function resolveAttempt({
    tenantId, attemptId, success, httpStatus, responseBody, durationMs, nextRetryAt,
})
{
    return withTenant(tenantId, async (tx) => {
        return tx.deliveryAttempt.update({
            where: {id: attemptId},
            data: {
                status: success ? 'success' : 'failed',
                httpStatus: httpStatus ?? null,
                responseBody: responseBody ?? null,
                durationMs: durationMs ?? null,
                nextRetryAt: nextRetryAt ?? null
            }
        });
    });
}