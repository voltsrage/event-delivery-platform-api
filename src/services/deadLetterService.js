import { withTenant } from "../utils/withTenant.js";

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
