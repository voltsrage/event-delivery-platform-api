import {prisma} from '../db/prisma.js';

export async function withTenant(tenantId, fn){
    return prisma.$transaction(async (tx) => {
        // SET LOCAL scopes the variables to this transaction only
        // When the transactions ends - commit or rollback - the variable is cleared.
        // A pooled connection reused by the next request cannot carry the previous
        // tenant's context. SET (without LOCAL) would persist on the connection and
        // leak context across requests

        // SET does not accept parameterized placeholders in PostgreSQL, so we
        // interpolate directly. tenantId is always a UUID from our own DB lookup.
        if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(tenantId)) {
            throw new Error(`Invalid tenantId: ${tenantId}`);
        }
        await tx.$executeRawUnsafe(`SET LOCAL app.current_tenant_id = '${tenantId}'`);

        return fn(tx);
    });
}