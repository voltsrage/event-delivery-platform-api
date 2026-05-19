import {prisma} from '../db/prisma.js';
import { generateApiKey } from '../utils/generateApiKey.js';
import {ConflictError} from '../errors/AppError.js';

export async function createTenant({name, email})
{
    const existing = await prisma.tenant.findUnique({where: {email}});
    if(existing)
        throw new ConflictError('An account with this email already exists', 'EMAIL_TAKEN');

    const {rawKey, keyHash, keyPrefix} = generateApiKey();

    // Both inserts are atomic - if apiKey.create fails, the tenant row is rolled back.
    const {tenant, apiKey} = await prisma.$transaction(async (tx) => {
        const tenant = await tx.tenant.create({
            data: {name, email}
        });

        const apiKey = await tx.apiKey.create({
            data: {
                tenantId: tenant.id,
                keyHash,
                keyPrefix,
                label: 'Default'
            }
        });

        return {tenant, apiKey};
    });

    // rawKey is returned here and discarded, it is never stored anywhere in the database
    // The caller is responsible for including it in the response body exactly once.

    return {
        tenant: toPublicTenant(tenant),
        apiKey: toPublicApiKey(apiKey),
        rawKey
    };
}

function toPublicTenant(tenant){
    return {
        id: tenant.id,
        name: tenant.name,
        email: tenant.email,
        maxSubscriptions: tenant.maxSubscriptions,
        createdAt: tenant.createdAt
    };
}

function toPublicApiKey(key)
{
    return {
        id: key.id,
        keyPrefix: key.keyPrefix,
        label: key.label,
        lastUsedAt: key.lastUsedAt,
        createdAt: key.createdAt
    };
}

export {toPublicApiKey};