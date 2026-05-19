import {prisma} from '../db/prisma.js';
import { generateApiKey } from '../utils/generateApiKey.js';
import { NotFoundError, ConflictError } from '../errors/AppError.js';
import { toPublicApiKey } from './tenantService.js';

export async function createApiKey({tenantId, label}){
    const {rawKey, keyHash, keyPrefix} = generateApiKey();

    const apiKey = await prisma.apiKey.create({
        data: {
            tenantId,
            keyHash,
            keyPrefix,
            label: label ?? 'Default'
        }
    });

    return {apiKey: toPublicApiKey(apiKey), rawKey};
}

export async function listApiKeys({tenantId}){
    const keys = await prisma.apiKey.findMany({
        where: {tenantId},
        // Explicitly select only safe fields - keyHash must never be returned.
        // RLS does not protect api_keys: this WHERE tenantId is the only enforcement
        select: {
            id: true,
            keyPrefix: true,
            label: true,
            lastUsedAt: true,
            revokedAt: true,
            createdAt: true
        },
        orderBy: {createdAt: 'desc'}
    });

    return keys;
}

export async function revokeApiKey({tenantId, keyId}){
    // Scope by tenantId prevents revoking another tenant's key even if the ID is known.
    // RLS does not protect this table - this WHERE clause is the only isolation
    const key = await prisma.apiKey.findFirst({
        where: {id: keyId, tenantId}
    });

    if(!key)
        throw new NotFoundError('API key not found', 'KEY_NOT_FOUND');

    if(key.revokedAt)
        throw new ConflictError('API key is already revoked', 'KEY_ALREADY_REVOKED');

    await prisma.apiKey.update({
        where: {id: keyId},
        data: {revokedAt: new Date()}
    });
}