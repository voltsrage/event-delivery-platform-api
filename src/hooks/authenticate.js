import crypto from 'crypto';
import {prisma} from '../db/prisma.js';
import { withTenant } from '../utils/withTenant.js';
import { UnauthorizedError } from '../errors/AppError.js';

export async function authenticate(req, res){
    const authHeader = req.headers["authorization"];

    if(!authHeader || !authHeader.startsWith('Bearer')){
        throw new UnauthorizedError('Missing or malformed Authorization header.')
    }

    const rawKey = authHeader.slice(7); // strip 'Bearer '
    const keyHash = crypto.createHash('sha256').update(rawKey).digest('hex');

    // idx_api_keys_hash is a partial index WHERE revoked_at IS NULL
    // Revoked keys are excluded from the index - a revoked key returns no rows
    // and the middleware's "no result = 401" path handles it without any explicit
    // revoked at check in the query. The index is both a performance optimization
    // and a correctness enforcement mechanism
    const apiKey = await prisma.apiKey.findFirst({
        where: {keyHash, revokedAt: null},
        select: {id: true, tenantId: true}
    });

    if(!apiKey)
        throw new UnauthorizedError('Invalid or revoked API key.')

    req.tenantId = apiKey.tenantId;

    // Fire and forget: last_used_at is an audit field, not a security check
    // setImmediate schedules this after the event loop iteration
    // If it fails, the request has already succeeded, a non-critical write
    // must not affect the client's response

    setImmediate(() => {
        prisma.apiKey
            .update({where : {id: apiKey.id}, data: {lastUsedAt: new Date()}})
            .catch((err) => req.log.warn({err}, 'Failed to update last_used_at.'))
    })

    // Bind withTenant to this request's tenantId so route handlers call
    // req.withTenant(async (tx) .....) without importing tenantId manually

    req.withTenant = (fn) => withTenant(req.tenantId, fn);
}   

