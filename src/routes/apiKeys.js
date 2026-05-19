import * as apiController from '../controllers/apiController.js';
import { authenticate } from '../hooks/authenticate.js';

export async function apiKeyRoutes(fastify, opts)
{
    fastify.addHook('onRequest', authenticate);
    
    // POST /api/v1/api-keys — create an additional key for the authenticated tenant
    fastify.post(
        '/',
        {
        schema: {
            body: {
            type: 'object',
            properties: {
                label: { type: 'string', maxLength: 100 },
            },
            additionalProperties: false,
            },
        },
        },
        apiController.createApiKey
    );

    // GET /api/v1/api-keys — list keys by prefix and label; never returns hash or raw key
    fastify.get('/', apiController.listApiKeys);

    // DELETE /api/v1/api-keys/:id — revoke a key; sets revoked_at
    fastify.delete('/:id', apiController.revokeApiKey);
}