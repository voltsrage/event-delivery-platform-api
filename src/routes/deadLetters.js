import * as deadLetterController from '../controllers/deadLetterController.js';
import { authenticate } from '../hooks/authenticate.js';

export async function deadLetterRoutes(fastify) {
    const uuidParam = {
        type: 'object',
        required: ['id'],
        properties: { id: { type: 'string', format: 'uuid' } },
    };

    fastify.addHook('onRequest', authenticate);

    fastify.get('/', {
        schema: {
            querystring: {
                type: 'object',
                properties: {
                    page:     { type: 'integer', minimum: 1, default: 1 },
                    pageSize: { type: 'integer', minimum: 1, maximum: 100, default: 20 },
                },
            },
        },
    }, deadLetterController.listDeadLetters);

    fastify.get('/:id', { schema: { params: uuidParam } }, deadLetterController.getDeadLetterById);

    fastify.post('/:id/retry', { schema: { params: uuidParam } }, deadLetterController.retryDeadLetter);
}