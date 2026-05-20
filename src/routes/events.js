import * as eventController from '../controllers/eventController.js';
import { authenticate } from '../hooks/authenticate.js';

export async function eventRoutes(fastify){
    fastify.addHook('onRequest', authenticate);
    
    // POST /api/v1/topics/:topicId/events
    fastify.post('/', {
        schema: {
        params: {
            type: 'object',
            required: ['topicId'],
            properties: {
            topicId: { type: 'string', format: 'uuid' },
            },
        },
        body: {
            type: 'object',
            required: ['eventType', 'payload'],
            additionalProperties: false,
            properties: {
            eventType:      { type: 'string', minLength: 1, maxLength: 200 },
            payload:        { type: 'object' },
            idempotencyKey: { type: 'string', maxLength: 200 },
            },
        },
        },
    }, eventController.createEvent);
}