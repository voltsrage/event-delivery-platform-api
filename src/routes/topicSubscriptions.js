import * as subscriptionController from '../controllers/subscriptionController.js';
import { authenticate } from '../hooks/authenticate.js';

export async function topicSubscriptionRoutes(fastify){
    fastify.addHook('onRequest', authenticate);

    // POST /api/v1/topics/:topicId/subscriptions
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
            required: ['endpoint'],
            properties: {
            endpoint: { type: 'string', minLength: 1 },
            },
        },
        },
    }, subscriptionController.createSubscription);

    // GET /api/v1/topics/:topicId/subscriptions
    fastify.get('/', {
        schema: {
        params: {
            type: 'object',
            required: ['topicId'],
            properties: {
            topicId: { type: 'string', format: 'uuid' },
            },
        },
        querystring: {
            type: 'object',
            properties: {
            page:     { type: 'integer', minimum: 1, default: 1 },
            pageSize: { type: 'integer', minimum: 1, maximum: 100, default: 20 },
            },
        },
        },
    }, subscriptionController.listSubscriptions);
}