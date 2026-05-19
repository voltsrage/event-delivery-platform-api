import * as subscriptionController from '../controllers/subscriptionController.js';
import { authenticate } from '../hooks/authenticate.js';

export async function subscriptionRoutes(fastify){
    const uuidParam ={
        type: 'object',
        required: ['id'],
        properties: {id: {type: 'string', format: 'uuid'}}
    };
    fastify.addHook('onRequest', authenticate);

    fastify.get('/:id',{schema: {params: uuidParam}} , subscriptionController.getSubscriptionById);

    fastify.put('/:id',{
        schema: {
        params: uuidParam,
        body: {
            type: 'object',
            properties: {
            endpoint:  { type: 'string', minLength: 1 },
            isEnabled: { type: 'boolean' },
            },
        },
        },
    }, subscriptionController.updateSubscription);

    fastify.delete('/:id',{schema: {params: uuidParam}}, subscriptionController.deleteSubscription);

    fastify.post('/:id/rotate-secret', {schema: {params: uuidParam}}, subscriptionController.rotateSecret)
}