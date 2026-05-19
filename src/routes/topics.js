import * as topicController from '../controllers/topicController.js';
import {authenticate} from '../hooks/authenticate.js';

export async function topicRoutes(fastify, opts){
    fastify.addHook('onRequest', authenticate);

    // POST /api/v1/topics
    fastify.post(
        '/',
        {
            schema: {
                body: {
                type: 'object',
                required: ['name'],
                properties: {
                    name:        { type: 'string', minLength: 1, maxLength: 200 },
                    description: { type: 'string', maxLength: 1000 },
                },
                additionalProperties: false,
                },
            },
        },
        topicController.createTopic
    );

    // GET /api/v1/topics?page=1&pageSize=20
    fastify.get(
        '/',
        {
            schema: {
                querystring: {
                type: 'object',
                properties: {
                    page:     { type: 'integer', minimum: 1, default: 1 },
                    pageSize: { type: 'integer', minimum: 1, maximum: 100, default: 20 },
                },
                },
            },
        },
        topicController.listTopics
    );

    // GET /api/v1/topics/:id
    fastify.get('/:id',topicController.getTopicById);

    // DELETE /api/v1/topics/:id
    fastify.delete('/:id', topicController.deleteTopic)
}