import * as deliveryLogController from '../controllers/deliveryLogController.js';
import { authenticate } from '../hooks/authenticate.js';

export async function deliveryLogRoutes(fastify){
    fastify.addHook('onRequest',authenticate);

    // GET /api/v1/delivery-logs
    fastify.get('/', {
        schema: {
        querystring: {
            type: 'object',
            properties: {
            status: {
                type: 'string',
                enum: ['success', 'failed', 'dead_lettered'],
            },
            topicName: { type: 'string', maxLength: 200 },
            // from / to accept ISO dates or date-time strings.
            // Elasticsearch's date field handles both "2026-05-01" and
            // "2026-05-01T00:00:00Z" without any special handling in the API.
            from:     { type: 'string' },
            to:       { type: 'string' },
            q:        { type: 'string', maxLength: 500 },
            page:     { type: 'integer', minimum: 1, default: 1 },
            pageSize: { type: 'integer', minimum: 1, maximum: 100, default: 20 },
            },
            additionalProperties: false,
        },
        },
    }, deliveryLogController.searchDeliveryLogs);

    // GET /api/v1/delivery-logs/:attemptId
    fastify.get('/:attemptId', {
        schema: {
        params: {
            type: 'object',
            required: ['attemptId'],
            properties: {
            // attemptId is a UUID (the PostgreSQL delivery_attempts.id).
            // Using 'string' without format:'uuid' because ES _id is technically
            // any string — avoid validation failures from format checking.
            attemptId: { type: 'string', minLength: 1 },
            },
        },
        },
    }, deliveryLogController.getDeliveryLogById);
}