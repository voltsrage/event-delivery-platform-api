import * as tenantController from '../controllers/tenantController.js';

export async function tenantRoutes(fastify, opts){
    fastify.post(
        '/',
        {
        schema: {
            body: {
            type: 'object',
            required: ['name', 'email'],
            properties: {
                name:  { type: 'string', minLength: 1, maxLength: 200 },
                email: { type: 'string', format: 'email', maxLength: 320 },
            },
            additionalProperties: false,
            },
            response: {
            201: {
                type: 'object',
                properties: {
                success:    { type: 'boolean' },
                statusCode: { type: 'integer' },
                data: {
                    type: 'object',
                    properties: {
                    tenant: {
                        type: 'object',
                        properties: {
                        id:               { type: 'string' },
                        name:             { type: 'string' },
                        email:            { type: 'string' },
                        maxSubscriptions: { type: 'integer' },
                        createdAt:        { type: 'string' },
                        },
                    },
                    apiKey: {
                        type: 'object',
                        properties: {
                        id:        { type: 'string' },
                        keyPrefix: { type: 'string' },
                        label:     { type: 'string' },
                        },
                    },
                    rawKey: {
                        type: 'string',
                        description: 'The API key. This is the only time it will be returned. Store it securely.',
                    },
                    },
                },
                error: { type: ['object', 'null'] },
                },
            },
            },
        },
        },
        tenantController.createTenant
    );
}