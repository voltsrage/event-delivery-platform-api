import 'dotenv/config';
import Fastify from 'fastify';
import { fastifyLoggerOptions } from './utils/logger.js';
import { AppError, TooManyRequestsError } from './errors/AppError.js';
import { ApiResponse } from './utils/ApiResponse.js';
import crypto from 'crypto';
import {tenantRoutes} from './routes/tenants.js';
import { apiKeyRoutes } from './routes/apiKeys.js';

const isDev = process.env.NODE_ENV != 'production';

export async function buildApp()
{
    const app = Fastify({
        logger: fastifyLoggerOptions,
        // genReqId provides a correlation ID on every request automatically,
        // Fastify includes it as 'reqId' in every Pino log line for the request
        genReqId: () => crypto.randomUUID()
    });

    if(isDev){
        await app.register(import('@fastify/swagger'), {
            openapi: {
                info: { title: 'Event Delivery Platform API', version: '1.0.0' },
                components: {
                securitySchemes: {
                    apiKey: {
                    type: 'http',
                    scheme: 'bearer',
                    description: 'API key — format: sk_live_...',
                    },
                },
                },
                security: [{ apiKey: [] }],
            },
        });

        await app.register(import('@fastify/swagger-ui'), {
            routePrefix: '/swagger',
        });
    }

    // Global error handler — Fastify's equivalent of Express's four-argument middleware.
    // AppError instances are expected domain errors: log as warn, return the status code.
    // Fastify schema validation errors have err.validation — treat as 422.
    // Anything else is unexpected: log as error, return 500 without leaking details.
    app.setErrorHandler((err, req, res) => {
        if (err instanceof TooManyRequestsError && err.retryAfter) {
            res.header('Retry-After', err.retryAfter);
        }

        if (err instanceof AppError) {
            req.log.warn({ err }, err.message);
            return res
                .status(err.statusCode)
                .send(ApiResponse.error(err.message, err.code, err.statusCode));
        }

        if (err.validation) {
            req.log.warn({ err }, 'Request validation failed');
            return res
                .status(422)
                .send(ApiResponse.error(err.message, 'VALIDATION_ERROR', 422));
        }

        req.log.error({ err }, 'Unhandled error');
        return res
            .status(500)
            .send(ApiResponse.error('Internal server error.', 'INTERNAL_ERROR', 500));
    });

    // 404 handler — unmatched routes return the standard error envelope
    app.setNotFoundHandler((req, res) => {
        return res
        .status(404)
        .send(ApiResponse.error('Route not found.', 'NOT_FOUND', 404));
    });

    // Liveness health check — public, no auth
    app.get('/health', async () => ({ status: 'ok' }));

    // Routes are registered here in later phases:
    await app.register(tenantRoutes, {prefix: '/api/v1/tenants'});

    await app.register(apiKeyRoutes, {prefix: '/api/v1/api-keys'});
    
    return app;
}