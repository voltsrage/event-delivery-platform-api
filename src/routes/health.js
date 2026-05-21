import { getReadiness } from '../services/healthService.js';

export async function healthRoutes(fastify) {
    // Liveness — always 200 if the process can handle requests.
    // No dependency checks: a slow database should not cause the process to be killed
    // and restarted, which would make the problem worse.
    fastify.get('/health', async (_req, res) => {
        return res.send({ status: 'ok' });
    });

    // Readiness — 200 when all dependencies are reachable, 503 otherwise.
    fastify.get('/health/ready', async (_req, res) => {
        const result = await getReadiness();
        const statusCode = result.status === 'healthy' ? 200 : 503;
        return res.status(statusCode).send(result);
    });
}