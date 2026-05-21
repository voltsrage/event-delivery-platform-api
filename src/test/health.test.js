import { describe, it, expect, beforeAll, afterAll, vi, afterEach } from 'vitest';
import { buildApp } from '../app.js';

vi.mock('../db/prisma.js', () => ({
    prisma: { $queryRaw: vi.fn() },
}));

vi.mock('../db/redis.js', () => ({
    redis: { ping: vi.fn() },
}));

vi.mock('../kafka/client.js', () => {
    const disconnect = vi.fn();
    const connect    = vi.fn();
    return {
        default: { admin: vi.fn(() => ({ connect, disconnect })) },
    };
});

vi.mock('../search/esClient.js', () => ({
    default: { ping: vi.fn() },
}));

let app;

async function setAllHealthy() {
    const { prisma }   = await import('../db/prisma.js');
    const { redis }    = await import('../db/redis.js');
    const kafka        = (await import('../kafka/client.js')).default;
    const esClient     = (await import('../search/esClient.js')).default;

    prisma.$queryRaw.mockResolvedValue([{ '?column?': 1 }]);
    redis.ping.mockResolvedValue('PONG');
    const admin = kafka.admin();
    admin.connect.mockResolvedValue(undefined);
    admin.disconnect.mockResolvedValue(undefined);
    esClient.ping.mockResolvedValue(true);
}

beforeAll(async () => {
    app = await buildApp();
    await app.ready();
});

afterAll(async () => { await app.close(); });
afterEach(() => vi.clearAllMocks());

// ── Liveness ──────────────────────────────────────────────────────────────────
describe('GET /health', () => {
    it('always returns 200 regardless of dependency state', async () => {
        // Do NOT set up mocks — dependencies are not called for liveness.
        const res = await app.inject({ method: 'GET', url: '/health' });
        expect(res.statusCode).toBe(200);
        expect(JSON.parse(res.body).status).toBe('ok');
    });
});

// ── Readiness — all healthy ───────────────────────────────────────────────────
describe('GET /health/ready — all healthy', () => {
    it('returns 200 with status healthy and all checks healthy', async () => {
        await setAllHealthy();

        const res  = await app.inject({ method: 'GET', url: '/health/ready' });
        const body = JSON.parse(res.body);

        expect(res.statusCode).toBe(200);
        expect(body.status).toBe('healthy');
        expect(body.checks.postgres).toBe('healthy');
        expect(body.checks.kafka).toBe('healthy');
        expect(body.checks.elasticsearch).toBe('healthy');
        expect(body.checks.redis).toBe('healthy');
    });
});

// ── Readiness — individual failures ──────────────────────────────────────────
describe('GET /health/ready — individual dependency failures', () => {
    it('returns 503 with postgres:error when Prisma query throws', async () => {
        await setAllHealthy();
        const { prisma } = await import('../db/prisma.js');
        prisma.$queryRaw.mockRejectedValueOnce(new Error('connection refused'));

        const res  = await app.inject({ method: 'GET', url: '/health/ready' });
        const body = JSON.parse(res.body);

        expect(res.statusCode).toBe(503);
        expect(body.status).toBe('degraded');
        expect(body.checks.postgres).toBe('error');
        // Other checks are still reported (Promise.allSettled, not Promise.all)
        expect(body.checks.kafka).toBe('healthy');
        expect(body.checks.elasticsearch).toBe('healthy');
        expect(body.checks.redis).toBe('healthy');
    });

    it('returns 503 with kafka:error when admin.connect throws', async () => {
        await setAllHealthy();
        const kafka = (await import('../kafka/client.js')).default;
        kafka.admin().connect.mockRejectedValueOnce(new Error('broker unavailable'));

        const res  = await app.inject({ method: 'GET', url: '/health/ready' });
        const body = JSON.parse(res.body);

        expect(res.statusCode).toBe(503);
        expect(body.checks.kafka).toBe('error');
        expect(body.checks.postgres).toBe('healthy');
    });

    it('returns 503 with elasticsearch:error when ping returns false', async () => {
        await setAllHealthy();
        const esClient = (await import('../search/esClient.js')).default;
        esClient.ping.mockResolvedValueOnce(false);

        const res  = await app.inject({ method: 'GET', url: '/health/ready' });
        const body = JSON.parse(res.body);

        expect(res.statusCode).toBe(503);
        expect(body.checks.elasticsearch).toBe('error');
        expect(body.checks.postgres).toBe('healthy');
    });

    it('returns 503 with redis:error when ping throws', async () => {
        await setAllHealthy();
        const { redis } = await import('../db/redis.js');
        redis.ping.mockRejectedValueOnce(new Error('ECONNREFUSED'));

        const res  = await app.inject({ method: 'GET', url: '/health/ready' });
        const body = JSON.parse(res.body);

        expect(res.statusCode).toBe(503);
        expect(body.checks.redis).toBe('error');
        expect(body.checks.kafka).toBe('healthy');
    });

    it('reports all four checks as error when all dependencies are down', async () => {
        await setAllHealthy();
        const { prisma }   = await import('../db/prisma.js');
        const { redis }    = await import('../db/redis.js');
        const kafka        = (await import('../kafka/client.js')).default;
        const esClient     = (await import('../search/esClient.js')).default;

        prisma.$queryRaw.mockRejectedValueOnce(new Error('pg down'));
        kafka.admin().connect.mockRejectedValueOnce(new Error('kafka down'));
        esClient.ping.mockResolvedValueOnce(false);
        redis.ping.mockRejectedValueOnce(new Error('redis down'));

        const res  = await app.inject({ method: 'GET', url: '/health/ready' });
        const body = JSON.parse(res.body);

        expect(res.statusCode).toBe(503);
        expect(body.status).toBe('degraded');
        expect(Object.values(body.checks).every((s) => s === 'error')).toBe(true);
    });
});

// ── Auth not required ─────────────────────────────────────────────────────────
describe('Health routes require no authentication', () => {
    it('GET /health/ready succeeds without Authorization header', async () => {
        await setAllHealthy();
        const res = await app.inject({ method: 'GET', url: '/health/ready' });
        expect(res.statusCode).not.toBe(401);
    });
});