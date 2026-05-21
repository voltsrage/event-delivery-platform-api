import { describe, it, expect, beforeAll, afterAll, vi, afterEach } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';
import { withTenant } from '../utils/withTenant.js';

// Mock the Redis client before any module that imports it is loaded.
// rateLimit.js imports redis from db/redis.js — mocking the module intercepts all calls.
vi.mock('../db/redis.js', () => ({
    redis: {
        incr:   vi.fn(),
        expire: vi.fn(),
        ttl:    vi.fn(),
    },
}));

let app;
let tenantKey, tenantId, topicId, subId;

beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    const reg = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: 'rl-tenant', email: 'rl4@example.com' },
    });
    const data = JSON.parse(reg.body).data;
    tenantKey = data.rawKey;
    tenantId  = data.tenant.id;

    const topicRes = await app.inject({
        method: 'POST', url: '/api/v1/topics',
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: { name: 'rl.test' },
    });
    topicId = JSON.parse(topicRes.body).data.id;

    // Create a subscription now (with incr returning 1 — under limit)
    const { redis } = await import('../db/redis.js');
    redis.incr.mockResolvedValue(1);
    redis.expire.mockResolvedValue(1);

    const subRes = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicId}/subscriptions`,
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: { endpoint: 'https://example.com/rl-hook' },
    });
    subId = JSON.parse(subRes.body).data.id;
});

afterAll(async () => {
    const tenant = await prisma.tenant.findUnique({ where: { email: 'rl4@example.com' } });
    if (tenant) {
        const tid = tenant.id;
        // dead_letters, delivery_attempts, and events have ON DELETE RESTRICT FKs
        // and RLS enabled — must delete them inside a withTenant transaction.
        // api_keys, topics, and subscriptions cascade-delete with the tenant.
        await withTenant(tid, async (tx) => {
            await tx.deadLetter.deleteMany({});
            await tx.deliveryAttempt.deleteMany({});
            await tx.event.deleteMany({});
        });
        await prisma.tenant.delete({ where: { id: tid } });
    }
    await prisma.$disconnect();
    await app.close();
});

afterEach(() => vi.clearAllMocks());

// ── Event publish rate limit ───────────────────────────────────────────────────
describe('Event publish rate limit (1,000 / minute)', () => {
    it('allows requests under the limit (returns 202)', async () => {
        const { redis } = await import('../db/redis.js');
        redis.incr.mockResolvedValue(1);
        redis.expire.mockResolvedValue(1);

        const res = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/events`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { eventType: 'rl.test', payload: { x: 1 } },
        });

        expect(res.statusCode).toBe(202);
    });

    it('returns 429 when the counter exceeds 1,000', async () => {
        const { redis } = await import('../db/redis.js');
        redis.incr.mockResolvedValue(1_001);
        redis.ttl.mockResolvedValue(45);

        const res = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/events`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { eventType: 'rl.test', payload: { x: 2 } },
        });

        expect(res.statusCode).toBe(429);
        expect(JSON.parse(res.body).error.code).toMatch(/RATE_LIMITED/i);
    });

    it('sets Retry-After header to remaining TTL', async () => {
        const { redis } = await import('../db/redis.js');
        redis.incr.mockResolvedValue(1_001);
        redis.ttl.mockResolvedValue(37);

        const res = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/events`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { eventType: 'rl.test', payload: { x: 3 } },
        });

        expect(res.statusCode).toBe(429);
        expect(res.headers['retry-after']).toBe('37');
    });

    it('uses key scoped to tenantId, not IP (incr called with tenant-scoped key)', async () => {
        const { redis } = await import('../db/redis.js');
        redis.incr.mockResolvedValue(1);
        redis.expire.mockResolvedValue(1);

        await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/events`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { eventType: 'rl.test', payload: {} },
        });

        const incrKey = redis.incr.mock.calls[0][0];
        expect(incrKey).toContain('events:publish');
        expect(incrKey).toContain(tenantId);
    });

    it('sets TTL only when counter is 1 (first request in window)', async () => {
        const { redis } = await import('../db/redis.js');
        redis.incr.mockResolvedValue(42); // not the first request
        redis.expire.mockResolvedValue(1);

        await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/events`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { eventType: 'rl.test', payload: {} },
        });

        // expire should NOT be called when count > 1
        expect(redis.expire).not.toHaveBeenCalled();
    });
});

// ── Subscription creation rate limit ──────────────────────────────────────────
describe('Subscription creation rate limit (10 / hour)', () => {
    it('allows requests under the limit (returns 201)', async () => {
        const { redis } = await import('../db/redis.js');
        redis.incr.mockResolvedValue(5);

        const res = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/subscriptions`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { endpoint: 'https://example.com/another-hook' },
        });

        // 201 or 409 (quota) — either way the rate limit did not block it
        expect(res.statusCode).not.toBe(429);
    });

    it('returns 429 when the counter exceeds 10', async () => {
        const { redis } = await import('../db/redis.js');
        redis.incr.mockResolvedValue(11);
        redis.ttl.mockResolvedValue(1800);

        const res = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/subscriptions`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { endpoint: 'https://example.com/blocked-hook' },
        });

        expect(res.statusCode).toBe(429);
    });

    it('uses a different Redis key from the events limit', async () => {
        const { redis } = await import('../db/redis.js');
        redis.incr.mockResolvedValue(1);
        redis.expire.mockResolvedValue(1);

        await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/subscriptions`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { endpoint: 'https://example.com/key-check' },
        });

        const incrKey = redis.incr.mock.calls[0][0];
        expect(incrKey).toContain('subscriptions:create');
        expect(incrKey).not.toContain('events:publish');
    });
});

// ── Routes not subject to rate limiting ───────────────────────────────────────
describe('Routes without rate limiting', () => {
    it('GET /topics does not call redis.incr', async () => {
        const { redis } = await import('../db/redis.js');

        await app.inject({
            method: 'GET', url: '/api/v1/topics',
            headers: { authorization: `Bearer ${tenantKey}` },
        });

        expect(redis.incr).not.toHaveBeenCalled();
    });
});