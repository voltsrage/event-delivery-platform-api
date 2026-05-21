import { describe, it, expect, beforeAll, afterAll, vi, afterEach } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';
import { withTenant } from '../utils/withTenant.js';
import { createDeadLetter } from '../services/deadLetterService.js';
import * as deliverModule from '../delivery/deliver.js';

let app;
let tenantKey, tenantId, topicId, subId;
let tenantBKey, tenantBId, tenantBDeadLetterId;
let deadLetterId, eventId;

beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    // Tenant A
    const regA = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: 'dl-tenant-a', email: 'dl-a@example.com' },
    });
    const dataA = JSON.parse(regA.body).data;
    tenantKey = dataA.rawKey;
    tenantId  = dataA.tenant.id;

    const topicRes = await app.inject({
        method: 'POST', url: '/api/v1/topics',
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: { name: 'dl.test' },
    });
    topicId = JSON.parse(topicRes.body).data.id;

    const subRes = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicId}/subscriptions`,
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: { endpoint: 'https://example.com/dl-hook' },
    });
    subId = JSON.parse(subRes.body).data.id;

    // Publish an event to get a real eventId
    const evRes = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicId}/events`,
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: { eventType: 'dl.test', payload: { orderId: 'order-1' } },
    });
    eventId = JSON.parse(evRes.body).data.id;

    // Seed a dead_letters row directly (simulates 5 failed auto-retries)
    const dl = await createDeadLetter({
        tenantId,
        eventId,
        subscriptionId: subId,
        totalAttempts:  5,
        lastError:      'connect ECONNREFUSED',
    });
    deadLetterId = dl.id;

    // Tenant B — for RLS isolation
    const regB = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: 'dl-tenant-b', email: 'dl-b@example.com' },
    });
    const dataB = JSON.parse(regB.body).data;
    tenantBKey = dataB.rawKey;
    tenantBId  = dataB.tenant.id;

    const topicBRes = await app.inject({
        method: 'POST', url: '/api/v1/topics',
        headers: { authorization: `Bearer ${tenantBKey}` },
        payload: { name: 'dl.test' },
    });
    const topicBId = JSON.parse(topicBRes.body).data.id;

    const subBRes = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicBId}/subscriptions`,
        headers: { authorization: `Bearer ${tenantBKey}` },
        payload: { endpoint: 'https://example.com/dl-hook-b' },
    });
    const subBId = JSON.parse(subBRes.body).data.id;

    const evBRes = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicBId}/events`,
        headers: { authorization: `Bearer ${tenantBKey}` },
        payload: { eventType: 'dl.test', payload: { x: 1 } },
    });
    const eventBId = JSON.parse(evBRes.body).data.id;

    const dlB = await createDeadLetter({
        tenantId: tenantBId, eventId: eventBId, subscriptionId: subBId,
        totalAttempts: 5, lastError: 'timeout',
    });
    tenantBDeadLetterId = dlB.id;
});

afterAll(async () => {
    if (tenantId) {
        await withTenant(tenantId, (tx) => tx.deliveryAttempt.deleteMany({}));
        await withTenant(tenantId, (tx) => tx.deadLetter.deleteMany({}));
        await withTenant(tenantId, (tx) => tx.event.deleteMany({}));
    }
    if (tenantBId) {
        await withTenant(tenantBId, (tx) => tx.deliveryAttempt.deleteMany({}));
        await withTenant(tenantBId, (tx) => tx.deadLetter.deleteMany({}));
        await withTenant(tenantBId, (tx) => tx.event.deleteMany({}));
    }
    await prisma.tenant.deleteMany({
        where: { email: { in: ['dl-a@example.com', 'dl-b@example.com'] } },
    });
    await prisma.$disconnect();
    await app.close();
});

afterEach(() => vi.restoreAllMocks());

// ── GET /dead-letters ─────────────────────────────────────────────────────────
describe('GET /dead-letters', () => {
    it('returns paginated dead letters with eventType and endpointUrl', async () => {
        const res = await app.inject({
            method: 'GET', url: '/api/v1/dead-letters',
            headers: { authorization: `Bearer ${tenantKey}` },
        });

        expect(res.statusCode).toBe(200);
        const body = JSON.parse(res.body);
        expect(body.data.total).toBeGreaterThanOrEqual(1);

        const item = body.data.items.find((i) => i.id === deadLetterId);
        expect(item).toBeDefined();
        expect(item.eventType).toBe('dl.test');
        expect(item.endpointUrl).toBe('https://example.com/dl-hook');
        expect(item.totalAttempts).toBe(5);
        expect(item.lastError).toBe('connect ECONNREFUSED');
        expect(item.resolvedAt).toBeNull();
    });

    it('only returns the authenticated tenant\'s dead letters', async () => {
        const res = await app.inject({
            method: 'GET', url: '/api/v1/dead-letters',
            headers: { authorization: `Bearer ${tenantKey}` },
        });
        const ids = JSON.parse(res.body).data.items.map((i) => i.id);
        expect(ids).not.toContain(tenantBDeadLetterId);
    });

    it('returns 401 for unauthenticated requests', async () => {
        const res = await app.inject({ method: 'GET', url: '/api/v1/dead-letters' });
        expect(res.statusCode).toBe(401);
    });
});

// ── GET /dead-letters/:id ─────────────────────────────────────────────────────
describe('GET /dead-letters/:id', () => {
    it('returns dead letter with full event and delivery attempt history', async () => {
        const res = await app.inject({
            method: 'GET', url: `/api/v1/dead-letters/${deadLetterId}`,
            headers: { authorization: `Bearer ${tenantKey}` },
        });

        expect(res.statusCode).toBe(200);
        const { data } = JSON.parse(res.body);
        expect(data.id).toBe(deadLetterId);
        expect(data.event).toBeDefined();
        expect(data.event.eventType).toBe('dl.test');
        expect(data.event.payload).toMatchObject({ orderId: 'order-1' });
        expect(Array.isArray(data.deliveryAttempts)).toBe(true);
    });

    it('returns 404 for a non-existent dead letter', async () => {
        const res = await app.inject({
            method: 'GET', url: '/api/v1/dead-letters/00000000-0000-0000-0000-000000000000',
            headers: { authorization: `Bearer ${tenantKey}` },
        });
        expect(res.statusCode).toBe(404);
        expect(JSON.parse(res.body).error.code).toMatch(/DEAD_LETTER_NOT_FOUND/i);
    });

    it('returns 404 when tenant A requests tenant B dead letter (RLS isolation)', async () => {
        const res = await app.inject({
            method: 'GET', url: `/api/v1/dead-letters/${tenantBDeadLetterId}`,
            headers: { authorization: `Bearer ${tenantKey}` },
        });
        expect(res.statusCode).toBe(404);
    });
});

// ── POST /dead-letters/:id/retry — success ────────────────────────────────────
describe('POST /dead-letters/:id/retry — success', () => {
    it('delivers, sets resolvedAt, returns success: true', async () => {
        vi.spyOn(deliverModule, 'deliverWebhook').mockResolvedValueOnce({
            success: true, httpStatus: 200, responseBody: 'OK', durationMs: 42,
        });

        const res = await app.inject({
            method: 'POST', url: `/api/v1/dead-letters/${deadLetterId}/retry`,
            headers: { authorization: `Bearer ${tenantKey}` },
        });

        expect(res.statusCode).toBe(200);
        const { data } = JSON.parse(res.body);
        expect(data.success).toBe(true);
        expect(data.httpStatus).toBe(200);
        expect(data.resolvedAt).not.toBeNull();

        // dead_letters.resolved_at persisted in DB
        const dl = await withTenant(tenantId, (tx) =>
            tx.deadLetter.findUnique({ where: { id: deadLetterId } })
        );
        expect(dl.resolvedAt).not.toBeNull();

        // A new delivery_attempt row was created with attemptNumber = totalAttempts + 1
        const attempt = await withTenant(tenantId, (tx) =>
            tx.deliveryAttempt.findFirst({
                where: { eventId, subscriptionId: subId, attemptNumber: 6 },
            })
        );
        expect(attempt).not.toBeNull();
        expect(attempt.status).toBe('success');
    });
});

// ── POST /dead-letters/:id/retry — failure ────────────────────────────────────
describe('POST /dead-letters/:id/retry — failure', () => {
    it('records failed attempt, leaves resolvedAt null, returns success: false', async () => {
        // Seed a second dead letter so we have an unresolved one to test with
        const ev2Res = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/events`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { eventType: 'dl.test2', payload: { x: 2 } },
        });
        const eventId2 = JSON.parse(ev2Res.body).data.id;

        const dl2 = await createDeadLetter({
            tenantId, eventId: eventId2, subscriptionId: subId,
            totalAttempts: 5, lastError: 'timeout',
        });

        vi.spyOn(deliverModule, 'deliverWebhook').mockResolvedValueOnce({
            success: false, httpStatus: 503, responseBody: 'Service Unavailable', durationMs: 30,
        });

        const res = await app.inject({
            method: 'POST', url: `/api/v1/dead-letters/${dl2.id}/retry`,
            headers: { authorization: `Bearer ${tenantKey}` },
        });

        expect(res.statusCode).toBe(200);
        const { data } = JSON.parse(res.body);
        expect(data.success).toBe(false);
        expect(data.httpStatus).toBe(503);
        expect(data.resolvedAt).toBeNull();

        // dead_letters.resolved_at still null
        const dlRow = await withTenant(tenantId, (tx) =>
            tx.deadLetter.findUnique({ where: { id: dl2.id } })
        );
        expect(dlRow.resolvedAt).toBeNull();

        // A delivery_attempt row was still created (attempt 6)
        const attempt = await withTenant(tenantId, (tx) =>
            tx.deliveryAttempt.findFirst({
                where: { eventId: eventId2, subscriptionId: subId, attemptNumber: 6 },
            })
        );
        expect(attempt).not.toBeNull();
        expect(attempt.status).toBe('failed');
    });
});

// ── POST /dead-letters/:id/retry — error cases ────────────────────────────────
describe('POST /dead-letters/:id/retry — errors', () => {
    it('returns 404 for non-existent dead letter', async () => {
        const res = await app.inject({
            method: 'POST', url: '/api/v1/dead-letters/00000000-0000-0000-0000-000000000000/retry',
            headers: { authorization: `Bearer ${tenantKey}` },
        });
        expect(res.statusCode).toBe(404);
        expect(JSON.parse(res.body).error.code).toMatch(/DEAD_LETTER_NOT_FOUND/i);
    });

    it('returns 404 when tenant A retries tenant B dead letter (RLS isolation)', async () => {
        const res = await app.inject({
            method: 'POST', url: `/api/v1/dead-letters/${tenantBDeadLetterId}/retry`,
            headers: { authorization: `Bearer ${tenantKey}` },
        });
        expect(res.statusCode).toBe(404);
    });
});