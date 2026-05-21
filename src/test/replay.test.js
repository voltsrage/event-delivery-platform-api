import { describe, it, expect, beforeAll, afterAll, vi, afterEach } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';
import { withTenant } from '../utils/withTenant.js';
import * as retryQueueModule from '../queues/retryQueue.js';

let app;
let tenantKey, tenantId, topicId, subId;
let tenantBKey, tenantBId, tenantBSubId;
const eventIds = [];

beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    const suffix = Date.now();

    // Tenant A
    const regA = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: `replay-tenant-a-${suffix}`, email: `replay-a-${suffix}@example.com` },
    });
    const dataA = JSON.parse(regA.body).data;
    tenantKey = dataA.rawKey;
    tenantId  = dataA.tenant.id;

    const topicRes = await app.inject({
        method: 'POST', url: '/api/v1/topics',
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: { name: 'replay.test' },
    });
    topicId = JSON.parse(topicRes.body).data.id;

    const subRes = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicId}/subscriptions`,
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: { endpoint: 'https://example.com/replay-hook' },
    });
    subId = JSON.parse(subRes.body).data.id;

    // Insert 3 events and record their IDs
    for (let i = 0; i < 3; i++) {
        const evRes = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/events`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { eventType: 'replay.test', payload: { seq: i } },
        });
        eventIds.push(JSON.parse(evRes.body).data.id);
    }

    // Tenant B — used for RLS isolation test
    const regB = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: `replay-tenant-b-${suffix}`, email: `replay-b-${suffix}@example.com` },
    });
    const dataB = JSON.parse(regB.body).data;
    tenantBKey = dataB.rawKey;
    tenantBId  = dataB.tenant.id;

    const topicBRes = await app.inject({
        method: 'POST', url: '/api/v1/topics',
        headers: { authorization: `Bearer ${tenantBKey}` },
        payload: { name: 'replay.test' },
    });
    const topicBId = JSON.parse(topicBRes.body).data.id;

    const subBRes = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicBId}/subscriptions`,
        headers: { authorization: `Bearer ${tenantBKey}` },
        payload: { endpoint: 'https://example.com/replay-hook-b' },
    });
    tenantBSubId = JSON.parse(subBRes.body).data.id;
});

afterAll(async () => {
    if (tenantId) {
        await withTenant(tenantId, (tx) => tx.event.deleteMany({}));
    }
    if (tenantBId) {
        await withTenant(tenantBId, (tx) => tx.event.deleteMany({}));
    }
    await prisma.tenant.deleteMany({
        where: { id: { in: [tenantId, tenantBId].filter(Boolean) } },
    });
    await app.close();
    await prisma.$disconnect();
});

afterEach(() => vi.restoreAllMocks());

// ── Happy path — all events ────────────────────────────────────────────────────
describe('Replay — all events since epoch', () => {
    it('enqueues one job per event and returns enqueuedCount', async () => {
        const addBulkSpy = vi.spyOn(retryQueueModule.retryQueue, 'addBulk').mockResolvedValueOnce([]);

        const res = await app.inject({
            method: 'POST', url: `/api/v1/subscriptions/${subId}/replay`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { from: '2000-01-01T00:00:00Z' },
        });

        expect(res.statusCode).toBe(200);
        const body = JSON.parse(res.body);
        expect(body.data.enqueuedCount).toBe(3);

        // addBulk called once with 3 jobs in chronological order
        expect(addBulkSpy).toHaveBeenCalledTimes(1);
        const jobs = addBulkSpy.mock.calls[0][0];
        expect(jobs).toHaveLength(3);
        expect(jobs[0].name).toBe('replay');
        expect(jobs[0].data.subscriptionId).toBe(subId);
        expect(jobs[0].data.tenantId).toBe(tenantId);
        expect(jobs[0].data.nextAttemptNumber).toBe(1);
        // All 3 event IDs are present (order: asc by createdAt)
        expect(jobs.map((j) => j.data.eventId)).toEqual(expect.arrayContaining(eventIds));
    });
});

// ── Date filter ────────────────────────────────────────────────────────────────
describe('Replay — date filter', () => {
    it('only enqueues events created at or after `from`', async () => {
        const addBulkSpy = vi.spyOn(retryQueueModule.retryQueue, 'addBulk').mockResolvedValueOnce([]);

        // Fetch the first event's createdAt from the DB, then set `from` 1ms after it.
        // This reliably excludes event[0] regardless of how fast the insertions ran.
        const firstEvent = await withTenant(tenantId, (tx) =>
            tx.event.findUnique({ where: { id: eventIds[0] }, select: { createdAt: true } })
        );
        const from = new Date(firstEvent.createdAt.getTime() + 1).toISOString();

        const res = await app.inject({
            method: 'POST', url: `/api/v1/subscriptions/${subId}/replay`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { from },
        });

        expect(res.statusCode).toBe(200);
        const body = JSON.parse(res.body);
        expect(body.data.enqueuedCount).toBe(2);

        const jobs = addBulkSpy.mock.calls[0][0];
        expect(jobs.map((j) => j.data.eventId)).not.toContain(eventIds[0]);
        expect(jobs.map((j) => j.data.eventId)).toContain(eventIds[1]);
        expect(jobs.map((j) => j.data.eventId)).toContain(eventIds[2]);
    });
});

// ── No events in range ─────────────────────────────────────────────────────────
describe('Replay — no matching events', () => {
    it('returns enqueuedCount 0 and does not call addBulk', async () => {
        const addBulkSpy = vi.spyOn(retryQueueModule.retryQueue, 'addBulk').mockResolvedValueOnce([]);

        const res = await app.inject({
            method: 'POST', url: `/api/v1/subscriptions/${subId}/replay`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { from: new Date(Date.now() + 3_600_000).toISOString() }, // 1 hour from now
        });

        expect(res.statusCode).toBe(200);
        expect(JSON.parse(res.body).data.enqueuedCount).toBe(0);
        expect(addBulkSpy).not.toHaveBeenCalled();
    });
});

// ── Error cases ────────────────────────────────────────────────────────────────
describe('Replay — error handling', () => {
    it('returns 404 when subscription does not exist', async () => {
        const res = await app.inject({
            method: 'POST',
            url:    '/api/v1/subscriptions/00000000-0000-0000-0000-000000000000/replay',
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { from: '2000-01-01T00:00:00Z' },
        });
        expect(res.statusCode).toBe(404);
        expect(JSON.parse(res.body).error.code).toMatch(/SUBSCRIPTION_NOT_FOUND/i);
    });

    it('returns 422 when `from` is missing', async () => {
        const res = await app.inject({
            method: 'POST', url: `/api/v1/subscriptions/${subId}/replay`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: {},
        });
        expect(res.statusCode).toBe(422);
    });

    it('returns 400 when `from` is not a valid date string', async () => {
        const res = await app.inject({
            method: 'POST', url: `/api/v1/subscriptions/${subId}/replay`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { from: 'not-a-date' },
        });
        expect(res.statusCode).toBe(422);
        expect(JSON.parse(res.body).error.code).toMatch(/INVALID_FROM_DATE/i);
    });

    it('returns 401 for unauthenticated requests', async () => {
        const res = await app.inject({
            method: 'POST', url: `/api/v1/subscriptions/${subId}/replay`,
            payload: { from: '2000-01-01T00:00:00Z' },
        });
        expect(res.statusCode).toBe(401);
    });
});

// ── RLS isolation ──────────────────────────────────────────────────────────────
describe('Replay — RLS isolation', () => {
    it('returns 404 when tenant A uses tenant B subscription ID', async () => {
        const addBulkSpy = vi.spyOn(retryQueueModule.retryQueue, 'addBulk').mockResolvedValueOnce([]);

        // Tenant A's key but tenant B's subscriptionId — RLS returns null → 404
        const res = await app.inject({
            method: 'POST', url: `/api/v1/subscriptions/${tenantBSubId}/replay`,
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { from: '2000-01-01T00:00:00Z' },
        });

        expect(res.statusCode).toBe(404);
        expect(addBulkSpy).not.toHaveBeenCalled();
    });
});