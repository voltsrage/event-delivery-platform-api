import { describe, it, expect, beforeAll, afterAll, vi, afterEach } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';
import { withTenant } from '../utils/withTenant.js';
import { processRetryJob } from '../workers/retryWorker.js';
import * as deliverModule from '../delivery/deliver.js';
import * as retryQueueModule from '../queues/retryQueue.js';

let app;
let tenantKey, tenantId, topicId, subId;

beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    const reg = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: 'retry-tenant', email: 'retry@example.com' },
    });
    const data = JSON.parse(reg.body).data;
    tenantKey = data.rawKey;
    tenantId  = data.tenant.id;

    const topicRes = await app.inject({
        method: 'POST', url: '/api/v1/topics',
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: { name: 'retry.test' },
    });
    topicId = JSON.parse(topicRes.body).data.id;

    const subRes = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicId}/subscriptions`,
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: { endpoint: 'https://example.com/retry-hook' },
    });
    subId = JSON.parse(subRes.body).data.id;
});

afterAll(async () => {
    if (tenantId) {
        await withTenant(tenantId, (tx) => tx.deadLetter.deleteMany({}));
        await withTenant(tenantId, (tx) => tx.deliveryAttempt.deleteMany({}));
        await withTenant(tenantId, (tx) => tx.event.deleteMany({}));
    }
    await prisma.tenant.deleteMany({ where: { email: 'retry@example.com' } });
    await prisma.$disconnect();
    await app.close();
});
afterEach(() => vi.restoreAllMocks());

// Helper: insert a real event row + return its ID.
async function insertEvent() {
    const res = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicId}/events`,
        headers: { authorization: `Bearer ${tenantKey}` },
        payload: {
        eventType: 'retry.test',
        payload:   { id: crypto.randomUUID() },
        },
    });
    return JSON.parse(res.body).data.id;
}

function makeJob(eventId, nextAttemptNumber) {
    return { data: { eventId, subscriptionId: subId, tenantId, nextAttemptNumber } };
}

// ── Success path ──────────────────────────────────────────────────────────────
describe('Retry job — success', () => {
    it('creates delivery_attempt with status success and does not enqueue next retry', async () => {
        const enqueueSpy = vi.spyOn(retryQueueModule.retryQueue, 'add').mockResolvedValueOnce(undefined);
        vi.spyOn(deliverModule, 'deliverWebhook').mockResolvedValueOnce({
            success: true, httpStatus: 200, responseBody: 'OK', durationMs: 50,
        });

        const eventId = await insertEvent();
        await processRetryJob(makeJob(eventId, 2));

        // delivery_attempts is RLS-protected — must go through withTenant
        const attempt = await withTenant(tenantId, (tx) =>
            tx.deliveryAttempt.findFirst({
                where: { eventId, subscriptionId: subId, attemptNumber: 2 },
            })
        );
        expect(attempt.status).toBe('success');
        expect(enqueueSpy).not.toHaveBeenCalled();
    });
});

// ── Intermediate failure ───────────────────────────────────────────────────────
describe('Retry job — intermediate failure (attempt < 5)', () => {
    it('marks attempt failed, sets nextRetryAt, and enqueues next retry', async () => {
        let enqueuedJob = null;
        vi.spyOn(retryQueueModule.retryQueue, 'add').mockImplementationOnce(async (name, data, opts) => {
            enqueuedJob = { name, data, opts };
        });
        vi.spyOn(deliverModule, 'deliverWebhook').mockResolvedValueOnce({
            success: false, httpStatus: 503, responseBody: 'down', durationMs: 100,
        });

        const eventId = await insertEvent();
        await processRetryJob(makeJob(eventId, 3)); // attempt 3 failing

        // delivery_attempts is RLS-protected — must go through withTenant
        const attempt = await withTenant(tenantId, (tx) =>
            tx.deliveryAttempt.findFirst({
                where: { eventId, subscriptionId: subId, attemptNumber: 3 },
            })
        );
        expect(attempt.status).toBe('failed');
        expect(attempt.nextRetryAt).not.toBeNull();

        // Next retry enqueued for attempt 4 with 30-minute delay.
        expect(enqueuedJob).not.toBeNull();
        expect(enqueuedJob.data.nextAttemptNumber).toBe(4);
        expect(enqueuedJob.opts.delay).toBe(1_800_000);
    });
});

// ── Dead letter path ──────────────────────────────────────────────────────────
describe('Retry job — final failure (attempt 5)', () => {
    it('marks attempt dead_lettered and creates dead_letters row', async () => {
        vi.spyOn(retryQueueModule.retryQueue, 'add').mockResolvedValue(undefined);
        vi.spyOn(deliverModule, 'deliverWebhook').mockResolvedValueOnce({
            success: false, httpStatus: 500, responseBody: 'still down', durationMs: 80,
        });

        const eventId = await insertEvent();
        await processRetryJob(makeJob(eventId, 5)); // attempt 5 — the last

        // delivery_attempts and dead_letters are RLS-protected — must go through withTenant
        const attempt = await withTenant(tenantId, (tx) =>
            tx.deliveryAttempt.findFirst({
                where: { eventId, subscriptionId: subId, attemptNumber: 5 },
            })
        );
        expect(attempt.status).toBe('dead_lettered');
        expect(attempt.nextRetryAt).toBeNull();

        const deadLetter = await withTenant(tenantId, (tx) =>
            tx.deadLetter.findFirst({ where: { eventId, subscriptionId: subId } })
        );
        expect(deadLetter).not.toBeNull();
        expect(deadLetter.totalAttempts).toBe(5);
        expect(deadLetter.lastError).toBe('still down');
        expect(deadLetter.resolvedAt).toBeNull(); // not resolved until Phase 13 manual retry
    });

    it('does not enqueue another retry after dead lettering', async () => {
        const enqueueSpy = vi.spyOn(retryQueueModule.retryQueue, 'add').mockResolvedValue(undefined);
        vi.spyOn(deliverModule, 'deliverWebhook').mockResolvedValueOnce({
            success: false, httpStatus: 500, responseBody: 'error', durationMs: 40,
        });

        const eventId = await insertEvent();
        await processRetryJob(makeJob(eventId, 5));

        expect(enqueueSpy).not.toHaveBeenCalled();
    });
});

// ── Dropped jobs ──────────────────────────────────────────────────────────────
describe('Retry job — dropped when resource deleted', () => {
    it('does nothing if subscription no longer exists', async () => {
        vi.spyOn(deliverModule, 'deliverWebhook');
        // delivery_attempts is RLS-protected — must go through withTenant
        const before = await withTenant(tenantId, (tx) => tx.deliveryAttempt.count());

        // Use a non-existent subscriptionId.
        const fakeJob = {
            data: {
                eventId:           await insertEvent(),
                subscriptionId:    '00000000-0000-0000-0000-000000000000',
                tenantId,
                nextAttemptNumber: 2,
            },
        };
        await processRetryJob(fakeJob);

        const after = await withTenant(tenantId, (tx) => tx.deliveryAttempt.count());
        expect(after).toBe(before); // no new rows
        expect(deliverModule.deliverWebhook).not.toHaveBeenCalled();
    });
});