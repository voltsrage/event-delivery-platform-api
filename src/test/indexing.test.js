import { describe, it, expect, vi, afterEach, beforeAll, afterAll } from 'vitest';
import { buildDeliveryLogDocument } from '../search/buildDeliveryLogDocument.js';
import { processIndexJob } from '../workers/indexingWorker.js';

// Mock the Elasticsearch client before any imports that use it.
vi.mock('../search/esClient.js', () => {
    const index = vi.fn().mockResolvedValue({ result: 'created' });
    return { default: { index, indices: { exists: vi.fn().mockResolvedValue(false), create: vi.fn() } } };
});

afterEach(() => vi.clearAllMocks());

// ── buildDeliveryLogDocument ──────────────────────────────────────────────────
describe('buildDeliveryLogDocument', () => {
    const base = {
        tenantId:       'tenant-a',
        eventId:        'evt-1',
        subscriptionId: 'sub-1',
        topicName:      'order.created',
        endpoint:       'https://example.com/hook',
        status:         'success',
        httpStatus:     200,
        attemptNumber:  1,
        payload:        { orderId: 'abc', amount: 100 },
        responseBody:   'OK',
        attemptedAt:    new Date('2026-05-14T10:00:00.000Z'),
        nextRetryAt:    null,
    };

    it('serialises payload as a JSON string for full-text indexing', () => {
        const doc = buildDeliveryLogDocument(base);
        expect(typeof doc.payload).toBe('string');
        expect(JSON.parse(doc.payload)).toEqual({ orderId: 'abc', amount: 100 });
    });

    it('maps camelCase inputs to snake_case ES field names', () => {
        const doc = buildDeliveryLogDocument(base);
        expect(doc.tenant_id).toBe('tenant-a');
        expect(doc.event_id).toBe('evt-1');
        expect(doc.subscription_id).toBe('sub-1');
        expect(doc.topic_name).toBe('order.created');
        expect(doc.endpoint_url).toBe('https://example.com/hook');
        expect(doc.attempt_number).toBe(1);
    });

    it('converts Date objects to ISO strings', () => {
        const doc = buildDeliveryLogDocument(base);
        expect(doc.attempted_at).toBe('2026-05-14T10:00:00.000Z');
    });

    it('passes null next_retry_at when not scheduled', () => {
        const doc = buildDeliveryLogDocument({ ...base, nextRetryAt: null });
        expect(doc.next_retry_at).toBeNull();
    });

    it('includes next_retry_at as ISO string when set', () => {
        const retryAt = new Date('2026-05-14T10:00:30.000Z');
        const doc     = buildDeliveryLogDocument({ ...base, nextRetryAt: retryAt });
        expect(doc.next_retry_at).toBe('2026-05-14T10:00:30.000Z');
    });
});

// ── processIndexJob ───────────────────────────────────────────────────────────
describe('processIndexJob', () => {
    it('calls esClient.index with attemptId as document _id', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        const doc      = buildDeliveryLogDocument({
            tenantId: 't', eventId: 'e', subscriptionId: 's',
            topicName: 'test', endpoint: 'https://example.com',
            status: 'success', httpStatus: 200, attemptNumber: 1,
            payload: {}, responseBody: 'OK',
            attemptedAt: new Date(), nextRetryAt: null,
        });

        await processIndexJob({ data: { attemptId: 'attempt-abc', document: doc } });

        expect(esClient.index).toHaveBeenCalledOnce();
        const call = esClient.index.mock.calls[0][0];
        expect(call.index).toBe('delivery-logs');
        expect(call.id).toBe('attempt-abc');  // attemptId used as ES document _id
        expect(call.document).toBe(doc);
    });

    it('uses the index name from DELIVERY_LOGS_INDEX constant', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        await processIndexJob({
        data: {
            attemptId: 'attempt-xyz',
            document:  { tenant_id: 't', status: 'failed' },
        },
        });
        expect(esClient.index.mock.calls[0][0].index).toBe('delivery-logs');
    });

    it('propagates Elasticsearch errors so BullMQ retries the job', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.index.mockRejectedValueOnce(new Error('connection refused'));

        await expect(
        processIndexJob({ data: { attemptId: 'fail-id', document: {} } })
        ).rejects.toThrow('connection refused');
        // BullMQ catches the thrown error and retries the job per queue defaultJobOptions.
    });
});

// ── Integration: indexing jobs enqueued after delivery ────────────────────────
describe('Indexing jobs enqueued by delivery worker', () => {
    it('enqueues one indexing job per subscription after processEvent (success)', async () => {
        const { buildApp } = await import('../app.js');
        const { prisma }   = await import('../db/prisma.js');
        const { withTenant } = await import('../utils/withTenant.js');
        const { processEvent } = await import('../workers/delivery.worker.js');
        const { indexQueue }   = await import('../queues/indexQueue.js');

        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
            ok: true, status: 200, text: async () => 'OK',
        }));
        const enqueueSpy = vi.spyOn(indexQueue, 'add').mockResolvedValue(undefined);

        const app = await buildApp();
        await app.ready();

        const reg = await app.inject({
            method: 'POST', url: '/api/v1/tenants',
            payload: { name: `idx-tenant-${Date.now()}`, email: `idx${Date.now()}@e.com` },
        });
        const { rawKey, tenant: {id: tenantId} } = JSON.parse(reg.body).data;

        const topicRes = await app.inject({
            method: 'POST', url: '/api/v1/topics',
            headers: { authorization: `Bearer ${rawKey}` },
            payload: { name: 'idx.test' },
        });
        const topicId = JSON.parse(topicRes.body).data.id;

        await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/subscriptions`,
            headers: { authorization: `Bearer ${rawKey}` },
            payload: { endpoint: 'https://example.com/idx-hook' },
        });

        const evtRes = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicId}/events`,
            headers: { authorization: `Bearer ${rawKey}` },
            payload: { eventType: 'idx.test', payload: { x: 1 } },
        });
        const eventId = JSON.parse(evtRes.body).data.id;

        // events is RLS-protected — must go through withTenant
        const evtRow = await withTenant(tenantId, (tx) =>
            tx.event.findUnique({ where: { id: eventId } })
        );

        await processEvent({
            eventId,
            tenantId,
            topicId,
            topicName:   'idx.test',
            eventType:   'idx.test',
            payload:     evtRow.payload,
            publishedAt: new Date().toISOString(),
        });

        // One indexing job per subscription (one subscription in this test).
        expect(enqueueSpy).toHaveBeenCalledOnce();

        const jobData = enqueueSpy.mock.calls[0][1];
        expect(jobData.attemptId).toBeDefined();
        expect(jobData.document.status).toBe('success');
        expect(jobData.document.tenant_id).toBe(tenantId);
        expect(typeof jobData.document.payload).toBe('string'); // serialised

        await prisma.$disconnect();
        await app.close();
        vi.restoreAllMocks();
    });
});