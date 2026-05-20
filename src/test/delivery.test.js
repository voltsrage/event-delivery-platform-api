import { describe, it, expect, beforeAll, afterAll, vi, afterEach } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';
import { withTenant } from '../utils/withTenant.js';
import { processEvent } from '../workers/delivery.worker.js';

let app;
let tenantAKey, tenantBKey;
let tenantAId;
let topicAId, subAId;

async function setup() {
    app = await buildApp();
    await app.ready();

    const regA = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: 'delivery-tenant-a', email: 'da1@example.com' },
    });
    const regB = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: 'delivery-tenant-b', email: 'db1@example.com' },
    });
    tenantAKey = JSON.parse(regA.body).data.rawKey;
    tenantBKey = JSON.parse(regB.body).data.rawKey;
    tenantAId  = JSON.parse(regA.body).data.tenant.id;

    const topicRes = await app.inject({
        method: 'POST', url: '/api/v1/topics',
        headers: { authorization: `Bearer ${tenantAKey}` },
        payload: { name: 'order.created' },
    });
    topicAId = JSON.parse(topicRes.body).data.id;

    const subRes = await app.inject({
        method: 'POST', url: `/api/v1/topics/${topicAId}/subscriptions`,
        headers: { authorization: `Bearer ${tenantAKey}` },
        payload: { endpoint: 'https://example.com/hook' },
    });
    subAId = JSON.parse(subRes.body).data.id;
}

beforeAll(setup);
afterAll(async () => {
    if (tenantAId) {
        await withTenant(tenantAId, (tx) => tx.deliveryAttempt.deleteMany({}));
        await withTenant(tenantAId, (tx) => tx.event.deleteMany({}));
    }
    await prisma.tenant.deleteMany({
        where: { email: { in: ['da1@example.com', 'db1@example.com'] } },
    });
    await prisma.$disconnect();
    await app.close();
});
afterEach(() => vi.restoreAllMocks());

// Helper: build the Kafka message envelope for a test event.
function makeEvent(overrides = {}) {
    return {
        eventId:     crypto.randomUUID(),
        tenantId:    JSON.parse(app.inject.toString()).tenantId, // resolved below
        topicId:     topicAId,
        topicName:   'order.created',
        eventType:   'order.created',
        payload:     { orderId: 'test-123' },
        publishedAt: new Date().toISOString(),
        ...overrides,
    };
}

// ── deliverWebhook ────────────────────────────────────────────────────────────
describe('HTTP delivery', () => {
    it('returns success: true for a 200 response', async () => {
        const { deliverWebhook } = await import('../delivery/deliver.js');
            vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce({
                ok:     true,
                status: 200,
                text:   async () => 'OK',
        }));

        const result = await deliverWebhook({
            subscription: { endpoint: 'https://example.com/hook', secretRaw: 'secret' },
            event: { eventId: 'e1', topicName: 'order.created', payload: { x: 1 } },
        });

        expect(result.success).toBe(true);
        expect(result.httpStatus).toBe(200);
        expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });

    it('returns success: false for a 500 response', async () => {
        const { deliverWebhook } = await import('../delivery/deliver.js');
            vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce({
                ok:     false,
                status: 500,
                text:   async () => 'Internal Server Error',
        }));

        const result = await deliverWebhook({
            subscription: { endpoint: 'https://example.com/hook', secretRaw: 'secret' },
            event: { eventId: 'e2', topicName: 'order.created', payload: { x: 1 } },
        });

        expect(result.success).toBe(false);
        expect(result.httpStatus).toBe(500);
        expect(result.responseBody).toBe('Internal Server Error');
    });

    it('returns success: false and "timeout" message when fetch is aborted', async () => {
        const { deliverWebhook } = await import('../delivery/deliver.js');
        const abortErr = new DOMException('The operation was aborted.', 'AbortError');
        vi.stubGlobal('fetch', vi.fn().mockRejectedValueOnce(abortErr));

        const result = await deliverWebhook({
            subscription: { endpoint: 'https://example.com/hook', secretRaw: 'secret' },
            event: { eventId: 'e3', topicName: 'order.created', payload: { x: 1 } },
        });

        expect(result.success).toBe(false);
        expect(result.httpStatus).toBeNull();
        expect(result.responseBody).toBe('timeout after 10s');
    });

    it('sends correct HMAC headers', async () => {
        const { deliverWebhook } = await import('../delivery/deliver.js');
        const { computeHmac }    = await import('../utils/computeHmac.js');

        let capturedHeaders;
        vi.stubGlobal('fetch', vi.fn().mockImplementation(async (url, opts) => {
            capturedHeaders = opts.headers;
            return { ok: true, status: 200, text: async () => '' };
        }));

        const payload = { orderId: 'sig-test' };
        await deliverWebhook({
            subscription: { endpoint: 'https://example.com/hook', secretRaw: 'my-secret' },
            event: { eventId: 'evt-sig', topicName: 'order.created', payload },
        });

        expect(capturedHeaders['X-Webhook-Event-Id']).toBe('evt-sig');
        expect(capturedHeaders['X-Webhook-Topic']).toBe('order.created');
        expect(capturedHeaders['X-Webhook-Signature']).toMatch(/^sha256=[0-9a-f]{64}$/);

        // Verify the signature matches what computeHmac would produce.
        const timestamp = capturedHeaders['X-Webhook-Timestamp'];
        const expected  = `sha256=${computeHmac('my-secret', timestamp, payload)}`;
        expect(capturedHeaders['X-Webhook-Signature']).toBe(expected);
    });
});

// ── processEvent ──────────────────────────────────────────────────────────────
describe('processEvent', () => {
    it('creates a delivery_attempt row and marks it success on 200', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
            ok: true, status: 200, text: async () => 'accepted',
        }));

        const eventId = crypto.randomUUID();
        await withTenant(tenantAId, (tx) => tx.event.create({
            data: { id: eventId, tenantId: tenantAId, topicId: topicAId, eventType: 'order.created', payload: { orderId: 'proc-test' } },
        }));

        await processEvent({
            eventId,
            tenantId:    tenantAId,
            topicId:     topicAId,
            topicName:   'order.created',
            eventType:   'order.created',
            payload:     { orderId: 'proc-test' },
            publishedAt: new Date().toISOString(),
        });

        // delivery_attempts is RLS-protected — must go through withTenant
        const attempts = await withTenant(tenantAId, (tx) =>
            tx.deliveryAttempt.findMany({
                where:   { subscriptionId: subAId },
                orderBy: { attemptedAt: 'desc' },
                take:    1,
            })
        );

        expect(attempts).toHaveLength(1);
        expect(attempts[0].status).toBe('success');
        expect(attempts[0].httpStatus).toBe(200);
        expect(attempts[0].responseBody).toBe('accepted');
    });

    it('marks attempt failed on non-2xx response', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
            ok: false, status: 503, text: async () => 'service unavailable',
        }));

        const eventId = crypto.randomUUID();
        await withTenant(tenantAId, (tx) => tx.event.create({
            data: { id: eventId, tenantId: tenantAId, topicId: topicAId, eventType: 'order.created', payload: { orderId: 'fail-test' } },
        }));

        await processEvent({
            eventId,
            tenantId:    tenantAId,
            topicId:     topicAId,
            topicName:   'order.created',
            eventType:   'order.created',
            payload:     { orderId: 'fail-test' },
            publishedAt: new Date().toISOString(),
        });

        // delivery_attempts is RLS-protected — must go through withTenant
        const attempt = await withTenant(tenantAId, (tx) =>
            tx.deliveryAttempt.findFirst({
                where:   { subscriptionId: subAId, status: 'failed' },
                orderBy: { attemptedAt: 'desc' },
            })
        );

        expect(attempt).not.toBeNull();
        expect(attempt.httpStatus).toBe(503);
    });

    it('creates no delivery_attempts when topic has no active subscriptions', async () => {
        vi.stubGlobal('fetch', vi.fn());

        // delivery_attempts is RLS-protected — must go through withTenant
        const beforeCount = await withTenant(tenantAId, (tx) => tx.deliveryAttempt.count());

        // Use a topicId that has no subscriptions.
        await processEvent({
            eventId:     crypto.randomUUID(),
            tenantId:    tenantAId,
            topicId:     '00000000-0000-0000-0000-000000000000', // no subscriptions
            topicName:   'ghost.topic',
            eventType:   'ghost.event',
            payload:     {},
            publishedAt: new Date().toISOString(),
        });

        const afterCount = await withTenant(tenantAId, (tx) => tx.deliveryAttempt.count());
        expect(afterCount).toBe(beforeCount); // no new rows
        expect(vi.mocked(fetch)).not.toHaveBeenCalled();
    });

    it('does not deliver tenant A events to tenant B subscriptions (RLS)', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
            ok: true, status: 200, text: async () => '',
        }));

        // Get tenantB's ID from DB.
        const regB = await app.inject({
            method: 'POST', url: '/api/v1/tenants',
            payload: { name: `rls-b-${Date.now()}`, email: `rls${Date.now()}@example.com` },
        });
        const tenantBId = JSON.parse(regB.body).data.tenant.id;

        // delivery_attempts is RLS-protected — count under tenant A to verify no leakage
        const beforeCount = await withTenant(tenantAId, (tx) => tx.deliveryAttempt.count());

        // Process an event with tenantBId but topicAId (which belongs to tenantA).
        // withTenant sets RLS to tenantB — the subscription query for topicAId under
        // tenantB returns zero rows (RLS filters them out). No deliveries should happen.
        await processEvent({
            eventId:     crypto.randomUUID(),
            tenantId:    tenantBId,
            topicId:     topicAId,  // tenant A's topic — invisible under tenant B's RLS context
            topicName:   'order.created',
            eventType:   'order.created',
            payload:     { orderId: 'rls-test' },
            publishedAt: new Date().toISOString(),
        });

        const afterCount = await withTenant(tenantAId, (tx) => tx.deliveryAttempt.count());
        expect(afterCount).toBe(beforeCount); // no deliveries to tenant A's subscriptions
        expect(vi.mocked(fetch)).not.toHaveBeenCalled();
    });
});