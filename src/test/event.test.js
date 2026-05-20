import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';
import { withTenant } from '../utils/withTenant.js';
import * as producerModule from '../kafka/producer.js';

vi.mock('../kafka/producer.js', () => ({
    connectProducer:    vi.fn().mockResolvedValue(undefined),
    disconnectProducer: vi.fn().mockResolvedValue(undefined),
    publishToKafka:     vi.fn().mockResolvedValue(undefined),
}));

let app;
let tenantAKey, tenantBKey;
let tenantAId, tenantBId;
let topicAId;

async function registerTenant(name) {
    const res = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name, email: `${name}@example.com` },
    });
    return JSON.parse(res.body).data;
}

async function createTopic(rawKey, name) {
    const res = await app.inject({
        method: 'POST', url: '/api/v1/topics',
        headers: { authorization: `Bearer ${rawKey}` },
        payload: { name },
    });
    return JSON.parse(res.body).data.id;
}

async function publishEvent(rawKey, topicId, overrides = {}) {
    return app.inject({
            method:  'POST',
            url:     `/api/v1/topics/${topicId}/events`,
            headers: { authorization: `Bearer ${rawKey}` },
            payload: {
            eventType: 'order.created',
            payload:   { orderId: 'abc-123', amount: 5000 },
            ...overrides,
        },
    });
}

beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    const a = await registerTenant('event-tenant-a1');
    const b = await registerTenant('event-tenant-a2');
    tenantAKey = a.rawKey;
    tenantBKey = b.rawKey;
    tenantAId  = a.tenant.id;
    tenantBId  = b.tenant.id;

    topicAId = await createTopic(tenantAKey, 'order.created');
});

afterAll(async () => {
    // events FK to tenants is RESTRICT — delete events first, then tenants.
    // withTenant sets the RLS context so deleteMany is scoped to each tenant.
    await withTenant(tenantAId, (tx) => tx.event.deleteMany({}));
    await withTenant(tenantBId, (tx) => tx.event.deleteMany({}));
    await prisma.tenant.deleteMany({ where: { id: { in: [tenantAId, tenantBId] } } });

    await prisma.$disconnect();
    await app.close();
});

// ── 202 response and stored state ─────────────────────────────────────────────
describe('Event publishing', () => {
    it('returns 202 with event ID and eventType', async () => {
        const res  = await publishEvent(tenantAKey, topicAId);
        const body = JSON.parse(res.body);
        expect(res.statusCode).toBe(202);
        expect(body.success).toBe(true);
        expect(body.data.id).toBeDefined();
        expect(body.data.eventType).toBe('order.created');
    });

    it('stores event with publishedToKafka = false', async () => {
        const res     = await publishEvent(tenantAKey, topicAId);
        const eventId = JSON.parse(res.body).data.id;
        // events is RLS-protected — must go through withTenant
        const row = await withTenant(tenantAId, (tx) =>
            tx.event.findUnique({ where: { id: eventId } })
        );
        expect(row.publishedToKafka).toBe(false);
    });

    it('does not expose tenantId or publishedToKafka in response', async () => {
        const data = JSON.parse((await publishEvent(tenantAKey, topicAId)).body).data;
        expect(data.tenantId).toBeUndefined();
        expect(data.publishedToKafka).toBeUndefined();
    });
});

// ── Idempotency ───────────────────────────────────────────────────────────────
describe('Idempotency key', () => {
    it('same key returns same event ID on second call', async () => {
        const key = `idem-${Date.now()}`;
        const first  = await publishEvent(tenantAKey, topicAId, { idempotencyKey: key });
        const second = await publishEvent(tenantAKey, topicAId, { idempotencyKey: key });

        expect(first.statusCode).toBe(202);
        expect(second.statusCode).toBe(202);
        expect(JSON.parse(first.body).data.id).toBe(JSON.parse(second.body).data.id);
    });

    it('different keys produce different event IDs', async () => {
        const ts    = Date.now();
        const first  = await publishEvent(tenantAKey, topicAId, { idempotencyKey: `key-a-${ts}` });
        const second = await publishEvent(tenantAKey, topicAId, { idempotencyKey: `key-b-${ts}` });
        expect(JSON.parse(first.body).data.id).not.toBe(JSON.parse(second.body).data.id);
    });

    it('same key under different tenants creates independent events', async () => {
        const topicBId = await createTopic(tenantBKey, 'order.created');
        const key      = `shared-${Date.now()}`;

        const resA = await publishEvent(tenantAKey, topicAId,  { idempotencyKey: key });
        const resB = await publishEvent(tenantBKey, topicBId,  { idempotencyKey: key });

        // The partial unique index is (tenant_id, idempotency_key) — same key under
        // different tenants is allowed. Each tenant gets a distinct event row.
        expect(JSON.parse(resA.body).data.id).not.toBe(JSON.parse(resB.body).data.id);
    });

    it('omitting idempotencyKey creates a new event on every call', async () => {
        const first  = await publishEvent(tenantAKey, topicAId);
        const second = await publishEvent(tenantAKey, topicAId);
        expect(JSON.parse(first.body).data.id).not.toBe(JSON.parse(second.body).data.id);
    });
});

// ── Validation ────────────────────────────────────────────────────────────────
describe('Validation', () => {
    it('rejects missing eventType with 422', async () => {
        const res = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicAId}/events`,
            headers: { authorization: `Bearer ${tenantAKey}` },
            payload: { payload: { foo: 'bar' } },
        });
        expect(res.statusCode).toBe(422);
    });

    it('rejects missing payload with 422', async () => {
        const res = await app.inject({
            method: 'POST', url: `/api/v1/topics/${topicAId}/events`,
            headers: { authorization: `Bearer ${tenantAKey}` },
            payload: { eventType: 'order.created' },
        });
        expect(res.statusCode).toBe(422);
    });
});

// ── Topic checks ──────────────────────────────────────────────────────────────
describe('Topic checks', () => {
    it('returns 404 for a non-existent topic', async () => {
        const res = await publishEvent(tenantAKey, '00000000-0000-0000-0000-000000000000');
        expect(res.statusCode).toBe(404);
        expect(JSON.parse(res.body).error.code).toBe('TOPIC_NOT_FOUND');
    });

    it('returns 404 for a soft-deleted topic', async () => {
        const created = await app.inject({
            method: 'POST', url: '/api/v1/topics',
            headers: { authorization: `Bearer ${tenantAKey}` },
            payload: { name: `ephemeral-${Date.now()}` },
        });
        const deletedId = JSON.parse(created.body).data.id;
        await app.inject({
            method: 'DELETE', url: `/api/v1/topics/${deletedId}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
        });

        const res = await publishEvent(tenantAKey, deletedId);
        expect(res.statusCode).toBe(404);
    });
});

// ── RLS isolation ─────────────────────────────────────────────────────────────
describe('RLS isolation', () => {
  it('tenant B cannot publish events to tenant A topic', async () => {
    // Tenant B's withTenant sets tenant B's ID as the RLS context.
    // The topic lookup finds no row with topicAId for tenant B → 404.
    // 404, not 403, to avoid confirming whether the topic exists.
    const res = await publishEvent(tenantBKey, topicAId);
    expect(res.statusCode).toBe(404);
    expect(JSON.parse(res.body).error.code).toBe('TOPIC_NOT_FOUND');
  });
});

// ── Outbox atomicity ──────────────────────────────────────────────────────────
describe('Outbox atomicity', () => {
    it('event stays unpublished if Kafka publish throws', async () => {
        producerModule.publishToKafka.mockRejectedValueOnce(new Error('Kafka unavailable'));

        const { pollOutbox } = await import('../workers/outboxWorker.js');

        // Publish an event so there is at least one unpublished row.
        const res     = await publishEvent(tenantAKey, topicAId);
        const eventId = JSON.parse(res.body).data.id;

        // Run one poll cycle — it should throw (Kafka mock), rolling back the transaction.
        await expect(pollOutbox()).rejects.toThrow('Kafka unavailable');

        // The event must still be unpublished — the UPDATE was rolled back.
        // events is RLS-protected — must go through withTenant
        const row = await withTenant(tenantAId, (tx) =>
            tx.event.findUnique({ where: { id: eventId } })
        );
        expect(row.publishedToKafka).toBe(false);
    });
});

describe('Outbox worker integration (mocked Kafka)', () => {
    it('marks event published_to_kafka = true after successful send', async () => {
        // publishToKafka resolves by default from the module-level mock.
        const res     = await publishEvent(tenantAKey, topicAId, { idempotencyKey: `outbox-${Date.now()}` });
        const eventId = JSON.parse(res.body).data.id;

        // Confirm starts unpublished.
        // events is RLS-protected — must go through withTenant
        const before = await withTenant(tenantAId, (tx) =>
            tx.event.findUnique({ where: { id: eventId } })
        );
        expect(before.publishedToKafka).toBe(false);

        // Import and run one poll cycle.
        const { pollOutbox } = await import('../workers/outboxWorker.js');
        await pollOutbox();

        const after = await withTenant(tenantAId, (tx) =>
            tx.event.findUnique({ where: { id: eventId } })
        );
        expect(after.publishedToKafka).toBe(true);
    });

    it('leaves event unpublished when Kafka send throws', async () => {
        producerModule.publishToKafka.mockRejectedValueOnce(new Error('broker down'));

        const res     = await publishEvent(tenantAKey, topicAId, { idempotencyKey: `outbox-fail-${Date.now()}` });
        const eventId = JSON.parse(res.body).data.id;

        const { pollOutbox } = await import('../workers/outboxWorker.js');
        await expect(pollOutbox()).rejects.toThrow('broker down');

        // events is RLS-protected — must go through withTenant
        const row = await withTenant(tenantAId, (tx) =>
            tx.event.findUnique({ where: { id: eventId } })
        );
        expect(row.publishedToKafka).toBe(false);
    });
});