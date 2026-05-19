import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';
import { withTenant } from '../utils/withTenant.js';
import crypto from 'crypto';

let app;

// Helper: register a fresh tenant and return its API key
async function registerTenant(suffix) {
    const res = await app.inject({
        method:  'POST',
        url:     '/api/v1/tenants',
        payload: { name: `Phase4 ${suffix}`, email: `phase4-${suffix}@test.example.com` },
    });
    expect(res.statusCode).toBe(201);
    return JSON.parse(res.body).data.rawKey;
}

beforeAll(async () => {
    app = await buildApp();
    await app.ready();
});

afterAll(async () => {
    // subscriptions, events, and topics are RLS-protected — must go through withTenant.
    // Query tenants (non-RLS) first to get IDs, then clean up per-tenant.
    const testTenants = await prisma.tenant.findMany({
        where:  { email: { endsWith: '@test.example.com' } },
        select: { id: true },
    });
    for (const { id } of testTenants) {
        await withTenant(id, async (tx) => {
            await tx.subscription.deleteMany();
            await tx.event.deleteMany();
            await tx.topic.deleteMany();
        });
    }
    await prisma.apiKey.deleteMany({
        where: { tenant: { email: { endsWith: '@test.example.com' } } },
    });
    await prisma.tenant.deleteMany({
        where: { email: { endsWith: '@test.example.com' } },
    });
    await app.close();
    await prisma.$disconnect();
});

// ─── Compound unique constraint ───────────────────────────────────────────────

describe('compound unique constraint on (tenant_id, name)', () => {
    it('same topic name under different tenants succeeds', async () => {
        const keyA = await registerTenant('cu-a');
        const keyB = await registerTenant('cu-b');

        const resA = await app.inject({
            method:  'POST',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${keyA}` },
            payload: { name: 'order.created' },
        });

        const resB = await app.inject({
            method:  'POST',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${keyB}` },
            payload: { name: 'order.created' },
        });

        // Both must succeed — the unique constraint is (tenant_id, name), not just name.
        expect(resA.statusCode).toBe(201);
        expect(resB.statusCode).toBe(201);

        // Each tenant's topic has a distinct ID even though the names are identical.
        const topicA = JSON.parse(resA.body).data;
        const topicB = JSON.parse(resB.body).data;
        expect(topicA.id).not.toBe(topicB.id);
    });

    it('same topic name under the same tenant returns 409', async () => {
        const key = await registerTenant('cu-c');

        await app.inject({
            method:  'POST',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${key}` },
            payload: { name: 'payment.failed' },
        });

        const res = await app.inject({
            method:  'POST',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${key}` },
            payload: { name: 'payment.failed' },
        });

        expect(res.statusCode).toBe(409);
        expect(JSON.parse(res.body).error.code).toBe('TOPIC_NAME_TAKEN');
    });
});

// ─── CRUD ─────────────────────────────────────────────────────────────────────

describe('topic CRUD', () => {
    let key;
    let topicId;

    beforeAll(async () => {
        key = await registerTenant('crud');
    });

    it('POST /api/v1/topics returns 201 with topic object', async () => {
        const res = await app.inject({
            method:  'POST',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${key}` },
            payload: { name: 'shipment.updated', description: 'Shipment status events' },
        });

        expect(res.statusCode).toBe(201);
        const { data } = JSON.parse(res.body);
        expect(data.id).toBeDefined();
        expect(data.name).toBe('shipment.updated');
        expect(data.description).toBe('Shipment status events');
        topicId = data.id;
    });

    it('GET /api/v1/topics lists only non-deleted topics with pagination', async () => {
        // Create a second topic
        await app.inject({
            method:  'POST',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${key}` },
            payload: { name: 'user.registered' },
        });

        const res = await app.inject({
            method:     'GET',
            url:        '/api/v1/topics?page=1&pageSize=10',
            headers:    { authorization: `Bearer ${key}` },
        });

        expect(res.statusCode).toBe(200);
        const { data } = JSON.parse(res.body);
        expect(data).toHaveProperty('items');
        expect(data).toHaveProperty('total');
        expect(data).toHaveProperty('page', 1);
        expect(data).toHaveProperty('pageSize', 10);
        expect(data).toHaveProperty('totalPages');
        expect(data.items.length).toBeGreaterThanOrEqual(2);
        // Response shape must not expose deletedAt or tenantId
        expect(data.items[0]).not.toHaveProperty('deletedAt');
        expect(data.items[0]).not.toHaveProperty('tenantId');
    });

    it('GET /api/v1/topics/:id returns topic details with subscription count', async () => {
        const res = await app.inject({
            method:  'GET',
            url:     `/api/v1/topics/${topicId}`,
            headers: { authorization: `Bearer ${key}` },
        });

        expect(res.statusCode).toBe(200);
        const { data } = JSON.parse(res.body);
        expect(data.id).toBe(topicId);
        expect(data).toHaveProperty('subscriptionCount');
        expect(typeof data.subscriptionCount).toBe('number');
    });

    it('GET /api/v1/topics/:id returns 404 for unknown id', async () => {
        const fakeId = crypto.randomUUID();
        const res = await app.inject({
            method:  'GET',
            url:     `/api/v1/topics/${fakeId}`,
            headers: { authorization: `Bearer ${key}` },
        });
        expect(res.statusCode).toBe(404);
        expect(JSON.parse(res.body).error.code).toBe('TOPIC_NOT_FOUND');
    });

    it('POST requires a name field', async () => {
        const res = await app.inject({
            method:  'POST',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${key}` },
            payload: { description: 'No name provided' },
        });
        expect(res.statusCode).toBe(422);
    });
});

// ─── RLS isolation ────────────────────────────────────────────────────────────

describe('RLS isolation on topics', () => {
    it('tenant A cannot see tenant B topics', async () => {
        const keyA = await registerTenant('rls-a');
        const keyB = await registerTenant('rls-b');

        // Create a topic for tenant B
        const createRes = await app.inject({
            method:  'POST',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${keyB}` },
            payload: { name: 'private.event' },
        });
        const topicBId = JSON.parse(createRes.body).data.id;

        // Tenant A tries to fetch tenant B's topic by ID
        const res = await app.inject({
            method:  'GET',
            url:     `/api/v1/topics/${topicBId}`,
            headers: { authorization: `Bearer ${keyA}` },
        });

        // RLS makes the topic invisible to tenant A — returns 404, not 403
        expect(res.statusCode).toBe(404);
    });
});

// ─── Delete: soft delete + subscription cascade ────────────────────────────────

describe('DELETE /api/v1/topics/:id', () => {
    let key;
    let tenantId;
    let topicId;
    let subscriptionId;

    beforeAll(async () => {
        key = await registerTenant('del');
        const tenantRes = await prisma.tenant.findFirst({
            where: { email: 'phase4-del@test.example.com' },
        });
        tenantId = tenantRes.id;

        // Create a topic via API
        const topicRes = await app.inject({
            method:  'POST',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${key}` },
            payload: { name: 'to.be.deleted' },
        });
        topicId = JSON.parse(topicRes.body).data.id;

        // Create a subscription directly via Prisma (Subscription API is Phase 5)
        const { secretHash, secretPrefix, secretRaw } = (() => {
        const raw = crypto.randomBytes(32).toString('hex');
        return {
            secretHash:   crypto.createHash('sha256').update(raw).digest('hex'),
            secretPrefix: raw.slice(0, 8),
            secretRaw:    raw,
        };
        })();

        // subscriptions is RLS-protected — must go through withTenant
        const sub = await withTenant(tenantId, (tx) =>
        tx.subscription.create({
            data: {
            tenantId,
            topicId,
            endpointUrl:  'https://webhook.example.com/hook',
            secretHash,
            secretPrefix,
            secretRaw,
            enabled: true,
            },
        })
        );
        subscriptionId = sub.id;
    });

    it('DELETE sets deleted_at on the topic row — it is not hard-deleted', async () => {
        const res = await app.inject({
            method:  'DELETE',
            url:     `/api/v1/topics/${topicId}`,
            headers: { authorization: `Bearer ${key}` },
        });

        expect(res.statusCode).toBe(200);

        // The topic row must still exist in the database (so events can keep referencing it)
        // topics is RLS-protected — must go through withTenant
        const row = await withTenant(tenantId, (tx) =>
            tx.topic.findUnique({ where: { id: topicId } })
        );
        expect(row).not.toBeNull();
        expect(row.deletedAt).not.toBeNull();
    });

    it('subscription is hard-deleted when its topic is soft-deleted', async () => {
        // subscriptions is RLS-protected — must go through withTenant
        const sub = await withTenant(tenantId, (tx) =>
            tx.subscription.findUnique({ where: { id: subscriptionId } })
        );
        // Subscription must be gone — explicitly deleted in the same transaction
        expect(sub).toBeNull();
    });

    it('deleted topic does not appear in GET /topics list', async () => {
        const res = await app.inject({
            method:  'GET',
            url:     '/api/v1/topics',
            headers: { authorization: `Bearer ${key}` },
        });

        const { data } = JSON.parse(res.body);
        const found = data.items.some((t) => t.id === topicId);
        expect(found).toBe(false);
    });

    it('GET /topics/:id returns 404 for a deleted topic', async () => {
        const res = await app.inject({
            method:  'GET',
            url:     `/api/v1/topics/${topicId}`,
            headers: { authorization: `Bearer ${key}` },
        });
        expect(res.statusCode).toBe(404);
    });

    it('DELETE a second time returns 404 (idempotent delete not required)', async () => {
        const res = await app.inject({
            method:  'DELETE',
            url:     `/api/v1/topics/${topicId}`,
            headers: { authorization: `Bearer ${key}` },
        });
        expect(res.statusCode).toBe(404);
    });
});