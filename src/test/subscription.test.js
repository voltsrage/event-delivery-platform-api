import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { buildApp } from '../app.js';
import prisma from '../db/prisma.js';

let app;
let tenantAKey, tenantBKey;
let tenantAId, tenantBId;
let topicAId;

async function registerTenant(name) {
    const res = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name, email: `${name}@example.com` },
    });
    const body = JSON.parse(res.body);
    return { tenantId: body.data.id, rawKey: body.data.rawKey };
}

async function createTopic(rawKey, name) {
    const res = await app.inject({
        method: 'POST', url: '/api/v1/topics',
        headers: { authorization: `Bearer ${rawKey}` },
        payload: { name },
    });
    return JSON.parse(res.body).data.id;
}

beforeAll(async () => {
    app = buildApp();
    await app.ready();

    const a = await registerTenant('sub-tenant-a');
    const b = await registerTenant('sub-tenant-b');
    tenantAKey = a.rawKey;
    tenantBKey = b.rawKey;
    tenantAId  = a.tenantId;
    tenantBId  = b.tenantId;

    topicAId = await createTopic(tenantAKey, 'orders');
});

afterAll(async () => {
    await prisma.$disconnect();
    await app.close();
});

// ── Helper ────────────────────────────────────────────────────────────────────
async function createSub(rawKey, topicId, endpoint = 'https://example.com/hook') {
    const res = await app.inject({
        method:  'POST',
        url:     `/api/v1/topics/${topicId}/subscriptions`,
        headers: { authorization: `Bearer ${rawKey}` },
        payload: { endpoint },
    });
    return res;
}

// ── HTTPS enforcement ─────────────────────────────────────────────────────────
describe('HTTPS enforcement', () => {
    it('rejects http:// endpoint at creation', async () => {
        const res = await createSub(tenantAKey, topicAId, 'http://example.com/hook');
        expect(res.statusCode).toBe(422);
        const body = JSON.parse(res.body);
        expect(body.code).toBe('ENDPOINT_MUST_BE_HTTPS');
    });

    it('rejects http:// endpoint at update', async () => {
        const created = await createSub(tenantAKey, topicAId);
        const subId   = JSON.parse(created.body).data.id;

        const res = await app.inject({
            method:  'PUT',
            url:     `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
            payload: { endpoint: 'http://example.com/new' },
        });
        expect(res.statusCode).toBe(422);
        expect(JSON.parse(res.body).code).toBe('ENDPOINT_MUST_BE_HTTPS');
    });
});

// ── Signing secret ────────────────────────────────────────────────────────────
describe('Signing secret', () => {
    it('returns secret in creation response', async () => {
        const res  = await createSub(tenantAKey, topicAId);
        const body = JSON.parse(res.body);
        expect(res.statusCode).toBe(201);
        expect(body.data.secret).toMatch(/^whsec_/);
    });

    it('does not return secret on subsequent GET', async () => {
        const created = await createSub(tenantAKey, topicAId);
        const subId   = JSON.parse(created.body).data.id;

        const get     = await app.inject({
            method:  'GET',
            url:     `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        const body = JSON.parse(get.body);
        expect(body.data.secret).toBeUndefined();
        expect(body.data.secretPrefix).toMatch(/^whsec_/);
    });

    it('secret_raw is never exposed in list response', async () => {
        const res  = await app.inject({
            method:  'GET',
            url:     `/api/v1/topics/${topicAId}/subscriptions`,
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        const items = JSON.parse(res.body).data.items;
        for (const item of items) {
        expect(item.secretRaw).toBeUndefined();
        expect(item.secretHash).toBeUndefined();
        }
    });
});

// ── Secret rotation ───────────────────────────────────────────────────────────
describe('Secret rotation', () => {
    it('returns new secret and invalidates old one', async () => {
        const created   = await createSub(tenantAKey, topicAId);
        const subId     = JSON.parse(created.body).data.id;
        const oldSecret = JSON.parse(created.body).data.secret;

        const rotated    = await app.inject({
            method:  'POST',
            url:     `/api/v1/subscriptions/${subId}/rotate-secret`,
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        const newSecret = JSON.parse(rotated.body).data.secret;

        expect(rotated.statusCode).toBe(200);
        expect(newSecret).toMatch(/^whsec_/);
        expect(newSecret).not.toBe(oldSecret);

        // Confirm DB now has the new hash, not the old one.
        const row = await prisma.subscription.findUnique({ where: { id: subId } });
        const oldHash = require('node:crypto').createHash('sha256').update(oldSecret).digest('hex');
        expect(row.secretHash).not.toBe(oldHash);
    });
});

// ── Quota enforcement ─────────────────────────────────────────────────────────
describe('Quota', () => {
    it('blocks creation when limit is reached', async () => {
        // Seed tenantA up to maxSubscriptions via direct DB insert (avoids repeated HTTP)
        const tenant = await prisma.tenant.findFirst({ where: { id: tenantAId } });
        const limit  = tenant.maxSubscriptions;

        // Count existing subscriptions for this tenant.
        const existing = await prisma.subscription.count({ where: { tenantId: tenantAId } });
        const needed   = limit - existing;

        for (let i = 0; i < needed; i++) {
            await createSub(tenantAKey, topicAId, `https://example.com/hook-${i}`);
        }

        // This one should be rejected.
        const res = await createSub(tenantAKey, topicAId, 'https://example.com/over-limit');
        expect(res.statusCode).toBe(409);
        expect(JSON.parse(res.body).code).toBe('SUBSCRIPTION_LIMIT_REACHED');
    });
});

// ── RLS isolation ─────────────────────────────────────────────────────────────
describe('RLS isolation', () => {
    it('tenant B cannot read tenant A subscriptions', async () => {
        const created = await createSub(tenantAKey, topicAId);
        const subId   = JSON.parse(created.body).data.id;

        const res = await app.inject({
            method:  'GET',
            url:     `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantBKey}` },
        });
        // RLS makes the row invisible; service maps that to 404 (not 403).
        expect(res.statusCode).toBe(404);
    });

    it('tenant B cannot delete tenant A subscription', async () => {
        const created = await createSub(tenantAKey, topicAId);
        const subId   = JSON.parse(created.body).data.id;

        const res = await app.inject({
            method:  'DELETE',
            url:     `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantBKey}` },
        });
        expect(res.statusCode).toBe(404);
    });
});

// ── Enable / disable ──────────────────────────────────────────────────────────
describe('Enable / disable', () => {
    it('disables and re-enables a subscription', async () => {
        const created = await createSub(tenantAKey, topicAId);
        const subId   = JSON.parse(created.body).data.id;

        const disable = await app.inject({
            method:  'PUT',
            url:     `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
            payload: { isEnabled: false },
        });
        expect(JSON.parse(disable.body).data.isEnabled).toBe(false);

        const enable = await app.inject({
            method:  'PUT',
            url:     `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
            payload: { isEnabled: true },
        });
        expect(JSON.parse(enable.body).data.isEnabled).toBe(true);
    });
});

// ── Delete ────────────────────────────────────────────────────────────────────
describe('Delete', () => {
    it('returns 204 and subscription is gone', async () => {
        const created = await createSub(tenantAKey, topicAId);
        const subId   = JSON.parse(created.body).data.id;

        const del = await app.inject({
            method:  'DELETE',
            url:     `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        expect(del.statusCode).toBe(204);

        const get = await app.inject({
            method:  'GET',
            url:     `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        expect(get.statusCode).toBe(404);
    });

    it('returns 404 on second delete attempt', async () => {
        const created = await createSub(tenantAKey, topicAId);
        const subId   = JSON.parse(created.body).data.id;

        await app.inject({
            method: 'DELETE', url: `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        const second = await app.inject({
            method: 'DELETE', url: `/api/v1/subscriptions/${subId}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        expect(second.statusCode).toBe(404);
    });
});