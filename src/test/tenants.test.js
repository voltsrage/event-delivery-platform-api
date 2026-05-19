import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';

let app;

beforeAll(async () => {
    app = await buildApp();
    await app.ready();
});

afterAll(async () => {
    // Clean up any tenants created during the test (identified by test email domain)
    await prisma.apiKey.deleteMany({
        where: { tenant: { email: { endsWith: '@test-phase3.example.com' } } },
    });
    await prisma.tenant.deleteMany({
        where: { email: { endsWith: '@test-phase3.example.com' } },
    });
    await app.close();
    await prisma.$disconnect();
});

describe('POST /api/v1/tenants', () => {
    it('creates a tenant and returns the raw key once', async () => {
        const res = await app.inject({
        method: 'POST',
        url: '/api/v1/tenants',
        payload: { name: 'Test Corp', email: 'signup@test-phase3.example.com' },
        });

        expect(res.statusCode).toBe(201);
        const { data } = JSON.parse(res.body);

        expect(data.tenant.id).toBeDefined();
        expect(data.tenant.email).toBe('signup@test-phase3.example.com');
        expect(data.apiKey.keyPrefix).toMatch(/^sk_live_/);
        expect(data.rawKey).toMatch(/^sk_live_/);
        expect(data.rawKey.length).toBeGreaterThan(15);

        // keyHash must never appear in the response
        expect(JSON.stringify(data)).not.toContain('keyHash');
    });

    it('returns a different raw key on each registration', async () => {
        const res1 = await app.inject({
            method: 'POST',
            url: '/api/v1/tenants',
            payload: { name: 'Alpha Inc', email: 'alpha@test-phase3.example.com' },
        });
        const res2 = await app.inject({
            method: 'POST',
            url: '/api/v1/tenants',
            payload: { name: 'Beta LLC', email: 'beta@test-phase3.example.com' },
        });

        const { data: d1 } = JSON.parse(res1.body);
        const { data: d2 } = JSON.parse(res2.body);

        expect(d1.rawKey).not.toBe(d2.rawKey);
        expect(d1.apiKey.keyPrefix).not.toBe(d2.apiKey.keyPrefix);
    });

    it('returns 409 if email is already registered', async () => {
        await app.inject({
            method: 'POST',
            url: '/api/v1/tenants',
            payload: { name: 'Dupe Corp', email: 'dupe@test-phase3.example.com' },
        });

        const res = await app.inject({
            method: 'POST',
            url: '/api/v1/tenants',
            payload: { name: 'Dupe Corp Again', email: 'dupe@test-phase3.example.com' },
        });

        expect(res.statusCode).toBe(409);
    });

    it('returns 422 for missing fields', async () => {
        const res = await app.inject({
            method: 'POST',
            url: '/api/v1/tenants',
            payload: { name: 'No Email Corp' },
        });
        expect(res.statusCode).toBe(422);
    });
});

describe('GET /api/v1/api-keys — raw key never returned after registration', () => {
    let tenantKey;

    beforeAll(async () => {
        const res = await app.inject({
            method: 'POST',
            url: '/api/v1/tenants',
            payload: { name: 'Key List Test', email: 'keylist@test-phase3.example.com' },
        });
        const { data } = JSON.parse(res.body);
        tenantKey = data.rawKey;
    });

    it('raw key is not present in the list response', async () => {
        const res = await app.inject({
            method: 'GET',
            url: '/api/v1/api-keys',
            headers: { authorization: `Bearer ${tenantKey}` },
        });

        expect(res.statusCode).toBe(200);
        const body = res.body;

        // Neither the raw key value nor the field name keyHash should appear
        expect(body).not.toContain(tenantKey);
        expect(body).not.toContain('keyHash');
    });

    it('list shows keyPrefix and label but not the secret', async () => {
        const res = await app.inject({
            method: 'GET',
            url: '/api/v1/api-keys',
            headers: { authorization: `Bearer ${tenantKey}` },
        });

        const { data } = JSON.parse(res.body);
        expect(data.items.length).toBeGreaterThan(0);
        expect(data.items[0]).toHaveProperty('keyPrefix');
        expect(data.items[0]).toHaveProperty('label');
        expect(data.items[0]).not.toHaveProperty('keyHash');
        expect(data.items[0]).not.toHaveProperty('rawKey');
    });
});

describe('POST /api/v1/api-keys — additional keys for authenticated tenant', () => {
    let tenantKey;

    beforeAll(async () => {
        const res = await app.inject({
            method: 'POST',
            url: '/api/v1/tenants',
            payload: { name: 'Multi Key Corp', email: 'multikey@test-phase3.example.com' },
        });
        tenantKey = JSON.parse(res.body).data.rawKey;
    });

    it('creates an additional key and returns it once', async () => {
        const res = await app.inject({
            method: 'POST',
            url: '/api/v1/api-keys',
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { label: 'Production' },
        });

        expect(res.statusCode).toBe(201);
        const { data } = JSON.parse(res.body);
        expect(data.rawKey).toMatch(/^sk_live_/);
        expect(data.apiKey.label).toBe('Production');
        expect(JSON.stringify(data)).not.toContain('keyHash');
    });

    it('additional key authenticates successfully', async () => {
        const createRes = await app.inject({
            method: 'POST',
            url: '/api/v1/api-keys',
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { label: 'Staging' },
        });
        const newKey = JSON.parse(createRes.body).data.rawKey;

        // The new key must work for authenticated endpoints
        const listRes = await app.inject({
            method: 'GET',
            url: '/api/v1/api-keys',
            headers: { authorization: `Bearer ${newKey}` },
        });
        expect(listRes.statusCode).toBe(200);
    });
});

describe('DELETE /api/v1/api-keys/:id — revocation', () => {
    let tenantKey;
    let keyToRevokeId;
    let keyToRevokeRaw;

    beforeAll(async () => {
        // Create tenant and capture its default key
        const tenantRes = await app.inject({
            method: 'POST',
            url: '/api/v1/tenants',
            payload: { name: 'Revoke Test Corp', email: 'revoke@test-phase3.example.com' },
        });
        tenantKey = JSON.parse(tenantRes.body).data.rawKey;

        // Create an additional key that will be revoked
        const keyRes = await app.inject({
            method: 'POST',
            url: '/api/v1/api-keys',
            headers: { authorization: `Bearer ${tenantKey}` },
            payload: { label: 'To Be Revoked' },
        });
        const { data } = JSON.parse(keyRes.body);
        keyToRevokeId  = data.apiKey.id;
        keyToRevokeRaw = data.rawKey;
    });

    it('revoked key returns 401 from authenticate hook', async () => {
        // Confirm the key works before revocation
        const beforeRes = await app.inject({
            method: 'GET',
            url: '/api/v1/api-keys',
            headers: { authorization: `Bearer ${keyToRevokeRaw}` },
        });
        expect(beforeRes.statusCode).toBe(200);

        // Revoke it
        const deleteRes = await app.inject({
            method: 'DELETE',
            url: `/api/v1/api-keys/${keyToRevokeId}`,
            headers: { authorization: `Bearer ${tenantKey}` },
        });
        expect(deleteRes.statusCode).toBe(200);

        // The revoked key must now return 401
        const afterRes = await app.inject({
            method: 'GET',
            url: '/api/v1/api-keys',
            headers: { authorization: `Bearer ${keyToRevokeRaw}` },
        });
        expect(afterRes.statusCode).toBe(401);
    });

    it('cannot revoke a key belonging to a different tenant', async () => {
        // Create a second tenant
        const otherRes = await app.inject({
            method: 'POST',
            url: '/api/v1/tenants',
            payload: { name: 'Other Corp', email: 'other@test-phase3.example.com' },
        });
        const otherKey    = JSON.parse(otherRes.body).data.rawKey;
        const otherKeyId  = JSON.parse(otherRes.body).data.apiKey.id;

        // Try to delete otherTenant's key using tenantKey — must return 404 (not 200 or 403)
        const res = await app.inject({
            method: 'DELETE',
            url: `/api/v1/api-keys/${otherKeyId}`,
            headers: { authorization: `Bearer ${tenantKey}` },
        });
        expect(res.statusCode).toBe(404);
        // The key must still work for the other tenant
        const checkRes = await app.inject({
            method: 'GET',
            url: '/api/v1/api-keys',
            headers: { authorization: `Bearer ${otherKey}` },
        });
        expect(checkRes.statusCode).toBe(200);
    });

    it('revoking an already-revoked key returns 409', async () => {
        const res = await app.inject({
            method: 'DELETE',
            url: `/api/v1/api-keys/${keyToRevokeId}`,
            headers: { authorization: `Bearer ${tenantKey}` },
        });
        expect(res.statusCode).toBe(409);
    });
});