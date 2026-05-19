import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';
import { withTenant } from '../utils/withTenant.js';
import { authenticate } from '../hooks/authenticate.js';
import { ApiResponse } from '../utils/ApiResponse.js';
import crypto from 'crypto';

let app;
let tenantAKey;
let tenantBKey;
let tenantAId;
let tenantBId;

beforeAll(async () => {
  app = await buildApp();

  // Temporary test-only route: returns all topics with no WHERE clause.
  // If RLS is correctly configured, each tenant's key returns only their topics.
  // This is the exact pattern Phase 4 route handlers will follow.
  app.get(
    '/test/topics',
    { onRequest: [authenticate] },
    async (req, res) => {
      const topics = await req.withTenant(async (tx) => {
        return tx.topic.findMany({
          select: { id: true, name: true, tenantId: true },
        });
      });
      return res.send(ApiResponse.success(topics));
    }
  );

  await app.ready();

  // Requires the seed script to have run first: node src/seed/seed.js
  const tenants = await prisma.tenant.findMany({
    take: 2,
    orderBy: { createdAt: 'asc' },
  });

  if (tenants.length < 2) {
    throw new Error('Run the seed script first: node src/seed/seed.js');
  }

  [tenantAId, tenantBId] = tenants.map((t) => t.id);

  const makeKey = async (tenantId) => {
    const rawKey  = `sk_live_${crypto.randomBytes(32).toString('hex')}`;
    const keyHash = crypto.createHash('sha256').update(rawKey).digest('hex');
    await prisma.apiKey.create({
      data: { tenantId, keyHash, keyPrefix: rawKey.slice(0, 15), label: 'vitest' },
    });
    return rawKey;
  };

  tenantAKey = await makeKey(tenantAId);
  tenantBKey = await makeKey(tenantBId);
});

afterAll(async () => {
  await prisma.apiKey.deleteMany({ where: { label: 'vitest' } });
  await app.close();
  await prisma.$disconnect();
});

describe('RLS tenant isolation through withTenant', () => {
  it('tenant A sees only its own topics', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/test/topics',
      headers: { authorization: `Bearer ${tenantAKey}` },
    });

    expect(res.statusCode).toBe(200);
    const { data } = JSON.parse(res.body);
    expect(data.length).toBeGreaterThan(0);
    // Every row must belong to tenant A — no WHERE clause in the query
    expect(data.every((t) => t.tenantId === tenantAId)).toBe(true);
  });

  it('tenant B sees only its own topics', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/test/topics',
      headers: { authorization: `Bearer ${tenantBKey}` },
    });

    expect(res.statusCode).toBe(200);
    const { data } = JSON.parse(res.body);
    expect(data.length).toBeGreaterThan(0);
    expect(data.every((t) => t.tenantId === tenantBId)).toBe(true);
  });

  it('tenant A topics do not appear in tenant B results', async () => {
    const tenantATopics = await withTenant(tenantAId, (tx) =>
      tx.topic.findMany({
        where: { tenantId: tenantAId },
        select: { id: true },
      })
    );
    const tenantAIds = new Set(tenantATopics.map((t) => t.id));

    const res = await app.inject({
      method: 'GET',
      url: '/test/topics',
      headers: { authorization: `Bearer ${tenantBKey}` },
    });

    const { data } = JSON.parse(res.body);
    expect(data.some((t) => tenantAIds.has(t.id))).toBe(false);
  });

  it('missing Authorization header returns 401', async () => {
    const res = await app.inject({ method: 'GET', url: '/test/topics' });
    expect(res.statusCode).toBe(401);
  });

  it('invalid API key returns 401', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/test/topics',
      headers: { authorization: 'Bearer sk_live_notavalidkey' },
    });
    expect(res.statusCode).toBe(401);
  });

  it('revoked API key returns 401', async () => {
    const rawKey  = `sk_live_${crypto.randomBytes(32).toString('hex')}`;
    const keyHash = crypto.createHash('sha256').update(rawKey).digest('hex');
    const created = await prisma.apiKey.create({
      data: {
        tenantId:  tenantAId,
        keyHash,
        keyPrefix: rawKey.slice(0, 15),
        label:     'vitest',
      },
    });

    await prisma.apiKey.update({
      where: { id: created.id },
      data:  { revokedAt: new Date() },
    });

    const res = await app.inject({
      method: 'GET',
      url: '/test/topics',
      headers: { authorization: `Bearer ${rawKey}` },
    });

    expect(res.statusCode).toBe(401);
  });
});