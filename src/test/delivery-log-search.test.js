import { describe, it, expect, beforeAll, afterAll, vi, afterEach } from 'vitest';
import { buildApp } from '../app.js';
import { prisma } from '../db/prisma.js';

// Mock the ES client module before importing anything that uses it.
vi.mock('../search/esClient.js', () => {
  const search = vi.fn();
  const get    = vi.fn();
  return { default: { search, get } };
});

let app;
let tenantAKey, tenantBKey;
let tenantAId,  tenantBId;

beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    const regA = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: 'search-tenant-a', email: 'sa@delivery-log-search.test.example.com' },
    });
    const regB = await app.inject({
        method: 'POST', url: '/api/v1/tenants',
        payload: { name: 'search-tenant-b', email: 'sb@delivery-log-search.test.example.com' },
    });
    const a = JSON.parse(regA.body).data;
    const b = JSON.parse(regB.body).data;
    tenantAKey = a.rawKey;
    tenantBKey = b.rawKey;
    tenantAId  = a.tenant.id;
    tenantBId  = b.tenant.id;
});

afterAll(async () => {
    await prisma.apiKey.deleteMany({
        where: { tenant: { email: { endsWith: '@delivery-log-search.test.example.com' } } },
    });
    await prisma.tenant.deleteMany({
        where: { email: { endsWith: '@delivery-log-search.test.example.com' } },
    });
    await app.close();
    await prisma.$disconnect();
});
afterEach(() => vi.clearAllMocks());

// ── Helper: build a mock ES search response ──────────────────────────────────
function mockSearchResponse(hits = [], total = 0) {
    return {
        hits: {
        total: { value: total, relation: 'eq' },
        hits:  hits.map((doc, i) => ({
            _id:     `attempt-${i}`,
            _source: doc,
        })),
        },
    };
}

// ── Query construction — tenant scoping ──────────────────────────────────────
describe('Tenant scoping', () => {
    it('always includes tenant_id term filter', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.search.mockResolvedValueOnce(mockSearchResponse());

        await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });

        const query = esClient.search.mock.calls[0][0].query;
        const tenantFilter = query.bool.filter.find(
        (f) => f.term?.tenant_id === tenantAId,
        );
        expect(tenantFilter).toBeDefined();
    });

    it('uses the authenticated tenant\'s ID, not a query param', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.search.mockResolvedValueOnce(mockSearchResponse());

        // Even if a malicious caller passes tenantBId as a query param,
        // the filter uses request.tenantId resolved from the API key.
        await app.inject({
            method: 'GET',
            url:    `/api/v1/delivery-logs?topicName=${tenantBId}`, // not a tenant scope param
            headers: { authorization: `Bearer ${tenantAKey}` },
        });

        const query = esClient.search.mock.calls[0][0].query;
        const tenantFilter = query.bool.filter.find((f) => f.term?.tenant_id);
        expect(tenantFilter.term.tenant_id).toBe(tenantAId);
        expect(tenantFilter.term.tenant_id).not.toBe(tenantBId);
    });
});

// ── Query construction — optional filters ────────────────────────────────────
describe('Optional filters', () => {
    async function captureQuery(qs) {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.search.mockResolvedValueOnce(mockSearchResponse());
        await app.inject({
            method: 'GET', url: `/api/v1/delivery-logs${qs ? '?' + qs : ''}`,
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        return esClient.search.mock.calls[0][0].query;
    }

    it('adds status term filter when status param is provided', async () => {
        const query = await captureQuery('status=failed');
        const statusFilter = query.bool.filter.find((f) => f.term?.status === 'failed');
        expect(statusFilter).toBeDefined();
    });

    it('adds topic_name term filter when topicName param is provided', async () => {
        const query = await captureQuery('topicName=order.created');
        const topicFilter = query.bool.filter.find((f) => f.term?.topic_name === 'order.created');
        expect(topicFilter).toBeDefined();
    });

    it('adds attempted_at range filter when from and to are provided', async () => {
        const query = await captureQuery('from=2026-05-01&to=2026-05-14');
        const rangeFilter = query.bool.filter.find((f) => f.range?.attempted_at);
        expect(rangeFilter).toBeDefined();
        expect(rangeFilter.range.attempted_at.gte).toBe('2026-05-01');
        expect(rangeFilter.range.attempted_at.lte).toBe('2026-05-14');
    });

    it('adds only gte when only from is provided', async () => {
        const query = await captureQuery('from=2026-05-01');
        const rangeFilter = query.bool.filter.find((f) => f.range?.attempted_at);
        expect(rangeFilter.range.attempted_at.gte).toBe('2026-05-01');
        expect(rangeFilter.range.attempted_at.lte).toBeUndefined();
    });

    it('omits range filter when neither from nor to is provided', async () => {
        const query = await captureQuery('status=success');
        const rangeFilter = query.bool.filter.find((f) => f.range?.attempted_at);
        expect(rangeFilter).toBeUndefined();
    });

    it('adds must match clause when q is provided (payload full-text)', async () => {
        const query = await captureQuery('q=orderId');
        // must clause added for scoring — payload is a text field
        expect(query.bool.must).toBeDefined();
        expect(query.bool.must[0].match.payload).toBe('orderId');
    });

    it('has no must clause when q is not provided', async () => {
        const query = await captureQuery('status=failed');
        expect(query.bool.must).toBeUndefined();
    });
});

// ── Pagination ────────────────────────────────────────────────────────────────
describe('Pagination', () => {
    it('passes correct from/size to Elasticsearch', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.search.mockResolvedValueOnce(mockSearchResponse([], 0));

        await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs?page=3&pageSize=10',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });

        const call = esClient.search.mock.calls[0][0];
        expect(call.from).toBe(20);  // (page 3 - 1) * pageSize 10
        expect(call.size).toBe(10);
    });

    it('returns paginated shape with totalPages', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.search.mockResolvedValueOnce(
        mockSearchResponse(
            [{ tenant_id: tenantAId, status: 'success', event_id: 'e1' }],
            55,
        ),
        );

        const res  = await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs?page=1&pageSize=20',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        const body = JSON.parse(res.body);
        expect(res.statusCode).toBe(200);
        expect(body.data.total).toBe(55);
        expect(body.data.totalPages).toBe(3); // ceil(55/20)
        expect(body.data.items).toHaveLength(1);
    });

    it('includes ES document _id as item id field', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.search.mockResolvedValueOnce(
        mockSearchResponse([{ status: 'success' }], 1),
        );

        const res   = await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        const items = JSON.parse(res.body).data.items;
        expect(items[0].id).toBe('attempt-0');
    });

    it('enforces track_total_hits: true for accurate counts', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.search.mockResolvedValueOnce(mockSearchResponse());

        await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });

        expect(esClient.search.mock.calls[0][0].track_total_hits).toBe(true);
    });
});

// ── GET by ID ─────────────────────────────────────────────────────────────────
describe('GET /api/v1/delivery-logs/:attemptId', () => {
    it('returns the document when it belongs to the requesting tenant', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.get.mockResolvedValueOnce({
            found:    true,
            _id:      'attempt-abc',
            _source:  { tenant_id: tenantAId, status: 'success', event_id: 'e1' },
        });

        const res  = await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs/attempt-abc',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        const body = JSON.parse(res.body);
        expect(res.statusCode).toBe(200);
        expect(body.data.id).toBe('attempt-abc');
        expect(body.data.status).toBe('success');
    });

    it('returns 404 when document belongs to a different tenant', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        // Document exists but belongs to tenant B.
        esClient.get.mockResolvedValueOnce({
            found:   true,
            _id:     'attempt-b',
            _source: { tenant_id: tenantBId, status: 'success' },
        });

        // Tenant A requests tenant B's document.
        const res = await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs/attempt-b',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        expect(res.statusCode).toBe(404);
        expect(JSON.parse(res.body).error.code).toBe('DELIVERY_LOG_NOT_FOUND');
    });

    it('returns 404 when ES returns found: false', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        esClient.get.mockResolvedValueOnce({ found: false });

        const res = await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs/nonexistent',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        expect(res.statusCode).toBe(404);
    });

    it('returns 404 when ES throws a 404 error', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        const notFoundErr = new Error('not found');
        notFoundErr.meta = { statusCode: 404 };
        esClient.get.mockRejectedValueOnce(notFoundErr);

        const res = await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs/gone',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        expect(res.statusCode).toBe(404);
    });

    it('propagates non-404 ES errors as 500', async () => {
        const esClient = (await import('../search/esClient.js')).default;
        const serverErr = new Error('connection refused');
        serverErr.meta  = { statusCode: 503 };
        esClient.get.mockRejectedValueOnce(serverErr);

        const res = await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs/bad',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        expect(res.statusCode).toBe(500);
    });
});

// ── Validation ────────────────────────────────────────────────────────────────
describe('Validation', () => {
    it('rejects invalid status value with 422', async () => {
        const res = await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs?status=unknown',
            headers: { authorization: `Bearer ${tenantAKey}` },
        });
        expect(res.statusCode).toBe(422);
    });

    it('rejects unauthenticated requests with 401', async () => {
        const res = await app.inject({
            method: 'GET', url: '/api/v1/delivery-logs',
        });
        expect(res.statusCode).toBe(401);
    });
});