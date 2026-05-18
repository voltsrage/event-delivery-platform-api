# PRD: Event Delivery Platform API

## Overview

A multi-tenant SaaS platform for reliable webhook event delivery. Tenants publish events to named topics; the platform fans them out to all registered subscriber endpoints with HMAC-signed payloads, exponential backoff retries, dead-letter storage, and a searchable delivery history. Think of it as a self-hosted Svix or Hookdeck.

This project covers four concepts absent from the rest of the portfolio: **multi-tenancy with PostgreSQL Row-Level Security**, **Kafka as an event log** (not a job queue), **webhook delivery as the core product** (not a side feature), and **Elasticsearch for delivery log search**. It also closes the remaining gaps in the ge-mid messaging question block (ge-mid-043 to 048) by making Kafka's broker-vs-log distinction concrete rather than theoretical.

**Stack:** Node.js, Fastify, PostgreSQL (RLS), Apache Kafka (KRaft), Elasticsearch, Redis, Pino → Seq, Vitest, Docker Compose, Nginx, GCP VM, GitLab CI/CD.

---

## Goals

- Implement multi-tenancy via PostgreSQL Row-Level Security so tenant isolation is enforced at the database layer, not the application layer
- Internalize Kafka's log model: consumer groups, partition keys, offset commits, consumer lag, and replay — concepts that do not exist in RabbitMQ or BullMQ
- Build the provider side of webhook delivery: HMAC signing, per-subscription secrets, retry queues, dead-letter storage, and delivery receipts
- Use Elasticsearch to index an append-only delivery log and expose a search API — full-text search across event payloads with date and status filters

## Non-Goals

- A frontend dashboard UI
- Multi-region Kafka replication
- Kafka Schema Registry or Avro serialization
- Billing or usage metering beyond rate limiting
- Event transformation or filtering rules

---

## API Conventions

This is a developer platform — tenants authenticate with API keys, not user sessions. There are no login flows. An API key is issued on tenant creation and is shown exactly once; thereafter only its prefix and a SHA-256 hash are stored.

**Authentication header:**
```
Authorization: Bearer sk_live_a1b2c3d4e5f6...
```

Every request is resolved to a tenant by hashing the incoming key and looking up the hash in `api_keys`. If no match is found, or the key is revoked, return 401.

**Response envelope:** Same shape as all other portfolio projects.

**Success:**
```json
{ "success": true, "statusCode": 200, "data": {}, "error": null }
```

**Error:**
```json
{
  "success": false,
  "statusCode": 404,
  "data": null,
  "error": { "message": "Topic not found.", "code": "TOPIC_NOT_FOUND" }
}
```

**Pagination:** Offset-based (`?page=1&pageSize=20`) on all list endpoints.

---

## Domain Model

A **tenant** is a company or team that signs up for the platform. All data — topics, subscriptions, events, delivery logs — is owned by a tenant and invisible to other tenants. RLS enforces this at the database layer.

An **API key** authenticates a tenant's requests. The raw key is shown once at creation and never stored. Only a SHA-256 hash and a short display prefix (`sk_live_a1b2c3`) are persisted.

A **topic** is a named event channel within a tenant's account (e.g., `order.created`, `payment.failed`). Tenants define their own topic names.

A **subscription** binds a topic to an endpoint URL. When an event is published to a topic, it is delivered to every active subscription on that topic. Each subscription has its own **signing secret** — a random 32-byte hex string used to compute the HMAC signature on each delivery. Compromising one subscription's secret does not affect others.

An **event** is a JSON payload published to a topic. Once published, it is immutable. The platform's job is to deliver it to all matching subscriptions at least once.

A **delivery attempt** records one HTTP POST to a subscription endpoint — the outcome (status code, response body), the attempt number, and when the next retry is scheduled. Every attempt is written to PostgreSQL and indexed in Elasticsearch.

A **dead letter** is created when a delivery attempt exhausts all retries. It holds the event, the subscription, and the failure history. Tenants can inspect and manually replay dead letters.

---

## Features

---

### 1. Tenant Registration and API Key Management

**Description:** A tenant signs up with a name and email. The platform generates a raw API key, shows it once, stores only its SHA-256 hash, and issues the tenant account. Tenants can create additional keys (for staging vs production environments), revoke keys, and list active keys by prefix.

**Endpoints:**
- `POST /api/v1/tenants` — create tenant; generate and return raw API key once (unauthenticated — this is the signup endpoint)
- `POST /api/v1/api-keys` — create additional API key for the authenticated tenant
- `GET /api/v1/api-keys` — list API keys by prefix and label (never returns the raw key or hash)
- `DELETE /api/v1/api-keys/:id` — revoke a key; sets `revoked_at`

**Key generation and storage:**
```typescript
const rawKey = `sk_live_${crypto.randomBytes(32).toString('hex')}`;
const keyHash = crypto.createHash('sha256').update(rawKey).digest('hex');
const keyPrefix = rawKey.slice(0, 15); // "sk_live_a1b2c3d"

// Store keyHash and keyPrefix — never rawKey
// Return rawKey in the response body exactly once
```

**Key lookup on every request:**
```sql
SELECT ak.tenant_id, ak.revoked_at
FROM api_keys ak
WHERE ak.key_hash = $hash AND ak.revoked_at IS NULL;
```

Update `last_used_at` asynchronously after the lookup — do not add this to the critical path.

**Concepts practiced:** API key authentication pattern vs JWT (sd-junior-012, sd-mid-012), why API keys are hashed before storage (cr-junior security — same principle as password hashing; a database breach must not expose live keys), prefix-based identification without storing the secret, key rotation and revocation patterns.

---

### 2. Tenant Context and Row-Level Security

**Description:** PostgreSQL Row-Level Security is the enforcement mechanism for tenant isolation. Every table that holds tenant data has an RLS policy that filters rows by `tenant_id`. The tenant context is injected into each database transaction before any query runs. This means a bug in the application layer that forgets to filter by tenant still cannot read another tenant's data — the database enforces it.

**RLS setup:**
```sql
-- Enable RLS on every tenant-scoped table
ALTER TABLE topics           ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriptions    ENABLE ROW LEVEL SECURITY;
ALTER TABLE events           ENABLE ROW LEVEL SECURITY;
ALTER TABLE delivery_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE dead_letters      ENABLE ROW LEVEL SECURITY;

-- Policy: a row is visible only if its tenant_id matches the current session variable
CREATE POLICY tenant_isolation ON topics
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

-- Repeat for each table
```

**Tenant context injection in the request handler:**
```typescript
// After API key lookup resolves tenant_id:
await prisma.$executeRaw`SET LOCAL app.current_tenant_id = ${tenantId}`;
// All subsequent queries in this transaction automatically respect the policy
```

`SET LOCAL` scopes the variable to the current transaction. When the transaction ends, the setting is cleared. This means a connection from the pool cannot carry a previous tenant's context into the next request.

**Concepts practiced:** Multi-tenancy implementation strategies (sd-mid-023 — schema-per-tenant vs shared schema with RLS vs application-layer filtering), why RLS is safer than application-layer filtering (a missing WHERE clause in application code becomes a data breach; an RLS policy makes that impossible), `SET LOCAL` vs `SET` and why transaction-scoped context is required for connection pool safety, the performance cost of RLS (every query gets an implicit WHERE clause appended — understand the index implications).

---

### 3. Topic Management

**Description:** Tenants create named event topics. Topic names follow a dot-separated convention (`order.created`, `payment.failed`) but the platform does not enforce this — it is a naming convention, not a hierarchy. Topic names are unique per tenant, enforced by a compound unique constraint.

**Endpoints:**
- `POST /api/v1/topics` — create a topic
- `GET /api/v1/topics` — list topics (paginated)
- `GET /api/v1/topics/:id` — get topic details and subscription count
- `DELETE /api/v1/topics/:id` — delete topic; cascade to subscriptions

**Concepts practiced:** Compound unique constraints (`tenant_id, name`), cascade delete semantics (db-junior), why topic names are a convention not a tree structure (the platform does not need to understand the hierarchy to route events — a string equality match is sufficient and simpler), soft delete consideration (topics are not soft-deleted — if a tenant deletes a topic, existing events retain their `topic_id` as a historical reference but new events cannot be published to a deleted topic; use `deleted_at` and check it at publish time).

---

### 4. Subscription Management

**Description:** Tenants register endpoint URLs to receive events for a topic. Each subscription gets its own per-subscription signing secret generated at creation — shown once, stored as a hash, used for HMAC signature generation on every delivery. Subscriptions can be enabled or disabled without being deleted.

**Endpoints:**
- `POST /api/v1/topics/:topicId/subscriptions` — create subscription; generate and return signing secret once
- `GET /api/v1/topics/:topicId/subscriptions` — list subscriptions (paginated)
- `GET /api/v1/subscriptions/:id` — get subscription details
- `PUT /api/v1/subscriptions/:id` — update endpoint URL or enabled state
- `DELETE /api/v1/subscriptions/:id` — delete subscription
- `POST /api/v1/subscriptions/:id/rotate-secret` — rotate the signing secret; show new secret once

**Signing secret generation:**
```typescript
const rawSecret = crypto.randomBytes(32).toString('hex');
const secretHash = crypto.createHash('sha256').update(rawSecret).digest('hex');
// Store secretHash — return rawSecret once
```

**Endpoint URL validation:** Validate that `endpointUrl` is an HTTPS URL. Reject HTTP — a non-TLS endpoint would expose event payloads and HMAC secrets in transit.

**Secret rotation:** When a tenant rotates a subscription's secret, the old secret is immediately invalidated. Deliveries in flight that used the old secret will fail the subscriber's HMAC verification — document this as a brief interruption window that tenants should plan for.

**Concepts practiced:** Per-resource secrets (same pattern as QR code per-event secrets in Diaspora Connect, but now applied to webhook subscriptions), secret rotation with a brief interruption window, why HTTP endpoints are rejected (cr-junior security), the difference between enabling/disabling a subscription (preserves history) vs deleting it (destroys history).

---

### 5. Event Publishing with the Outbox Pattern

**Description:** Tenants POST events to a topic. The API writes the event to PostgreSQL and publishes it to Kafka — but these two writes must happen atomically. The outbox pattern solves this: the event is written to an `events` table with `published_to_kafka = false` inside the same database transaction as any other business logic. A separate outbox worker polls for unpublished events, publishes them to Kafka, and marks them `published_to_kafka = true`.

**Endpoint:**
- `POST /api/v1/topics/:topicId/events` — publish an event; returns event ID immediately with 202 Accepted

**Request body:**
```json
{
  "eventType": "order.created",
  "payload": { "orderId": "abc-123", "amount": 5000, "currency": "TWD" },
  "idempotencyKey": "order-abc-123-created"
}
```

**Publish flow:**
1. Validate the topic exists and belongs to the authenticated tenant (RLS handles the ownership check)
2. Check idempotency: `SELECT id FROM events WHERE idempotency_key = $key AND tenant_id = $tenantId`. If found, return the original event ID — do not insert again.
3. Insert the event row with `published_to_kafka = false`
4. Return `202 Accepted` with the event ID immediately — the client does not wait for Kafka

**Outbox worker (separate process):**
```sql
-- Poll for unpublished events
SELECT id, tenant_id, topic_id, payload, created_at
FROM events
WHERE published_to_kafka = false
ORDER BY created_at ASC
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

`FOR UPDATE SKIP LOCKED` prevents two outbox worker instances from processing the same event simultaneously. After a successful Kafka publish, set `published_to_kafka = true`.

**Kafka message:**
```json
{
  "eventId": "uuid",
  "tenantId": "uuid",
  "topicId": "uuid",
  "topicName": "order.created",
  "eventType": "order.created",
  "payload": { "orderId": "abc-123", "amount": 5000 },
  "publishedAt": "2026-05-10T10:00:00Z"
}
```

**Partition key:** `tenantId`. All events for the same tenant go to the same partition, preserving ordering within a tenant. The trade-off is hot partitions if a single tenant dominates volume — discuss this in the design decisions section.

**Concepts practiced:** Outbox pattern (ge-mid-033 — outbox vs CDC, already in cold chain and digital wallet but now in a different context), `FOR UPDATE SKIP LOCKED` for concurrent worker safety (db-mid-011 — optimistic vs pessimistic locking), idempotency key at the event level (sd-mid-013, sd-junior-020), 202 Accepted for async operations (REST conventions covered in Fleet Telemetry but now the async model is Kafka not a background job), Kafka partition key selection and the hot partition trade-off (sd-mid-036, ge-mid-044).

---

### 6. Webhook Delivery Worker

**Description:** A Kafka consumer that reads published events and delivers them to each active subscription on the event's topic. Every delivery attempt is recorded. Failed deliveries are retried with exponential backoff. After the maximum retry count, the event is moved to the dead letter store.

**Consumer setup:**
```typescript
const consumer = kafka.consumer({ groupId: 'webhook-delivery-worker' });
await consumer.subscribe({ topic: 'platform.events', fromBeginning: false });

await consumer.run({
  eachMessage: async ({ message }) => {
    const event = JSON.parse(message.value.toString());
    await processEvent(event);
  }
});
```

**Per-event processing:**
1. Load all active subscriptions for `(tenant_id, topic_id)` from PostgreSQL
2. For each subscription, create a `delivery_attempts` row with `status = 'pending'`
3. POST to the subscription's `endpoint_url` with:
   - `Content-Type: application/json`
   - `X-Webhook-Event-Id: {eventId}`
   - `X-Webhook-Topic: {topicName}`
   - `X-Webhook-Timestamp: {unixTimestampSeconds}`
   - `X-Webhook-Signature: sha256={hmacHex}`
4. On 2xx response: update attempt to `status = 'success'`; index in Elasticsearch
5. On non-2xx or timeout: update attempt to `status = 'failed'`; schedule next retry
6. After max retries (5): create dead letter record; update attempt to `status = 'dead_lettered'`

**HMAC signature computation:**
```typescript
const signedPayload = `${timestamp}.${JSON.stringify(eventPayload)}`;
const hmac = crypto.createHmac('sha256', subscription.signingSecretRaw)
  .update(signedPayload)
  .digest('hex');
// Header: X-Webhook-Signature: sha256=<hmac>
```

Signing the timestamp alongside the payload prevents replay attacks — a captured webhook request with a valid signature cannot be replayed with a different timestamp.

**Retry schedule (exponential backoff):**
| Attempt | Delay before retry |
|---|---|
| 1 (initial) | immediate |
| 2 | 30 seconds |
| 3 | 5 minutes |
| 4 | 30 minutes |
| 5 | 2 hours |
| Dead letter | no further retries |

Retries use a separate BullMQ queue rather than Kafka — the retry is a time-delayed job targeting a specific subscription, not a re-publication to the log. Kafka is for the initial fan-out; BullMQ is for per-subscription retry scheduling.

**Timeout:** 10 seconds per delivery attempt. A subscriber endpoint that hangs must not block the worker. Use `AbortController` with a timeout signal.

**Kafka offset commitment:** The consumer commits its offset only after all subscriptions for an event have been processed (either delivered or scheduled for retry). If the worker crashes mid-fan-out, the offset is not committed and the event is re-processed. Delivery attempt records prevent duplicate deliveries from becoming duplicate charges — the subscriber endpoint receives the same `X-Webhook-Event-Id` on a redelivery and can deduplicate.

**Concepts practiced:** Kafka consumer groups and offset management (ge-mid-044, ge-mid-047 — what happens when a consumer restarts from the last committed offset), at-least-once delivery semantics (ge-mid-046 — why Kafka does not guarantee exactly-once without additional mechanisms), HMAC replay attack prevention via timestamp signing (cr-junior security), why BullMQ handles retries but Kafka handles fan-out (ge-mid-048 — using the right tool for each concern), `AbortController` for HTTP timeout (nodejs-mid — body parsing and DoS), back pressure between Kafka consumption rate and HTTP delivery rate (sd-mid-030, ge-mid-047).

---

### 7. Delivery Log Search (Elasticsearch)

**Description:** Every delivery attempt is indexed in Elasticsearch immediately after it completes. Tenants can search their own delivery history by event type, status, endpoint, date range, or free text across the event payload. The search is scoped to the authenticated tenant — a `term` filter on `tenant_id` is applied to every query.

**Endpoints:**
- `GET /api/v1/delivery-logs?status=failed&topicName=order.created&from=2026-05-01&to=2026-05-10&page=1` — search delivery logs
- `GET /api/v1/delivery-logs/:attemptId` — get a single attempt detail

**Elasticsearch index mapping:**
```json
{
  "mappings": {
    "properties": {
      "tenant_id":        { "type": "keyword" },
      "event_id":         { "type": "keyword" },
      "subscription_id":  { "type": "keyword" },
      "topic_name":       { "type": "keyword" },
      "endpoint_url":     { "type": "keyword" },
      "status":           { "type": "keyword" },
      "http_status":      { "type": "integer" },
      "attempt_number":   { "type": "integer" },
      "payload":          { "type": "text", "analyzer": "standard" },
      "response_body":    { "type": "text" },
      "attempted_at":     { "type": "date" },
      "next_retry_at":    { "type": "date" }
    }
  }
}
```

**Query construction:**
```typescript
const query = {
  bool: {
    filter: [
      { term: { tenant_id: tenantId } },          // always applied — tenant scope
      ...(status    ? [{ term: { status } }]       : []),
      ...(topicName ? [{ term: { topic_name: topicName } }] : []),
      ...(from || to ? [{ range: { attempted_at: { gte: from, lte: to } } }] : [])
    ],
    ...(q ? { must: [{ match: { payload: q } }] } : {})
  }
};
```

`filter` clauses use term/range queries that do not affect relevance scoring and are cached by Elasticsearch. `must` clauses (free-text search on `payload`) use full-text analysis and affect scoring. Separating them is an Elasticsearch query optimization — filter cache hits are essentially free.

**Indexing after delivery:** After writing the delivery attempt to PostgreSQL, index it to Elasticsearch asynchronously via BullMQ. Do not index synchronously in the delivery worker — if Elasticsearch is temporarily unavailable, it must not block webhook deliveries.

**Concepts practiced:** Elasticsearch index mapping and query DSL (db-mid-012 — full-text search beyond pg_trgm), `keyword` vs `text` field types (keyword for exact-match filters like status and topic_name; text for full-text search on payload), `filter` vs `must` context and the filter cache (query optimization analogous to database index usage), why Elasticsearch is the right tool for this workload (append-only, high-volume, arbitrary query patterns — pg_trgm and JSONB queries on millions of delivery records would be too slow), async indexing via BullMQ to isolate Elasticsearch availability from delivery correctness (sd-mid-014 — eventual consistency).

---

### 8. Event Replay

**Description:** Tenants can replay all events for a subscription from a specified start time. This is the killer feature that distinguishes Kafka from a job queue — the event log is retained and can be re-read from any offset. Replay creates new delivery attempts for the specified subscription without re-publishing new events to the log.

**Endpoint:**
- `POST /api/v1/subscriptions/:id/replay` — body: `{ "from": "2026-05-01T00:00:00Z" }`

**Replay flow:**
1. Validate the subscription belongs to the authenticated tenant (RLS)
2. Query `events` in PostgreSQL for `topic_id = subscription.topicId AND created_at >= from`
3. For each event, enqueue a delivery job to BullMQ targeting this specific subscription — do not republish to Kafka
4. Return the count of events enqueued for replay

Replay does not use Kafka consumer offset reset. Resetting a consumer group's offset is a cluster-wide operation that would replay events for all tenants on that partition. Instead, replay reads from the PostgreSQL events table (which is the source of truth) and injects delivery jobs directly into BullMQ.

**Concepts practiced:** Kafka replay as a concept (ge-mid-047 — retention and replay as a first-class Kafka feature), why this replay implementation uses PostgreSQL instead of Kafka offset reset (partition ownership and multi-tenancy — one tenant's replay must not affect another tenant's consumer position), replay as a recovery mechanism for subscriber downtime (if a subscriber's server was down for 2 hours, replay delivers everything missed).

---

### 9. Dead Letter Management

**Description:** Tenants inspect events that exhausted all delivery retries and can manually trigger a single retry for any dead-lettered event to a specific subscription.

**Endpoints:**
- `GET /api/v1/dead-letters?subscriptionId=...&page=1` — list dead letters for the tenant
- `GET /api/v1/dead-letters/:id` — get dead letter details including full delivery attempt history
- `POST /api/v1/dead-letters/:id/retry` — enqueue a single retry attempt to BullMQ; does not move the dead letter record until delivery succeeds

**Concepts practiced:** Dead-letter queue pattern (sd-mid-016, ge-mid-047), manual retry as an operator escape hatch, why the dead letter record is not deleted on a retry attempt (if the retry also fails, the history is preserved — deleting and re-queueing would lose the attempt count).

---

### 10. Rate Limiting Per Tenant

**Description:** Event publishing is rate-limited per tenant to prevent a single tenant from overwhelming the platform. Limits are enforced via Redis using the same `SET NX EX + INCR` atomic pattern as other projects.

**Limits:**
- Event publishing: 1000 events per tenant per minute
- Subscription creation: 10 per tenant per hour
- Replay: 5 concurrent replay jobs per tenant

**Redis keys:**
```
ratelimit:publish:{tenantId}      STRING  value = count  TTL = 60s
ratelimit:subscribe:{tenantId}    STRING  value = count  TTL = 3600s
```

**Tenant quota stored in PostgreSQL:** A `tenants.max_subscriptions` column caps how many subscriptions a tenant can create. Checked at subscription creation time inside a transaction:
```sql
SELECT COUNT(*) FROM subscriptions WHERE tenant_id = $tenantId
-- If count >= tenant.max_subscriptions, reject with 429
```

**Concepts practiced:** sd-mid-002 (rate limiting design), Redis atomic rate limiting (same pattern as other projects but now applied to a multi-tenant context where the key must be scoped by tenant), quota enforcement at the database layer vs application layer, 429 with `Retry-After`.

---

### 11. Health Checks

**Endpoints:**
- `GET /health` — liveness: returns 200 immediately
- `GET /health/ready` — readiness: checks PostgreSQL (`SELECT 1`), Kafka (admin client `listTopics`), Elasticsearch (`cluster.health`), and Redis (`PING`)

**Response shape:**
```json
{
  "status": "healthy",
  "checks": {
    "postgres": "healthy",
    "kafka": "healthy",
    "elasticsearch": "healthy",
    "redis": "healthy"
  }
}
```

A partial failure — Elasticsearch down while Kafka and PostgreSQL are healthy — should return 503 with the specific failing check identified. Delivery can continue without Elasticsearch (attempts are written to PostgreSQL and queued for indexing); this distinction is worth documenting but for simplicity, any dependency failure returns 503.

**Concepts practiced:** Liveness vs readiness (sd-junior), multi-dependency readiness checks, what partial degradation looks like (Elasticsearch down = search unavailable but delivery continues; Kafka down = publishing unavailable; PostgreSQL down = nothing works).

---

## PostgreSQL Schema

```sql
-- Tenants
CREATE TABLE tenants (
  id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  name             VARCHAR(200) NOT NULL,
  email            VARCHAR(320) NOT NULL UNIQUE,
  max_subscriptions INT          NOT NULL DEFAULT 100,
  created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- API Keys
CREATE TABLE api_keys (
  id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    UUID         NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  key_hash     VARCHAR(64)  NOT NULL UNIQUE,  -- SHA-256 of raw key; never store raw key
  key_prefix   VARCHAR(15)  NOT NULL,          -- display only: "sk_live_a1b2c3d"
  label        VARCHAR(100) NOT NULL DEFAULT 'Default',
  last_used_at TIMESTAMPTZ  NULL,
  revoked_at   TIMESTAMPTZ  NULL,
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_api_keys_hash     ON api_keys (key_hash) WHERE revoked_at IS NULL;
CREATE INDEX idx_api_keys_tenant   ON api_keys (tenant_id);

-- Topics
CREATE TABLE topics (
  id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   UUID         NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name        VARCHAR(200) NOT NULL,
  description TEXT         NULL,
  deleted_at  TIMESTAMPTZ  NULL,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, name)
);

CREATE INDEX idx_topics_tenant ON topics (tenant_id) WHERE deleted_at IS NULL;

-- Enable RLS
ALTER TABLE topics ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON topics
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

-- Subscriptions
CREATE TABLE subscriptions (
  id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id      UUID         NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  topic_id       UUID         NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
  endpoint_url   TEXT         NOT NULL,
  secret_hash    VARCHAR(64)  NOT NULL,  -- SHA-256 of signing secret; worker loads from secure config
  secret_prefix  VARCHAR(10)  NOT NULL,  -- display only
  enabled        BOOLEAN      NOT NULL DEFAULT TRUE,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_subscriptions_topic  ON subscriptions (topic_id) WHERE enabled = TRUE;
CREATE INDEX idx_subscriptions_tenant ON subscriptions (tenant_id);

ALTER TABLE subscriptions ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON subscriptions
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

-- Events (outbox table — source of truth for all published events)
CREATE TABLE events (
  id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id          UUID        NOT NULL REFERENCES tenants(id),
  topic_id           UUID        NOT NULL REFERENCES topics(id),
  event_type         VARCHAR(200) NOT NULL,
  payload            JSONB       NOT NULL,
  idempotency_key    VARCHAR(200) NULL,
  published_to_kafka BOOLEAN     NOT NULL DEFAULT FALSE,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_events_idempotency ON events (tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_events_outbox   ON events (created_at ASC) WHERE published_to_kafka = FALSE;
CREATE INDEX idx_events_topic    ON events (topic_id, created_at DESC);
CREATE INDEX idx_events_tenant   ON events (tenant_id, created_at DESC);

ALTER TABLE events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON events
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

-- Delivery Attempts
CREATE TABLE delivery_attempts (
  id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id        UUID         NOT NULL REFERENCES events(id),
  subscription_id UUID         NOT NULL REFERENCES subscriptions(id),
  tenant_id       UUID         NOT NULL REFERENCES tenants(id),
  attempt_number  SMALLINT     NOT NULL DEFAULT 1,
  status          VARCHAR(20)  NOT NULL DEFAULT 'pending', -- pending | success | failed | dead_lettered
  http_status     SMALLINT     NULL,
  response_body   TEXT         NULL,
  duration_ms     INT          NULL,
  next_retry_at   TIMESTAMPTZ  NULL,
  attempted_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_attempts_event        ON delivery_attempts (event_id);
CREATE INDEX idx_attempts_subscription ON delivery_attempts (subscription_id, attempted_at DESC);
CREATE INDEX idx_attempts_tenant       ON delivery_attempts (tenant_id, attempted_at DESC);
CREATE INDEX idx_attempts_retry        ON delivery_attempts (next_retry_at ASC)
  WHERE status = 'failed' AND next_retry_at IS NOT NULL;

ALTER TABLE delivery_attempts ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON delivery_attempts
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

-- Dead Letters
CREATE TABLE dead_letters (
  id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id        UUID         NOT NULL REFERENCES events(id),
  subscription_id UUID         NOT NULL REFERENCES subscriptions(id),
  tenant_id       UUID         NOT NULL REFERENCES tenants(id),
  total_attempts  SMALLINT     NOT NULL,
  last_error      TEXT         NULL,
  resolved_at     TIMESTAMPTZ  NULL,  -- set when a manual retry succeeds
  created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dead_letters_tenant ON dead_letters (tenant_id, created_at DESC)
  WHERE resolved_at IS NULL;

ALTER TABLE dead_letters ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON dead_letters
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);
```

**Schema design notes:**

`signing_secret` is never stored in the `subscriptions` table in plaintext. Only `secret_hash` (for verification) and `secret_prefix` (for display) are stored. The actual secret needed for HMAC computation during delivery is stored in a separate secrets store (environment variable per subscription, or a secrets manager entry keyed by `subscription_id`). For this project, store the plaintext secret encrypted in an environment-accessible secrets file — document this as the simplification and explain how a production system would use Vault or AWS Secrets Manager.

`idx_events_outbox` is a partial index on `published_to_kafka = FALSE`. Only unpublished events are in the index. As events are marked published, they leave the index. This keeps the outbox poll query fast regardless of how many historical events exist.

`idx_attempts_retry` is a partial index on failed attempts with a scheduled retry time. The outbox-style retry poller uses this to find due retries without scanning the full `delivery_attempts` table.

---

## Design Decisions

### Multi-Tenancy: RLS vs Application-Layer Filtering

Three approaches exist: separate databases per tenant, separate schemas per tenant, and shared schema with `tenant_id` plus RLS.

Separate databases and schemas scale poorly — provisioning a database or schema per tenant requires infrastructure automation and makes cross-tenant reporting impossible. Shared schema with application-layer filtering (manually adding `WHERE tenant_id = $id` to every query) works but any forgotten WHERE clause is a data breach.

RLS is the correct choice at this scale: one schema, one connection pool, isolation enforced by the database. The cost is that every query gets an implicit predicate appended by the policy — ensure indexed columns cover the `tenant_id` filter, or the implicit join will cause sequential scans.

### Kafka vs BullMQ for the Event Bus

BullMQ already appears in other projects. The key distinction this project makes concrete: BullMQ is a **job queue** — each job is consumed once and disappears. Kafka is a **log** — messages are retained, consumer groups track their own offsets, and any consumer can re-read from any point in the past.

This matters for replay. BullMQ cannot replay events from 3 days ago because consumed jobs are gone. Kafka can. Event retention is configured per topic in Kafka (default 7 days in this project). Replay reads from the PostgreSQL events table rather than resetting Kafka offsets, but Kafka's model is what makes replay conceptually possible — the event is durably stored, not discarded on consumption.

### Kafka for Fan-Out, BullMQ for Retry

The delivery worker uses Kafka for the initial fan-out (one event → all subscriptions) and BullMQ for per-subscription retry scheduling (one failed attempt → one retry job with a specific delay). Mixing them would mean republishing to Kafka on retry — which would re-fan-out to all subscriptions, not just the failed one. BullMQ's `delay` option is the right tool for time-delayed, per-target retries.

### HMAC Signing: Timestamp in the Signed Payload

Signing only the event payload would allow a captured webhook to be replayed against the subscriber endpoint indefinitely. Signing `timestamp.payload` means the signature is only valid for the specific timestamp included in the request. Subscribers validate: (1) the HMAC matches, (2) the timestamp is within a 5-minute window of `now()`. An attacker who captures a valid webhook cannot replay it after 5 minutes because the timestamp check will fail even though the HMAC is still valid.

### Signing Secrets: Per-Subscription, Not Per-Tenant

A single tenant-level signing secret means compromising one subscriber endpoint (the attacker reads the secret from the subscriber's config) compromises all subscriptions for that tenant. Per-subscription secrets contain the blast radius to one endpoint. Rotating a compromised subscription's secret requires one API call and does not affect any other subscription.

### Elasticsearch vs PostgreSQL JSONB for Delivery Logs

PostgreSQL with JSONB indexes could support simple delivery log queries. At scale, delivery logs grow at `events_per_day × subscriptions_per_event × retry_factor` — potentially millions of rows per day for an active tenant. Arbitrary query patterns (full-text search on payload, filter by HTTP status, date range across millions of rows) are the exact workload Elasticsearch was designed for. PostgreSQL JSONB queries at that scale would require aggressive partitioning, careful index design, and regular archival. Elasticsearch handles this as its default operating mode.

The trade-off: Elasticsearch adds operational complexity and the delivery log is eventually consistent (indexed after the attempt, not in the same transaction). Both are acceptable for an audit/search feature where the source of truth is PostgreSQL.

### Outbox Worker: `FOR UPDATE SKIP LOCKED`

Two outbox worker instances running simultaneously would otherwise process the same unpublished events twice, producing duplicate Kafka messages. `FOR UPDATE SKIP LOCKED` tells PostgreSQL to lock selected rows and skip any rows already locked by another transaction. Worker A and Worker B each get a distinct non-overlapping batch of events. This is the correct primitive for concurrent worker safety — it is cheaper and more correct than application-level distributed locks.

---

## Non-Functional Requirements

| Concern | Target |
|---|---|
| Auth | API key (SHA-256 hash lookup); no JWT; key shown once at creation |
| Multi-tenancy | PostgreSQL RLS on all tenant-scoped tables; `SET LOCAL app.current_tenant_id` per transaction |
| Logging | Pino → Seq; structured fields: `tenantId`, `eventId`, `subscriptionId`, `attemptId`, `correlationId` |
| API docs | Swagger/OpenAPI at `/swagger` in development; all endpoints, schemas, error codes |
| Health checks | `GET /health` (liveness), `GET /health/ready` (PostgreSQL + Kafka + Elasticsearch + Redis) |
| Delivery timeout | 10 seconds per attempt; `AbortController` with timeout signal |
| Retry schedule | 5 attempts: immediate, 30s, 5m, 30m, 2h; then dead letter |
| Event retention | Kafka topic retention: 7 days |
| Testing | Vitest; unit tests for HMAC signing, idempotency key logic, retry schedule; integration tests for RLS isolation (two tenants must not see each other's data), outbox worker, delivery attempt recording |
| Config | All secrets via environment variables; never committed |
| CI/CD | GitLab CI: lint → test → build → deploy |

---

## Build Order

| Phase | Feature | Concepts / Question Group |
|---|---|---|
| 1 | Prisma 7.x configuration (`prisma.config.ts` + `schema.prisma`) + PostgreSQL schema + RLS policies + seed data | db-junior-001 to 010 (indexing), db-mid-002 (isolation levels) |
| 2 | Fastify setup + API key auth middleware + tenant context injection | sd-mid-012 (auth system), cr-junior-001 to 010 |
| 3 | Tenant registration + API key management | cr-junior (hashing secrets, never storing plaintext) |
| 4 | Topic management | db-junior (compound unique constraints, cascade delete) |
| 5 | Subscription management + per-subscription signing secret | cr-junior (per-resource secrets, HTTPS enforcement) |
| 6 | Event publishing + outbox pattern + idempotency | sd-mid-013, sd-junior-020, ge-mid-033 (outbox vs CDC) |
| 7 | Kafka setup (KRaft) + outbox worker (`FOR UPDATE SKIP LOCKED`) | ge-mid-044, ge-mid-045, db-mid-011 |
| 8 | Webhook delivery worker (Kafka consumer + HMAC + HTTP POST) | sd-junior-033, ge-mid-046, ge-mid-048 |
| 9 | Retry scheduling with BullMQ + dead letter creation | sd-mid-016, ge-mid-047 |
| 10 | Elasticsearch setup + delivery log indexing (async via BullMQ) | db-mid-012, sd-mid-014 |
| 11 | Delivery log search API | db-mid-012 (keyword vs text, filter vs must) |
| 12 | Event replay | ge-mid-047 (retention and replay), sd-mid-014 |
| 13 | Dead letter management | sd-mid-016, ge-mid-047 |
| 14 | Rate limiting + tenant quota | sd-mid-002, sd-mid-023 (multi-tenant cache keys) |
| 15 | Health checks (Kafka + Elasticsearch + PostgreSQL + Redis) | sd-junior (liveness vs readiness) |
| 16 | Docker Compose (Kafka KRaft, Elasticsearch, PostgreSQL, Redis, Seq, Nginx) | cloud-junior-001 to 036 |
| 17 | GitLab CI/CD | git-junior-001 to 020 |
| 18 | Git hygiene: branch per feature, MR self-review | git-junior |

---

## Step-by-Step Guide

---

### Phase 1 — Prisma Configuration, PostgreSQL Schema, RLS, and Seed Data

**What to do:**
1. Configure Prisma before writing any models. In `schema.prisma`, set the generator to `prisma-client` with the output path — do **not** put the database URL in the `datasource` block:
   ```prisma
   generator client {
     provider = "prisma-client"
     output   = "../generated/prisma"
   }

   datasource db {
     provider = "postgresql"
   }
   ```
   The `url` property belongs in `prisma.config.ts`, not in `schema.prisma`. This is the Prisma 7.x pattern: `prisma.config.ts` is the single place connection strings are declared, keeping the schema file free of environment-specific values:
   ```typescript
   // prisma.config.ts
   import "dotenv/config";
   import { defineConfig, env } from "prisma/config";

   export default defineConfig({
     schema: "prisma/schema.prisma",
     migrations: { path: "prisma/migrations" },
     datasource: { url: env("DATABASE_URL") },
   });
   ```
   The `prisma-client` provider is Prisma 7's new TypeScript-first generator. Unlike the old `prisma-client-js` provider (which output pre-compiled `.js` + `.d.ts` declaration files usable in JavaScript projects), `prisma-client` outputs raw `.ts` source files and expects your build toolchain to handle compilation. The generated client lands in `generated/prisma/` as `.ts` files. The project must therefore use TypeScript — install `tsx` and `typescript` as dev dependencies and write all application code in `.ts` files. Run scripts with `tsx` rather than `node`.

   Prisma 7.x with the `prisma-client` generator also requires a driver adapter — install `@prisma/adapter-pg` and `pg`, then instantiate as:
   ```typescript
   import { PrismaPg } from '@prisma/adapter-pg';
   import { PrismaClient } from '../generated/prisma/client';

   const adapter = new PrismaPg({ connectionString: process.env.DATABASE_URL });
   const prisma = new PrismaClient({ adapter });
   ```
   Import from `../generated/prisma/client` (no extension) — not from `@prisma/client`.

2. Read db-junior-001 to 010 and db-mid-002 before writing any models. Isolation levels matter here: every request sets a session-local variable that RLS policies depend on. Understand what `READ COMMITTED` guarantees about when this variable is visible within a transaction.
3. Create the schema in migration order: `tenants` → `api_keys` → `topics` → `subscriptions` → `events` → `delivery_attempts` → `dead_letters`. Foreign keys enforce this order — you cannot create `topics` before `tenants` exists.
4. Enable RLS on every tenant-scoped table and create the `tenant_isolation` policy. Test immediately in `psql`:
   ```sql
   SET app.current_tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
   SELECT * FROM topics; -- should return only this tenant's topics
   SET app.current_tenant_id = 'bbbbbbbb-0000-0000-0000-000000000000';
   SELECT * FROM topics; -- should return only the other tenant's topics
   ```
   Verify RLS is working before writing a single line of application code. If RLS is misconfigured, no amount of application-layer testing will catch cross-tenant data leaks.
5. After running `prisma generate`, write the seed script at `src/seed/seed.ts`. The file must be TypeScript (`.ts`) and run via `tsx`. Add a `seed` entry to `package.json` scripts:
   ```json
   "seed": "tsx src/seed/seed.ts"
   ```
   Instantiate the client with the `PrismaPg` adapter at the top of the file — every script and application file that uses Prisma needs this pattern:
   ```typescript
   import 'dotenv/config';
   import { PrismaPg } from '@prisma/adapter-pg';
   import { PrismaClient } from '../../generated/prisma/client';

   const adapter = new PrismaPg({ connectionString: process.env.DATABASE_URL });
   const prisma = new PrismaClient({ adapter });
   ```
   The seed clears all tables in reverse foreign-key order (dead_letters → delivery_attempts → events → subscriptions → topics → api_keys → tenants), then inserts two tenants, one API key per tenant, three topics per tenant, two subscriptions per topic, and fifty events per tenant. Signing secrets are stored as SHA-256 hashes only — the raw secret is never persisted. Run with `npm run seed`; the raw API keys are printed to stdout for use in manual testing.

6. Run `EXPLAIN ANALYZE` on the outbox query:
   ```sql
   SET app.current_tenant_id = 'aaaaaaaa-0000-0000-0000-000000000000';
   EXPLAIN ANALYZE
   SELECT * FROM events WHERE published_to_kafka = FALSE ORDER BY created_at ASC LIMIT 100
   FOR UPDATE SKIP LOCKED;
   ```
   Confirm `idx_events_outbox` (partial index on `published_to_kafka = FALSE`) is used.

**Why:**
Testing RLS in `psql` before the application exists is non-negotiable. RLS misconfiguration is invisible in application tests unless those tests explicitly query as a second tenant. The only way to verify isolation is to set the session variable to tenant A, query, set it to tenant B, query again, and confirm the results are different. Building this habit in Phase 1 means every subsequent phase is built on a verified isolation foundation.

---

### Phase 2 — Fastify Setup and API Key Auth Middleware

**What to do:**
1. Build the API key auth middleware as a Fastify `onRequest` hook. Extract the key from `Authorization: Bearer <key>`, compute `SHA-256(key)`, query `api_keys` by hash. On success, attach `request.tenantId` to the request object.
2. After resolving `tenantId`, inject the tenant context into every database transaction:
   ```typescript
   await prisma.$executeRaw`SET LOCAL app.current_tenant_id = ${request.tenantId}`;
   ```
   This must happen inside the same transaction as the business logic — `SET LOCAL` is transaction-scoped and resets when the transaction ends.
3. Write a test that makes two requests with different API keys and verifies that each request's database queries only return that tenant's data — no application-level filter added, relying entirely on RLS.
4. Update `api_keys.last_used_at` via a fire-and-forget `setImmediate` — do not await it in the request path. A key lookup is on every request's critical path; the `last_used_at` write is not.

**Why:**
The RLS + `SET LOCAL` pattern is the architectural heart of this project. Step 3 — the isolation test without any application-level WHERE clause — proves that RLS works end-to-end through the application stack. Any future developer who accidentally omits a `tenant_id` filter from a query is still protected. This is the concrete answer to "how do you prevent one customer's data from being visible to another?" in a system design interview.

---

### Phase 3 — Tenant Registration and API Key Management

**What to do:**
1. `POST /api/v1/tenants` is the only unauthenticated endpoint. Generate the raw key, hash it, store the hash and prefix, return the raw key in the response body once. Write a test that calls this endpoint twice and verifies the raw key is different each time and is never returned in a subsequent `GET /api/v1/api-keys` response.
2. `POST /api/v1/api-keys` creates additional keys for an authenticated tenant. Same generation and storage pattern.
3. `DELETE /api/v1/api-keys/:id` sets `revoked_at = NOW()`. The partial index on `api_keys (key_hash) WHERE revoked_at IS NULL` means revoked keys are immediately excluded from the lookup index — no application code needs to check `revoked_at` in the middleware; the index enforces it by not returning the row.
4. Test revocation: create a key, make a successful request, revoke the key, make another request — confirm 401.

**Why:**
The partial index on `revoked_at IS NULL` in step 3 is a detail worth explaining in an interview. Instead of the middleware checking `IF result.revoked_at IS NOT NULL THEN 401`, the index simply doesn't include revoked keys — the lookup returns no rows, and the middleware's "no result = 401" path handles it. The index is both a performance optimization and a correctness enforcement mechanism.

---

### Phase 4 — Topic and Subscription Management

**What to do:**
1. Build topic CRUD. Test that creating a topic with the same name under two different tenants succeeds (the compound unique constraint is `(tenant_id, name)`, not just `name`).
2. Build subscription CRUD. Validate `endpoint_url` is HTTPS — reject HTTP with `422 ENDPOINT_MUST_BE_HTTPS`. This check must happen before anything is stored; a subscription with an HTTP endpoint would expose HMAC secrets in transit.
3. `POST /api/v1/subscriptions/:id/rotate-secret`: generate a new signing secret, update `secret_hash` and `secret_prefix`, return the new raw secret once. Do not invalidate in-flight deliveries — they were signed with the old secret and the subscriber will validate against the old secret. Document a grace period: the subscriber should update their secret verification within 5 minutes of rotation.
4. Test topic deletion cascade: delete a topic, confirm its subscriptions are also deleted (PostgreSQL `ON DELETE CASCADE`), and confirm events retain their `topic_id` as a historical reference (events reference topics but are not cascaded — topics use `deleted_at`, not hard delete).

**Why:**
The cascade behavior in step 4 requires separate schema decisions for topics and their children. Subscriptions are deleted with their topic because they have no meaning without the topic. Events are retained because they are the historical record — an event that was published to a now-deleted topic still happened and should be auditable.

---

### Phase 5 — Event Publishing and Outbox Pattern

**What to do:**
1. Build `POST /api/v1/topics/:topicId/events`. Validate the topic exists and is not deleted (RLS handles tenant scope, the handler checks `deleted_at IS NULL`). Check idempotency before inserting. Insert with `published_to_kafka = false`. Return 202 with the event ID.
2. Build the outbox worker as a separate Node.js process. Poll using `SELECT ... FOR UPDATE SKIP LOCKED`. After publishing each event to Kafka, update `published_to_kafka = true` in the same transaction. If the Kafka publish fails, the transaction rolls back and the event remains unpublished — it will be retried on the next poll cycle.
3. Test the atomicity guarantee: mock `kafkaProducer.send` to throw an error on the first call. Confirm the event row remains `published_to_kafka = false` and is retried on the next poll. Confirm it is not published twice when the mock succeeds on the second attempt.
4. Run both the API and the outbox worker via `docker compose up`. Publish an event through the API and watch the outbox worker pick it up within the poll interval (default 1 second).

**Why:**
Step 3's test is the proof of correctness for the outbox pattern. The guarantee is "the event reaches Kafka if and only if the database transaction committed." Without this test, you do not know whether a Kafka failure leaves the event in a consistent state. The test also makes the `FOR UPDATE SKIP LOCKED` behavior concrete: with two outbox worker instances running, each batch of events is processed exactly once — not because of application-level coordination but because of the database lock.

---

### Phase 6 — Kafka Setup (KRaft Mode)

**What to do:**
1. Add Kafka to `docker-compose.yml` using KRaft mode (no ZooKeeper). Use `bitnami/kafka:3.7` which supports KRaft. Configure a single broker for local development with the `KAFKA_CFG_NODE_ID`, `KAFKA_CFG_PROCESS_ROLES=broker,controller`, and `KAFKA_CFG_KRAFT_CLUSTER_ID` environment variables.
2. Create the `platform.events` topic with 6 partitions and a 7-day retention period:
   ```bash
   kafka-topics.sh --create --topic platform.events --partitions 6 --replication-factor 1 \
     --config retention.ms=604800000
   ```
3. Create the producer in the outbox worker using `kafkajs`. Set the partition key to `tenantId`. Confirm in the Kafka logs that events from two different tenants land on different partitions (or the same partition, depending on the hash — the point is that `tenantId` consistently determines the partition for ordering guarantees).
4. Run `kafka-consumer-groups.sh --describe --group webhook-delivery-worker` while the delivery worker is running. Observe consumer lag per partition. This is the operational view of back pressure — if lag grows, the consumer is falling behind.

**Why:**
KRaft mode removes ZooKeeper from the Docker Compose file — one fewer service to manage locally. The consumer lag observation in step 4 makes sd-mid-030 (back pressure) and ge-mid-047 (operations: retention, replay, backpressure) concrete. Consumer lag is the metric that tells you when Kafka consumers are falling behind producers — the operational equivalent of a queue depth gauge.

---

### Phase 7 — Webhook Delivery Worker

**What to do:**
1. Build the Kafka consumer. For each message, load active subscriptions from PostgreSQL for the event's `(tenant_id, topic_id)`. Use `idx_subscriptions_topic` — confirm with EXPLAIN ANALYZE.
2. For each subscription, compute the HMAC:
   ```typescript
   const timestamp = Math.floor(Date.now() / 1000).toString();
   const signedPayload = `${timestamp}.${JSON.stringify(event.payload)}`;
   const signature = crypto.createHmac('sha256', subscriptionSecret)
     .update(signedPayload)
     .digest('hex');
   ```
   POST with headers `X-Webhook-Timestamp` and `X-Webhook-Signature: sha256=<hex>`.
3. Wrap the HTTP call in an `AbortController` with a 10-second timeout. Any endpoint that does not respond within 10 seconds receives a `failed` attempt and is scheduled for retry — the delivery worker must not block on slow endpoints.
4. Commit the Kafka offset only after all subscriptions for an event have been processed (delivered or queued for retry). If the worker crashes after processing 3 of 5 subscriptions, the offset is not committed, the event is re-consumed, and subscriptions 1-3 receive a duplicate delivery (which they handle via `X-Webhook-Event-Id`). This is at-least-once delivery — document it explicitly.
5. Test with a local HTTP server that returns 500. Confirm the delivery attempt is recorded as `failed`, a BullMQ retry job is scheduled, and the Kafka offset is committed (the event was processed; the retry is separate from re-consuming the Kafka message).

**Why:**
Step 4 makes at-least-once delivery concrete. The delivery worker does not guarantee exactly-once — it guarantees that every event reaches every subscriber *at least* once, with possible duplicates on worker restart. The `X-Webhook-Event-Id` header gives subscribers the information they need to deduplicate on their side. This is the honest answer to "what delivery guarantees does your platform provide?" in an interview — not "exactly-once" (which Kafka can support but requires significant additional complexity), but "at-least-once with idempotency keys for subscriber deduplication."

---

### Phase 8 — Retry Logic and Dead Letters

**What to do:**
1. Build the BullMQ retry queue. When a delivery attempt fails, compute the next retry time based on `attempt_number` and enqueue a BullMQ job with `delay: msUntilNextRetry`. The job payload includes `eventId`, `subscriptionId`, and `attemptNumber`.
2. The retry worker (consuming the BullMQ queue) re-runs the HMAC computation and HTTP delivery for the specific `(event, subscription)` pair. It does not go through Kafka — the retry targets one subscription, not all subscribers.
3. After attempt 5 fails, create a `dead_letters` row and mark the delivery attempt `dead_lettered`. Do not retry further without manual intervention.
4. Test the full retry cycle: configure a test endpoint that fails 4 times and succeeds on attempt 5. Confirm delivery attempt records 1-4 have `status = 'failed'` with `next_retry_at` set, and attempt 5 has `status = 'success'`.
5. Test dead lettering: configure an endpoint that always returns 500. After 5 attempts, confirm a `dead_letters` row exists and no further BullMQ jobs are enqueued.

**Why:**
The separation between Kafka (initial fan-out) and BullMQ (per-subscription retry) is the key architectural insight of this phase. A retry is not a re-publication — it targets one specific subscription that failed, not all subscriptions for the event. If retries went back through Kafka, they would re-fan-out to all subscriptions, delivering duplicates to the ones that already succeeded. BullMQ's per-job targeting is exactly right for this use case.

---

### Phase 9 — Elasticsearch Setup and Delivery Log Indexing

**What to do:**
1. Add Elasticsearch 8 to `docker-compose.yml` in single-node mode. Disable security for local development (`xpack.security.enabled=false`).
2. Create the `delivery-logs` index with the mapping defined in the Features section. Use the Elasticsearch JavaScript client (`@elastic/elasticsearch`).
3. After each delivery attempt completes (success, failure, or dead letter), enqueue a BullMQ indexing job with the attempt data. The indexing job calls `esClient.index({ index: 'delivery-logs', document: attemptData })`. Do not call Elasticsearch synchronously in the delivery worker — if Elasticsearch is slow or unavailable, it must not delay or block webhook deliveries.
4. Build `GET /api/v1/delivery-logs`. Every query must include `{ term: { tenant_id: tenantId } }` in the `filter` context. This is the Elasticsearch equivalent of RLS — the application enforces it, not the database. Test that a tenant cannot retrieve another tenant's delivery logs by crafting a query with a different `tenant_id` in the search params.
5. Run `GET /_cat/indices/delivery-logs?v` and observe the document count growing as events are delivered.

**Why:**
The async indexing via BullMQ in step 3 is the correct architectural choice and mirrors the same pattern used in the photo tagging feature of Diaspora Connect. Elasticsearch availability must not be on the critical path of webhook delivery — delivery is the core product; search is a value-added feature. A BullMQ job that fails to index simply retries later; the delivery itself was already recorded in PostgreSQL. This is sd-mid-014 (eventual consistency) applied to a search index: the index is eventually consistent with PostgreSQL, and that is acceptable for a search feature.

---

### Phase 10 — Event Replay and Dead Letter Management

**What to do:**
1. Build `POST /api/v1/subscriptions/:id/replay`. Query `events` in PostgreSQL for `topic_id = subscription.topicId AND created_at >= fromDate AND tenant_id = $tenantId` (RLS handles the tenant filter). For each event, enqueue a BullMQ delivery job targeting this specific subscription. Return the enqueued count.
2. Test replay: publish 10 events to a topic, then disable the subscription, then re-enable it, then replay from 1 hour ago. Confirm all 10 events are delivered.
3. Build `POST /api/v1/dead-letters/:id/retry`. Enqueue a single BullMQ delivery job. Do not delete the dead letter record until the retry succeeds — on success, set `resolved_at = NOW()`.
4. Test that replay and manual retry both produce `X-Webhook-Event-Id` headers matching the original event ID — subscribers can use this to detect replays and avoid double-processing.

**Why:**
Replay from PostgreSQL rather than Kafka offset reset is the right design for a multi-tenant platform. Resetting a consumer group's offset is a global operation on a Kafka partition — it would replay events for all tenants sharing that partition, not just the requesting tenant. PostgreSQL is the authoritative event store; Kafka is the delivery transport. Reading from PostgreSQL for replay maintains tenant isolation and does not disturb other tenants' delivery consumers.

---

### Phase 11 — Rate Limiting, Health Checks, and Deployment

**What to do:**
1. Apply per-tenant rate limiting to the event publishing endpoint. The Redis key is `ratelimit:publish:{tenantId}` — tenant-scoped, not IP-scoped, because tenants authenticate with API keys and a single tenant may publish from many servers.
2. Build the health check endpoints. The Kafka readiness check should use the admin client to list topics — a successful response confirms the broker is reachable and the `platform.events` topic exists.
3. Write `docker-compose.yml` with: `api` (Fastify), `outbox-worker`, `delivery-worker`, `retry-worker`, `postgres`, `kafka` (KRaft), `elasticsearch`, `redis`, `seq`, `nginx`. The workers are separate services using the same Docker image with different `command` entries.
4. Confirm the full stack starts with `docker compose up` and a published event flows end-to-end: API → PostgreSQL outbox → Kafka → delivery worker → subscriber endpoint → Elasticsearch index.

---

## Self-Review Checklist (per MR)

- [ ] Is RLS enabled on every tenant-scoped table? Does `SET LOCAL app.current_tenant_id` happen inside the database transaction — not before it?
- [ ] Does every Elasticsearch query include `{ term: { tenant_id: tenantId } }` in the `filter` context?
- [ ] Is the raw API key or signing secret stored anywhere in the database? (It must not be — only hashes.)
- [ ] Is `SET LOCAL` used (transaction-scoped) — not `SET` (session-scoped)? Session-scoped settings persist across connection pool reuse.
- [ ] Does the outbox worker use `FOR UPDATE SKIP LOCKED` — not a plain `SELECT`?
- [ ] Is the HMAC computed over `timestamp.payload` — not `payload` alone? Is the timestamp included in the delivery headers?
- [ ] Does the delivery worker commit the Kafka offset only after all subscriptions are processed — not per-subscription?
- [ ] Is Elasticsearch indexing done asynchronously via BullMQ — not synchronously in the delivery worker?
- [ ] Are retries handled by BullMQ targeting the specific subscription — not by republishing to Kafka?
- [ ] Does the dead letter record persist until `resolved_at` is set by a successful manual retry — not deleted on retry attempt?
- [ ] Does replay use the PostgreSQL `events` table — not Kafka consumer offset reset?
- [ ] Is the event publishing endpoint rate-limited by `tenantId` — not by IP?
- [ ] Are all `prisma.$queryRaw` calls using tagged template literals — not string concatenation?
- [ ] Does the health check verify Kafka topic existence — not just broker reachability?
- [ ] Are Pino log calls using message templates — not string interpolation?

---

## Success Criteria

The project is complete when:

1. RLS isolation is verified in `psql`: querying any tenant-scoped table with one tenant's `app.current_tenant_id` returns zero rows belonging to a different tenant
2. The full event flow works end-to-end: `POST /events` → PostgreSQL outbox → Kafka → delivery worker → subscriber endpoint → Elasticsearch — verified with `docker compose up` and a live subscriber server
3. The HMAC signature is verified correctly: a subscriber that validates `X-Webhook-Signature` using the signing secret accepts legitimate deliveries and rejects tampered payloads
4. Replay delivers all events published after a given timestamp to the specified subscription, with original event IDs in the headers
5. The retry schedule is verified: a subscriber endpoint returning 500 produces 5 delivery attempts with the correct delays before dead-lettering
6. Dead letter manual retry succeeds, sets `resolved_at`, and the event reaches the subscriber
7. The RLS + `SET LOCAL` pattern is proven safe under connection pool reuse: a request from tenant A followed immediately by a request from tenant B (same connection) cannot see tenant A's data
8. Elasticsearch delivery log search returns only the authenticated tenant's records — a query for another tenant's `event_id` returns zero results
9. Consumer lag is observable via `kafka-consumer-groups.sh` when the delivery worker is paused
10. The full stack starts with `docker compose up` with no manual setup steps beyond `docker compose up`
11. The GitLab CI/CD pipeline runs lint → test → build → deploy on every merge to `main`
12. You can explain: why RLS is safer than application-layer tenant filtering; why Kafka retains events while BullMQ discards consumed jobs; why the HMAC includes a timestamp; why the Kafka offset is committed per-event not per-subscription; and why Elasticsearch indexing is async
