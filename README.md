# Event Delivery Platform API

A multi-tenant SaaS platform for reliable webhook event delivery. Tenants publish events to named topics; the platform fans them out to all registered subscriber endpoints with HMAC-signed payloads, exponential backoff retries, dead-letter storage, and a searchable delivery history. Think of it as a self-hosted Svix or Hookdeck.

Built on one architectural constraint that drives every decision: **tenant data must be invisible to other tenants at the database layer, not just the application layer**. PostgreSQL Row-Level Security enforces isolation even if application code forgets a `WHERE tenant_id = ...` clause.

## Features

- **API Key Authentication** — tenants authenticate with `sk_live_*` keys; the raw key is shown once at creation and never stored; only its SHA-256 hash and a short display prefix are persisted
- **Multi-Tenancy via RLS** — PostgreSQL Row-Level Security on every tenant-scoped table; `SET LOCAL app.current_tenant_id` injects tenant context per transaction so no application bug can leak cross-tenant data
- **Topic Management** — tenants define named event channels (`order.created`, `payment.failed`); topic names are unique per tenant via a compound unique constraint; soft-deleted with `deleted_at` so historical events remain valid
- **Subscription Management** — each subscription binds a topic to an HTTPS endpoint URL with its own per-subscription signing secret; enabling/disabling preserves history; secret rotation invalidates the old secret immediately
- **Event Publishing with Outbox Pattern** — events are written to PostgreSQL with `published_to_kafka = false` inside the same transaction; a background outbox worker polls with `FOR UPDATE SKIP LOCKED` and publishes to Kafka atomically; returns `202 Accepted` immediately
- **Idempotent Event Publishing** — clients supply an `idempotencyKey`; a unique index on `(tenant_id, idempotency_key)` prevents duplicate inserts; the original event ID is returned on re-submission
- **Kafka Fan-Out** — the Kafka consumer reads from `platform.events` and fans out each event to all active subscriptions on the topic; partition key is `tenantId` to preserve per-tenant ordering
- **HMAC-Signed Payloads** — each delivery POST is signed over `timestamp.payload` using the subscription's secret; timestamp inclusion prevents replay attacks; subscribers validate within a 5-minute window
- **Exponential Backoff Retries** — failed deliveries retry 5 times (immediate → 30s → 5m → 30m → 2h) via BullMQ; Kafka handles initial fan-out, BullMQ handles per-subscription retry scheduling
- **Dead Letter Storage** — events exhausting all retries are written to `dead_letters`; tenants can inspect failure history and trigger a single manual retry
- **Event Replay** — tenants replay events to a subscription from any past timestamp; reads from the PostgreSQL `events` table (source of truth), not Kafka offset reset, so one tenant's replay cannot disturb another tenant's consumer position
- **Delivery Log Search** — every delivery attempt is indexed asynchronously to Elasticsearch via BullMQ; search by status, topic name, date range, or free text across event payloads; `filter` context applies tenant scope on every query
- **Per-Tenant Rate Limiting** — event publishing capped at 1,000 events/minute per tenant; subscription creation capped at 10/hour; Redis atomic counter pattern scoped by `tenantId`, not IP
- **Subscription Quota** — `tenants.max_subscriptions` column caps subscriptions per tenant; checked inside a transaction at creation time
- **10-Second Delivery Timeout** — each HTTP POST wrapped in `AbortController`; slow subscriber endpoints cannot block the delivery worker
- **At-Least-Once Delivery** — Kafka offset committed only after all subscriptions for an event are processed; `X-Webhook-Event-Id` header gives subscribers the information to deduplicate on their side
- **Structured Logging** — Pino → Seq; per-request correlation IDs; `tenantId`, `eventId`, `subscriptionId`, `attemptId` as structured fields
- **API Docs** — Swagger UI at `/swagger` in development
- **Health Checks** — liveness (`GET /health`) + readiness (`GET /health/ready`) checking PostgreSQL, Kafka, Elasticsearch, and Redis; `503` on any dependency failure

## Architecture

```
HTTP request  →  Fastify (routes → controllers → services)
                      │
          ┌───────────┴──────────────┐
          ▼                          ▼
    PostgreSQL (RLS)             Kafka (KRaft)
  (tenants, api_keys,          (platform.events topic)
   topics, subscriptions,              │
   events, delivery_attempts,          ▼
   dead_letters)              Delivery Worker
          │                   (Kafka consumer)
          │  outbox worker            │
          │  (FOR UPDATE              ├──► HTTP POST + HMAC → subscriber
          │   SKIP LOCKED)            │
          └────────────────►  BullMQ (retry queue + indexing queue)
                                      │
                    ┌─────────────────┼──────────────────┐
                    ▼                 ▼                   ▼
              Retry Worker     Indexing Worker       Redis
           (re-delivers to    (indexes attempt    (rate limits,
            one subscription)  to Elasticsearch)   last_used_at)
                                      │
                               Elasticsearch
                              (delivery-logs index)
```

The core event flow: a tenant `POST`s an event → PostgreSQL outbox row written → outbox worker publishes to Kafka → delivery worker fans out to all active subscriptions → each delivery attempt written to PostgreSQL → attempt indexed asynchronously to Elasticsearch. MongoDB is not used; this project is PostgreSQL + Kafka + Elasticsearch.

### Delivery Pipeline Detail

```
POST /topics/:id/events
      │
      ▼
Idempotency check (SELECT by idempotency_key)
      │ new event
      ▼
INSERT events (published_to_kafka = false)
→ 202 Accepted (client does not wait for Kafka)
      │
      ▼  [outbox worker — every 1s]
SELECT ... WHERE published_to_kafka = false FOR UPDATE SKIP LOCKED
→ kafkaProducer.send({ key: tenantId, value: event })
→ UPDATE events SET published_to_kafka = true
      │
      ▼  [delivery worker — Kafka consumer]
Load active subscriptions for (tenant_id, topic_id)
→ for each subscription:
    compute HMAC over (timestamp + payload)
    POST to endpoint_url with X-Webhook-* headers
    record delivery_attempt
    on 2xx: index to Elasticsearch
    on failure: enqueue BullMQ retry job
    after max retries: create dead_letter
→ commit Kafka offset (after all subscriptions processed)
```

### Retry Schedule

| Attempt | Delay before retry |
|---|---|
| 1 (initial) | immediate |
| 2 | 30 seconds |
| 3 | 5 minutes |
| 4 | 30 minutes |
| 5 | 2 hours |
| Dead letter | no further retries |

## Tech Stack

| Layer | Technology | Phase |
|---|---|---|
| SQL Database | PostgreSQL with Row-Level Security (Prisma) | 1 — installed |
| Server | Node.js, Fastify | 2 — installed |
| Auth | API key — SHA-256 hash lookup; no JWT | 2 — installed |
| Logging | Pino (pino-pretty in dev) | 2 — installed |
| Docs | Swagger UI (`@fastify/swagger`) | 2 — installed |
| Testing | Vitest integration tests | 3 — installed |
| Event Log | Apache Kafka (KRaft — no ZooKeeper) | 7 |
| Job Queue | BullMQ (retry scheduling + async Elasticsearch indexing) | 9 — installed |
| Cache / Rate Limits | Redis (ioredis) | 9 (BullMQ backend) / 14 (rate limits) |
| Search | Elasticsearch 8 (`@elastic/elasticsearch` v8) | 10 — installed |

## Project Structure

**Current files (Phases 1–10):**

```
prisma/
├── migrations/                       # Database migration history
└── schema.prisma                     # All 7 Prisma models with RLS-compatible field mapping
prisma.config.ts                      # Prisma 7.x datasource config — DATABASE_URL lives here, not in schema.prisma
docker-compose.yml                    # apache/kafka:3.9.0 (KRaft) + kafka-init + elasticsearch:8.13.0 + redis
src/
├── index.js                          # Server entry point — Fastify startup, PORT from env
├── app.js                            # Fastify plugin chain: swagger, error handler, routes
├── db/
│   ├── prisma.js                     # Prisma client singleton (PrismaPg adapter)
│   └── workerClient.js               # Separate Prisma client using DATABASE_URL_WORKER (BYPASSRLS role)
├── hooks/
│   └── authenticate.js               # API key auth: SHA-256 hash lookup → req.tenantId + req.withTenant
├── kafka/
│   ├── client.js                     # KafkaJS singleton — clientId + comma-split brokers array
│   └── producer.js                   # KafkaJS producer — allowAutoTopicCreation: false, tenantId partition key
├── delivery/
│   └── deliver.js                    # HTTP POST with AbortController 10s timeout + HMAC headers; captures response body
├── search/
│   ├── esClient.js                   # @elastic/elasticsearch v8 singleton — connection pool managed internally
│   ├── deliveryLogsIndex.js          # DELIVERY_LOGS_INDEX constant, field mapping, ensureDeliveryLogsIndex (idempotent)
│   └── buildDeliveryLogDocument.js   # Shared helper — camelCase inputs → snake_case ES fields; payload serialised to JSON string
├── routes/
│   ├── tenants.js                    # POST /api/v1/tenants (unauthenticated signup)
│   ├── apiKeys.js                    # POST/GET/DELETE /api/v1/api-keys (authenticated)
│   ├── topics.js                     # POST/GET/DELETE /api/v1/topics (authenticated)
│   ├── topicSubscriptions.js         # POST/GET /api/v1/topics/:topicId/subscriptions
│   ├── subscriptions.js              # GET/PUT/DELETE /api/v1/subscriptions/:id + rotate-secret
│   └── events.js                     # POST /api/v1/topics/:topicId/events (202 Accepted)
├── controllers/
│   ├── tenantController.js
│   ├── apiController.js
│   ├── topicController.js
│   ├── subscriptionController.js
│   └── eventController.js
├── queues/
│   ├── retryQueue.js                 # BullMQ Queue('webhook-retry') + redisConnection; attempts:1 (retry logic is explicit, not BullMQ-managed)
│   └── indexQueue.js                 # BullMQ Queue('delivery-log-index'); attempts:5, exponential backoff — idempotent indexing
├── services/
│   ├── tenantService.js              # Tenant creation + initial API key in one transaction
│   ├── apiKeyService.js              # Additional key creation, list, revocation
│   ├── topicService.js               # Topic CRUD; soft-delete via deleted_at; compound unique (tenant_id, name)
│   ├── subscriptionService.js        # Subscription CRUD; quota check; per-subscription secret management
│   ├── eventService.js               # Publish: idempotency check → INSERT with published_to_kafka = false
│   ├── deliveryAttemptService.js     # createPendingAttempt + resolveAttempt + deadLetterAttempt; uses withTenant for RLS
│   └── deadLetterService.js          # createDeadLetter; writes dead_letters row after attempt 5 exhausted
├── workers/
│   ├── outboxWorker.js               # FOR UPDATE SKIP LOCKED poller; exports pollOutbox for testing
│   ├── delivery.worker.js            # Kafka consumer; autoCommit: false; enqueues indexing job after each delivery; exports processEvent for testing
│   ├── retryWorker.js                # BullMQ Worker('webhook-retry'); reloads event+subscription from DB; enqueues indexing job after each resolution; exports processRetryJob for testing
│   └── indexingWorker.js             # BullMQ Worker('delivery-log-index'); uses attemptId as ES _id (idempotent upsert); concurrency:20; exports processIndexJob for testing
├── utils/
│   ├── ApiResponse.js                # Standard { success, statusCode, data, error } envelope
│   ├── generateApiKey.js             # sk_live_* raw key generation + SHA-256 hash
│   ├── generateSigningSecret.js      # whsec_* signing secret generation + SHA-256 hash (prefix capped at 10 chars)
│   ├── computeHmac.js                # HMAC-SHA256 over timestamp.payload — timestamp prevents replay attacks
│   ├── retrySchedule.js              # computeRetryDelay (30s→5m→30m→2h by attempt), isLastAttempt, MAX_ATTEMPTS=5
│   ├── paginate.js                   # parsePagination + paginatedResponse helpers
│   ├── withTenant.js                 # SET LOCAL app.current_tenant_id per-transaction
│   └── logger.js                     # Pino options (pino-pretty in dev, JSON in prod)
├── errors/
│   └── AppError.js                   # AppError + NotFoundError, ValidationError, ConflictError,
│                                     #   UnauthorizedError, ForbiddenError, TooManyRequestsError
├── test/
│   ├── tenants.test.js               # Integration tests: POST /tenants, GET/POST/DELETE /api-keys
│   ├── rls-isolation.test.js         # RLS isolation: tenant A queries return zero rows for tenant B
│   ├── topics.test.js                # Topic CRUD, compound unique constraint, RLS isolation, soft delete
│   ├── subscription.test.js          # Subscription CRUD, HTTPS enforcement, quota, secret rotation, RLS
│   ├── event.test.js                 # Event publishing, idempotency, validation, topic checks, RLS, outbox atomicity
│   ├── hmac.test.js                  # HMAC unit tests: determinism, reference verification, replay protection
│   ├── delivery.test.js              # Delivery integration tests: HTTP delivery, processEvent fan-out, RLS isolation
│   ├── retry-schedule.test.js        # Retry schedule unit tests: delay values per attempt, isLastAttempt, MAX_ATTEMPTS
│   ├── retry-worker.test.js          # Retry worker integration tests: success path, intermediate failure, dead letter, dropped job
│   └── indexing.test.js              # Indexing tests: buildDeliveryLogDocument shape, processIndexJob ES call, integration fan-out
└── seed/
    └── seed.ts                       # Dev seed: two tenants, topics, subscriptions, events
generated/
└── prisma/                           # Generated Prisma client (output of `npm run db:generate`)
```

**Planned layout (all phases):**

```
src/
├── index.js                          # Server startup, DB connections, worker bootstrap
├── app.js                            # Fastify plugin chain + route mounting
├── swagger.js                        # OpenAPI spec setup
├── db/
│   ├── prisma.js                     # Prisma client (singleton)
│   ├── kafka.js                      # KafkaJS producer + admin client
│   ├── elasticsearch.js              # Elasticsearch JS client
│   └── redis.js                      # Shared Redis client (ioredis)
├── middleware/
│   ├── authenticate.js               # API key auth: hash lookup → attach tenantId to request
│   ├── tenantContext.js              # SET LOCAL app.current_tenant_id per transaction
│   ├── correlationId.js             # Per-request UUID injected into all log lines
│   └── errorHandler.js              # Global error hook → standard envelope
├── routes/
│   ├── tenants.js                    # Tenant registration (unauthenticated)
│   ├── apiKeys.js                    # API key CRUD
│   ├── topics.js                     # Topic CRUD
│   ├── subscriptions.js              # Subscription CRUD + rotate-secret + replay
│   ├── events.js                     # Event publishing
│   ├── deliveryLogs.js               # Delivery log search (Elasticsearch)
│   ├── deadLetters.js                # Dead letter list + manual retry
│   └── health.js                     # Liveness + readiness
├── controllers/
│   ├── tenantController.js
│   ├── apiKeyController.js
│   ├── topicController.js
│   ├── subscriptionController.js
│   ├── eventController.js
│   ├── deliveryLogController.js
│   └── deadLetterController.js
├── services/
│   ├── tenantService.js              # Tenant creation + API key generation (raw key shown once)
│   ├── apiKeyService.js              # Additional key creation + revocation
│   ├── topicService.js               # Topic CRUD; soft-delete via deleted_at
│   ├── subscriptionService.js        # Subscription CRUD + per-subscription secret management
│   ├── eventService.js               # Publish: idempotency check → INSERT with published_to_kafka=false
│   ├── deliveryService.js            # HMAC computation + HTTP POST + attempt recording
│   ├── replayService.js              # Bulk BullMQ job enqueue from PostgreSQL events table
│   ├── deadLetterService.js          # Dead letter inspection + single retry enqueue
│   ├── outboxWorker.js               # FOR UPDATE SKIP LOCKED poller → Kafka publish
│   ├── deliveryWorker.js             # Kafka consumer → fan-out → BullMQ retry jobs
│   ├── retryWorker.js                # BullMQ consumer → single-subscription re-delivery
│   └── indexingWorker.js             # BullMQ consumer → Elasticsearch index
├── utils/
│   ├── ApiResponse.js                # Standard { success, statusCode, data, error } envelope
│   ├── apiKey.js                     # Raw key generation + SHA-256 hashing
│   ├── hmac.js                       # HMAC-SHA256 over timestamp.payload
│   ├── retrySchedule.js              # Exponential backoff delay lookup by attempt number
│   ├── rateLimit.js                  # Redis INCR/EXPIRE per-tenant rate limit checks
│   └── logger.js                     # Pino instance
├── errors/
│   └── AppError.js                   # Custom error classes (Validation, NotFound, Forbidden, …)
└── seed/
    └── seed.ts                       # Dev seed: two tenants, topics, subscriptions, events
prisma/
└── schema.prisma                     # All models + RLS-compatible field mapping
prisma.config.ts                      # Prisma 7.x config — datasource.url reads DATABASE_URL here (not in schema.prisma)
```

## Architecture Decisions

### Multi-Tenancy: RLS Instead of Application-Layer Filtering

Three approaches exist: separate databases per tenant, separate schemas per tenant, and shared schema with `tenant_id` plus application filtering. Separate databases and schemas cannot share a connection pool and make cross-tenant analytics impossible. Application-layer filtering works but any missing `WHERE tenant_id = $id` becomes a data breach — and there is no way to enforce that every developer adds it to every query.

RLS is the correct choice: one schema, one connection pool, isolation enforced by the database. The tenant context is injected before each transaction:

```typescript
await prisma.$executeRaw`SET LOCAL app.current_tenant_id = ${tenantId}`;
```

`SET LOCAL` scopes the variable to the current transaction and resets when the transaction ends — a critical detail for connection pool safety. A connection returned to the pool carries no tenant context into the next request.

### API Key Authentication vs JWT

This is a developer platform — tenants are companies integrating the API, not end users logging in through a browser. API keys fit better than JWTs here: keys do not expire by default, are managed by the tenant, and can be revoked immediately via the `revoked_at` column. The raw key is shown once at creation; only its SHA-256 hash is stored. A database breach does not expose live keys.

The partial index `ON api_keys (key_hash) WHERE revoked_at IS NULL` means revoked keys fall out of the lookup index immediately — the middleware's "no result = 401" path handles revocation without any explicit check.

### Kafka as an Event Log, Not a Job Queue

BullMQ (used for retries) is a job queue — consumed jobs disappear. Kafka is a log — messages are retained (7 days, configurable), consumer groups track their own offsets, and any consumer can re-read from any point in the past.

This distinction matters for replay. You cannot replay events from 3 days ago using BullMQ because consumed jobs are gone. Kafka retains them. The replay implementation reads from the PostgreSQL `events` table rather than resetting Kafka consumer offsets — resetting an offset is a partition-wide operation that would affect all tenants on that partition. PostgreSQL is the source of truth; Kafka is the delivery transport.

### Kafka for Fan-Out, BullMQ for Retry

The delivery worker uses Kafka for initial fan-out (one event → all subscriptions) and BullMQ for per-subscription retry scheduling. Mixing them would mean republishing to Kafka on retry, which re-fans-out to all subscriptions — delivering duplicates to the ones that already succeeded. BullMQ's `delay` option targets one specific `(event, subscription)` pair at a specific time. The right tool for each concern.

### HMAC: Timestamp in the Signed Payload

Signing only the event payload would allow a captured webhook to be replayed indefinitely — the signature would still be valid. Signing `timestamp.payload` ties the signature to a specific moment. Subscribers validate: (1) the HMAC matches, and (2) the timestamp is within 5 minutes of `now()`. An attacker who captures a valid webhook cannot replay it after 5 minutes because the timestamp check fails even though the HMAC is still cryptographically correct.

### Per-Subscription Signing Secrets

A single tenant-level signing secret means compromising one subscriber's environment (the attacker reads the secret from the subscriber's config) compromises every subscription for that tenant. Per-subscription secrets contain the blast radius to one endpoint. Rotating a compromised subscription requires one API call and does not touch any other subscription.

### Outbox Pattern: Why `FOR UPDATE SKIP LOCKED`

The outbox worker polls `events WHERE published_to_kafka = false`. Two worker instances running simultaneously would otherwise process the same event, producing duplicate Kafka messages. `FOR UPDATE SKIP LOCKED` tells PostgreSQL to lock selected rows and skip rows already locked by another worker. Worker A and Worker B each get a non-overlapping batch — correct and cheaper than any application-level distributed lock.

The `idx_events_outbox` partial index covers only `published_to_kafka = false` rows. Published events are not in the index. The index stays small and the poll query stays fast regardless of how many historical events exist.

### Elasticsearch for Delivery Logs

PostgreSQL JSONB could handle simple delivery log queries. At scale, delivery logs grow at `events_per_day × subscriptions_per_event × retry_factor` — potentially millions of rows per day per active tenant. Arbitrary query patterns (full-text search on payload, filter by HTTP status code, date range across arbitrary time windows) are exactly the workload Elasticsearch was built for.

The trade-off: Elasticsearch adds operational complexity and the delivery log is eventually consistent — indexed after the attempt, not in the same transaction. Both are acceptable for an audit/search feature where the source of truth is PostgreSQL.

### At-Least-Once Delivery Semantics

The Kafka offset is committed only after all subscriptions for an event are processed. If the delivery worker crashes mid-fan-out (after processing 3 of 5 subscriptions), the offset is not committed, the event is re-consumed, and subscriptions 1–3 receive a duplicate delivery. The `X-Webhook-Event-Id` header gives subscribers the information they need to deduplicate. This is the honest guarantee: every event reaches every subscriber at least once, not exactly once.

---

## Getting Started

### Prerequisites

**Phases 1–10 (current):**
- Node.js ≥ 20 (Prisma 7.x requires it; Node 18 causes a silent ESM crash in `@prisma/dev` that leaves the generated client stale)
- PostgreSQL 14+
- Docker + Docker Compose (for Kafka, Redis, and Elasticsearch)
- Redis 7+ (BullMQ backend for retry queue and indexing queue)
- Apache Kafka 3.9+ (KRaft mode — no ZooKeeper required, via Docker)
- Elasticsearch 8+ (delivery log indexing)

### Install

```bash
npm install
```

### Configure

```bash
cp .env.example .env
```

Edit `.env`:

```env
DATABASE_URL=postgresql://admin:password@localhost:5432/event_delivery
# Outbox worker role — must have BYPASSRLS. Falls back to DATABASE_URL in development.
DATABASE_URL_WORKER=postgresql://outbox_worker:change_in_env@localhost:5432/event_delivery
KAFKA_BROKERS=localhost:9092
ELASTICSEARCH_URL=http://localhost:9200
REDIS_URL=redis://localhost:6379
NODE_ENV=development
LOG_LEVEL=info
PORT=3096
```

`DATABASE_URL` is read by `prisma.config.ts` via `env("DATABASE_URL")`. The `datasource db` block in `schema.prisma` intentionally has no `url` property — Prisma 7.x reads the connection string from `prisma.config.ts`, not from the schema file.

### Migrate and Generate Prisma Client

```bash
npm run db:migrate   # runs prisma migrate dev && prisma generate
```

`prisma migrate dev` auto-invokes `prisma generate` at the end, but that internal call can fail silently on Node 18 (see Prerequisites). `db:migrate` chains an explicit second `prisma generate` so any failure exits visibly. The `postinstall` script also runs `prisma generate` automatically after every `npm install`.

The generated client is written to `generated/prisma/`. Import it in application code as:

```js
import { PrismaClient } from '../generated/prisma/index.js'
```

### Seed (optional)

```bash
npm run seed
```

Seeds two tenants, three topics each, two subscriptions per topic, and fifty events per tenant. The two-tenant seed is specifically designed to verify RLS isolation — you need a second tenant's data to confirm cross-tenant queries return nothing.

### Kafka Setup (Phase 7+)

**One-time setup per machine:**

```bash
# 1. Create the shared Docker network used by all local services
docker network create local-services

# 2. Add kafka hostname to /etc/hosts so the host app can reach the broker
#    (The broker advertises kafka:9092; this maps it to the Docker port binding)
echo "127.0.0.1  kafka" | sudo tee -a /etc/hosts
```

**Start Kafka:**

```bash
# Starts the broker and runs kafka-init to create the platform.events topic
docker compose up kafka kafka-init -d

# Verify the topic was created
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic platform.events
# Expect: PartitionCount=6, ReplicationFactor=1, retention.ms=604800000
```

`kafka-init` waits 15 seconds then creates the `platform.events` topic with 6 partitions. `--if-not-exists` makes it safe to re-run. After `kafka-init` exits cleanly, the broker is ready.

Add to `.env`:

```env
KAFKA_BROKERS=kafka:9092
KAFKA_CLIENT_ID=event-platform-outbox
KAFKA_TOPIC=platform.events
```

### Redis Setup (Phase 9+)

```bash
# Start Redis (defined in docker-compose.yml)
docker compose up redis -d

# Verify
docker compose exec redis redis-cli ping
# Expect: PONG
```

Add to `.env`:

```env
REDIS_HOST=localhost
REDIS_PORT=6379
```

### Elasticsearch Setup (Phase 10+)

```bash
# Start Elasticsearch (defined in docker-compose.yml)
docker compose up elasticsearch -d

# Wait for green cluster health (~30–60s)
watch -n5 'curl -sf http://localhost:9200/_cluster/health | jq .status'
# Expect: "green"
```

Add to `.env`:

```env
ELASTICSEARCH_URL=http://localhost:9200
ELASTICSEARCH_INDEX=delivery-logs
```

The indexing worker calls `ensureDeliveryLogsIndex` on startup — it creates the `delivery-logs` index with the correct field mapping if it does not exist, and is safe to call multiple times.

### Run

```bash
# Development (auto-reload via nodemon)
npm run dev

# Production
npm start
```

The outbox worker, delivery worker, and retry worker each run as separate processes. Start them in additional terminals after Kafka and Redis are up:

```bash
node src/workers/outboxWorker.js
# Expect: [outbox-worker] connecting to Kafka
# Expect: [outbox-worker] connected - starting poll loop

node src/workers/delivery.worker.js
# Expect: [delivery-worker] starting...
# Expect: [delivery-worker] running - waiting for events

node src/workers/retryWorker.js
# Expect: [retry-worker] started — concurrency=10

node src/workers/indexingWorker.js
# Expect: [es] index "delivery-logs" created  (first run only)
# Expect: [indexing-worker] running
```

The outbox worker is safe to run as multiple instances — `FOR UPDATE SKIP LOCKED` ensures each event is claimed by exactly one worker. In production (Phase 16) all workers become their own Docker Compose services.

API docs available at `http://localhost:3096/swagger` (development only).

### Test

```bash
# Run all tests once
npm test

# Watch mode
npm run test:watch
```

Integration tests cover tenant registration, API key CRUD, authentication edge cases, RLS isolation, topic and subscription CRUD, event publishing (idempotency, validation, topic existence checks, cross-tenant RLS enforcement, and outbox atomicity), HMAC signing, webhook delivery (HTTP fan-out, delivery attempt recording, and cross-tenant RLS enforcement in the delivery worker), retry scheduling (delay values per attempt number, `isLastAttempt`, `MAX_ATTEMPTS`), retry worker behaviour (success path, intermediate failure with BullMQ re-enqueue, dead letter creation at attempt 5, and dropped jobs for deleted subscriptions), and delivery log indexing (`buildDeliveryLogDocument` shape, `processIndexJob` Elasticsearch call and error propagation, and integration fan-out verifying one indexing job per subscription per delivery). The RLS isolation suite (`rls-isolation.test.js`) requires seed data (`npm run seed`) to be present before running.

---

## API Reference

All endpoints are prefixed `/api/v1` except health checks. Authenticated routes require:

```
Authorization: Bearer sk_live_<raw_key>
```

Responses follow a standard envelope:

```json
{ "success": true, "statusCode": 200, "data": {}, "error": null }
```

### Tenants

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/tenants` | — | Register tenant; returns raw API key once (signup endpoint) |

### API Keys

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api-keys` | ✓ | Create additional API key; returns raw key once |
| GET | `/api-keys` | ✓ | List keys by prefix and label (raw key never returned) |
| DELETE | `/api-keys/:id` | ✓ | Revoke key — sets `revoked_at`; takes effect immediately |

### Topics

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/topics` | ✓ | Create a topic |
| GET | `/topics` | ✓ | List topics (paginated) |
| GET | `/topics/:id` | ✓ | Get topic details and subscription count |
| DELETE | `/topics/:id` | ✓ | Soft-delete topic; cascades to subscriptions; events retain `topic_id` |

### Subscriptions

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/topics/:topicId/subscriptions` | ✓ | Create subscription; returns signing secret once |
| GET | `/topics/:topicId/subscriptions` | ✓ | List subscriptions (paginated) |
| GET | `/subscriptions/:id` | ✓ | Get subscription details |
| PUT | `/subscriptions/:id` | ✓ | Update endpoint URL or enabled state |
| DELETE | `/subscriptions/:id` | ✓ | Delete subscription |
| POST | `/subscriptions/:id/rotate-secret` | ✓ | Rotate signing secret; old secret invalidated immediately; returns new secret once |
| POST | `/subscriptions/:id/replay` | ✓ | Enqueue delivery jobs for all events since a given timestamp |

**Replay request body:**

```json
{ "from": "2026-05-01T00:00:00Z" }
```

Returns the count of events enqueued. Replay reads from the PostgreSQL `events` table and injects BullMQ delivery jobs directly — it does not reset Kafka consumer offsets.

### Events

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/topics/:topicId/events` | ✓ | Publish event; returns `202 Accepted` with event ID immediately |

**Request body:**

```json
{
  "eventType": "order.created",
  "payload": { "orderId": "abc-123", "amount": 5000, "currency": "TWD" },
  "idempotencyKey": "order-abc-123-created"
}
```

`idempotencyKey` is optional but strongly recommended. On re-submission of a known key, the original event ID is returned — no duplicate insert.

### Delivery Logs

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/delivery-logs` | ✓ | Search delivery attempts via Elasticsearch |
| GET | `/delivery-logs/:attemptId` | ✓ | Get single attempt detail |

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `status` | string | `pending` \| `success` \| `failed` \| `dead_lettered` |
| `topicName` | string | Exact topic name match |
| `from` | ISO 8601 | Start of date range (inclusive) |
| `to` | ISO 8601 | End of date range (exclusive) |
| `q` | string | Full-text search across event payload |
| `page` | integer | Page number (default 1) |
| `pageSize` | integer | Page size (default 20, max 100) |

### Dead Letters

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/dead-letters` | ✓ | List dead letters for the tenant (paginated) |
| GET | `/dead-letters/:id` | ✓ | Get dead letter details including full delivery attempt history |
| POST | `/dead-letters/:id/retry` | ✓ | Enqueue a single retry; `resolved_at` set only on success |

### Health

Not prefixed with `/api/v1`. No authentication required. Intended for load balancers and orchestrators.

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness — `200` if the process is running |
| GET | `/health/ready` | Readiness — checks PostgreSQL, Kafka, Elasticsearch, and Redis; `503` if any fail |

**Readiness response (healthy):**

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

**Readiness response (degraded):**

```json
{
  "status": "degraded",
  "checks": {
    "postgres": "healthy",
    "kafka": "healthy",
    "elasticsearch": "error",
    "redis": "healthy"
  }
}
```

---

## Data Models

### PostgreSQL (Prisma)

**tenants**
```
id                UUID         PK, gen_random_uuid()
name              VARCHAR(200)
email             VARCHAR(320) unique
max_subscriptions INT          default 100; subscription creation cap
created_at        TIMESTAMPTZ
```

RLS is enabled on every tenant-scoped table with the policy:
```sql
USING (tenant_id = current_setting('app.current_tenant_id')::uuid)
```

**api_keys**
```
id           UUID        PK
tenant_id    UUID        → tenants (ON DELETE CASCADE)
key_hash     VARCHAR(64) unique — SHA-256 of raw key; raw key never stored
key_prefix   VARCHAR(15) display only ("sk_live_a1b2c3d")
label        VARCHAR(100) default "Default"
last_used_at TIMESTAMPTZ nullable; updated asynchronously (not on critical path)
revoked_at   TIMESTAMPTZ nullable; partial index WHERE revoked_at IS NULL
created_at   TIMESTAMPTZ
```

Indexes: `(key_hash) WHERE revoked_at IS NULL` (partial — excludes revoked keys from lookup), `(tenant_id)`

**topics**
```
id          UUID         PK
tenant_id   UUID         → tenants (ON DELETE CASCADE)
name        VARCHAR(200) unique per tenant — compound unique (tenant_id, name)
description TEXT         nullable
deleted_at  TIMESTAMPTZ  nullable; soft-delete; events retain reference after deletion
created_at  TIMESTAMPTZ
```

Indexes: `(tenant_id) WHERE deleted_at IS NULL`

**subscriptions**
```
id            UUID        PK
tenant_id     UUID        → tenants (ON DELETE CASCADE)
topic_id      UUID        → topics (ON DELETE CASCADE)
endpoint_url  TEXT        must be HTTPS — HTTP rejected at creation and update
secret_hash   VARCHAR(64) SHA-256 of signing secret
secret_prefix VARCHAR(10) display only; returned on every GET
secret_raw    TEXT        plaintext secret — project simplification (no Vault); raw value returned once at creation/rotation only
enabled       BOOLEAN     default true; disable preserves history
created_at    TIMESTAMPTZ
```

Indexes: `(topic_id) WHERE enabled = true` (partial — used by delivery worker subscription lookup), `(tenant_id)`

**events**
```
id                 UUID          PK
tenant_id          UUID          → tenants
topic_id           UUID          → topics
event_type         VARCHAR(200)
payload            JSONB
idempotency_key    VARCHAR(200)  nullable; unique per tenant via partial index
published_to_kafka BOOLEAN       default false; outbox flag
created_at         TIMESTAMPTZ
```

Indexes:
- `(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL` (unique partial — prevents duplicate inserts)
- `(created_at ASC) WHERE published_to_kafka = false` (partial — outbox poll query; shrinks as events are published)
- `(topic_id, created_at DESC)` (replay queries)
- `(tenant_id, created_at DESC)`

**delivery_attempts**
```
id              UUID         PK
event_id        UUID         → events
subscription_id UUID         → subscriptions
tenant_id       UUID         → tenants
attempt_number  SMALLINT     default 1
status          VARCHAR(20)  pending | success | failed | dead_lettered
http_status     SMALLINT     nullable — HTTP response code from subscriber
response_body   TEXT         nullable
duration_ms     INT          nullable
next_retry_at   TIMESTAMPTZ  nullable — set on failure; cleared on success
attempted_at    TIMESTAMPTZ
```

Indexes:
- `(event_id)`
- `(subscription_id, attempted_at DESC)`
- `(tenant_id, attempted_at DESC)`
- `(next_retry_at ASC) WHERE status = 'failed' AND next_retry_at IS NOT NULL` (partial — retry poller query)

**dead_letters**
```
id              UUID         PK
event_id        UUID         → events
subscription_id UUID         → subscriptions
tenant_id       UUID         → tenants
total_attempts  SMALLINT     total attempt count at time of dead-lettering
last_error      TEXT         nullable
resolved_at     TIMESTAMPTZ  nullable; set when a manual retry succeeds
created_at      TIMESTAMPTZ
```

Indexes: `(tenant_id, created_at DESC) WHERE resolved_at IS NULL` (partial — active dead letters only)

### Elasticsearch

**delivery-logs index**
```
tenant_id        keyword   always filtered — Elasticsearch-layer tenant scope
event_id         keyword
subscription_id  keyword
topic_name       keyword   exact-match filter
endpoint_url     keyword
status           keyword   pending | success | failed | dead_lettered
http_status      integer
attempt_number   integer
payload          text      full-text analyzed — free-text search target
response_body    text
attempted_at     date      range filter
next_retry_at    date
```

`keyword` fields use `filter` context (cached, no relevance score). `payload` uses `must` context (full-text, affects score). Every query includes `{ term: { tenant_id: tenantId } }` in the `filter` context — the application enforces tenant scope because Elasticsearch has no RLS equivalent.

---

## Outbox Worker

The outbox worker runs as a separate process. It is safe to run multiple instances — `FOR UPDATE SKIP LOCKED` ensures each event is processed by exactly one worker.

```
Poll cycle (every 1s):
  1. Open PostgreSQL transaction
  2. SELECT ... WHERE published_to_kafka = false ORDER BY created_at ASC LIMIT 100
     FOR UPDATE SKIP LOCKED
  3. For each event: kafkaProducer.send({ key: tenantId, value: event })
  4. UPDATE events SET published_to_kafka = true WHERE id IN (processed ids)
  5. Commit
  6. On Kafka failure: transaction rolls back; event stays published_to_kafka = false;
     retried on next poll cycle
```

The Kafka message envelope:

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

Partition key is `tenantId`. All events for the same tenant go to the same partition, preserving ordering within a tenant. The trade-off is hot partitions if a single tenant dominates volume — documented as a known limitation for this project scale.

---

## Implemented Phases

| Phase | Feature |
|---|---|
| 1 | PostgreSQL schema — Prisma models for all 7 entities (`Tenant`, `ApiKey`, `Topic`, `Subscription`, `Event`, `DeliveryAttempt`, `DeadLetter`) with RLS-compatible field mapping, partial indexes, compound unique constraints, and cascade delete semantics; Prisma 7.x config in `prisma.config.ts` (datasource URL lives here, not in `schema.prisma`); generator output to `generated/prisma/`; `src/db/prisma.js` Prisma client singleton using `PrismaPg` driver adapter; `src/seed/seed.ts` dev seed (2 tenants, 3 topics each, 2 subscriptions per topic, 50 events per tenant) |
| 2 | Fastify server (`src/index.js` + `src/app.js`) with Swagger UI, global error handler, 404 handler, `GET /health` liveness check; `authenticate` hook (SHA-256 hash lookup, async `last_used_at` update, `req.withTenant` bound to transaction); `withTenant` utility (`SET LOCAL app.current_tenant_id` scoped to transaction); Pino logger (pino-pretty in dev, JSON in prod); `ApiResponse` envelope; `AppError` hierarchy |
| 3 | Tenant registration (`POST /api/v1/tenants` — unauthenticated, returns raw key once); API key management (`POST /GET /DELETE /api/v1/api-keys` — authenticated); Vitest integration tests for all three endpoints including cross-tenant isolation and revocation; RLS isolation test suite (`rls-isolation.test.js`) |
| 4 | Topic management (`POST /GET /DELETE /api/v1/topics`, `GET /api/v1/topics/:id`); soft delete via `deleted_at` (topic row kept so event FKs stay valid; subscriptions hard-deleted in the same transaction); compound unique `(tenant_id, name)`; subscription count in GET-by-id response; Vitest tests covering CRUD, duplicate name rejection, RLS isolation, and soft-delete cascade |
| 5 | Subscription management (`POST /GET /api/v1/topics/:topicId/subscriptions`, `GET /PUT /DELETE /api/v1/subscriptions/:id`, `POST /api/v1/subscriptions/:id/rotate-secret`); per-subscription `whsec_*` signing secret — raw value returned once at creation and rotation, never again; HTTPS enforcement at creation and update; subscription quota checked atomically inside `withTenant` transaction (TOCTOU-safe); Vitest tests covering HTTPS rejection, secret lifecycle, quota, RLS isolation, enable/disable toggle, and delete idempotency |
| 6 | Event publishing (`POST /api/v1/topics/:topicId/events`, `202 Accepted`); idempotency via partial unique index on `(tenant_id, idempotency_key)`; outbox pattern — `published_to_kafka = false` on insert, background worker polls with `FOR UPDATE SKIP LOCKED` and marks published atomically; `src/workers/outboxWorker.js` exports `pollOutbox` and guards `run()` behind an `isMain` check so test imports do not start the loop; raw SQL table names schema-qualified (`public.events`, `public.topics`) for search_path safety; `src/db/workerClient.js` uses `DATABASE_URL_WORKER` (BYPASSRLS role) with the same `PrismaPg` adapter as the API; Kafka producer is a Phase 6 stub (console log); 13 Vitest tests covering 202 response shape, DB state, idempotency, validation, topic existence, soft-delete, RLS cross-tenant isolation, and outbox rollback atomicity |
| 7 | Kafka setup — `apache/kafka:3.9.0` in KRaft mode (no ZooKeeper) via Docker Compose on an external `local-services` network; `kafka-init` service creates `platform.events` (6 partitions, 7-day retention) after a 15-second startup wait; `src/kafka/client.js` KafkaJS singleton with comma-split broker array; `src/kafka/producer.js` replaces Phase 6 stub — `allowAutoTopicCreation: false`, `tenantId` partition key, full message envelope; outbox worker connects producer on startup with `SIGTERM` handler for clean shutdown; `vi.mock('../kafka/producer.js')` at the file level in `event.test.js` (file-level hoisting required — `vi.spyOn` does not intercept ESM named imports inside the worker); 4 Vitest unit tests for producer message shape + partition key; 3 Vitest integration tests for outbox atomicity and publish marking |
| 8 | Webhook delivery worker — `src/utils/computeHmac.js` signs `timestamp.payload` with HMAC-SHA256 (timestamp prevents replay attacks; subscribers validate within 5 minutes); `src/delivery/deliver.js` makes the HTTP POST with `AbortController` 10s timeout, HMAC headers, and full response body capture; `src/services/deliveryAttemptService.js` writes `pending` → `success`/`failed` rows via `withTenant` (RLS enforced in the delivery path); `src/workers/delivery.worker.js` Kafka consumer with `autoCommit: false` — offset committed only after all subscriptions for an event are processed (at-least-once guarantee); `isMain` guard prevents Kafka connection on test import; `processEvent` exported for direct Vitest invocation; 5 HMAC unit tests + 8 delivery integration tests |
| 9 | Retry scheduling + dead letter creation — `src/utils/retrySchedule.js` exports `computeRetryDelay` (30s → 5m → 30m → 2h indexed by failed attempt number), `isLastAttempt`, and `MAX_ATTEMPTS = 5`; `src/queues/retryQueue.js` creates a BullMQ `Queue` named `webhook-retry` with `attempts: 1` (retry logic is managed explicitly — BullMQ-level retries would double-count attempts) and `removeOnComplete/Fail` bounds to cap Redis memory; `src/services/deadLetterService.js` writes `dead_letters` rows; `src/services/deliveryAttemptService.js` gains `deadLetterAttempt` (sets status = `dead_lettered`, clears `nextRetryAt`); `src/workers/delivery.worker.js` failure branch updated — `resolveAttempt` now receives a computed `nextRetryAt` and a BullMQ job is enqueued for attempt 2 with 30s delay; `src/workers/retryWorker.js` BullMQ Worker with `concurrency: 10` — reloads event + subscription from PostgreSQL on each job (picks up endpoint changes between enqueue and processing), delivers, on success resolves, on intermediate failure schedules next attempt with the appropriate backoff delay, on attempt 5 dead-letters the attempt and creates a `dead_letters` row; `isMain` guard prevents Worker construction on test import; `processRetryJob` exported for direct Vitest invocation; 8 retry schedule unit tests + 5 retry worker integration tests |
| 10 | Elasticsearch setup + async delivery log indexing — `@elastic/elasticsearch` v8 client singleton (`src/search/esClient.js`); `delivery-logs` index with explicit field mapping (`keyword` for IDs/status/topic/endpoint, `text` for payload and response_body, `integer` for http_status/attempt_number, `date` for timestamps) created idempotently on worker startup via `ensureDeliveryLogsIndex` (`src/search/deliveryLogsIndex.js`); `buildDeliveryLogDocument` helper serialises `payload` as a JSON string for full-text search and maps camelCase inputs to snake_case ES fields (`src/search/buildDeliveryLogDocument.js`); `src/queues/indexQueue.js` BullMQ Queue with `attempts: 5` + exponential backoff — indexing is idempotent so BullMQ-managed retries are safe; `src/workers/indexingWorker.js` BullMQ Worker with `concurrency: 20` — uses `attemptId` as the Elasticsearch `_id` turning every index call into an upsert (no duplicate documents on retry); delivery worker and retry worker each enqueue one indexing job after every attempt resolution (success, failure, or dead-lettered); Elasticsearch is not on the delivery critical path — delivery continues unaffected during an ES outage and the indexing queue drains automatically on recovery; `docker-compose.yml` updated with `elasticsearch:8.13.0` service (`discovery.type=single-node`, `xpack.security.enabled=false` for dev, `number_of_replicas: 0` for green single-node health); 9 Vitest tests |

## Roadmap

| Phase | Feature |
|---|---|
| 11 | Delivery log search API |
| 12 | Event replay |
| 13 | Dead letter management |
| 14 | Rate limiting + tenant subscription quota |
| 15 | Health checks (PostgreSQL + Kafka + Elasticsearch + Redis) |
| 16 | Docker Compose (Kafka KRaft, Elasticsearch, PostgreSQL, Redis, Seq, Nginx) |
| 17 | GitLab CI/CD |
| 18 | Git hygiene: branch per feature, MR self-review |
