# EventDeliveryPlatformAPI — Interview Questions by Level

This document maps each major interview question this project can answer to the specific phases and implementation details that support a strong answer.

---

## Junior

---

### "What is REST and how do you design endpoints?"

**Phases**: 3–5, 11–13

REST is an architectural style for APIs that uses HTTP verbs and resource-oriented URLs. This project applies it consistently:

- `POST /topics` creates a topic; `GET /topics` lists them; `DELETE /topics/:id` soft-deletes
- `POST /events` publishes an event and returns `202 Accepted` — not `200` — because delivery is async. The event was accepted for processing, not completed
- `GET /delivery-logs` supports query params (`?status=`, `?topicName=`, `?from=`, `?to=`, `?q=`) for filtering without changing the resource shape
- `POST /dead-letters/:id/retry` uses a sub-resource action for a non-CRUD operation

The project also deliberately uses `404` instead of `403` when a tenant tries to access another tenant's delivery log by ID. Returning `403` would confirm the resource exists, which itself leaks information.

---

### "How does authentication work in an API?"

**Phase**: 2

API key authentication works by: the client sends a key in a header (`Authorization: Bearer <key>`), the server looks up the hashed version of that key in the database, and if it finds a match with a non-revoked row, the request is authenticated.

Key design points:
- The hash comparison uses SHA-256, not bcrypt. This is intentional — API keys are high-entropy random strings (unlike passwords), so bcrypt's slow hashing provides no benefit and adds latency on every request
- The auth check runs as an `onRequest` Fastify hook, meaning it executes before any route handler. A failed check short-circuits the entire request
- After a successful auth check, the `last_used_at` field is updated as a fire-and-forget operation — it doesn't block the response
- Every request gets a correlation ID via `genReqId()` which appears in all log lines for that request, making distributed tracing easier

---

### "What is hashing and why don't you store secrets in plaintext?"

**Phases**: 2, 3, 5

Hashing transforms data into a fixed-length digest that cannot be reversed. The project uses SHA-256 in two places:

1. **API keys** (Phase 3): When a tenant registers, an API key is generated. The raw key is returned once in the response and never stored. Only the SHA-256 hash is persisted. On subsequent requests, the incoming key is hashed and the hash is compared — the raw key is never in the database.

2. **Webhook signing secrets** (Phase 5): Same pattern — a `whsec_`-prefixed raw secret returned once at creation, only the hash stored. On delivery, the raw secret is retrieved from `secret_raw` (a project simplification — production would use Vault) to compute the HMAC signature.

The `keyPrefix` (first 15 characters of the raw key) is stored separately so tenants can identify which key is which in list views, without exposing the full secret.

---

### "What is pagination?"

**Phase**: 4

Pagination limits how many records are returned per response so you don't load millions of rows into memory. The project implements offset-based pagination:

- A `pageSize` param controls results per page, clamped to a maximum of 100 to prevent abuse
- A `page` param offsets into the result set
- Responses include `totalPages` so clients know when they've reached the end

The clamp on `pageSize` is important — without it, a client could send `pageSize=1000000` and trigger an expensive query that could take down the database.

---

### "What is a webhook?"

**Phases**: 5, 8

A webhook is a way for a server to push data to a client when an event happens, rather than the client polling repeatedly. The client registers an HTTPS endpoint URL (a subscription), and whenever a matching event is published, the platform sends an HTTP POST to that URL with the event payload.

The project adds HMAC signing for security: each subscription has its own secret, and the delivery worker includes a `X-Webhook-Signature: sha256=<hex>` header on every request. The receiving server recomputes the HMAC and compares it to verify the payload wasn't tampered with and came from the platform.

The signature also includes a timestamp (`X-Webhook-Timestamp`) in the signed string — `timestamp.payload` — so a valid signature from yesterday can't be replayed today.

---

### "What is idempotency?"

**Phase**: 6

Idempotency means calling an operation multiple times produces the same result as calling it once. In the context of event publishing, if a client crashes after sending a request but before receiving the response, it will retry — which could create duplicate events.

The project solves this with an idempotency key: the client sends a unique `idempotencyKey` with each publish request. The platform stores a partial unique index on `(tenant_id, idempotency_key)`. If the same key is received again, the original event is returned immediately without re-processing.

The response is the same `202 Accepted` as the original, so the client can't tell whether this was a fresh request or a replay — which is the correct behavior.

---

### "What is a dead letter queue?"

**Phases**: 9, 13

A dead letter queue (DLQ) stores messages that have exhausted all retry attempts and could not be delivered. Instead of silently dropping failed deliveries, the platform creates a `dead_letters` row after 5 failed attempts, preserving the full event and the history of all delivery attempts.

The DLQ is queryable via `GET /dead-letters` and individual entries can be manually retried via `POST /dead-letters/:id/retry`. The `resolvedAt` field is only set on successful manual retry — failed retries leave it null, keeping the entry in the unresolved backlog.

---

### "What is rate limiting?"

**Phase**: 14

Rate limiting caps how many requests a client can make in a time window to protect the platform from abuse or accidental overload. The project limits event publishing to 1,000 events/minute per tenant and subscription creation to 10/hour per tenant.

When a tenant exceeds the limit, the API returns `429 Too Many Requests` with a `Retry-After` header indicating how many seconds until the window resets. The client should back off and retry after that duration.

---

### "What is a health check endpoint?"

**Phase**: 15

A health check is an endpoint that monitoring systems and orchestrators (like Kubernetes) can poll to know whether a service is functioning. The project has two:

- `GET /health` — liveness probe. Always returns `200`. If this endpoint responds, the process is alive.
- `GET /health/ready` — readiness probe. Returns `200` only if all dependencies (PostgreSQL, Kafka, Elasticsearch, Redis) are reachable. Returns `503` if any are degraded.

Kubernetes uses these differently: if liveness fails, the pod is restarted; if readiness fails, the pod is removed from the load balancer but not restarted.

---

### "What is Docker and what is a multi-stage build?"

**Phase**: 16

Docker packages an application and its dependencies into a portable container image. A multi-stage build uses multiple `FROM` steps in a single Dockerfile to produce a smaller final image:

1. **Build stage**: Installs all dependencies (including dev dependencies), runs Prisma generate, compiles TypeScript
2. **Runtime stage**: Copies only the compiled output and production dependencies from the build stage — no build tools, no source files

The project produces a single image that runs five different processes (API + 4 workers) by overriding the `command` in Docker Compose. This avoids maintaining five separate Dockerfiles for nearly identical containers.

---

## Mid-Level

---

### "How do you design a multi-tenant database schema?"

**Phase**: 1

Multi-tenancy means multiple customers share the same database infrastructure while their data remains isolated. The project uses a **shared schema** approach: one set of tables, with a `tenant_id` foreign key on every tenant-scoped table.

Design decisions:
- `tenants` and `api_keys` are global tables with no `tenant_id` (the auth layer needs unrestricted access to look up keys)
- All other tables (`topics`, `subscriptions`, `events`, `delivery_attempts`, `dead_letters`) have `tenant_id` as a non-nullable FK
- Compound unique constraints like `(tenant_id, name)` on `topics` allow two different tenants to have topics with the same name without collision
- Soft deletes use a `deleted_at` timestamp column so event FK references remain valid in history even after a topic is "deleted"
- UUIDs as primary keys prevent sequential ID enumeration across tenants

The alternative approaches — schema-per-tenant and database-per-tenant — provide stronger isolation but add significant operational overhead (schema migrations must run N times, connection pooling is harder, cross-tenant analytics are harder).

---

### "What is Row-Level Security and when would you use it?"

**Phases**: 1, 2

Row-Level Security (RLS) is a PostgreSQL feature that attaches a filter policy to a table so that queries automatically see only the rows they're allowed to see, regardless of what the application query says. It's enforced by the database engine, not application code.

The project implements RLS via a session variable: before any query inside a `withTenant(tenantId, fn)` call, the wrapper runs `SET LOCAL app.current_tenant_id = '<id>'`. The RLS policy on each table reads this variable and filters rows automatically:

```sql
CREATE POLICY tenant_isolation ON events
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);
```

This means even if a developer writes `SELECT * FROM events` without a WHERE clause inside a `withTenant` block, they only get that tenant's events. The database enforces isolation as a last line of defense.

`tenants` and `api_keys` are intentionally excluded from RLS — the auth middleware runs before the tenant is known, so it needs unrestricted access to look up the key and identify the tenant.

---

### "What are partial indexes and when do you use them?"

**Phases**: 1, 3, 4

A partial index only indexes rows that match a WHERE condition. This keeps the index smaller and faster for the specific queries it's designed to serve.

The project uses four:

| Index | Condition | Purpose |
|-------|-----------|---------|
| `idx_topics_tenant` | `WHERE deleted_at IS NULL` | Topic lookups only ever query live topics |
| `idx_api_keys_hash` | `WHERE revoked_at IS NULL` | Auth lookups only ever query active keys |
| `idx_events_outbox` | `WHERE published_to_kafka = false` | Outbox worker only polls unpublished events |
| `idx_events_idempotency` | `WHERE idempotency_key IS NOT NULL` | Only events with idempotency keys need this lookup |

For the API key case, the partial index also makes revocation instantaneous: once `revoked_at` is set, the row falls out of the index and auth lookups return no match — without any extra query complexity.

---

### "What is soft delete and when do you choose it over hard delete?"

**Phase**: 4

Soft delete marks a record as deleted (via a `deleted_at` timestamp) instead of removing the row from the database. The record still exists but is excluded from normal queries via the partial index.

The project uses soft delete for topics because events have a FK reference to `topic_id`. A hard delete would fail due to the FK constraint (or cascade-delete all events, destroying history). Keeping the topic row with `deleted_at` set allows event history to remain intact.

Subscriptions are hard-deleted when their topic is soft-deleted. The reasoning: subscriptions only exist to deliver events for a topic. Once the topic is gone, subscriptions serve no purpose, so keeping them wastes space and creates confusing state.

The trade-off of soft delete: queries must always filter `WHERE deleted_at IS NULL`, storage grows over time, and you need a background job to eventually purge old records if needed.

---

### "What is the Outbox pattern and how does it solve the dual-write problem?"

**Phase**: 6

The dual-write problem: when you need to write to a database AND publish to a message broker (like Kafka), a crash between the two operations leaves the system inconsistent — either the DB write succeeded but Kafka didn't get it, or vice versa.

The Outbox pattern solves this by making the Kafka publish a two-phase operation:

1. Write the event to PostgreSQL with `published_to_kafka = false` in the same transaction as the business logic. Either both succeed or both roll back — no inconsistency.
2. A separate outbox worker polls for rows where `published_to_kafka = false`, publishes them to Kafka, then marks them `true`.

If the worker crashes between publishing and marking, the event gets published to Kafka again on restart — which is acceptable (at-least-once delivery) and handled by idempotent consumers. What's not acceptable is losing the event entirely, which the Outbox prevents.

---

### "What is `FOR UPDATE SKIP LOCKED`?"

**Phase**: 6

`FOR UPDATE SKIP LOCKED` is a PostgreSQL locking hint used when multiple workers compete to process rows from a queue. Standard `FOR UPDATE` would block all workers until one finishes. `SKIP LOCKED` tells each worker to skip rows that are already locked by another worker, immediately moving to the next available row.

The outbox worker uses this so multiple instances can run in parallel without blocking each other or producing deadlocks. Each worker atomically claims a batch of unpublished events and processes them independently.

---

### "How does Kafka partitioning work and how do you choose a partition key?"

**Phase**: 7

Kafka topics are divided into partitions. Messages within a partition are ordered; messages across partitions are not. A partition key determines which partition a message goes to (via consistent hashing).

The project uses `tenantId` as the partition key. This means all events from the same tenant land on the same partition, preserving per-tenant ordering. A tenant's events will always be consumed in the order they were published.

The trade-off: a high-volume tenant creates a "hot partition" — one partition receives far more messages than others, limiting parallelism for that tenant. An alternative like `eventId` distributes load more evenly but loses ordering guarantees.

---

### "What is at-least-once delivery and what problems does it create?"

**Phase**: 8

At-least-once delivery guarantees a message will be delivered at least one time, but possibly more than once. The delivery worker achieves this by:

- Setting `autoCommit: false` on the Kafka consumer
- Manually committing the offset only after all subscriptions for an event have been processed (attempted, not necessarily succeeded)

If the worker crashes mid-fan-out, Kafka replays from the last committed offset. Some subscriptions may receive the event a second time. Downstream systems must be idempotent (tolerate duplicate deliveries) — the webhook receiver should check the `X-Webhook-Event-Id` header and deduplicate if needed.

The alternative — `autoCommit: true` — risks losing events: Kafka might commit the offset before the fan-out completes, and a crash loses those deliveries with no way to recover.

---

### "What is exponential backoff?"

**Phase**: 9

Exponential backoff increases the wait time between retry attempts by a multiplying factor. Instead of hammering a failing endpoint every second, you give it time to recover.

The retry schedule: **30s → 5m → 30m → 2h → dead-letter** (5 total attempts). Each gap is roughly 6–7x the previous. After the 5th failure, the delivery is dead-lettered instead of retried.

BullMQ handles the scheduling — each failed delivery attempt creates a job with a `delay` set to the next retry interval. The retry worker reloads the event and subscription fresh from the database on each attempt, so if a subscription's endpoint URL was updated between retries, the next attempt uses the new URL.

---

### "What is the difference between `filter` and `must` in Elasticsearch?"

**Phase**: 11

Both `filter` and `must` narrow down search results, but they work differently:

- **`filter` context**: Boolean match, no relevance scoring. Results are cached. Use for hard requirements: `status = 'failed'`, `tenant_id = X`, date ranges.
- **`must` context**: Relevance-scored match. Not cached. Use for full-text search where you want results ranked by how well they match.

The project puts tenant scoping, status, topic name, and date range filters in `filter` context (exact matches that benefit from caching) and the free-text `?q=` param in `must` context (scored full-text search against payload and response body).

Mixing them correctly means common filtered queries are fast and cached, while full-text queries still rank results by relevance.

---

### "What is the difference between fixed-window and sliding-window rate limiting?"

**Phase**: 14

**Fixed-window**: A counter resets at fixed intervals (e.g., every minute starting at :00). Simple to implement with Redis INCR + EXPIRE. The problem: a client can send 1,000 requests at :59 and another 1,000 at :01 — 2,000 requests in 2 seconds — because both fall within different windows.

**Sliding-window**: Tracks requests within a rolling time window using a sorted set. Eliminates the burst-at-boundary problem but is more complex and uses more Redis memory.

The project uses fixed-window (INCR/EXPIRE). The implementation sets the TTL only on the first increment (when INCR returns 1), which avoids resetting the window on every request. For an event delivery platform, the burst-at-boundary issue is acceptable — operators can tune limits accordingly.

---

### "What is `Promise.allSettled` vs `Promise.all`?"

**Phase**: 15

`Promise.all` resolves when all promises resolve, but rejects immediately if any one rejects. In a health check context, this is wrong: if PostgreSQL is down, `Promise.all` would throw before checking Kafka, Elasticsearch, or Redis — hiding their status entirely.

`Promise.allSettled` always waits for every promise to finish, regardless of individual success or failure. The readiness endpoint uses it to run all four dependency checks in parallel and report each result independently. An operator gets the full picture: PostgreSQL down, everything else healthy.

---

## Senior

---

### "How do you enforce tenant isolation without trusting application code?"

**Phases**: 1, 2

Application-level filtering (WHERE tenant_id = X in every query) is the first line of defense, but it's fragile — a missing WHERE clause, a copy-paste bug, or a Prisma query written without the filter leaks cross-tenant data.

The project adds database-enforced RLS as a second layer. The `withTenant` wrapper runs `SET LOCAL app.current_tenant_id = X` before any query in the transaction. RLS policies on all tenant-scoped tables automatically filter to that tenant ID. A missing WHERE clause in application code is caught by the database before rows are returned.

The isolation is tested directly: seed data has two tenants (A and B), and tests verify that authenticating as tenant A and querying topics, events, etc. returns zero rows from tenant B — even with intentionally simple queries.

---

### "Walk me through designing a webhook delivery system with reliability guarantees."

**Phases**: 6–9

The full pipeline:

1. **Publish** (`POST /events`): Event written to PostgreSQL with `published_to_kafka = false`. Idempotency key prevents duplicate ingestion. Returns `202 Accepted` — delivery is async.

2. **Outbox worker**: Polls PostgreSQL for `published_to_kafka = false` events using `FOR UPDATE SKIP LOCKED`. Publishes to Kafka topic `platform.events` with `tenantId` as partition key. Marks `published_to_kafka = true`. Horizontally scalable.

3. **Delivery worker** (Kafka consumer): Reads events, fans out to all active subscriptions. For each subscription: computes HMAC signature (`sha256(timestamp.payload)` using per-subscription secret), sends HTTP POST with 10-second timeout via AbortController, records attempt (status, HTTP status, response body, duration). Manual offset commit only after full fan-out — at-least-once guarantee.

4. **Retry worker** (BullMQ): Failures trigger a delayed job for the next retry interval. Retry worker reloads event + subscription from DB (not job payload) so in-flight config changes apply. Attempt 5 failure creates a `dead_letters` row.

5. **Dead letter management**: `GET /dead-letters`, `POST /dead-letters/:id/retry` (synchronous delivery, immediate result). `resolvedAt` only set on success.

Each stage is isolated — Elasticsearch indexing runs asynchronously off the critical path via a separate BullMQ queue, so search unavailability never blocks delivery.

---

### "How do you handle secret rotation without downtime?"

**Phase**: 5

Secret rotation is an atomic database update: the `secret_raw` and `secret_hash` columns are both updated in a single write. The old secret is immediately invalid — the next delivery attempt uses the new secret to compute the HMAC signature.

There's no grace period where both secrets are valid simultaneously. This is a deliberate simplification: if a rotation breaks a subscriber's verification, they'll see signature failures and know to update their secret. In production, you'd typically want a dual-secret window (accept old or new) during the rotation period, which would require storing both secrets and adding logic to the delivery worker.

The raw secret is only exposed at creation and rotation time. The response includes `secretRaw` in both cases; list responses omit it entirely.

---

### "How do you replay events safely?"

**Phase**: 12

Event replay re-delivers past events to a subscription from a specified timestamp. The key design decision: replay reads from **PostgreSQL, not Kafka**.

Reading from Kafka would require resetting consumer group offsets, which affects all subscriptions consuming that partition — not just the one requesting replay. It also requires keeping Kafka retention long enough to cover the replay window.

Reading from PostgreSQL is RLS-scoped (only the requesting tenant's events), uses existing BullMQ `webhook-retry` queue (no new infrastructure), and has no impact on other subscriptions or Kafka consumers. Events are queried in chronological order (`ORDER BY created_at ASC`) with a `from` timestamp filter and re-enqueued as `nextAttemptNumber: 1` (fresh attempt, not a retry).

---

### "How do you design async search indexing that doesn't block the critical path?"

**Phase**: 10

Delivery attempts are indexed into Elasticsearch asynchronously via a separate BullMQ queue. The delivery worker records the attempt to PostgreSQL and enqueues an indexing job — it does not wait for Elasticsearch to respond.

This means:
- Elasticsearch being slow or down has zero impact on delivery latency or reliability
- The indexing queue can drain at its own pace (concurrency: 20 jobs simultaneously, higher than retry because it's I/O-bound)
- If Elasticsearch is unavailable, jobs accumulate in the BullMQ queue and drain when it recovers

Idempotency: `attemptId` is used as the Elasticsearch document `_id`. If an indexing job is retried (BullMQ retry on failure), it overwrites the same document — no duplicates in the search index.

---

### "How do you enforce tenant isolation in Elasticsearch?"

**Phase**: 11

Unlike PostgreSQL, Elasticsearch has no RLS equivalent. Tenant isolation is enforced entirely by the application:

1. Every search query includes a `term` filter on `tenant_id` in filter context. This is not optional — the service layer always adds it, regardless of what the caller sends.
2. `GET /delivery-logs/:id` fetches by Elasticsearch `_id` (the `attemptId`) and then verifies that the returned document's `tenant_id` matches the authenticated tenant's ID.
3. Cross-tenant access returns `404`, not `403`. A `403` would confirm the log exists, which leaks information about another tenant's activity.

This is weaker than database RLS — it relies on application code being correct — which is why it's defense-in-depth: PostgreSQL RLS is the authoritative isolation layer, Elasticsearch is a derived index for search only.

---

### "What are the trade-offs of synchronous vs asynchronous delivery for manual retries?"

**Phase**: 13

Normal delivery (Phases 6–9) is fully asynchronous: publish returns `202`, and you find out about success/failure via delivery logs or dead letters later.

Manual dead-letter retry is synchronous: `POST /dead-letters/:id/retry` makes the HTTP call inline and returns `{ success: true, httpStatus: 200 }` or `{ success: false, error: '...' }` in the response.

**Why synchronous for manual retry?**
- Operators trigger manual retries when investigating a specific problem. Immediate feedback tells them whether the fix worked.
- Manual retries happen infrequently (human-initiated), so latency to the operator is more important than throughput.
- The result is still recorded as a new `delivery_attempts` row (attempt number = total + 1), so the audit trail is complete.

**The trade-off**: If the endpoint is slow, the HTTP request holds the operator's request open. For typical webhook retry scenarios, a 10-second timeout is acceptable.

---

### "How would you design health checks for a distributed system?"

**Phase**: 15

Health checks serve two different consumers with different needs:

**Liveness** (`GET /health`): Used by the process supervisor to know if the process is alive. Should always return 200 as long as the event loop is running. Never check external dependencies here — a dependency being down should not cause a pod restart (which won't fix the dependency).

**Readiness** (`GET /health/ready`): Used by the load balancer to know if this instance should receive traffic. Checks all four dependencies: PostgreSQL (raw query), Kafka (admin client connect/disconnect), Elasticsearch (cluster info), Redis (PING). Uses `Promise.allSettled` so all four results are reported independently. Returns `200` only if all pass; `503` otherwise.

The Kafka check creates a short-lived admin client per poll rather than reusing a persistent connection. This catches mid-session failures like leader elections that a persistent connection might not surface until the next operation.

No authentication is required on health endpoints — monitoring systems need to reach them before authentication context is established.

---

## Architecture / Staff

---

### "How do you design multi-tenant data isolation and what are the trade-offs of different approaches?"

**Phases**: 1, 2

Three common approaches:

| Approach | Isolation | Operational Cost | Used here |
|----------|-----------|-----------------|-----------|
| Separate databases | Strongest | High (N migration runs, N connection pools) | No |
| Separate schemas | Strong | Medium (N schema migrations, shared server) | No |
| Shared schema + RLS | Moderate | Low (one migration, one pool) | Yes |

The project uses shared schema with RLS because it minimizes operational complexity — one migration file updates all tenants simultaneously, one connection pool serves all tenants, cross-tenant analytics are straightforward.

The RLS layer adds database-enforced isolation on top of application-level filtering. Even if application code has a bug (missing WHERE clause), the database policy prevents cross-tenant data from being returned. The `withTenant` wrapper ensures the session variable is always set before any query executes.

For a real production system at scale, you'd evaluate separate schemas if tenants needed schema customization, or separate databases if compliance required hard data residency boundaries.

---

### "How do you guarantee at-least-once delivery in a distributed system?"

**Phases**: 6–9

At-least-once delivery means every event is delivered at least once, though possibly more than once. The system achieves this through several layers:

1. **Outbox → Kafka**: The outbox pattern ensures an event written to PostgreSQL will always be published to Kafka, even across crashes. The outbox worker retries until `published_to_kafka = true`.

2. **Kafka → Delivery worker**: Manual offset commit after fan-out means Kafka replays from the last committed offset on crash. Events may be re-delivered to subscriptions (idempotent receiver required).

3. **Delivery worker → Subscription endpoint**: Failed HTTP calls are retried via BullMQ with exponential backoff. The 5-attempt limit with dead-lettering ensures no event is silently dropped — every failure is visible and actionable.

4. **Event idempotency key**: Prevents duplicate event ingestion from retry-happy publishers.

What's NOT guaranteed: exactly-once delivery. Achieving exactly-once would require distributed transactions across Kafka and PostgreSQL, significantly increasing complexity. At-least-once with idempotent consumers is the standard industry approach for this class of system.

---

### "How would you scale this system?"

**Phases**: 6–8, 16

Each component scales independently:

- **API servers**: Stateless, scale horizontally behind Nginx. Rate limiting uses Redis so counters are shared across instances.
- **Outbox worker**: Multiple instances use `FOR UPDATE SKIP LOCKED` to distribute rows without conflict.
- **Delivery worker**: Kafka consumer group handles partition assignment automatically. Add more instances; Kafka redistributes partitions.
- **Retry worker / Indexing worker**: BullMQ workers are horizontally scalable — multiple instances pull from the same Redis-backed queue.
- **PostgreSQL**: Vertical scaling + read replicas for delivery log queries. Partitioning the `events` table by `created_at` would be the next step for very high volume.
- **Elasticsearch**: Add shards/replicas for search scaling without touching the application.

The single Docker image with command overrides (Phase 16) makes scaling individual workers simple in Docker Compose or Kubernetes — just increase the replica count for the specific service.

The primary bottleneck at scale would be the PostgreSQL outbox worker poll interval and the fan-out in the delivery worker (O(subscriptions) HTTP calls per event). Solutions: increase outbox worker replicas, add per-tenant subscription limits, introduce subscription filtering to reduce fan-out.

---

### "Walk me through the security model of this platform end-to-end."

**Phases**: 2–3, 5, 8, 11, 14

**Layer 1 — Authentication** (Phase 2–3): Every request (except health checks and tenant registration) requires an `Authorization: Bearer <key>` header. The key is SHA-256 hashed and compared against the `api_keys` table. Revoked keys fall out of the partial index and return no match. A successful auth check sets the tenant context for all downstream operations.

**Layer 2 — Database isolation** (Phase 1–2): RLS policies enforce tenant scoping at the database layer. `withTenant` sets the session variable before every query. No query inside a tenant context can return another tenant's data, regardless of application code correctness.

**Layer 3 — Webhook signing** (Phase 5, 8): Each subscription has its own HMAC secret. Deliveries include `X-Webhook-Signature: sha256=<hex>` computed over `timestamp.payload`. The timestamp is included in the signed string to prevent replay attacks — a valid signature from an old delivery cannot be reused.

**Layer 4 — Application-layer ES isolation** (Phase 11): Every Elasticsearch query includes a `tenant_id` filter. Cross-tenant GET by ID returns `404` to avoid confirming existence.

**Layer 5 — Rate limiting** (Phase 14): Fixed-window limits on publishing and subscription creation prevent resource exhaustion attacks. Redis-backed counters are shared across API instances.

**Known simplifications for production**: `secret_raw` stored in plaintext (should use Vault/KMS). No mTLS between internal services. No audit log for admin operations. No IP allowlisting on webhook subscriptions.

---

### "What would you change moving this to production at scale?"

**Phases**: 5, 7, 14, 16 (notes throughout plans)

The plans explicitly call out several production gaps:

| Simplification | Production replacement |
|---------------|----------------------|
| `secret_raw` in PostgreSQL plaintext | Vault or AWS KMS for secret storage |
| Single Kafka topic for all tenants | Evaluate per-tenant topics for stronger isolation and per-tenant retention control |
| Fixed-window rate limiting | Sliding-window or token bucket for smoother enforcement |
| Docker Compose with `network_mode: host` | Kubernetes with HPA, proper CNI networking, and resource limits |
| No Vault for secrets | Secrets management with rotation support |
| No mTLS between workers | Service mesh (Istio/Linkerd) for internal auth |
| Synchronous `prisma migrate deploy` on startup | Separate migration job in CI/CD pipeline |
| BullMQ on single Redis instance | Redis Cluster or Redis Sentinel for HA |

The project architecture is sound — the operational gaps are tooling choices, not design flaws. Each component (outbox, delivery worker, retry, indexing) is independently deployable and scalable.

---

### "How would you extend this to support different delivery guarantees per subscription?"

**Phases**: 5, 8–9

The current system has a global retry policy (5 attempts, fixed backoff schedule). To support per-subscription policies, the extension points are:

1. Add `maxAttempts`, `backoffMultiplier`, `timeoutSeconds` columns to the `subscriptions` table
2. The retry worker already reloads the subscription from the database on each attempt (not from the job payload) — so it would read the per-subscription config automatically
3. The dead-letter threshold changes from the hardcoded 5 to `subscription.maxAttempts`
4. The signing algorithm (currently SHA-256 HMAC) could become a subscription field if different subscribers need different signing schemes

The webhook delivery headers and payload format are already per-subscription (each uses its own secret), so the transport layer needs no changes. The retry scheduling and dead-letter logic are the only components that need to become subscription-aware.
