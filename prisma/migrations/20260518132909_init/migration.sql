-- CreateTable
CREATE TABLE "tenants" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "name" VARCHAR(200) NOT NULL,
    "email" VARCHAR(320) NOT NULL,
    "max_subscriptions" INTEGER NOT NULL DEFAULT 100,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "tenants_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "api_keys" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "tenant_id" UUID NOT NULL,
    "key_hash" VARCHAR(64) NOT NULL,
    "key_prefix" VARCHAR(15) NOT NULL,
    "label" VARCHAR(100) NOT NULL DEFAULT 'Default',
    "last_used_at" TIMESTAMPTZ,
    "revoked_at" TIMESTAMPTZ,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "api_keys_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "topics" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "tenant_id" UUID NOT NULL,
    "name" VARCHAR(200) NOT NULL,
    "description" TEXT NOT NULL,
    "deleted_at" TIMESTAMPTZ NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "topics_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "subscriptions" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "tenant_id" UUID NOT NULL,
    "topic_id" UUID NOT NULL,
    "endpoint_url" TEXT NOT NULL,
    "secret_hash" VARCHAR(64) NOT NULL,
    "secret_prefix" VARCHAR(10) NOT NULL,
    "enabled" BOOLEAN NOT NULL DEFAULT true,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "subscriptions_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "events" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "tenant_id" UUID NOT NULL,
    "topic_id" UUID NOT NULL,
    "event_type" VARCHAR(200) NOT NULL,
    "payload" JSONB NOT NULL,
    "idempotency_key" VARCHAR(200),
    "published_to_kafka" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "events_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "delivery_attempts" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "event_id" UUID NOT NULL,
    "subscription_id" UUID NOT NULL,
    "tenant_id" UUID NOT NULL,
    "attempt_number" SMALLINT NOT NULL DEFAULT 1,
    "status" VARCHAR(20) NOT NULL DEFAULT 'pending',
    "http_status" SMALLINT,
    "response_body" TEXT,
    "duration_ms" INTEGER,
    "next_retry_at" TIMESTAMPTZ,
    "attempted_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "delivery_attempts_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "dead_letters" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "event_id" UUID NOT NULL,
    "subscription_id" UUID NOT NULL,
    "tenant_id" UUID NOT NULL,
    "total_attempts" SMALLINT NOT NULL,
    "last_error" TEXT,
    "resolved_at" TIMESTAMPTZ,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "dead_letters_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "tenants_email_key" ON "tenants"("email");

-- CreateIndex
CREATE UNIQUE INDEX "api_keys_key_hash_key" ON "api_keys"("key_hash");

-- CreateIndex
CREATE UNIQUE INDEX "topics_tenant_id_name_key" ON "topics"("tenant_id", "name");

-- AddForeignKey
ALTER TABLE "api_keys" ADD CONSTRAINT "api_keys_tenant_id_fkey" FOREIGN KEY ("tenant_id") REFERENCES "tenants"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "topics" ADD CONSTRAINT "topics_tenant_id_fkey" FOREIGN KEY ("tenant_id") REFERENCES "tenants"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "subscriptions" ADD CONSTRAINT "subscriptions_tenant_id_fkey" FOREIGN KEY ("tenant_id") REFERENCES "tenants"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "subscriptions" ADD CONSTRAINT "subscriptions_topic_id_fkey" FOREIGN KEY ("topic_id") REFERENCES "topics"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "events" ADD CONSTRAINT "events_tenant_id_fkey" FOREIGN KEY ("tenant_id") REFERENCES "tenants"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "events" ADD CONSTRAINT "events_topic_id_fkey" FOREIGN KEY ("topic_id") REFERENCES "topics"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "delivery_attempts" ADD CONSTRAINT "delivery_attempts_event_id_fkey" FOREIGN KEY ("event_id") REFERENCES "events"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "delivery_attempts" ADD CONSTRAINT "delivery_attempts_tenant_id_fkey" FOREIGN KEY ("tenant_id") REFERENCES "tenants"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "delivery_attempts" ADD CONSTRAINT "delivery_attempts_subscription_id_fkey" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "dead_letters" ADD CONSTRAINT "dead_letters_event_id_fkey" FOREIGN KEY ("event_id") REFERENCES "events"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "dead_letters" ADD CONSTRAINT "dead_letters_subscription_id_fkey" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "dead_letters" ADD CONSTRAINT "dead_letters_tenant_id_fkey" FOREIGN KEY ("tenant_id") REFERENCES "tenants"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- api_keys: partial index — revoked keys fall out of this index immediately on revocation
CREATE INDEX "idx_api_keys_hash" ON "api_keys"("key_hash") WHERE revoked_at IS NULL;
CREATE INDEX "idx_api_keys_tenants" ON "api_keys"("tenant_id");

-- topics: partial index — soft-deleted topics are invisible to the poller
CREATE INDEX "idx_topics_tenant" ON "topics"("tenant_id") WHERE deleted_at IS NULL;

-- subscriptions
CREATE INDEX idx_subscriptions_topic ON subscriptions(topic_id) WHERE enabled = TRUE;
CREATE INDEX idx_subscriptions_tenant ON subscriptions(tenant_id);

-- events: partial index for outbox poller — published events leave the index automatically
CREATE UNIQUE INDEX idx_events_idempotency ON events(tenant_id,idempotency_key)
    WHERE idempotency_key IS NOT NULL;  
CREATE INDEX idx_events_outbox ON events(created_at ASC) WHERE published_to_kafka = FALSE;
CREATE INDEX idx_events_topic ON events(topic_id, created_at DESC);
CREATE INDEX idx_events_tenant ON events(tenant_id, created_at DESC);

-- delivery_attempts
CREATE INDEX idx_attempts_event ON delivery_attempts(event_id);
CREATE INDEX idx_attempts_subscription ON delivery_attempts(subscription_id, attempted_at DESC);
CREATE INDEX idx_attempts_tenant ON delivery_attempts(tenant_id, attempted_at DESC);

-- Partial index: retry poller only needs failed attempts with a scheduled retry time
CREATE INDEX idx_attempts_retry ON delivery_attempts(next_retry_at ASC)
    WHERE status = 'failed' AND next_retry_at IS NOT NULL;

-- dead_letters: partial index — resolved records fall out, keeping the index small
CREATE INDEX idx_dead_letters_tenant ON dead_letters(tenant_id, created_at DESC)
    WHERE resolved_at IS NULL;

-- Enable RLS on every tenant-scoped table
ALTER TABLE topics ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE delivery_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE dead_letters ENABLE ROW LEVEL SECURITY;

-- Policy: a row is visible only when its tenant_id matches the session-local variable.
-- The Fastify auth middleware calls SET LOCAL app.current_tenant_id = '<uuid>'
-- inside every database transaction before any business query runs.

CREATE POLICY tenant_isolation ON topics
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

CREATE POLICY tenant_isolation ON subscriptions
    USING(tenant_id = current_setting('app.current_tenant_id')::uuid);

CREATE POLICY tenant_isolation ON events
    USING(tenant_id = current_setting('app.current_tenant_id')::uuid);

CREATE POLICY tenant_isolation ON delivery_attempts
    USING(tenant_id = current_setting('app.current_tenant_id')::uuid);

CREATE POLICY tenant_isolation ON dead_letters
    USING(tenant_id = current_setting('app.current_tenant_id')::uuid);