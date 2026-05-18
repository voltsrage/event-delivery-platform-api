-- DropIndex
DROP INDEX "idx_api_keys_tenants";

-- DropIndex
DROP INDEX "idx_attempts_event";

-- DropIndex
DROP INDEX "idx_attempts_subscription";

-- DropIndex
DROP INDEX "idx_attempts_tenant";

-- DropIndex
DROP INDEX "idx_events_tenant";

-- DropIndex
DROP INDEX "idx_events_topic";

-- DropIndex
DROP INDEX "idx_subscriptions_tenant";

-- AlterTable
ALTER TABLE "topics" ALTER COLUMN "description" DROP NOT NULL,
ALTER COLUMN "deleted_at" DROP NOT NULL;
