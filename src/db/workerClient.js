import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '../../generated/prisma/client.ts';

// The outbox worker is a platform-level process that reads ALL tenant's events.
// DATABASE_URL_WORKER connects as a PostgreSQL role with BYPASSRLS
// THe regular DATABASE URL role has RLS enforced (tenant-scoped per request)
// If DATABASE_URL_WORKER is not set, falls back to DATABASE_URL (development only)
const adapter = new PrismaPg({ connectionString: process.env.DATABASE_URL_WORKER ?? process.env.DATABASE_URL });

export const workerPrisma = new PrismaClient({ adapter });