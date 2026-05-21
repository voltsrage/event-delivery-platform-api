// Runs after `prisma migrate deploy` to grant table-level permissions to the
// application roles. Uses the postgres superuser connection (DATABASE_URL in
// the migrate service) so it has rights to GRANT on objects it didn't create.
// Idempotent — PostgreSQL emits a NOTICE on duplicate grants, never an error.
import pg from 'pg';

const { Client } = pg;

const appUser    = process.env.APP_DB_USER    ?? 'api_user';
const workerUser = process.env.WORKER_DB_USER ?? 'outbox_worker';

const client = new Client({ connectionString: process.env.DATABASE_URL });
await client.connect();

// GRANT ON ALL TABLES covers every table that Prisma just created.
// ALTER DEFAULT PRIVILEGES covers tables added by future migrations.
await client.query(`
    GRANT USAGE ON SCHEMA public TO ${appUser}, ${workerUser};

    GRANT SELECT, INSERT, UPDATE, DELETE
        ON ALL TABLES IN SCHEMA public
        TO ${appUser}, ${workerUser};

    GRANT USAGE, SELECT
        ON ALL SEQUENCES IN SCHEMA public
        TO ${appUser}, ${workerUser};

    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES
        TO ${appUser}, ${workerUser};

    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT USAGE, SELECT ON SEQUENCES
        TO ${appUser}, ${workerUser};
`);

await client.end();
console.log('[migrate] table permissions granted to', appUser, 'and', workerUser);
