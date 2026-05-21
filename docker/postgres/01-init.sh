#!/bin/bash
# Runs once on first postgres container startup (when data directory is empty).
# Creates the two application roles used by the platform:
#   api_user     — RLS enforced (normal user); used by the Fastify API
#   outbox_worker — BYPASSRLS; used by background workers that must see all tenants' data
set -e

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname   "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE ${APP_DB_USER} WITH LOGIN PASSWORD '${APP_DB_PASSWORD}';
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${APP_DB_USER};

    CREATE ROLE ${WORKER_DB_USER} WITH LOGIN PASSWORD '${WORKER_DB_PASSWORD}' BYPASSRLS;
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${WORKER_DB_USER};
EOSQL
