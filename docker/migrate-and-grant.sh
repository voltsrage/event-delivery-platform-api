#!/bin/sh
# Entrypoint for the `migrate` Docker Compose service.
# Runs Prisma migrations then grants table-level permissions to the app roles.
# Both steps use DATABASE_URL which must be the postgres superuser connection.
set -e

echo '[migrate] running Prisma migrations...'
node_modules/.bin/prisma migrate deploy

echo '[migrate] granting table permissions...'
node docker/grant-permissions.js

echo '[migrate] done'
