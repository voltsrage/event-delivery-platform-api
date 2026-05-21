import Redis from "ioredis";

// lazyConnect: true - the client connects on the first command, not a import time.
// This prevents test failures when Redis is not running and the module is imported
// by test files that mock it before any command is issued.

export const redis = new Redis({
    host: process.env.REDIS_HOST ?? 'localhost',
    port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
    maxRetriesPerRequest: null,
    lazyConnect: true
})

// `maxRetriesPerRequest: null` mirrors the BullMQ connection config — without it, ioredis times out on blocking commands.