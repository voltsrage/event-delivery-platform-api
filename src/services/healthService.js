import { prisma } from '../db/prisma.js';
import { redis } from '../db/redis.js';
import kafka from '../kafka/client.js';
import esClient from '../search/esClient.js';

async function checkPostgres() {
    await prisma.$queryRaw`SELECT 1`;
}

async function checkKafka() {
    const admin = kafka.admin();
    await admin.connect();
    await admin.disconnect();
}

async function checkElasticsearch() {
    const ok = await esClient.ping();
    if (!ok) throw new Error('Elasticsearch ping returned false');
}

async function checkRedis() {
    const reply = await redis.ping();
    if (reply !== 'PONG') throw new Error(`Unexpected Redis ping reply: ${reply}`);
}

export async function getReadiness() {
    const [pg, kfk, es, rd] = await Promise.allSettled([
        checkPostgres(),
        checkKafka(),
        checkElasticsearch(),
        checkRedis(),
    ]);

    const toStatus = (result) => result.status === 'fulfilled' ? 'healthy' : 'error';

    const checks = {
        postgres:      toStatus(pg),
        kafka:         toStatus(kfk),
        elasticsearch: toStatus(es),
        redis:         toStatus(rd),
    };

    const healthy = Object.values(checks).every((s) => s === 'healthy');

    return {
        status: healthy ? 'healthy' : 'degraded',
        checks,
    };
}