import 'dotenv/config';
import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '../../generated/prisma/client';
import crypto from 'crypto';

const adapter = new PrismaPg({ connectionString: process.env.DATABASE_URL });
const prisma = new PrismaClient({ adapter });

function generateApiKey() {
  const rawKey   = `sk_live_${crypto.randomBytes(32).toString('hex')}`;
  const keyHash  = crypto.createHash('sha256').update(rawKey).digest('hex');
  const keyPrefix = rawKey.slice(0, 15);
  return { rawKey, keyHash, keyPrefix };
}

function generateSecret() {
  const rawSecret    = crypto.randomBytes(32).toString('hex');
  const secretHash   = crypto.createHash('sha256').update(rawSecret).digest('hex');
  const secretPrefix = rawSecret.slice(0, 8);
  return { rawSecret, secretHash, secretPrefix };
}

async function seed() {
  // Clear in reverse dependency order to satisfy foreign key constraints
  await prisma.deadLetter.deleteMany();
  await prisma.deliveryAttempt.deleteMany();
  await prisma.event.deleteMany();
  await prisma.subscription.deleteMany();
  await prisma.topic.deleteMany();
  await prisma.apiKey.deleteMany();
  await prisma.tenant.deleteMany();

  // Two tenants — required for RLS isolation verification in Step 6
  const tenants = await Promise.all([
    prisma.tenant.create({ data: { name: 'Acme Corp',  email: 'admin@acme.example.com'   } }),
    prisma.tenant.create({ data: { name: 'Globex Inc', email: 'admin@globex.example.com' } }),
  ]);

  // One API key per tenant (raw key printed to stdout for manual testing)
  for (const tenant of tenants) {
    const { rawKey, keyHash, keyPrefix } = generateApiKey();
    await prisma.apiKey.create({
      data: { tenantId: tenant.id, keyHash, keyPrefix, label: 'Default' },
    });
    console.log(`Tenant "${tenant.name}" (${tenant.id}) — API key: ${rawKey}`);
  }

  // Three topics per tenant
  const topicNames = ['order.created', 'payment.failed', 'shipment.updated'];
  const allTopics  = [];

  for (const tenant of tenants) {
    for (const name of topicNames) {
      const topic = await prisma.topic.create({
        data: { tenantId: tenant.id, name, description: `${name} events` },
      });
      allTopics.push({ tenant, topic });
    }
  }

  // Two subscriptions per topic
  for (const { tenant, topic } of allTopics) {
    for (let i = 1; i <= 2; i++) {
      const { secretHash, secretPrefix } = generateSecret();
      const slug = tenant.name.toLowerCase().replace(/\s+/g, '');
      await prisma.subscription.create({
        data: {
          tenantId:    tenant.id,
          topicId:     topic.id,
          endpointUrl: `https://webhook.${slug}.example.com/hook-${i}`,
          secretHash,
          secretPrefix,
          enabled: true,
        },
      });
    }
  }

  // 50 events per tenant — published_to_kafka = false (outbox worker not yet running)
  for (const tenant of tenants) {
    const tenantTopics = allTopics.filter(t => t.tenant.id === tenant.id).map(t => t.topic);
    for (let i = 0; i < 50; i++) {
      const topic = tenantTopics[i % tenantTopics.length];
      await prisma.event.create({
        data: {
          tenantId:        tenant.id,
          topicId:         topic.id,
          eventType:       topic.name,
          payload:         { index: i, tenantName: tenant.name, sample: true },
          idempotencyKey:  `seed-${tenant.id}-${topic.id}-${i}`,
          publishedToKafka: false,
        },
      });
    }
  }

  console.log(`Seeded: ${tenants.length} tenants, ${allTopics.length} topics, 12 subscriptions, 100 events`);
  await prisma.$disconnect();
}

seed().catch(err => { console.error(err); process.exit(1); });