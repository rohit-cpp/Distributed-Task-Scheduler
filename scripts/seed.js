import 'dotenv/config';
import IORedis from 'ioredis';
import { Queue } from 'bullmq';

const UPSTASH_REST_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

function buildConnection() {
  if (UPSTASH_REST_URL && UPSTASH_TOKEN) {
    const host = UPSTASH_REST_URL.replace(/^https?:\/\//, '');
    return new IORedis({
      host,
      port: 6379,
      password: UPSTASH_TOKEN,
      tls: {},
      maxRetriesPerRequest: null,
    });
  }
  return new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null,
  });
}

const connection = buildConnection();
const queue = new Queue('tasks', { connection });

const TYPES = ['email', 'report', 'webhook', 'notification', 'sync'];

async function seed() {
  const count = parseInt(process.env.SEED_COUNT || '10000', 10);
  const batchSize = 100;

  console.log(`Seeding ${count} jobs...`);

  for (let i = 0; i < count; i += batchSize) {
    const batch = [];
    const end = Math.min(i + batchSize, count);

    for (let j = i; j < end; j++) {
      const type = TYPES[j % TYPES.length];
      const priority = j % 3 === 0 ? 1 : j % 3 === 1 ? 5 : 10;
      const delay = j % 10 === 0 ? 5000 : 0;

      batch.push(
        queue.add(type, {
          type,
          payload: {
            id: `seed-${j}`,
            to: `user${j}@example.com`,
            count: Math.floor(Math.random() * 1000),
            url: `https://hook.example.com/${j}`,
          },
        }, {
          priority,
          delay,
        })
      );
    }

    await Promise.all(batch);

    if ((i + batchSize) % 1000 === 0 || i + batchSize >= count) {
      console.log(`  ${Math.min(i + batchSize, count)} / ${count} enqueued`);
    }
  }

  const counts = await queue.getJobCounts();
  console.log('\nDone. Queue state:', counts);

  await queue.close();
  connection.disconnect();
}

seed().catch(err => {
  console.error('Seed failed:', err);
  process.exit(1);
});
