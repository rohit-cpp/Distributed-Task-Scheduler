import 'dotenv/config';
import IORedis from 'ioredis';

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
      maxRetriesPerRequest: 3,
      retryStrategy: times => Math.min(times * 50, 2000),
    });
  }
  return new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null,
  });
}

const connection = buildConnection();
const prefix = 'bull:tasks:';

async function cleanup() {
  let cursor = '0';
  let deleted = 0;

  do {
    const [nextCursor, keys] = await connection.scan(cursor, 'MATCH', `${prefix}*`, 'COUNT', '200');
    cursor = nextCursor;

    if (keys.length > 0) {
      await connection.del(...keys);
      deleted += keys.length;
      console.log(`  Deleted ${keys.length} keys (${deleted} total)`);
    }
  } while (cursor !== '0');

  console.log(`\nDone. Removed ${deleted} Redis keys.`);
  connection.disconnect();
}

cleanup().catch(err => {
  console.error('Cleanup failed:', err.message);
  process.exit(1);
});
