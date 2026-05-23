import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import IORedis from 'ioredis';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const UPSTASH_REST_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

export const PORT = parseInt(process.env.PORT || '3000', 10);
export const DEFAULT_CONCURRENCY = parseInt(process.env.WORKER_CONCURRENCY || '5', 10);

function buildConnection() {
  if (UPSTASH_REST_URL && UPSTASH_TOKEN) {
    const host = UPSTASH_REST_URL.replace(/^https?:\/\//, '');
    return new IORedis({
      host,
      port: 6379,
      password: UPSTASH_TOKEN,
      tls: {},
      maxRetriesPerRequest: null,
      enableOfflineQueue: false,
    });
  }

  return new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null,
    enableOfflineQueue: false,
  });
}

export const connection = buildConnection();

export const defaultQueueOptions = {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: { age: 3600, count: 500 },
    removeOnFail: { age: 86400, count: 100 },
  },
};
