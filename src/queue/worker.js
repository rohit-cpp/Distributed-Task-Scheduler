import { Worker } from 'bullmq';
import { connection, DEFAULT_CONCURRENCY } from '../config/index.js';

async function processTask(job) {
  const { type, payload } = job.data;

  job.updateProgress(10);

  switch (type) {
    case 'email': {
      await sleep(500);
      job.updateProgress(50);
      await sleep(500);
      job.updateProgress(100);
      return { sent: true, to: payload.to };
    }
    case 'report': {
      await sleep(1000);
      job.updateProgress(50);
      await sleep(1000);
      job.updateProgress(100);
      return { generated: true, rows: payload.count || 100 };
    }
    case 'webhook': {
      await sleep(300);
      job.updateProgress(100);
      return { called: true, url: payload.url };
    }
    default: {
      await sleep(200);
      job.updateProgress(100);
      return { processed: true, type };
    }
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export const taskWorker = new Worker('tasks', processTask, {
  connection,
  concurrency: DEFAULT_CONCURRENCY,
  stalledInterval: 30000,
});
