import { Worker } from 'bullmq';
import path from 'path';
import { fileURLToPath } from 'url';
import { connection } from '../config/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const sandboxWorker = new Worker('tasks', path.resolve(__dirname, './sandbox-processor.js'), {
  connection,
  concurrency: 3,
  useWorkerThreads: true,
  stalledInterval: 30000,
});
