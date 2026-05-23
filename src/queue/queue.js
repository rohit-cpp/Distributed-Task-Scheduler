import { Queue } from 'bullmq';
import { connection, defaultQueueOptions } from '../config/index.js';

export const taskQueue = new Queue('tasks', {
  ...defaultQueueOptions,
  defaultJobOptions: {
    ...defaultQueueOptions.defaultJobOptions,
    priority: 10,
  },
  limiter: {
    max: 100,
    duration: 60000,
  },
});
