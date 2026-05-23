import { QueueEvents } from 'bullmq';
import { connection } from '../config/index.js';

export const taskEvents = new QueueEvents('tasks', { connection });

taskEvents.on('completed', ({ jobId }) => {
  console.log(`[COMPLETED] job ${jobId}`);
});

taskEvents.on('failed', ({ jobId, failedReason }) => {
  console.error(`[FAILED] job ${jobId} — ${failedReason}`);
});

taskEvents.on('progress', ({ jobId, data }) => {
  console.log(`[PROGRESS] job ${jobId} — ${data}%`);
});

taskEvents.on('stalled', ({ jobId }) => {
  console.warn(`[STALLED] job ${jobId}`);
});

taskEvents.on('waiting', ({ jobId }) => {
  console.log(`[WAITING] job ${jobId}`);
});

taskEvents.on('active', ({ jobId }) => {
  console.log(`[ACTIVE] job ${jobId}`);
});
