import express from 'express';
import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { ExpressAdapter } from '@bull-board/express';

import { PORT } from './config/index.js';
import { taskQueue, taskWorker, taskEvents } from './queue/index.js';
import tasksRouter from './routes/tasks.js';
import healthRouter from './routes/health.js';

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

createBullBoard({
  queues: [new BullMQAdapter(taskQueue)],
  serverAdapter,
});

const app = express();
app.use(express.json());

app.get('/', (req, res) => {
  res.json({
    service: 'Smart Job Scheduler',
    version: '1.0.0',
    endpoints: {
      health: '/health',
      tasks: '/tasks',
      dashboard: '/admin/queues',
    },
  });
});

app.use('/tasks', tasksRouter);
app.use('/health', healthRouter);
app.use('/admin/queues', serverAdapter.getRouter());

const server = app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
  console.log(`Bull Board at http://localhost:${PORT}/admin/queues`);
});

async function shutdown(signal) {
  console.log(`\n${signal} received — shutting down gracefully...`);

  await taskWorker.pause();
  await taskWorker.close();
  await taskQueue.close();
  await taskEvents.close();

  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
