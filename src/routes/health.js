import { Router } from 'express';
import { taskQueue } from '../queue/index.js';
import { connection } from '../config/index.js';

const router = Router();

router.get('/', async (req, res) => {
  try {
    await connection.ping();
    const counts = await taskQueue.getJobCounts();
    res.json({
      success: true,
      data: {
        status: 'healthy',
        redis: 'connected',
        queue: counts,
      },
    });
  } catch (err) {
    res.status(503).json({
      success: false,
      data: {
        status: 'unhealthy',
        redis: 'disconnected',
        error: err.message,
      },
    });
  }
});

export default router;
