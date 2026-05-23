import { Router } from 'express';
import { taskQueue, taskFlow, createJobChain } from '../queue/index.js';

const router = Router();

router.post('/', async (req, res) => {
  try {
    const { type, payload, priority, delay, repeat, deduplication, jobId } = req.body;

    if (!type) {
      return res.status(400).json({ success: false, error: 'type is required' });
    }

    const opts = {};
    if (priority !== undefined) opts.priority = priority;
    if (delay !== undefined) opts.delay = delay;
    if (repeat !== undefined) opts.repeat = repeat;
    if (deduplication !== undefined) opts.deduplication = deduplication;
    if (jobId !== undefined) opts.jobId = jobId;

    const job = await taskQueue.add(type, { type, payload }, opts);

    res.status(201).json({ success: true, data: { jobId: job.id } });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

router.get('/', async (req, res) => {
  try {
    const counts = await taskQueue.getJobCounts();
    res.json({ success: true, data: counts });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

router.get('/:id', async (req, res) => {
  try {
    const job = await taskQueue.getJob(req.params.id);
    if (!job) {
      return res.status(404).json({ success: false, error: 'job not found' });
    }
    const state = await job.getState();
    res.json({
      success: true,
      data: {
        id: job.id,
        name: job.name,
        data: job.data,
        state,
        progress: job.progress,
        attemptsMade: job.attemptsMade,
        failedReason: job.failedReason,
        timestamp: job.timestamp,
        finishedOn: job.finishedOn,
        processedOn: job.processedOn,
        returnvalue: job.returnvalue,
      },
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

router.delete('/:id', async (req, res) => {
  try {
    const job = await taskQueue.getJob(req.params.id);
    if (!job) {
      return res.status(404).json({ success: false, error: 'job not found' });
    }
    await job.remove();
    res.json({ success: true, data: { removed: true } });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

router.post('/flow', async (req, res) => {
  try {
    const { parent, child } = req.body;
    if (!parent || !child) {
      return res.status(400).json({ success: false, error: 'parent and child objects are required' });
    }

    const tree = await createJobChain(
      parent.payload || parent,
      child.payload || child,
      parent.name || 'parent-task',
      child.name || 'child-task'
    );

    res.status(201).json({
      success: true,
      data: {
        parentJobId: tree.job.id,
        childJobId: tree.children[0].job.id,
      },
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

router.get('/flows/chain', async (req, res) => {
  try {
    const { parentJobId, childJobId } = req.query;
    const parent = await taskQueue.getJob(parentJobId);
    const child = await taskQueue.getJob(childJobId);
    res.json({
      success: true,
      data: {
        parent: parent ? { id: parent.id, state: await parent.getState() } : null,
        child: child ? { id: child.id, state: await child.getState() } : null,
      },
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

router.post('/pause', async (req, res) => {
  try {
    await taskQueue.pause();
    res.json({ success: true, data: { paused: true } });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

router.post('/resume', async (req, res) => {
  try {
    await taskQueue.resume();
    res.json({ success: true, data: { resumed: true } });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

router.delete('/queue', async (req, res) => {
  try {
    await taskQueue.obliterate({ force: true });
    res.json({ success: true, data: { obliterated: true } });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

export default router;
