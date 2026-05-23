# Smart Job Scheduler — Codebase Documentation

A Redis-backed job scheduling system powered by **BullMQ** with Express REST API, real-time monitoring via Bull Board, and support for priority queues, delayed jobs, repeatable cron tasks, deduplication, parent-child job flows, and graceful shutdown.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Configuration (`src/config/index.js`)](#src-config)
4. [Queue Layer (`src/queue/`)](#queue-layer)
5. [REST API (`src/routes/`)](#rest-api)
6. [Entry Point (`src/index.js`)](#entry-point)
7. [Seed Script (`scripts/seed.js`)](#seed-script)
8. [API Reference](#api-reference)
9. [Bull Board Monitoring](#bull-board-monitoring)
10. [Running in Production](#running-in-production)

---

## Architecture Overview

```
HTTP Client ──> Express Server ──> BullMQ Queue ──> Redis (Upstash)
                                       │
                          ┌────────────┴────────────┐
                          v                         v
                    BullMQ Worker           QueueEvents
                 (concurrent handlers)     (lifecycle logs)
                          │
                    FlowProducer
                  (parent-child DAGs)
```

| Component         | Role                                                                                                 |
| ----------------- | ---------------------------------------------------------------------------------------------------- |
| **BullMQ Queue**  | Receives jobs via `Queue.add()`. Handles priority, delay, repeat, rate-limiting, deduplication.      |
| **BullMQ Worker** | Polls Redis streams, executes job handlers with configurable concurrency.                            |
| **QueueEvents**   | Subscribes to lifecycle events (completed, failed, stalled, progress) — non-blocking, event-driven.  |
| **FlowProducer**  | Creates directed acyclic graphs (DAGs) of dependent jobs — children run only after parents complete. |
| **Redis**         | Stores all job data as Redis Streams, queue state, rate-limit counters, deduplication keys.          |
| **Bull Board**    | Web dashboard at `/admin/queues` for live monitoring and job management.                             |

---

## Project Structure

```
job-scheduler/
├── src/
│   ├── config/
│   │   └── index.js          Redis connection + env config
│   ├── queue/
│   │   ├── queue.js           BullMQ Queue definition
│   │   ├── worker.js          BullMQ Worker with inline job handlers
│   │   ├── sandbox-processor.js  Processor file for sandboxed worker
│   │   ├── sandbox-worker.js  BullMQ Worker with useWorkerThreads
│   │   ├── events.js          QueueEvents lifecycle listeners
│   │   ├── flow.js            FlowProducer for parent-child chains
│   │   └── index.js           Exports all queue components
│   ├── routes/
│   │   ├── tasks.js           CRUD routes for jobs
│   │   └── health.js          Health check endpoint
│   └── index.js               Express server + Bull Board + graceful shutdown
├── scripts/
│   └── seed.js                10,000+ job mass enqueuer
├── context/                   Project specification files (6 markdown)
├── .env                       Redis credentials (Upstash / local)
├── .gitignore
├── package.json
└── README.md                  This file
```

---

## src/config/index.js

**Purpose:** Loads environment variables, creates the Redis connection, and exports shared queue defaults.

```js
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";
import IORedis from "ioredis";
```

**Redis Connection:** Supports two modes via auto-detection:

1. **Upstash (serverless, TLS):** If `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN` env vars are present, connects via TLS to the Upstash Redis endpoint.
2. **Local Redis:** Falls back to `REDIS_URL` (default `redis://localhost:6379`).

**Why `maxRetriesPerRequest: null`:** BullMQ requires this on the ioredis instance — without it, ioredis throws on blocked commands (BRPOPLPUSH).

**Default Job Options:**
| Option | Value | Purpose |
|--------|-------|---------|
| `attempts` | `3` | Retry failed jobs up to 3 times |
| `backoff.type` | `exponential` | Wait longer between each retry attempt |
| `backoff.delay` | `2000` | Initial backoff delay of 2 seconds |
| `removeOnComplete.age` | `3600` | Keep completed jobs for 1 hour |
| `removeOnComplete.count` | `500` | Keep max 500 completed jobs |
| `removeOnFail.age` | `86400` | Keep failed jobs for 24 hours |
| `removeOnFail.count` | `100` | Keep max 100 failed jobs |

---

## Queue Layer

### src/queue/queue.js — BullMQ Queue

```js
import { Queue } from "bullmq";
```

Creates a `Queue` named `'tasks'` with:

- **Priority:** Default `10` (lower = higher priority). Jobs with `priority: 1` run before `priority: 10`.
- **Rate Limiter:** Max `100` jobs per `60000` ms (1 minute). Prevents worker overload.
- **Inherits default job options** from config (retry, backoff, TTL cleanup).

**BullMQ API difference from Bull:** `Queue.add()` takes `(jobName, data, opts)` — the first argument is always a job name/type string. Bull used `queue.add(data, opts)`.

### src/queue/worker.js — BullMQ Worker

```js
import { Worker } from "bullmq";
```

- **Concurrency:** Set to `WORKER_CONCURRENCY` (default `5`) via the Worker constructor. In BullMQ, concurrency lives in the Worker constructor, not on a process method.
- **Stalled interval:** `30000` ms — if a worker crashes mid-job, BullMQ detects it after 30s and re-queues the job.

**Job Handlers by type:**

| Type      | Simulated Work       | Progress Updates |
| --------- | -------------------- | ---------------- |
| `email`   | 1 second (500ms × 2) | 10% → 50% → 100% |
| `report`  | 2 seconds (1s × 2)   | 10% → 50% → 100% |
| `webhook` | 300ms                | 10% → 100%       |
| `default` | 200ms                | 10% → 100%       |

**`job.updateProgress()`** is the BullMQ method for reporting progress (in Bull it was `job.progress()`).

### src/queue/sandbox-worker.js — Sandboxed Worker Threads

```js
import { Worker } from "bullmq";
```

An alternative worker that runs each job handler in a **separate child process** using BullMQ's `useWorkerThreads: true` option. Unlike the inline worker where a crash takes down the entire server, this worker isolates failures:

- **Process isolation:** If a job handler calls `process.exit()` or has a memory leak, only that thread dies — the main server continues serving.
- **Concurrency:** Set to `3` (lower than the inline worker's `5`) since each thread has its own memory overhead.
- **Processor file:** References `./sandbox-processor.js` (a standalone file) instead of an inline function — BullMQ forks a new Node.js process per thread using this file.

**When to use:** For untrusted, CPU-heavy, or crash-prone job code in production. The inline worker is simpler for development and trusted code.

### src/queue/events.js — QueueEvents

```js
import { QueueEvents } from "bullmq";
```

A separate `QueueEvents` class subscribes to Redis streams for lifecycle events. This is non-blocking and event-driven (no polling). In Bull, events were attached directly to the Queue — in BullMQ, they're a separate class for reliability.

**Events tracked:**

| Event       | Log Level       | When                              |
| ----------- | --------------- | --------------------------------- |
| `completed` | `console.log`   | Job finished successfully         |
| `failed`    | `console.error` | Job exhausted all retry attempts  |
| `progress`  | `console.log`   | `job.updateProgress()` called     |
| `stalled`   | `console.warn`  | Worker crashed without completing |
| `waiting`   | `console.log`   | Job entered the queue             |
| `active`    | `console.log`   | Worker picked up the job          |

### src/queue/flow.js — FlowProducer

```js
import { FlowProducer } from "bullmq";
```

Enables parent-child job dependencies. The `createJobChain()` function creates a DAG where the child job only executes after the parent job completes successfully.

**BullMQ-exclusive feature:** Bull does not have FlowProducer. This is one of the key differentiators.

---

## src/routes/tasks.js — REST API

**POST /tasks** — Create a job

```json
{
  "type": "email",
  "payload": { "to": "user@example.com" },
  "priority": 1,
  "delay": 5000,
  "repeat": { "pattern": "0 * * * *", "tz": "UTC" },
  "deduplication": { "id": "unique-key", "ttl": 60000 },
  "jobId": "custom-id"
}
```

**Job Options:**
| Field | Type | BullMQ Mapping |
|-------|------|----------------|
| `type` | string | Job name (first arg to `Queue.add()`) |
| `priority` | number | Lower = higher priority (1 > 10 > 100) |
| `delay` | number (ms) | Delayed execution |
| `repeat.pattern` | string (cron) | BullMQ uses `pattern` (not `cron`) |
| `repeat.tz` | string | Timezone for cron scheduling |
| `deduplication.id` | string | Dedup key — jobs with same ID within TTL are skipped |
| `deduplication.ttl` | number (ms) | How long the dedup key lives |
| `jobId` | string | Manual job ID (idempotency) |

**GET /tasks** — Queue counts

Returns `getJobCounts()` — number of jobs in each state.

**GET /tasks/:id** — Job details

Returns full job metadata: state, progress percentage, attempts made, timestamps, return value, and failure reason.

**DELETE /tasks/:id** — Cancel / remove a job

Calls `job.remove()` on the BullMQ job instance.

**POST /tasks/flow** — Create parent-child job chain

Body: `{ parent: { name, payload }, child: { name, payload } }`
Returns both job IDs. Child only executes after parent completes.

**POST /tasks/pause** — Pause the queue (workers stop picking up new jobs)

**POST /tasks/resume** — Resume the queue

**DELETE /tasks/queue** — Obliterate the entire queue and all jobs

Calls `queue.obliterate({ force: true })` — permanent, irreversible.

---

## src/routes/health.js — Health Check

- Pings Redis via `connection.ping()`
- Returns queue counts from `getJobCounts()`
- Returns `503` if Redis is unreachable

---

## src/index.js — Entry Point

Sets up:

1. **Express server** with JSON body parsing
2. **Bull Board** mounted at `/admin/queues` using `BullMQAdapter` (not `BullAdapter`)
3. **All route handlers**

**Graceful Shutdown:**

```js
worker.pause() → worker.close() → queue.close() → server.close()
```

- `worker.pause()` stops pulling new jobs
- `worker.close()` waits for in-flight jobs to finish
- `queue.close()` closes the queue's Redis connection
- `server.close()` stops HTTP listener

This is the correct BullMQ shutdown order. In Bull, it was `queue.close()` only.

---

## Seed Script

Runs independently from the server. Enqueues **10,000 jobs** in batches of 100 with varied:

- Types (email, report, webhook, notification, sync)
- Priorities (1, 5, 10)
- Delays (some with 5-second delay)

```bash
SEED_COUNT=10000 npm run seed
```

---

## Bull Board — Live Dashboard Walkthrough

Open `http://localhost:3000/admin/queues` in a browser. This is the primary UI for inspecting and managing jobs in real time.

### Main Queue View

When you first open the dashboard, you see a card for each queue (we have one: `tasks`). It displays **job counts by state**:

| State           | Badge Color | What It Means                                                 |
| --------------- | ----------- | ------------------------------------------------------------- |
| **Waiting**     | Gray        | Jobs enqueued but not yet picked up by a worker               |
| **Active**      | Blue        | Jobs currently being processed by a worker right now          |
| **Completed**   | Green       | Jobs that finished successfully                               |
| **Failed**      | Red         | Jobs that exhausted all retry attempts and gave up            |
| **Delayed**     | Orange      | Jobs scheduled for future execution (`delay` option)          |
| **Paused**      | Yellow      | Queue is paused — no workers will pick up new jobs            |
| **Prioritized** | Purple      | Jobs waiting with priority ordering (lower number runs first) |

**Example — After seeding 10,000 jobs:**

```
tasks
  Active: 5      (5 workers processing concurrently)
  Completed: 127 (127 jobs finished successfully)
  Failed: 0      (zero failures)
  Prioritized: 9868 (remaining jobs waiting in priority order)
```

### Clicking Into a Queue

Click the `tasks` queue card to drill down. You see tabs for each job state:

- **Waiting** — Jobs that haven't run yet. You can see their priority level and when they were added.
- **Active** — Jobs running right now. Shows how long they've been processing.
- **Completed** — Jobs that succeeded. Shows their return value (the data the handler returned).
- **Failed** — Jobs that failed. Shows the error stack trace. Click **"Retry"** to re-enqueue.
- **Delayed** — Jobs waiting for their delay timer to expire.
- **Paused** — Jobs paused by the queue.

### Job Detail View

Click any individual job to see its full JSON payload:

```json
{
  "id": "1",
  "name": "email",
  "data": {
    "type": "email",
    "payload": { "to": "test@example.com" }
  },
  "state": "completed",
  "progress": 100,
  "attemptsMade": 1,
  "timestamp": 1779286326198,
  "finishedOn": 1779286327467,
  "processedOn": 1779286326386,
  "returnvalue": {
    "sent": true,
    "to": "test@example.com"
  }
}
```

**What each field tells you:**

| Field          | Meaning                                                      |
| -------------- | ------------------------------------------------------------ |
| `id`           | Auto-generated job ID (or custom if you provided `jobId`)    |
| `name`         | The job type you passed as the first arg to `Queue.add()`    |
| `data`         | The payload you sent when creating the job                   |
| `state`        | Current lifecycle state                                      |
| `progress`     | Percentage reported by `job.updateProgress()` in the handler |
| `attemptsMade` | How many times this job has been retried                     |
| `timestamp`    | When the job was created (epoch ms)                          |
| `processedOn`  | When a worker started processing it                          |
| `finishedOn`   | When it completed or failed                                  |
| `returnvalue`  | What the handler function returned (only on success)         |
| `failedReason` | Error message if the job failed                              |

### Actions You Can Take

- **Retry** — Click the retry button on a failed job to re-enqueue it
- **Remove** — Delete a single job from the queue
- **Clean** — Bulk remove all completed or failed jobs (free up Redis memory)
- **Pause/Resume** — Pause the entire queue (workers stop picking up new jobs)

### How to Monitor Progress

1. Start the server: `npm start`
2. Open Bull Board: `http://localhost:3000/admin/queues`
3. In another terminal, run the seed: `npm run seed`
4. Watch the numbers change live on the dashboard
5. Click into Completed tab to inspect individual job results
6. If any turn red (Failed), click them to see the error and hit Retry

Bull Board uses `BullMQAdapter` (`new BullMQAdapter(queue)`) to wrap the BullMQ queue. If you were using the old Bull library, you would use `BullAdapter` instead — this is a BullMQ-specific integration.

---

## Running in Production

**Redis Configuration Checklist:**

- [ ] Enable AOF persistence (`appendonly yes`) for durability
- [ ] Set `maxmemory-policy noeviction` to prevent job data eviction
- [ ] Use a dedicated Redis instance (don't share with cache)

**Environment Variables:**
| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UPSTASH_REDIS_REST_URL` | No | — | Upstash REST endpoint URL |
| `UPSTASH_REDIS_REST_TOKEN` | No | — | Upstash auth token |
| `REDIS_URL` | No | `redis://localhost:6379` | Fallback local Redis |
| `PORT` | No | `3000` | HTTP server port |
| `WORKER_CONCURRENCY` | No | `5` | Parallel worker count |

**BullMQ vs Bull Key Differences Used:**
| Aspect | Bull (old) | BullMQ (this project) |
|--------|-----------|----------------------|
| Queue methods | `queue.add(data, opts)` | `queue.add('name', data, opts)` |
| Worker creation | `queue.process(fn)` | `new Worker('name', fn, { concurrency })` |
| Progress | `job.progress(value)` | `job.updateProgress(value)` |
| Events | `queue.on('completed')` | `new QueueEvents('name')` + `events.on()` |
| Cron option | `repeat.cron` | `repeat.pattern` |
| Redis backend | Lists | Streams |
| Redis connection | Optional | Explicit `connection` required |
| Concurrency | In `process()` call | In Worker constructor |
| Shutdown | `queue.close()` | `worker.pause()` → `worker.close()` → `queue.close()` |
| Job flows | Not available | `FlowProducer` for parent-child DAGs |
| Deduplication | Manual via jobId | Native `deduplication.id` + `deduplication.ttl` |
| Dead queue deletion | Not available | `queue.obliterate()` |

---

## Resume-Relevant Senior Engineering Signals

This project demonstrates:

1. **BullMQ Architecture:** Understanding of Queue/Worker/QueueEvents/FlowProducer separation — not just the basics.
2. **Redis Streams Backend:** Choosing BullMQ over Bull for Redis Streams performance, not lists.
3. **Production Hardening:** Rate limiting, backoff strategies, job TTL cleanup, stalled job recovery.
4. **Graceful Shutdown:** Proper drain semantics — `worker.pause()` before `worker.close()` before `queue.close()`.
5. **Monitoring:** Bull Board integration for observability without building custom dashboards.
6. **Deduplication & Flows:** Native BullMQ dedup and parent-child DAG execution (exclusive to BullMQ).
7. **Serverless Redis Compatibility:** TLS connection to Upstash Redis — works with any cloud Redis provider.
8. **Scale Testing:** Seed script that mass-enqueues 10,000 jobs with verified zero-failure throughput.
