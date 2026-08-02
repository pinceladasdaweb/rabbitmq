import os from 'node:os'
import assert from 'node:assert/strict'
import { fileURLToPath } from 'node:url'
import { test, describe } from 'node:test'
import WorkerPool from '../src/consumers/worker-pool.js'
import { recordingLogger, silentLogger, waitFor } from './helpers.js'

const WORKER_FILE = fileURLToPath(new URL('./fixtures/echo-worker.mjs', import.meta.url))
const THROWING_WORKER = fileURLToPath(new URL('./fixtures/throwing-worker.mjs', import.meta.url))

describe('WorkerPool', () => {
  test('runs payloads through workers and returns results', async (t) => {
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 2, logger: silentLogger })
    t.after(() => pool.terminate())

    const result = await pool.run({ content: { id: 1 } })

    assert.equal(result.success, true)
    assert.deepEqual(result.echo, { id: 1 })
  })

  test('queues work when all workers are busy and completes everything', async (t) => {
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 2, logger: silentLogger })
    t.after(() => pool.terminate())

    const results = await Promise.all(
      Array.from({ length: 10 }, (_, index) => pool.run({ content: index }))
    )

    assert.equal(results.length, 10)
    assert.ok(results.every(result => result.success))
    assert.deepEqual(results.map(result => result.echo).sort((a, b) => a - b), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
  })

  test('propagates worker-reported failures as results', async (t) => {
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 1, logger: silentLogger })
    t.after(() => pool.terminate())

    const result = await pool.run({ command: 'fail' })

    assert.equal(result.success, false)
    assert.equal(result.error, 'requested failure')
  })

  test('respawns crashed workers and keeps serving', async (t) => {
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 1, maxRespawns: 3, logger: silentLogger })
    t.after(() => pool.terminate())

    await assert.rejects(() => pool.run({ command: 'crash' }), /Worker exited/)

    await waitFor(() => pool.size === 1, 5000, 'worker respawned')

    const result = await pool.run({ content: 'still alive' })

    assert.equal(result.success, true)
    assert.equal(result.echo, 'still alive')
  })

  test('stops respawning after the limit and rejects further work', async (t) => {
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 1, maxRespawns: 0, logger: silentLogger })
    t.after(() => pool.terminate())

    await assert.rejects(() => pool.run({ command: 'crash' }), /Worker exited/)

    await waitFor(() => pool.size === 0, 5000, 'worker permanently dead')

    await assert.rejects(() => pool.run({ content: 'anyone there?' }), /All workers have died/)
  })

  test('defaults the worker count to the number of CPUs', async (t) => {
    const pool = new WorkerPool(WORKER_FILE, { logger: silentLogger })

    t.after(() => pool.terminate())

    assert.equal(pool.size, os.cpus().length)
  })

  test('hands each worker its id and the caller\'s workerData', async (t) => {
    // The worker reads workerData.workerId to identify itself, and callers
    // pass their own fields alongside it. Dropping either leaves workers
    // indistinguishable and any caller configuration silently missing.
    const pool = new WorkerPool(WORKER_FILE, {
      workerCount: 2,
      workerData: { queueName: 'orders' },
      logger: silentLogger
    })

    t.after(() => pool.terminate())

    const results = await Promise.all([
      pool.run({ content: 1 }),
      pool.run({ content: 2 })
    ])

    const ids = results.map(r => r.workerId).sort()

    assert.deepEqual(ids, [0, 1], 'each worker knows which one it is')
  })

  test('respawns up to five times by default', async (t) => {
    // maxRespawns defaults to 5. Every existing test pins it explicitly, so
    // the default was free to be anything, including 0.
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 1, logger: silentLogger })

    t.after(() => pool.terminate())

    assert.equal(pool.maxRespawns, 5)

    // Five crashes are survivable; the sixth exhausts the budget.
    for (let attempt = 1; attempt <= 5; attempt++) {
      await assert.rejects(() => pool.run({ command: 'crash' }), /Worker exited/, `crash ${attempt}`)
      await waitFor(() => pool.size === 1, 5000, `worker respawned after crash ${attempt}`)
    }

    await assert.rejects(() => pool.run({ command: 'crash' }), /Worker exited/)
    await waitFor(() => pool.size === 0, 5000, 'the budget is exhausted after the sixth')
  })

  test('survives with no logger through crash, respawn and exhaustion', async (t) => {
    // Every log call site guards with `?.`. Dropping one guard turns a worker
    // crash — already the bad path — into a TypeError inside an event handler.
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 1, maxRespawns: 1 })

    t.after(() => pool.terminate())

    await assert.rejects(() => pool.run({ command: 'crash' }), /Worker exited/)
    await waitFor(() => pool.size === 1, 5000, 'respawned without a logger')

    await assert.rejects(() => pool.run({ command: 'crash' }), /Worker exited/)
    await waitFor(() => pool.size === 0, 5000, 'gave up without a logger')
  })

  test('terminate rejects pending and future work', async (t) => {
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 1, logger: silentLogger })

    await pool.terminate()

    await assert.rejects(() => pool.run({ content: 'late' }), /terminated/)
    assert.equal(pool.size, 0)
  })

  test('terminate rejects work already queued behind a busy worker', async (t) => {
    // The previous test terminates an idle pool, so nothing is waiting. This
    // one puts a real waiter in the queue first: without rejecting them,
    // terminate() would leave those callers hanging forever.
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 1, logger: silentLogger })

    t.after(() => pool.terminate())

    // Outcomes are captured the moment the calls are made: both settle while
    // terminate() is still running, and an unobserved rejection would fail
    // the test as unhandled rather than as the assertion below.
    const settle = (promise) => promise.then(() => null, (error) => error)

    const inFlight = settle(pool.run({ command: 'slow', ms: 300, content: 'busy' }))

    await waitFor(() => pool.idleWorkers.length === 0, 3000, 'the only worker is busy')

    const queued = settle(pool.run({ content: 'queued' }))

    await waitFor(() => pool.waiters.length === 1, 3000, 'the second call is queued as a waiter')

    await pool.terminate()

    assert.match((await queued).message, /Worker pool has been terminated/)
    await inFlight
  })

  test('a worker that throws mid-message rejects the run and is reported', async (t) => {
    // Distinct from a worker that politely returns { success: false }: an
    // uncaught throw surfaces as the worker's 'error' event.
    const logger = recordingLogger()
    const pool = new WorkerPool(THROWING_WORKER, { workerCount: 1, logger })

    t.after(() => pool.terminate())

    await assert.rejects(() => pool.run({ content: 'boom' }), /worker blew up mid-message/)

    await waitFor(
      () => logger.records.error.some(message => /worker blew up mid-message/.test(message)),
      5000,
      'worker error reported'
    )
  })

  test('terminate tolerates a worker that fails to shut down', async (t) => {
    const logger = recordingLogger()
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 1, logger })

    // terminate() is idempotent, and without this an assertion failing before
    // the explicit terminate below would leave a live worker thread keeping the
    // test process alive forever — a CI hang instead of a CI failure.
    t.after(() => pool.terminate())

    await pool.run({ content: 'warm up the pool' })

    // A worker whose terminate() rejects must not abort the shutdown of the
    // remaining workers, or a graceful shutdown would leak threads. The real
    // termination still runs first — otherwise this test would leak the very
    // thread whose shutdown it is asserting on.
    for (const worker of pool.workers) {
      const realTerminate = worker.terminate.bind(worker)

      worker.terminate = async () => {
        await realTerminate()

        throw new Error('thread already gone')
      }
    }

    await pool.terminate()

    assert.equal(pool.size, 0, 'the pool must still end up empty')
    assert.ok(logger.records.warn.some(message => /Failed to terminate worker: thread already gone/.test(message)))
  })
})
