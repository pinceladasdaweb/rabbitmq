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

  test('terminate rejects pending and future work', async (t) => {
    const pool = new WorkerPool(WORKER_FILE, { workerCount: 1, logger: silentLogger })

    await pool.terminate()

    await assert.rejects(() => pool.run({ content: 'late' }), /terminated/)
    assert.equal(pool.size, 0)
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
