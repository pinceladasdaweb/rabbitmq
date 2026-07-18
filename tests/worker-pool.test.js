import assert from 'node:assert/strict'
import { fileURLToPath } from 'node:url'
import { test, describe } from 'node:test'
import WorkerPool from '../src/consumers/worker-pool.js'

const WORKER_FILE = fileURLToPath(new URL('./fixtures/echo-worker.mjs', import.meta.url))

const silentLogger = { info: () => {}, warn: () => {}, error: () => {} }

const waitFor = async (predicate, timeoutMs = 5000, label = 'condition') => {
  const start = Date.now()

  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return

    await new Promise(resolve => setTimeout(resolve, 50))
  }

  throw new Error(`Timeout waiting for: ${label}`)
}

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
})
