import os from 'node:os'
import assert from 'node:assert/strict'
import { fileURLToPath } from 'node:url'
import { test, describe } from 'node:test'
import { EventEmitter } from 'node:events'
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

// Fake-worker suite: the spawn seam replaces real threads with synchronous
// EventEmitters, making the pool's bookkeeping — idle rotation, respawn
// budget, waiter handoff, listener cleanup — observable without paying
// thread startup or racing the scheduler.
describe('WorkerPool bookkeeping (fake workers)', () => {
  class FakeWorker extends EventEmitter {
    constructor (file, options) {
      super()
      this.file = file
      this.options = options
      this.posted = []
      this.terminated = false
    }

    postMessage (payload) {
      this.posted.push(payload)
    }

    async terminate () {
      this.terminated = true
      this.emit('exit', 0)
    }
  }

  const createFakePool = (options = {}) => {
    const spawned = []
    const pool = new WorkerPool('ignored.js', {
      workerCount: 1,
      logger: silentLogger,
      createWorker: (file, workerOptions) => {
        const worker = new FakeWorker(file, workerOptions)

        spawned.push(worker)

        return worker
      },
      ...options
    })

    return { pool, spawned }
  }

  test('workerData defaults to an empty object and always carries the workerId', () => {
    const bare = createFakePool()

    assert.deepEqual(bare.spawned[0].options.workerData, { workerId: 0 })

    const configured = createFakePool({ workerCount: 2, workerData: { region: 'sa-east-1' } })

    assert.deepEqual(configured.spawned[0].options.workerData, { region: 'sa-east-1', workerId: 0 })
    assert.deepEqual(configured.spawned[1].options.workerData, { region: 'sa-east-1', workerId: 1 })
  })

  test('a nonzero exit code is reported, a clean exit is not', () => {
    const logger = recordingLogger()
    const { pool, spawned } = createFakePool({ workerCount: 2, maxRespawns: 0, logger })

    spawned[0].emit('exit', 1)
    spawned[1].emit('exit', 0)

    assert.equal(pool.size, 0)
    assert.ok(logger.records.error.some(line => line.includes('exited with code: 1')), 'the crash is reported')
    assert.equal(
      logger.records.error.some(line => line.includes('exited with code: 0')),
      false,
      'a clean exit is not an error'
    )
  })

  test('an idle worker exiting leaves no corpse in the idle list', () => {
    const { pool, spawned } = createFakePool({ workerCount: 2, maxRespawns: 0 })

    spawned[0].emit('exit', 1)

    assert.equal(pool.idleWorkers.length, 1, 'only the survivor remains idle')
    assert.equal(pool.idleWorkers[0], spawned[1])
  })

  test('a busy worker exiting must not evict an unrelated idle worker', async () => {
    // The exit handler splices by indexOf; a busy worker is NOT in the idle
    // list (indexOf -1) and splice(-1, 1) would silently remove the LAST
    // idle entry — leaking a healthy worker out of rotation forever.
    const { pool, spawned } = createFakePool({ workerCount: 2, maxRespawns: 0 })

    const busyRun = pool.run({ job: 1 })

    await new Promise(resolve => setImmediate(resolve))
    assert.equal(spawned[0].posted.length, 1, 'w0 took the job')

    spawned[0].emit('exit', 1)

    await assert.rejects(() => busyRun, /Worker exited while processing/)

    const rescued = pool.run({ job: 2 })

    await new Promise(resolve => setImmediate(resolve))
    assert.equal(spawned[1].posted.length, 1, 'the idle survivor picks up the next job')

    spawned[1].emit('message', 'done')

    assert.equal(await rescued, 'done')
  })

  test('giving up on respawns rejects queued waiters and reports the budget', async () => {
    const logger = recordingLogger()
    const { pool, spawned } = createFakePool({ maxRespawns: 0, logger })

    const running = pool.run({ job: 1 })

    await new Promise(resolve => setImmediate(resolve))

    const queued = pool.run({ job: 2 })
    const both = Promise.allSettled([running, queued])

    spawned[0].emit('exit', 1)

    const [first, second] = await both

    assert.match(first.reason.message, /exited while processing/)
    assert.match(second.reason.message, /All workers have died and exceeded the respawn limit/)
    assert.ok(logger.records.error.some(line => line.includes('exceeded the respawn limit')))
  })

  test('releasing a dead worker never returns it to the idle list', async () => {
    const { pool, spawned } = createFakePool({ maxRespawns: 0 })

    const running = pool.run({ job: 1 })

    await new Promise(resolve => setImmediate(resolve))
    spawned[0].emit('exit', 1)

    await assert.rejects(() => running, /exited while processing/)

    assert.equal(pool.idleWorkers.length, 0, 'the corpse was not re-shelved by the finally-release')
  })

  test('a settled run removes exactly its own listeners', async () => {
    const { pool, spawned } = createFakePool()
    const worker = spawned[0]

    const baseline = {
      message: worker.listenerCount('message'),
      error: worker.listenerCount('error'),
      exit: worker.listenerCount('exit')
    }

    const running = pool.run({ job: 1 })

    await new Promise(resolve => setImmediate(resolve))
    worker.emit('message', 'done')
    await running

    assert.deepEqual({
      message: worker.listenerCount('message'),
      error: worker.listenerCount('error'),
      exit: worker.listenerCount('exit')
    }, baseline, 'per-run listeners were cleaned up; the spawn-time ones remain')
  })

  test('one worker giving up does not reject waiters while others still live', async () => {
    // rejectAllWaiters is the LAST-worker measure: with a healthy sibling
    // remaining, a queued job must stay queued and be served by it.
    const { pool, spawned } = createFakePool({ workerCount: 2, maxRespawns: 0 })

    const run1 = pool.run({ job: 1 })
    const run2 = pool.run({ job: 2 })

    await new Promise(resolve => setImmediate(resolve))

    const queued = pool.run({ job: 3 })

    spawned[0].emit('exit', 1)
    await assert.rejects(() => run1, /exited while processing/)

    spawned[1].emit('message', 'done-2')
    assert.equal(await run2, 'done-2')

    await new Promise(resolve => setImmediate(resolve))
    spawned[1].emit('message', 'done-3')

    assert.equal(await queued, 'done-3', 'the queued job rode out the sibling loss')
  })

  test('a run settled by error still removes its unfired message listener', async () => {
    // The message listener is a `once`: on the message path it removes
    // itself, so only an error/exit settlement proves cleanup removes it.
    const { pool, spawned } = createFakePool()
    const worker = spawned[0]
    const baseline = worker.listenerCount('message')

    const running = pool.run({ job: 1 })

    await new Promise(resolve => setImmediate(resolve))
    worker.emit('error', new Error('mid-flight'))

    await assert.rejects(() => running, /mid-flight/)

    assert.equal(worker.listenerCount('message'), baseline, 'the never-fired once-listener was cleaned up')
  })

  test('works without a logger on the error and failing-terminate paths', async () => {
    const { pool, spawned } = createFakePool({ logger: undefined })

    spawned[0].emit('error', new Error('worker trouble'))

    spawned[0].terminate = async () => { throw new Error('stuck') }

    await pool.terminate()

    assert.equal(pool.size, 0)
  })
})
