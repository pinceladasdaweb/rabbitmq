import os from 'node:os'
import { Worker } from 'node:worker_threads'

class WorkerPool {
  constructor (processorFile, options = {}) {
    this.processorFile = processorFile
    this.workerCount = options.workerCount || os.cpus().length
    this.workerData = options.workerData || {}
    this.maxRespawns = options.maxRespawns ?? 5
    this.logger = options.logger
    this.workers = new Set()
    this.idleWorkers = []
    this.waiters = []
    this.respawnCounts = new Map()
    this.terminated = false

    for (let i = 0; i < this.workerCount; i++) {
      this.#spawn(i)
    }
  }

  get size () {
    return this.workers.size
  }

  #spawn (workerId) {
    const worker = new Worker(this.processorFile, {
      workerData: { ...this.workerData, workerId }
    })

    worker.on('error', (error) => {
      // Stryker disable next-line StringLiteral: log phrasing is not contract
      this.logger?.error(`Worker ${workerId} error: ${error.message}`)
    })

    worker.on('exit', (code) => {
      this.workers.delete(worker)

      const idleIndex = this.idleWorkers.indexOf(worker)

      if (idleIndex !== -1) {
        this.idleWorkers.splice(idleIndex, 1)
      }

      if (this.terminated) return

      if (code !== 0) {
        // Stryker disable next-line StringLiteral: log phrasing is not contract
        this.logger?.error(`Worker ${workerId} exited with code: ${code}`)
      }

      const respawns = this.respawnCounts.get(workerId) || 0

      if (respawns < this.maxRespawns) {
        this.respawnCounts.set(workerId, respawns + 1)
        // Stryker disable next-line StringLiteral: log phrasing is not contract
        this.logger?.warn(`Respawning worker ${workerId} (attempt ${respawns + 1}/${this.maxRespawns})`)
        this.#spawn(workerId)
      } else {
        // Stryker disable next-line StringLiteral: log phrasing is not contract
        this.logger?.error(`Worker ${workerId} exceeded the respawn limit (${this.maxRespawns}) and will not be recreated`)

        if (this.workers.size === 0) {
          this.#rejectAllWaiters(new Error('All workers have died and exceeded the respawn limit'))
        }
      }
    })

    this.workers.add(worker)
    this.#release(worker)

    return worker
  }

  #rejectAllWaiters (error) {
    const waiters = this.waiters
    this.waiters = []

    for (const waiter of waiters) {
      waiter.reject(error)
    }
  }

  #release (worker) {
    if (!this.workers.has(worker)) return

    const waiter = this.waiters.shift()

    if (waiter) {
      waiter.resolve(worker)
    } else {
      this.idleWorkers.push(worker)
    }
  }

  #acquire () {
    if (this.terminated) {
      return Promise.reject(new Error('Worker pool has been terminated'))
    }

    while (this.idleWorkers.length > 0) {
      const worker = this.idleWorkers.shift()

      if (this.workers.has(worker)) {
        return Promise.resolve(worker)
      }
    }

    if (this.workers.size === 0) {
      return Promise.reject(new Error('All workers have died and exceeded the respawn limit'))
    }

    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject })
    })
  }

  async run (payload) {
    const worker = await this.#acquire()

    try {
      return await new Promise((resolve, reject) => {
        const onMessage = (result) => {
          cleanup()
          resolve(result)
        }

        const onError = (error) => {
          cleanup()
          reject(error)
        }

        const onExit = () => {
          cleanup()
          reject(new Error('Worker exited while processing a message'))
        }

        const cleanup = () => {
          worker.off('message', onMessage)
          worker.off('error', onError)
          worker.off('exit', onExit)
        }

        worker.once('message', onMessage)
        worker.once('error', onError)
        worker.once('exit', onExit)
        worker.postMessage(payload)
      })
    } finally {
      this.#release(worker)
    }
  }

  async terminate () {
    this.terminated = true
    this.#rejectAllWaiters(new Error('Worker pool has been terminated'))

    for (const worker of [...this.workers]) {
      try {
        await worker.terminate()
      } catch (error) {
        // Stryker disable next-line StringLiteral: log phrasing is not contract
        this.logger?.warn(`Failed to terminate worker: ${error.message}`)
      }
    }

    this.workers.clear()
    this.idleWorkers = []
  }
}

export { WorkerPool }
export default WorkerPool
