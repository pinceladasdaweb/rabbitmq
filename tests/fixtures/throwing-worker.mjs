import { parentPort } from 'node:worker_threads'

// Throws inside the message handler so the worker emits an 'error' event
// instead of replying — exercising WorkerPool's error propagation path (as
// opposed to a worker that politely reports { success: false }).
parentPort.on('message', () => {
  throw new Error('worker blew up mid-message')
})
