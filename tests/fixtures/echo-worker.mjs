import { parentPort, workerData } from 'node:worker_threads'

parentPort.on('message', (message) => {
  if (message.command === 'crash') {
    process.exit(1)
  }

  // Keeps the worker busy so a second run() has to queue as a waiter.
  if (message.command === 'slow') {
    setTimeout(() => {
      parentPort.postMessage({ success: true, echo: message.content, workerId: workerData.workerId })
    }, message.ms ?? 200)

    return
  }

  if (message.command === 'fail') {
    parentPort.postMessage({ success: false, error: 'requested failure' })

    return
  }

  parentPort.postMessage({ success: true, echo: message.content, workerId: workerData.workerId })
})
