import { parentPort, workerData } from 'node:worker_threads'

parentPort.on('message', (message) => {
  if (message.command === 'crash') {
    process.exit(1)
  }

  if (message.command === 'fail') {
    parentPort.postMessage({ success: false, error: 'requested failure' })

    return
  }

  parentPort.postMessage({ success: true, echo: message.content, workerId: workerData.workerId })
})
