import { parentPort } from 'node:worker_threads'

// subscribeParallel wraps each queue message as { content } before handing it
// to the worker — this fixture fails or succeeds based on that content, which
// is what a real processor sees (unlike echo-worker's top-level commands,
// which only direct WorkerPool.run() calls can trigger).
parentPort.on('message', (message) => {
  if (message.content?.shouldFail) {
    parentPort.postMessage({ success: false, error: 'requested failure' })

    return
  }

  parentPort.postMessage({ success: true, echo: message.content })
})
