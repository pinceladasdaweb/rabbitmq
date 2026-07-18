import { parentPort, workerData } from 'node:worker_threads'

// CPU-intensive function to simulate heavy processing
function fibonacci (n) {
  if (n <= 1) return n
  return fibonacci(n - 1) + fibonacci(n - 2)
}

// Message processing function
async function processMessage (message) {
  const startTime = Date.now()
  try {
    // Logs the start of processing
    console.log(`\n[Worker ${workerData.workerId}] 🔄 Processing message:`)
    console.log(`   ID: ${message.content.id}`)
    console.log(`   Type: ${message.content.taskType}`)
    console.log(`   Complexity: ${message.content.complexity}`)

    // Simulates CPU-intensive processing
    if (message.content.taskType === 'heavy') {
      // Heavy task: computes fibonacci of a larger number
      fibonacci(Math.min(40, message.content.complexity % 40))
    } else {
      // Normal task: computes fibonacci of a smaller number
      fibonacci(Math.min(30, message.content.complexity % 30))
    }

    const processingTime = Date.now() - startTime
    console.log(`\n[Worker ${workerData.workerId}] ✅ Message processed:`)
    console.log(`   Processing time: ${processingTime}ms`)

    return {
      success: true,
      processingTime,
      workerId: workerData.workerId
    }
  } catch (error) {
    console.error(`\n[Worker ${workerData.workerId}] ❌ Error:`, error.message)
    return {
      success: false,
      error: error.message,
      workerId: workerData.workerId
    }
  }
}

// Worker setup
parentPort.on('message', async (message) => {
  try {
    const result = await processMessage(message)
    parentPort.postMessage(result)
  } catch (error) {
    parentPort.postMessage({
      success: false,
      error: error.message,
      workerId: workerData.workerId
    })
  }
})
