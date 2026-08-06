// Per-key time-ordered entry list plus a running total. Expired entries are
// evicted from the front — O(evicted) per check instead of a full
// filter+reduce scan of the window. Shared by the sliding-window and
// leaky-bucket strategies, which differ only in what the total is compared
// against.
class WindowLog {
  constructor (windowMs) {
    this.windowMs = windowMs
    this.windows = new Map()
  }

  get (key) {
    let windowData = this.windows.get(key)

    if (!windowData) {
      windowData = { entries: [], total: 0 }
      this.windows.set(key, windowData)
    }

    return windowData
  }

  peek (key) {
    return this.windows.get(key)
  }

  evictExpired (windowData, now) {
    while (windowData.entries.length > 0 && now - windowData.entries[0].timestamp > this.windowMs) {
      windowData.total -= windowData.entries.shift().cost
    }
  }

  cleanup (now) {
    for (const [key, windowData] of this.windows) {
      this.evictExpired(windowData, now)

      if (windowData.entries.length === 0) {
        this.windows.delete(key)
      }
    }
  }

  delete (key) {
    this.windows.delete(key)
  }

  clear () {
    this.windows.clear()
  }
}

export { WindowLog }
export default WindowLog
