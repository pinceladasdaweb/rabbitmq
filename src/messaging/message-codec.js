import zlib from 'node:zlib'
import util from 'node:util'
import { nonNegativeNumber } from '../utils/options.js'

const gzip = util.promisify(zlib.gzip)
const gunzip = util.promisify(zlib.gunzip)

class MessageCodec {
  constructor (options = {}) {
    this.serializer = options.serializer || JSON.stringify
    this.deserializer = options.deserializer || JSON.parse
    this.useCompression = options.useCompression || false
    // Same rule as setCompressionThreshold: 0 ("compress everything") is a real
    // request, junk fails at construction. `|| 1000` rewrote the 0 and a bare
    // `??` let NaN through to compress every message. See utils/options.js.
    this.compressionThreshold = nonNegativeNumber(options.compressionThreshold, 'compressionThreshold', 1000)
    this.logger = options.logger
  }

  toBuffer (message) {
    if (Buffer.isBuffer(message)) {
      return message
    }

    if (typeof message === 'string') {
      return Buffer.from(message, 'utf-8')
    }

    if (message === undefined) {
      throw new Error('Message must not be undefined.')
    }

    return Buffer.from(this.serializer(message), 'utf-8')
  }

  fromBuffer (buffer) {
    if (!Buffer.isBuffer(buffer)) {
      throw new Error('Input must be a Buffer.')
    }

    const str = buffer.toString('utf-8')

    try {
      return this.deserializer(str)
    } catch (error) {
      return str
    }
  }

  async compressIfNeeded (buffer) {
    if (!this.useCompression || buffer.length <= this.compressionThreshold) {
      return { content: buffer, compressed: false }
    }

    try {
      const compressed = await gzip(buffer)

      return { content: compressed, compressed: true }
    } catch (error) {
      this.logger?.warn(`Failed to compress message: ${error.message}. Sending uncompressed.`)

      return { content: buffer, compressed: false }
    }
  }

  // Only ever asked for content that IS compressed: decode() takes the
  // uncompressed fast path itself, so a flag here would be a branch no caller
  // can reach.
  async #decompress (buffer) {
    try {
      return await gunzip(buffer)
    } catch (error) {
      this.logger?.error(`Failed to decompress message: ${error.message}`)

      throw error
    }
  }

  async encode (message) {
    const buffer = this.toBuffer(message)

    return this.compressIfNeeded(buffer)
  }

  async decode (content, isCompressed) {
    // The uncompressed path is the hot one (compression off, or the payload
    // under threshold): no promise and no microtask hop for a no-op inflate.
    if (!isCompressed) return this.fromBuffer(content)

    return this.fromBuffer(await this.#decompress(content))
  }
}

export { MessageCodec }
export default MessageCodec
