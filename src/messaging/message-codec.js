import zlib from 'node:zlib'
import util from 'node:util'

const gzip = util.promisify(zlib.gzip)
const gunzip = util.promisify(zlib.gunzip)

class MessageCodec {
  constructor (options = {}) {
    this.serializer = options.serializer || JSON.stringify
    this.deserializer = options.deserializer || JSON.parse
    this.useCompression = options.useCompression || false
    // ?? and not ||: setCompressionThreshold explicitly accepts 0 ("compress
    // everything"), so the constructor rewriting that same 0 to 1000 made the
    // two entry points disagree about the identical request.
    this.compressionThreshold = options.compressionThreshold ?? 1000
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

  async decompressIfNeeded (buffer, isCompressed) {
    if (!isCompressed) {
      return buffer
    }

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
    const buffer = await this.decompressIfNeeded(content, isCompressed)

    return this.fromBuffer(buffer)
  }
}

export { MessageCodec }
export default MessageCodec
