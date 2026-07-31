import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import MessageCodec from '../src/messaging/message-codec.js'
import { recordingLogger, silentLogger } from './helpers.js'

describe('MessageCodec toBuffer/fromBuffer', () => {
  test('passes buffers through untouched', () => {
    const codec = new MessageCodec({ logger: silentLogger })
    const buffer = Buffer.from('raw-bytes')

    assert.equal(codec.toBuffer(buffer), buffer)
  })

  test('encodes strings as UTF-8 without serializing', () => {
    const codec = new MessageCodec({ logger: silentLogger })

    assert.equal(codec.toBuffer('plain text').toString(), 'plain text')
  })

  test('rejects undefined messages', () => {
    const codec = new MessageCodec({ logger: silentLogger })

    assert.throws(() => codec.toBuffer(undefined), /must not be undefined/)
  })

  test('serializes everything else with the configured serializer', () => {
    const codec = new MessageCodec({
      logger: silentLogger,
      serializer: (message) => `custom:${JSON.stringify(message)}`
    })

    assert.equal(codec.toBuffer({ id: 1 }).toString(), 'custom:{"id":1}')
  })

  test('fromBuffer rejects non-buffer input', () => {
    const codec = new MessageCodec({ logger: silentLogger })

    assert.throws(() => codec.fromBuffer('not a buffer'), /must be a Buffer/)
  })

  test('fromBuffer falls back to the raw string when deserialization fails', () => {
    const codec = new MessageCodec({ logger: silentLogger })

    assert.equal(codec.fromBuffer(Buffer.from('not-json{')), 'not-json{')
    assert.deepEqual(codec.fromBuffer(Buffer.from('{"ok":true}')), { ok: true })
  })
})

describe('MessageCodec compression', () => {
  test('compresses only above the threshold and decode round-trips both cases', async () => {
    const codec = new MessageCodec({ logger: silentLogger, useCompression: true, compressionThreshold: 100 })

    const small = await codec.encode({ tiny: true })

    assert.equal(small.compressed, false)
    assert.deepEqual(await codec.decode(small.content, small.compressed), { tiny: true })

    const bigPayload = { blob: 'x'.repeat(500) }
    const big = await codec.encode(bigPayload)

    assert.equal(big.compressed, true)
    assert.ok(big.content.length < 500, 'gzip must shrink the repetitive payload')
    assert.deepEqual(await codec.decode(big.content, big.compressed), bigPayload)
  })

  test('never compresses when the feature is disabled', async () => {
    const codec = new MessageCodec({ logger: silentLogger, useCompression: false, compressionThreshold: 10 })

    const { compressed } = await codec.encode({ blob: 'x'.repeat(500) })

    assert.equal(compressed, false)
  })

  test('falls back to the uncompressed payload when gzip fails', async () => {
    // Compression is an optimization, never a reason to lose a message: a gzip
    // failure must warn and send the original bytes.
    const logger = recordingLogger()
    const codec = new MessageCodec({ logger, useCompression: true, compressionThreshold: 10 })
    const payload = { blob: 'x'.repeat(500) }
    const buffer = Buffer.from(JSON.stringify(payload))

    // Forcing gzip to reject requires an input `toBuffer` itself would never
    // produce (it returns a Buffer or throws), so this reaches the branch
    // artificially. What it verifies is therefore limited to the contract that
    // matters: a compression failure is reported and does not fail the encode.
    // In production the reachable trigger is resource exhaustion inside zlib.
    codec.toBuffer = () => Object.assign(Object.create(null), { length: buffer.length })

    const { compressed } = await codec.encode(payload)

    assert.equal(compressed, false, 'the message must still be publishable')
    assert.ok(logger.records.warn.some(message => /Failed to compress message/.test(message)))
  })

  test('decode rethrows decompression failures for content flagged as compressed', async () => {
    const codec = new MessageCodec({ logger: silentLogger })

    await assert.rejects(() => codec.decode(Buffer.from('not-gzip'), true), /incorrect header check/)
  })
})
