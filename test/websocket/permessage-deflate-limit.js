'use strict'

const { test } = require('node:test')
const assert = require('node:assert')
const { once } = require('node:events')
const { randomFillSync } = require('node:crypto')
const { deflateRawSync, createDeflateRaw, constants } = require('node:zlib')
const { setTimeout: sleep } = require('node:timers/promises')
const { WebSocketServer } = require('ws')
const { WebSocket, Agent } = require('../..')

test('Compressed message under limit decompresses successfully', async (t) => {
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: true
  })

  t.after(() => server.close())

  await once(server, 'listening')

  server.on('connection', (ws) => {
    // Send 1 KB of data (well under any reasonable limit)
    ws.send(Buffer.alloc(1024, 0x41), { binary: true })
  })

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`)

  const [event] = await once(client, 'message')
  assert.strictEqual(event.data.size, 1024)
  client.close()
})

test('Agent webSocketOptions.maxPayloadSize is read correctly', async (t) => {
  const customLimit = 128 * 1024 * 1024 // 128 MB
  const agent = new Agent({
    webSocket: {
      maxPayloadSize: customLimit
    }
  })

  t.after(() => agent.close())

  // Verify the option is stored and retrievable
  assert.strictEqual(agent.webSocketOptions.maxPayloadSize, customLimit)
})

test('Agent with default webSocketOptions uses 128 MB limit', async (t) => {
  const agent = new Agent()

  t.after(() => agent.close())

  // Default should be 128 MB
  assert.strictEqual(agent.webSocketOptions.maxPayloadSize, 128 * 1024 * 1024)
})

test('Custom maxPayloadSize allows messages under limit', async (t) => {
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: true
  })

  t.after(() => server.close())
  await once(server, 'listening')

  const dataSize = 512 * 1024 // 512 KB

  server.on('connection', (ws) => {
    ws.send(Buffer.alloc(dataSize, 0x41), { binary: true })
  })

  // Set custom limit of 1 MB via Agent
  const agent = new Agent({
    webSocket: {
      maxPayloadSize: 1 * 1024 * 1024
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  const [event] = await once(client, 'message')
  assert.strictEqual(event.data.size, dataSize, 'Message under limit should be received')
  client.close()
})

test('Messages at exactly the limit succeed', async (t) => {
  const limit = 1 * 1024 * 1024 // 1 MB
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: true
  })

  t.after(() => server.close())
  await once(server, 'listening')

  server.on('connection', (ws) => {
    ws.send(Buffer.alloc(limit, 0x41), { binary: true })
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: limit
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  const [event] = await once(client, 'message')
  assert.strictEqual(event.data.size, limit, 'Message at exactly the limit should succeed')
  client.close()
})

test('Compressed frame payload over wire-size limit is rejected', async (t) => {
  const limit = 64 * 1024
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: true
  })

  t.after(() => server.close())
  await once(server, 'listening')

  let payload = null
  for (let i = 0; i < 10; i++) {
    const candidate = randomFillSync(Buffer.alloc(limit))
    if (deflateRawSync(candidate).length > limit) {
      payload = candidate
      break
    }
  }

  assert.ok(payload, 'Expected incompressible payload with compressed wire size over the limit')

  let messageReceived = false

  server.on('connection', (ws) => {
    ws.send(payload, { binary: true, compress: true })
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: limit
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  client.addEventListener('message', () => {
    messageReceived = true
  })

  const closePromise = once(client, 'close')
  const timeoutPromise = sleep(5000)

  await Promise.race([closePromise, timeoutPromise])

  assert.strictEqual(messageReceived, false, 'Compressed frame over wire-size limit should be rejected')
  assert.strictEqual(client.readyState, WebSocket.CLOSED, 'Connection should be closed after exceeding limit')
})

test('Messages over the limit are rejected', async (t) => {
  const limit = 1 * 1024 * 1024 // 1 MB
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: true
  })

  t.after(() => server.close())
  await once(server, 'listening')

  let messageReceived = false
  let closeEvent = null

  server.on('connection', (ws) => {
    // Send 2 MB of data, which exceeds the 1 MB limit
    ws.send(Buffer.alloc(2 * 1024 * 1024, 0x41), { binary: true })
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: limit
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  client.addEventListener('message', () => {
    messageReceived = true
  })

  client.addEventListener('close', (event) => {
    closeEvent = event
  })

  // Wait for connection to close (should happen when limit is exceeded)
  // Use Promise.race with a timeout to avoid hanging forever
  const closePromise = once(client, 'close')
  const timeoutPromise = sleep(5000)

  await Promise.race([closePromise, timeoutPromise])

  assert.strictEqual(messageReceived, false, 'Message over limit should be rejected')
  assert.ok(closeEvent !== null, 'Close event should have been emitted')
  assert.strictEqual(client.readyState, WebSocket.CLOSED, 'Connection should be closed after exceeding limit')
})

test('Limit can be disabled by setting maxPayloadSize to 0', async (t) => {
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: true
  })

  t.after(() => server.close())
  await once(server, 'listening')

  const dataSize = 100 * 1024 * 1024 // 100 MB

  server.on('connection', (ws) => {
    ws.send(Buffer.alloc(dataSize, 0x41), { binary: true })
  })

  // Set limit to 0 (disabled)
  const agent = new Agent({
    webSocket: {
      maxPayloadSize: 0
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  // Use Promise.race with timeout since large message takes time
  const messagePromise = once(client, 'message')
  const timeoutPromise = sleep(10000)

  const result = await Promise.race([messagePromise, timeoutPromise])

  if (result) {
    assert.strictEqual(result[0].data.size, dataSize, 'Large message should be received when limit is disabled')
    client.close()
  } else {
    t.fail('Test timed out waiting for large message')
  }
})

test('Fragmented compressed payload over total limit is rejected', async (t) => {
  const limit = 1 * 1024 * 1024 // 1 MB
  const fragmentSize = 768 * 1024 // 768 KB
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: true
  })

  t.after(() => server.close())
  await once(server, 'listening')

  let messageReceived = false

  server.on('connection', (ws) => {
    ws.send(Buffer.alloc(fragmentSize, 0x41), {
      binary: true,
      compress: true,
      fin: false
    })

    ws.send(Buffer.alloc(fragmentSize, 0x41), {
      binary: true,
      compress: true,
      fin: true
    })
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: limit
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  client.addEventListener('message', () => {
    messageReceived = true
  })

  const closePromise = once(client, 'close')
  const timeoutPromise = sleep(5000)

  await Promise.race([closePromise, timeoutPromise])

  assert.strictEqual(messageReceived, false, 'Fragmented compressed message over total limit should be rejected')
  assert.strictEqual(client.readyState, WebSocket.CLOSED, 'Connection should be closed after exceeding limit')
})

test('Raw uncompressed payload over immediate limit is rejected', async (t) => {
  const limit = 100
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: false // Disable compression
  })

  t.after(() => server.close())
  await once(server, 'listening')

  let messageReceived = false

  server.on('connection', (ws) => {
    // Send 101 bytes uncompressed so the inline payload length path is used.
    ws.send(Buffer.alloc(101, 0x41), { binary: true })
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: limit
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  client.addEventListener('message', () => {
    messageReceived = true
  })

  const closePromise = once(client, 'close')
  const timeoutPromise = sleep(5000)

  await Promise.race([closePromise, timeoutPromise])

  assert.strictEqual(messageReceived, false, 'Raw uncompressed message over limit should be rejected')
  assert.strictEqual(client.readyState, WebSocket.CLOSED, 'Connection should be closed after exceeding limit')
})

test('Raw uncompressed payload over 16-bit extended limit is rejected', async (t) => {
  const limit = 1 * 1024 // 1 KB
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: false // Disable compression
  })

  t.after(() => server.close())
  await once(server, 'listening')

  let messageReceived = false

  server.on('connection', (ws) => {
    // Send 2 KB uncompressed so the extended 16-bit payload length path is used.
    ws.send(Buffer.alloc(2 * 1024, 0x41), { binary: true })
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: limit
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  client.addEventListener('message', () => {
    messageReceived = true
  })

  const closePromise = once(client, 'close')
  const timeoutPromise = sleep(5000)

  await Promise.race([closePromise, timeoutPromise])

  assert.strictEqual(messageReceived, false, 'Raw uncompressed message over limit should be rejected')
  assert.strictEqual(client.readyState, WebSocket.CLOSED, 'Connection should be closed after exceeding limit')
})

test('Raw uncompressed payload over 64-bit extended limit is rejected', async (t) => {
  const limit = 1 * 1024 * 1024 // 1 MB
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: false // Disable compression
  })

  t.after(() => server.close())
  await once(server, 'listening')

  let messageReceived = false

  server.on('connection', (ws) => {
    // Send 2 MB uncompressed so the extended 64-bit payload length path is used.
    ws.send(Buffer.alloc(2 * 1024 * 1024, 0x41), { binary: true })
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: limit
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  client.addEventListener('message', () => {
    messageReceived = true
  })

  const closePromise = once(client, 'close')
  const timeoutPromise = sleep(5000)

  await Promise.race([closePromise, timeoutPromise])

  assert.strictEqual(messageReceived, false, 'Raw uncompressed message over limit should be rejected')
  assert.strictEqual(client.readyState, WebSocket.CLOSED, 'Connection should be closed after exceeding limit')
})

test('cumulative payload size', (t, done) => {
  const LIMIT = 100
  const FRAGMENT_SIZE = 60
  const NUM_FRAGMENTS = 10

  const server = new WebSocketServer({ port: 0 })

  server.on('connection', (ws) => {
    const socket = ws._socket
    const payload = Buffer.alloc(FRAGMENT_SIZE, 0x41)

    for (let i = 0; i < NUM_FRAGMENTS; i++) {
      const fin = i === NUM_FRAGMENTS - 1 ? 0x80 : 0x00
      const opcode = i === 0 ? 0x02 : 0x00
      const header = Buffer.alloc(2)
      header[0] = fin | opcode
      header[1] = FRAGMENT_SIZE
      socket.write(header)
      socket.write(payload)
    }
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: LIMIT
    }
  })

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  t.after(async () => {
    client.close()
    server.close()
    await agent.close()
  })

  client.onmessage = () => assert.fail('message should not be received')

  client.addEventListener('error', (event) => {
    assert.ok(event)
    done()
  })
})

test('cumulative payload size is enforced when a frame announces its length', (t, done) => {
  // A first fragment sitting exactly on the limit does not trip the check that
  // runs after the payload is buffered, so the only thing that can stop the
  // peer from announcing yet another limit-sized continuation frame is the
  // announce-time check - and that check has to account for the bytes that
  // were already accumulated. The body of the second frame is deliberately
  // never written: a parser that validates the announced length on its own
  // waits for those bytes forever while holding onto the first fragment.
  const LIMIT = 100

  function maybeDone () {
    if (++maybeDone.callCount === 2) {
      done()
    }
  }

  maybeDone.callCount = 0

  const server = new WebSocketServer({ port: 0 })

  server.on('connection', (ws) => {
    ws.on('error', () => {})

    ws.on('close', (code, reason) => {
      assert.strictEqual(code, 1009)
      assert.strictEqual(reason.toString(), 'Payload size exceeds maximum allowed size')
      maybeDone()
    })

    const socket = ws._socket

    // Binary frame, fin = 0, payload length === LIMIT (fills the whole budget).
    socket.write(Buffer.from([0x02, LIMIT]))
    socket.write(Buffer.alloc(LIMIT, 0x41))

    // Continuation frame, fin = 1, announcing another LIMIT bytes. Header only.
    socket.write(Buffer.from([0x80, LIMIT]))
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: LIMIT
    }
  })

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  t.after(async () => {
    client.close()
    server.close()
    await agent.close()
  })

  client.onmessage = () => assert.fail('message should not be received')

  client.addEventListener('error', () => {
    assert.ok(true)
  })

  client.addEventListener('close', (event) => {
    assert.strictEqual(event.code, 1006)
    maybeDone()
  })
})

test('Malformed over-limit compressed payload closes connection without unhandled error', async (t) => {
  const limit = 1 * 1024 * 1024 // 1 MB
  const server = new WebSocketServer({
    port: 0,
    perMessageDeflate: true
  })

  t.after(() => server.close())
  await once(server, 'listening')

  server.on('connection', (ws) => {
    ws.on('error', () => {})

    const socket = ws._socket

    // Craft a non-final DEFLATE stream that decompresses beyond the size limit,
    // followed by a malformed stored block (BTYPE=00 stored with LEN != ~NLEN) so
    // the inflater would emit Z_DATA_ERROR after the size-limit cleanup runs.
    const def = createDeflateRaw()
    const out = []
    def.on('data', (data) => out.push(data))
    def.write(Buffer.alloc(limit + 64 * 1024))
    def.flush(constants.Z_SYNC_FLUSH, () => {
      const stream = Buffer.concat(out)
      const bad = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff])
      const payload = Buffer.concat([stream, bad])

      // Server -> client binary frame, FIN=1, RSV1=1 (compressed), 64-bit length
      const header = Buffer.alloc(10)
      header[0] = 0xC2
      header[1] = 0x7f
      header.writeUInt32BE(0, 2)
      header.writeUInt32BE(payload.length, 6)
      socket.write(Buffer.concat([header, payload]))
    })
  })

  const agent = new Agent({
    webSocket: {
      maxPayloadSize: limit
    }
  })

  t.after(() => agent.close())

  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}`, { dispatcher: agent })

  let messageReceived = false
  client.addEventListener('message', () => {
    messageReceived = true
  })

  const closePromise = once(client, 'close')
  const timeoutPromise = sleep(5000)

  await Promise.race([closePromise, timeoutPromise])

  assert.strictEqual(messageReceived, false, 'Malformed over-limit payload should be rejected')
  assert.strictEqual(client.readyState, WebSocket.CLOSED, 'Connection should be closed without crashing the process')
})
