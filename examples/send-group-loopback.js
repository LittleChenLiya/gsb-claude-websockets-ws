'use strict';

//
// Loopback demo for `websocket.sendGroup()`.
//
// Run with:
//
//   node examples/send-group-loopback.js
//
// Part 1 shows that the fragments of message A are never interleaved with
// message B, while a ping control frame is still injected between the two
// fragments of A (three frames on the wire: A-frag1, ping, A-frag2).
//
// Part 2 shows that aborting a producer closes the connection without
// sending a pseudo-complete message: the peer only sees a non-final
// fragment followed by the closing handshake, and never a complete message.
//

const assert = require('assert');

const { WebSocket, WebSocketServer } = require('..');

const OPCODES = {
  0x00: 'continuation',
  0x01: 'text',
  0x02: 'binary',
  0x08: 'close',
  0x09: 'ping',
  0x0a: 'pong'
};

/**
 * Creates a parser that reassembles the WebSocket frames received on a raw
 * socket so the exact frames on the wire can be printed.
 *
 * @param {Function} onFrame Called with `{fin, opcode, payload}` per frame
 * @return {Function} A `'data'` listener for the socket
 */
function createFrameParser(onFrame) {
  let buf = Buffer.alloc(0);

  return (chunk) => {
    buf = Buffer.concat([buf, chunk]);

    while (true) {
      if (buf.length < 2) return;

      const fin = (buf[0] & 0x80) !== 0;
      const opcode = buf[0] & 0x0f;
      const masked = (buf[1] & 0x80) !== 0;
      let length = buf[1] & 0x7f;
      let offset = 2;

      if (length === 126) {
        if (buf.length < 4) return;
        length = buf.readUInt16BE(2);
        offset = 4;
      } else if (length === 127) {
        if (buf.length < 10) return;
        length = Number(buf.readBigUInt64BE(2));
        offset = 10;
      }

      const maskLength = masked ? 4 : 0;
      if (buf.length < offset + maskLength + length) return;

      let payload = buf.slice(
        offset + maskLength,
        offset + maskLength + length
      );

      if (masked) {
        const mask = buf.slice(offset, offset + 4);
        payload = Buffer.from(payload);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      }

      buf = buf.slice(offset + maskLength + length);
      onFrame({ fin, opcode, payload });
    }
  };
}

function describeFrame(frame) {
  const kind = OPCODES[frame.opcode] || `0x${frame.opcode.toString(16)}`;
  const fin = frame.fin ? 'FIN' : '...';
  const payload = frame.payload.length
    ? ` ${JSON.stringify(frame.payload.toString())}`
    : '';

  return `${kind} (${fin})${payload}`;
}

/**
 * Opens a loopback server/client pair and resolves when the client is open.
 *
 * @param {Function} onFrame Called for every frame the server receives
 * @return {Promise<Object>} The `{wss, client, server}` triple
 */
function loopback(onFrame) {
  const wss = new WebSocketServer({ port: 0, perMessageDeflate: false });
  const state = { wss, client: null, server: null };

  wss.on('connection', (server) => {
    state.server = server;
    server._socket.prependListener('data', createFrameParser(onFrame));
  });

  return new Promise((resolve) => {
    wss.on('listening', () => {
      state.client = new WebSocket(`ws://localhost:${wss.address().port}`, {
        perMessageDeflate: false
      });
      state.client.on('open', () => resolve(state));
    });
  });
}

async function part1() {
  console.log('--- Part 1: interleaving ---');
  console.log(
    'A is sent in two fragments, B is queued behind it, and a ping is'
  );
  console.log('injected between the fragments of A.\n');

  let finishA;
  const frames = [];
  const messages = [];

  const { wss, client, server } = await loopback((frame) => {
    frames.push(frame);
    console.log(`  wire <- ${describeFrame(frame)}`);

    //
    // The first fragment of A just went out. Inject a ping so that it lands
    // between the two fragments of A on the wire, then let the producer of
    // A finish the message.
    //
    if (frame.opcode === 0x01 && !frame.fin) {
      client.ping('ping-in-the-middle', () => finishA());
    }
  });

  server.on('message', (data) => {
    messages.push(data.toString());
    console.log(`  message: ${JSON.stringify(data.toString())}`);

    if (messages.length === 2) server.close();
  });

  async function* producerA() {
    yield 'Hello, ';
    yield 'world';
    await new Promise((resolve) => {
      finishA = resolve;
    });
  }

  async function* producerB() {
    yield 'message-B';
  }

  const sentA = new Promise((resolve) =>
    client.sendGroup(producerA, (err) => {
      console.log(`  sendGroup(A) callback: ${err || 'ok (written out)'}`);
      resolve();
    })
  );
  const sentB = new Promise((resolve) =>
    client.sendGroup(producerB, (err) => {
      console.log(`  sendGroup(B) callback: ${err || 'ok (written out)'}`);
      resolve();
    })
  );

  await Promise.all([sentA, sentB]);
  await new Promise((resolve) => client.on('close', resolve));

  assert.deepStrictEqual(messages, ['Hello, world', 'message-B']);
  assert.deepStrictEqual(
    frames
      .filter((frame) => frame.opcode !== 0x08)
      .map((frame) => [frame.opcode, frame.fin]),
    [
      [0x01, false],
      [0x09, true],
      [0x00, true],
      [0x01, true]
    ]
  );

  console.log('\n  => peer reassembled A ("Hello, world") before B, and the');
  console.log('     ping was interleaved between the fragments of A.\n');

  await new Promise((resolve) => wss.close(resolve));
}

async function part2() {
  console.log('--- Part 2: abort ---');
  console.log('The producer of A is aborted mid-message.\n');

  const controller = new AbortController();
  const frames = [];
  const messages = [];

  const { wss, client, server } = await loopback((frame) => {
    frames.push(frame);
    console.log(`  wire <- ${describeFrame(frame)}`);

    if (frame.opcode === 0x01 && !frame.fin) controller.abort();
  });

  server.on('message', (data) => messages.push(data.toString()));

  const serverClosed = new Promise((resolve) =>
    server.on('close', (code) => {
      console.log(`  server saw close: code ${code}`);
      resolve();
    })
  );

  async function* producerA() {
    yield 'this-is-';
    yield 'never-completed';
    await new Promise(() => {}); // Suspended forever; aborted here.
  }

  const results = [];
  client.sendGroup(producerA, { signal: controller.signal }, (err) =>
    results.push(['A', err])
  );
  client.sendGroup(['queued-behind-A'], (err) => results.push(['B', err]));

  const clientClosed = new Promise((resolve) =>
    client.on('close', (code) => {
      console.log(`  client closed: code ${code}`);
      resolve();
    })
  );

  await Promise.all([serverClosed, clientClosed]);

  console.log(`  sendGroup(A) callback: ${results[0][1]}`);
  console.log(`  sendGroup(B) callback: ${results[1][1]}`);
  console.log(`  messages delivered to peer: ${messages.length}`);

  const dataFrames = frames.filter((frame) => frame.opcode < 0x08);
  assert.strictEqual(dataFrames.length, 1);
  assert.strictEqual(dataFrames[0].fin, false);
  assert.strictEqual(messages.length, 0);
  assert.strictEqual(results[0][1].name, 'AbortError');
  assert.match(results[1][1].message, /discarded/);

  console.log('\n  => no pseudo-complete message: the peer only received a');
  console.log('     non-final fragment before the connection closed, and the');
  console.log('     queued group was rejected without being sent.\n');

  await new Promise((resolve) => wss.close(resolve));
}

part1()
  .then(part2)
  .then(() => {
    console.log('Demo finished successfully.');
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
