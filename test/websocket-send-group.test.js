'use strict';

const assert = require('assert');
const http = require('http');

const WebSocket = require('..');
const { WebSocketServer } = require('..');

class CustomAgent extends http.Agent {
  addRequest() {}
}

/**
 * Creates a parser that reassembles the WebSocket frames received on a raw
 * socket. Used to observe the exact frames a peer sends on the wire.
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

/**
 * Creates a loopback server/client pair with `perMessageDeflate` disabled.
 *
 * @param {Function} setup Called with the shared state object
 * @return {Object} The shared state object
 * @private
 */
function loopback(setup) {
  const wss = new WebSocketServer({ port: 0, perMessageDeflate: false });
  const frames = [];
  const messages = [];
  const state = { wss, frames, messages, client: null, server: null };

  wss.on('connection', (server) => {
    state.server = server;
    server._socket.prependListener(
      'data',
      createFrameParser((frame) => frames.push(frame))
    );
    server.on('message', (data, isBinary) => {
      messages.push([data.toString(), isBinary]);
    });
  });

  wss.on('listening', () => {
    state.client = new WebSocket(`ws://localhost:${wss.address().port}`, {
      perMessageDeflate: false
    });
    setup(state);
  });

  return state;
}

describe('WebSocket#sendGroup', () => {
  it('throws an error when the ready state is CONNECTING', () => {
    const ws = new WebSocket('ws://localhost', { agent: new CustomAgent() });

    assert.throws(
      () => ws.sendGroup([]),
      /^Error: WebSocket is not open: readyState 0 \(CONNECTING\)$/
    );
  });

  it('serializes groups and allows control frames between fragments', (done) => {
    let finishA;
    const results = [];
    let client;

    const { wss, frames, messages } = loopback((state) => {
      client = state.client;

      client.on('open', () => {
        //
        // Message A is produced in two fragments. The producer pauses before
        // completing so that a ping can be injected on the wire between the
        // two fragments. Message B is queued while A is in flight.
        //
        async function* producerA() {
          yield 'Hello, ';
          yield 'world';
          await new Promise((resolve) => {
            finishA = resolve;
          });
        }

        async function* producerB() {
          yield 'B-message';
        }

        client.sendGroup(producerA, (err) => results.push(['A', err]));
        client.sendGroup(producerB, (err) => results.push(['B', err]));
      });

      client.on('close', (code) => {
        assert.strictEqual(code, 1005);
        assert.deepStrictEqual(
          results.map(([name, err]) => [name, err && err.message]),
          [
            ['A', null],
            ['B', null]
          ]
        );
        wss.close(done);
      });
    });

    wss.on('connection', (server) => {
      let pingSent = false;

      server._socket.prependListener(
        'data',
        createFrameParser((frame) => {
          //
          // As soon as the first fragment of A is seen, inject a ping from
          // the client so that it lands between the two fragments of A on
          // the wire, then let the producer finish A.
          //
          if (!pingSent && frame.opcode === 0x01 && !frame.fin) {
            pingSent = true;
            client.ping('mid', () => finishA());
          }
        })
      );

      server.on('message', () => {
        if (messages.length === 2) {
          assert.deepStrictEqual(messages, [
            ['Hello, world', false],
            ['B-message', false]
          ]);

          const wire = frames.map((frame) => [
            frame.opcode,
            frame.fin,
            frame.payload.toString()
          ]);

          assert.deepStrictEqual(wire, [
            [0x01, false, 'Hello, '],
            [0x09, true, 'mid'],
            [0x00, true, 'world'],
            [0x01, true, 'B-message']
          ]);

          server.close();
        }
      });
    });
  });

  it('supports sync iterables and empty producers', (done) => {
    const results = [];

    const { wss, messages } = loopback(({ client }) => {
      client.on('open', () => {
        client.sendGroup(['foo', 'bar'], (err) => results.push(err));
        client.sendGroup([], (err) => results.push(err));
      });

      client.on('close', () => {
        assert.deepStrictEqual(results, [null, null]);
        wss.close(done);
      });
    });

    wss.on('connection', (server) => {
      server.on('message', () => {
        if (messages.length === 2) {
          assert.deepStrictEqual(messages, [
            ['foobar', false],
            ['', false]
          ]);
          server.close();
        }
      });
    });
  });

  it('closes the connection without a pseudo-complete message on abort', (done) => {
    const controller = new AbortController();
    const results = [];

    const { wss, frames, messages } = loopback((state) => {
      const client = state.client;

      client.on('open', () => {
        async function* producerA() {
          yield 'partial-1';
          yield 'partial-2';
          //
          // Never completes. The group is aborted while the producer is
          // suspended here.
          //
          await new Promise(() => {});
        }

        client.sendGroup(producerA, { signal: controller.signal }, (err) =>
          results.push(['A', err])
        );
        client.sendGroup(['never-sent'], (err) => results.push(['B', err]));
      });

      client.on('close', (code) => {
        assert.strictEqual(code, 1000);

        //
        // The peer never saw a complete message: the only data frame sent
        // does not have the FIN bit set and no message was delivered.
        //
        const dataFrames = frames.filter((frame) => frame.opcode < 0x08);
        assert.strictEqual(dataFrames.length, 1);
        assert.strictEqual(dataFrames[0].fin, false);
        assert.strictEqual(messages.length, 0);

        assert.strictEqual(results[0][0], 'A');
        assert.strictEqual(results[0][1].name, 'AbortError');
        assert.strictEqual(results[1][0], 'B');
        assert.match(
          results[1][1].message,
          /^The message group was discarded before it was sent: /
        );

        //
        // New groups are rejected once the connection is closed.
        //
        client.sendGroup(['x'], (err) => {
          assert.match(
            err.message,
            /^WebSocket is not open: readyState 3 \(CLOSED\)$/
          );
          wss.close(done);
        });
      });
    });

    wss.on('connection', (server) => {
      server.on('close', (code) => {
        assert.strictEqual(code, 1000);
      });

      server._socket.prependListener(
        'data',
        createFrameParser((frame) => {
          if (frame.opcode === 0x01 && !frame.fin) controller.abort();
        })
      );
    });
  });

  it('closes the connection without a pseudo-complete message on failure', (done) => {
    const results = [];

    const { wss, frames, messages } = loopback(({ client }) => {
      client.on('open', () => {
        async function* producerA() {
          yield 'part1';
          yield 'part2';
          throw new Error('boom');
        }

        client.sendGroup(producerA, (err) => results.push(['A', err]));
        client.sendGroup(['never-sent'], (err) => results.push(['B', err]));
      });

      client.on('close', (code) => {
        assert.strictEqual(code, 1011);

        const dataFrames = frames.filter((frame) => frame.opcode < 0x08);
        assert.strictEqual(dataFrames.length, 1);
        assert.strictEqual(dataFrames[0].fin, false);
        assert.strictEqual(messages.length, 0);

        assert.strictEqual(results[0][0], 'A');
        assert.strictEqual(results[0][1].message, 'boom');
        assert.strictEqual(results[1][0], 'B');
        assert.match(
          results[1][1].message,
          /^The message group was discarded before it was sent: boom$/
        );

        wss.close(done);
      });
    });

    wss.on('connection', (server) => {
      server.on('close', (code) => {
        assert.strictEqual(code, 1011);
      });
    });
  });

  it('rejects the active and queued groups when the connection closes', (done) => {
    const results = [];

    const { wss } = loopback(({ client }) => {
      client.on('open', () => {
        async function* producerA() {
          yield 'chunk';
          yield 'chunk';
          await new Promise(() => {});
        }

        client.sendGroup(producerA, (err) => results.push(['A', err]));
        client.sendGroup(['queued'], (err) => results.push(['B', err]));

        //
        // The server goes away while A is in flight.
        //
        setImmediate(() => {
          for (const server of wss.clients) server.terminate();
        });
      });

      client.on('close', () => {
        assert.strictEqual(results.length, 2);
        assert.strictEqual(results[0][0], 'A');
        assert.match(
          results[0][1].message,
          /^WebSocket closed before the message group was sent$/
        );
        assert.strictEqual(results[1][0], 'B');
        assert.match(
          results[1][1].message,
          /^WebSocket closed before the message group was sent$/
        );
        wss.close(done);
      });
    });
  });
});
