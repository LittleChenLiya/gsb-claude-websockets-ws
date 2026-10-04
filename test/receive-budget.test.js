'use strict';

const assert = require('assert');
const zlib = require('zlib');

const PerMessageDeflate = require('../lib/permessage-deflate');
const Receiver = require('../lib/receiver');
const Sender = require('../lib/sender');
const WebSocket = require('..');
const { WebSocketServer } = WebSocket;
const { kStatusCode } = require('../lib/constants');

//
// Fixtures are intentionally tiny (tens of bytes): a highly compressible
// text whose compressed form is much shorter than the receive budgets.
//
const TEXT = 'a'.repeat(100);
const COMPRESSED_TEXT = zlib.deflateRawSync(Buffer.from(TEXT));

assert.ok(
  COMPRESSED_TEXT.length < 64,
  'fixture sanity: compressed frame must be tens of bytes'
);
assert.ok(TEXT.length > 64, 'fixture sanity: decompressed text exceeds 64');

describe('receive budget', () => {
  describe('WebSocketServer integration', () => {
    it('rejects a short compressed frame that inflates beyond the message budget', (done) => {
      let serverMessages = 0;
      let serverError = null;

      const wss = new WebSocketServer(
        {
          maxDecompressedMessageBytes: 64,
          perMessageDeflate: true,
          port: 0
        },
        () => {
          const ws = new WebSocket(`ws://127.0.0.1:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });

          ws.on('open', () => ws.send(TEXT));
          ws.on('close', (code) => {
            assert.strictEqual(code, 1009);

            setTimeout(() => {
              assert.strictEqual(serverMessages, 0);
              assert.ok(serverError);
              assert.strictEqual(
                serverError.code,
                'WS_ERR_DECOMPRESSED_MESSAGE_BUDGET_EXCEEDED'
              );
              wss.close(done);
            }, 50);
          });
        }
      );

      wss.on('connection', (ws) => {
        ws.on('message', () => serverMessages++);
        ws.on('error', (err) => {
          serverError = err;
        });
      });
    });

    it('reports maxPayload and the message budget with distinct reasons', (done) => {
      let serverError = null;

      const wss = new WebSocketServer(
        {
          maxDecompressedMessageBytes: 1024,
          maxPayload: 32,
          perMessageDeflate: true,
          port: 0
        },
        () => {
          const ws = new WebSocket(`ws://127.0.0.1:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });

          ws.on('open', () => ws.send(TEXT));
          ws.on('close', (code) => {
            assert.strictEqual(code, 1009);

            setTimeout(() => {
              assert.ok(serverError);
              assert.strictEqual(
                serverError.code,
                'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH'
              );
              assert.strictEqual(
                serverError.message,
                'Max payload size exceeded'
              );
              wss.close(done);
            }, 50);
          });
        }
      );

      wss.on('connection', (ws) => {
        ws.on('error', (err) => {
          serverError = err;
        });
      });
    });

    it('enforces the cumulative connection budget after successful messages', (done) => {
      const received = [];
      let serverError = null;

      const wss = new WebSocketServer(
        {
          maxConnectionBytes: 150,
          perMessageDeflate: true,
          port: 0
        },
        () => {
          const ws = new WebSocket(`ws://127.0.0.1:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });

          ws.on('open', () => {
            ws.send('b'.repeat(60));
            ws.send('c'.repeat(60));
            ws.send('d'.repeat(60));
          });
          ws.on('close', (code) => {
            assert.strictEqual(code, 1009);

            setTimeout(() => {
              //
              // The first two messages (120 decompressed bytes in total)
              // are delivered; the third pushes the connection to 180
              // bytes, past the 150 byte budget, and is not delivered.
              //
              assert.deepStrictEqual(received, [
                'b'.repeat(60),
                'c'.repeat(60)
              ]);
              assert.ok(serverError);
              assert.strictEqual(
                serverError.code,
                'WS_ERR_CONNECTION_BUDGET_EXCEEDED'
              );
              wss.close(done);
            }, 50);
          });
        }
      );

      wss.on('connection', (ws) => {
        ws.on('message', (data) => received.push(data.toString()));
        ws.on('error', (err) => {
          serverError = err;
        });
      });
    });

    it('does not charge control frames against the connection budget', (done) => {
      const received = [];
      const pings = [];
      let serverError = null;

      const wss = new WebSocketServer(
        { maxConnectionBytes: 100, port: 0 },
        () => {
          const ws = new WebSocket(`ws://127.0.0.1:${wss.address().port}`, {
            perMessageDeflate: false
          });

          ws.on('open', () => {
            ws.ping('x'.repeat(40));
            ws.send('e'.repeat(40));
            ws.send('f'.repeat(40));
            ws.send('g'.repeat(40));
          });
          ws.on('close', (code) => {
            assert.strictEqual(code, 1009);

            setTimeout(() => {
              assert.deepStrictEqual(pings, ['x'.repeat(40)]);
              assert.deepStrictEqual(received, [
                'e'.repeat(40),
                'f'.repeat(40)
              ]);
              assert.ok(serverError);
              assert.strictEqual(
                serverError.code,
                'WS_ERR_CONNECTION_BUDGET_EXCEEDED'
              );
              wss.close(done);
            }, 50);
          });
        }
      );

      wss.on('connection', (ws) => {
        ws.on('ping', (data) => pings.push(data.toString()));
        ws.on('message', (data) => received.push(data.toString()));
        ws.on('error', (err) => {
          serverError = err;
        });
      });
    });

    it('applies the message budget to uncompressed messages too', (done) => {
      let serverMessages = 0;
      let serverError = null;

      const wss = new WebSocketServer(
        { maxDecompressedMessageBytes: 32, port: 0 },
        () => {
          const ws = new WebSocket(`ws://127.0.0.1:${wss.address().port}`, {
            perMessageDeflate: false
          });

          ws.on('open', () => ws.send('h'.repeat(40)));
          ws.on('close', (code) => {
            assert.strictEqual(code, 1009);

            setTimeout(() => {
              assert.strictEqual(serverMessages, 0);
              assert.ok(serverError);
              assert.strictEqual(
                serverError.code,
                'WS_ERR_DECOMPRESSED_MESSAGE_BUDGET_EXCEEDED'
              );
              wss.close(done);
            }, 50);
          });
        }
      );

      wss.on('connection', (ws) => {
        ws.on('message', () => serverMessages++);
        ws.on('error', (err) => {
          serverError = err;
        });
      });
    });

    it('closes with 1007 and does not deliver the message when decompression fails', (done) => {
      let serverMessages = 0;
      let serverError = null;

      const wss = new WebSocketServer(
        { maxDecompressedMessageBytes: 64, perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://127.0.0.1:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });

          ws.on('open', () => {
            //
            // Bypass the sender and write a raw compressed frame whose
            // payload is not valid deflate data.
            //
            const frame = Sender.frame(Buffer.from('garbage-deflate-data'), {
              fin: true,
              rsv1: true,
              opcode: 0x01,
              mask: true,
              readOnly: false
            });

            ws._socket.write(Buffer.concat(frame));
          });
          ws.on('close', (code) => {
            assert.strictEqual(code, 1007);

            setTimeout(() => {
              assert.strictEqual(serverMessages, 0);
              assert.ok(serverError);
              assert.ok(
                serverError.code !==
                  'WS_ERR_DECOMPRESSED_MESSAGE_BUDGET_EXCEEDED' &&
                  serverError.code !== 'WS_ERR_CONNECTION_BUDGET_EXCEEDED'
              );
              wss.close(done);
            }, 50);
          });
        }
      );

      wss.on('connection', (ws) => {
        ws.on('message', () => serverMessages++);
        ws.on('error', (err) => {
          serverError = err;
        });
      });
    });

    it('keeps the default receiving behavior when no budgets are set', (done) => {
      const wss = new WebSocketServer(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://127.0.0.1:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });

          ws.on('open', () => ws.send('z'.repeat(2000)));
          ws.on('close', () => wss.close(done));
        }
      );

      wss.on('connection', (ws) => {
        ws.on('message', (data) => {
          assert.strictEqual(data.toString(), 'z'.repeat(2000));
          ws.close();
        });
        ws.on('error', done);
      });
    });
  });

  describe('Receiver', () => {
    it('rejects during inflation when the message budget is exceeded', (done) => {
      const perMessageDeflate = new PerMessageDeflate({ isServer: false });
      perMessageDeflate.accept([{}]);

      const receiver = new Receiver({
        extensions: { 'permessage-deflate': perMessageDeflate },
        isServer: false,
        maxDecompressedMessageBytes: 64
      });

      let messages = 0;

      receiver.on('message', () => messages++);
      receiver.on('error', (err) => {
        assert.ok(err instanceof RangeError);
        assert.strictEqual(
          err.code,
          'WS_ERR_DECOMPRESSED_MESSAGE_BUDGET_EXCEEDED'
        );
        assert.strictEqual(err.message, 'Decompressed message budget exceeded');
        assert.strictEqual(err[kStatusCode], 1009);
        assert.strictEqual(messages, 0);
        done();
      });

      perMessageDeflate.compress(Buffer.from(TEXT), true, (err, data) => {
        if (err) return done(err);

        assert.ok(data.length < 64);
        receiver.write(Buffer.from([0xc1, data.length]));
        receiver.write(data);
      });
    });

    it('shares the message budget between the fragments of a message', (done) => {
      const perMessageDeflate = new PerMessageDeflate({ isServer: false });
      perMessageDeflate.accept([{}]);

      const receiver = new Receiver({
        extensions: { 'permessage-deflate': perMessageDeflate },
        isServer: false,
        maxDecompressedMessageBytes: 64
      });
      const buf = Buffer.from('A'.repeat(40));

      let messages = 0;

      receiver.on('message', () => messages++);
      receiver.on('error', (err) => {
        assert.strictEqual(
          err.code,
          'WS_ERR_DECOMPRESSED_MESSAGE_BUDGET_EXCEEDED'
        );
        assert.strictEqual(err[kStatusCode], 1009);
        assert.strictEqual(messages, 0);
        done();
      });

      perMessageDeflate.compress(buf, false, (err, fragment1) => {
        if (err) return done(err);

        receiver.write(Buffer.from([0x41, fragment1.length]));
        receiver.write(fragment1);

        perMessageDeflate.compress(buf, true, (err, fragment2) => {
          if (err) return done(err);

          receiver.write(Buffer.from([0x80, fragment2.length]));
          receiver.write(fragment2);
        });
      });
    });

    it('charges a legal fragmented message once against the connection budget', (done) => {
      const perMessageDeflate = new PerMessageDeflate({ isServer: false });
      perMessageDeflate.accept([{}]);

      const receiver = new Receiver({
        extensions: { 'permessage-deflate': perMessageDeflate },
        isServer: false,
        maxConnectionBytes: 100,
        maxDecompressedMessageBytes: 128
      });
      const buf = Buffer.from('A'.repeat(40));

      const messages = [];

      receiver.on('message', (data) => messages.push(data.toString()));
      receiver.on('error', (err) => {
        //
        // The fragmented message (2 x 40 bytes) is delivered and charges 80
        // bytes against the 100 byte connection budget. The next 40 byte
        // message exceeds the budget and is rejected.
        //
        assert.strictEqual(err.code, 'WS_ERR_CONNECTION_BUDGET_EXCEEDED');
        assert.strictEqual(err.message, 'Connection receive budget exceeded');
        assert.strictEqual(err[kStatusCode], 1009);
        assert.deepStrictEqual(messages, ['A'.repeat(80)]);
        done();
      });

      perMessageDeflate.compress(buf, false, (err, fragment1) => {
        if (err) return done(err);

        receiver.write(Buffer.from([0x41, fragment1.length]));
        receiver.write(fragment1);

        perMessageDeflate.compress(buf, true, (err, fragment2) => {
          if (err) return done(err);

          receiver.write(Buffer.from([0x80, fragment2.length]));
          receiver.write(fragment2);

          perMessageDeflate.compress(buf, true, (err, message) => {
            if (err) return done(err);

            receiver.write(Buffer.from([0xc1, message.length]));
            receiver.write(message);
          });
        });
      });
    });

    it('does not charge control frames against the budgets', (done) => {
      const receiver = new Receiver({
        isServer: true,
        maxConnectionBytes: 40,
        maxDecompressedMessageBytes: 40
      });

      const pings = [];

      receiver.on('ping', (data) => pings.push(data));
      receiver.on('message', (data, isBinary) => {
        assert.strictEqual(data.toString(), 'i'.repeat(40));
        assert.ok(!isBinary);
        assert.strictEqual(pings.length, 1);
        done();
      });
      receiver.on('error', done);

      //
      // A masked 40 byte ping followed by a masked 40 byte text message.
      // The ping must not consume the 40 byte connection budget.
      //
      const ping = Sender.frame(Buffer.from('x'.repeat(40)), {
        fin: true,
        rsv1: false,
        opcode: 0x09,
        mask: true,
        readOnly: false
      });
      const message = Sender.frame(Buffer.from('i'.repeat(40)), {
        fin: true,
        rsv1: false,
        opcode: 0x01,
        mask: true,
        readOnly: false
      });

      receiver.write(Buffer.concat([...ping, ...message]));
    });
  });
});
