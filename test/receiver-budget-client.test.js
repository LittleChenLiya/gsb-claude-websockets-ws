'use strict';

const assert = require('assert');
const http = require('http');

const WebSocket = require('..');

describe('Receiver budget (client-server)', () => {
  it('rejects a compressed frame that decompresses over the message budget', (done) => {
    const server = http.createServer();
    const wss = new WebSocket.Server({
      server,
      perMessageDeflate: { threshold: 0 },
      messageBudget: 50
    });

    let messageReceived = false;

    wss.on('connection', (ws) => {
      ws.on('error', () => {});
      ws.on('message', () => {
        messageReceived = true;
      });
    });

    server.listen(() => {
      const port = server.address().port;
      const ws = new WebSocket(`ws://localhost:${port}`, {
        perMessageDeflate: { threshold: 0 }
      });

      ws.on('open', () => {
        // A highly compressible string that decompresses to > 50 bytes.
        // The compressed frame is only a few bytes long.
        ws.send('A'.repeat(200));
      });

      ws.on('message', () => {
        done(new Error('the client should not receive any message'));
      });

      ws.on('close', (code) => {
        setTimeout(() => {
          assert.strictEqual(messageReceived, false);
          assert.strictEqual(code, 1009);
          server.close(done);
        }, 50);
      });

      ws.on('error', () => {});
    });
  });

  it('rejects a message that pushes the connection cumulative over budget', (done) => {
    const server = http.createServer();
    const wss = new WebSocket.Server({
      server,
      perMessageDeflate: { threshold: 0 },
      connectionBudget: 100
    });

    let messageCount = 0;

    wss.on('connection', (ws) => {
      ws.on('error', () => {});
      ws.on('message', () => {
        messageCount++;
      });
    });

    server.listen(() => {
      const port = server.address().port;
      const ws = new WebSocket(`ws://localhost:${port}`, {
        perMessageDeflate: { threshold: 0 }
      });

      ws.on('open', () => {
        // A ping must not count toward the budget.
        ws.ping();

        // First message: 60 bytes (cumulative = 60, within budget).
        ws.send('A'.repeat(60));

        // Second message: 50 bytes (cumulative = 110 > 100, rejected).
        setTimeout(() => {
          ws.send('B'.repeat(50));
        }, 150);
      });

      ws.on('close', (code) => {
        setTimeout(() => {
          assert.strictEqual(messageCount, 1);
          assert.strictEqual(code, 1008);
          server.close(done);
        }, 50);
      });

      ws.on('error', () => {});
    });
  });

  it('shares the message budget across fragments of a compressed message', (done) => {
    const server = http.createServer();
    const wss = new WebSocket.Server({
      server,
      perMessageDeflate: { threshold: 0 },
      messageBudget: 50
    });

    let messageCount = 0;
    let lastSize = 0;

    wss.on('connection', (ws) => {
      ws.on('error', () => {});
      ws.on('message', (data) => {
        messageCount++;
        lastSize = data.length;
      });
    });

    server.listen(() => {
      const port = server.address().port;
      const ws = new WebSocket(`ws://localhost:${port}`, {
        perMessageDeflate: { threshold: 0 }
      });

      ws.on('open', () => {
        // Fragmented message within budget: 20 + 20 = 40 bytes.
        ws.send('A'.repeat(20), { fin: false });
        ws.send('A'.repeat(20), { fin: true });

        // Fragmented message over budget: 30 + 30 = 60 > 50 bytes.
        setTimeout(() => {
          ws.send('B'.repeat(30), { fin: false });
          ws.send('B'.repeat(30), { fin: true });
        }, 200);
      });

      ws.on('close', (code) => {
        setTimeout(() => {
          assert.strictEqual(messageCount, 1);
          assert.strictEqual(lastSize, 40);
          assert.strictEqual(code, 1009);
          server.close(done);
        }, 50);
      });

      ws.on('error', () => {});
    });
  });
});
