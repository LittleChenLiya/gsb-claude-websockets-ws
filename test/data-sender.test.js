'use strict';

const assert = require('assert');

const WebSocket = require('..');
const DataSender = require('../lib/data-sender');

function deferred() {
  let resolve, reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function createPair(onConnection) {
  return new Promise((resolve) => {
    const wss = new WebSocket.Server({ port: 0 }, () => {
      if (onConnection) wss.on('connection', onConnection);
      const ws = new WebSocket(`ws://localhost:${wss.address().port}`);
      ws.on('open', () => resolve({ wss, ws }));
    });
  });
}

function closePair(wss, ws, done) {
  if (ws.readyState === WebSocket.CLOSED) {
    wss.close(done);
  } else {
    ws.on('close', () => wss.close(done));
    ws.close();
  }
}

describe('DataSender', () => {
  it('sends a message atomically: A (2 fragments) then B (1 fragment), with a ping in between', (done) => {
    const received = [];
    let pingCount = 0;

    createPair((peer) => {
      peer.on('message', (data) => received.push(data));
      peer.on('ping', () => pingCount++);
    }).then(({ wss, ws }) => {
      const sender = new DataSender(ws);
      const pingSent = deferred();

      async function* aProducer() {
        yield { data: Buffer.from('A1'), fin: false };
        await pingSent.promise;
        yield { data: Buffer.from('A2'), fin: true };
      }

      async function* bProducer() {
        yield { data: Buffer.from('B1'), fin: true };
      }

      sender.on('frame', (frame) => {
        if (frame.fragment.toString() === 'A1') {
          ws.ping();
          pingSent.resolve();
        }
      });

      const group = sender.addGroup([
        { producer: aProducer(), binary: true },
        { producer: bProducer(), binary: true }
      ]);

      group.then(() => {
        setTimeout(() => {
          assert.strictEqual(received.length, 2);
          assert.deepStrictEqual(
            received[0],
            Buffer.concat([Buffer.from('A1'), Buffer.from('A2')])
          );
          assert.deepStrictEqual(received[1], Buffer.from('B1'));
          assert.strictEqual(pingCount, 1);
          closePair(wss, ws, done);
        }, 50);
      });
    });
  });

  it('closes the connection without a pseudo-complete message when a producer is cancelled midway', (done) => {
    const received = [];
    let connectionClosed = false;

    createPair((peer) => {
      peer.on('message', (data) => received.push(data));
      peer.on('close', () => {
        connectionClosed = true;
      });
    }).then(({ wss, ws }) => {
      const sender = new DataSender(ws);
      const aController = new AbortController();

      async function* aProducer() {
        yield { data: Buffer.from('A1'), fin: false };
        yield { data: Buffer.from('A2'), fin: true };
      }

      async function* bProducer() {
        yield { data: Buffer.from('B1'), fin: true };
      }

      sender.on('frame', (frame) => {
        if (frame.fragment.toString() === 'A1') {
          aController.abort();
        }
      });

      const group = sender.addGroup([
        { producer: aProducer(), binary: true, signal: aController.signal },
        { producer: bProducer(), binary: true }
      ]);

      group.then(
        () => done(new Error('group should have been rejected')),
        (err) => {
          assert.ok(err instanceof Error);
          setTimeout(() => {
            assert.strictEqual(received.length, 0);
            assert.strictEqual(connectionClosed, true);
            assert.strictEqual(sender.closed, true);
            closePair(wss, ws, done);
          }, 50);
        }
      );
    });
  });

  it('explicitly rejects waiting groups when the active group is cancelled', (done) => {
    createPair((peer) => peer.on('message', () => {})).then(({ wss, ws }) => {
      const sender = new DataSender(ws);
      const group1Controller = new AbortController();

      async function* slowProducer() {
        yield { data: Buffer.from('S1'), fin: true };
      }

      async function* fastProducer() {
        yield { data: Buffer.from('F1'), fin: true };
      }

      const group1 = sender.addGroup(
        [{ producer: slowProducer(), binary: true }],
        { signal: group1Controller.signal }
      );

      const group2 = sender.addGroup([
        { producer: fastProducer(), binary: true }
      ]);

      sender.on('frame', (frame) => {
        if (frame.fragment.toString() === 'S1') {
          group1Controller.abort();
        }
      });

      let group1Rejected = false;
      let group2Rejected = false;

      group1.then(
        () => done(new Error('group1 should have been rejected')),
        (err) => {
          assert.ok(err instanceof Error);
          group1Rejected = true;
          checkDone();
        }
      );

      group2.then(
        () => done(new Error('group2 should have been rejected')),
        (err) => {
          assert.ok(err instanceof Error);
          group2Rejected = true;
          checkDone();
        }
      );

      function checkDone() {
        if (group1Rejected && group2Rejected) {
          assert.strictEqual(sender.closed, true);
          closePair(wss, ws, done);
        }
      }
    });
  });

  it('rejects new groups after the sender is closed', (done) => {
    createPair((peer) => peer.on('message', () => {})).then(({ wss, ws }) => {
      const sender = new DataSender(ws);

      async function* producer() {
        yield { data: Buffer.from('X1'), fin: true };
      }

      sender.close();

      const group = sender.addGroup([{ producer: producer(), binary: true }]);

      group.then(
        () => done(new Error('group should have been rejected')),
        (err) => {
          assert.ok(err instanceof Error);
          assert.strictEqual(sender.closed, true);
          closePair(wss, ws, done);
        }
      );
    });
  });

  it('sends a three-frame message as one complete message to the peer', (done) => {
    const received = [];

    createPair((peer) => {
      peer.on('message', (data) => received.push(data));
    }).then(({ wss, ws }) => {
      const sender = new DataSender(ws);

      async function* producer() {
        yield { data: Buffer.from('f1'), fin: false };
        yield { data: Buffer.from('f2'), fin: false };
        yield { data: Buffer.from('f3'), fin: true };
      }

      const group = sender.addGroup([{ producer: producer(), binary: true }]);

      group.then(() => {
        setTimeout(() => {
          assert.strictEqual(received.length, 1);
          assert.deepStrictEqual(
            received[0],
            Buffer.concat([
              Buffer.from('f1'),
              Buffer.from('f2'),
              Buffer.from('f3')
            ])
          );
          closePair(wss, ws, done);
        }, 50);
      });
    });
  });

  it('resolves the group when all fragments are dispatched (not peer-acknowledged)', (done) => {
    createPair((peer) => peer.on('message', () => {})).then(({ wss, ws }) => {
      const sender = new DataSender(ws);

      async function* producer() {
        yield { data: Buffer.from('d1'), fin: true };
      }

      const group = sender.addGroup([{ producer: producer(), binary: true }]);

      group.then(() => {
        assert.strictEqual(sender.closed, false);
        closePair(wss, ws, done);
      });
    });
  });
});
