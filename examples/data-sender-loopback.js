'use strict';

const WebSocket = require('..');
const DataSender = require('../lib/data-sender');

const PORT = 0;

function runDemo() {
  const wss = new WebSocket.Server({ port: PORT }, () => {
    const port = wss.address().port;
    console.log(`[loopback] server listening on port ${port}`);

    const ws = new WebSocket(`ws://localhost:${port}`);

    ws.on('open', () => {
      console.log('[loopback] client connected');
      const sender = new DataSender(ws);

      sender.on('frame', (frame) => {
        console.log(
          `[loopback] frame written: fragment=${frame.fragment.toString()} fin=${frame.fin}`
        );
      });

      sender.on('close', (err) => {
        console.log(`[loopback] sender closed: ${err.message}`);
      });

      //
      // Demo 1: a three-frame message sent as one complete message.
      //
      async function* threeFrameProducer() {
        yield { data: Buffer.from('frame-1'), fin: false };
        yield { data: Buffer.from('frame-2'), fin: false };
        yield { data: Buffer.from('frame-3'), fin: true };
      }

      //
      // Demo 2: a ping (control frame) interleaved between fragments.
      //
      async function* pingPongProducer() {
        yield { data: Buffer.from('ping-demo-1'), fin: false };
        yield { data: Buffer.from('ping-demo-2'), fin: true };
      }

      const group1 = sender.addGroup([
        { producer: threeFrameProducer(), binary: true }
      ]);

      group1.then(() => {
        console.log(
          '[loopback] three-frame group dispatched (not peer-acknowledged)'
        );

        //
        // Send a ping while the next group is in progress.
        //
        const group2 = sender.addGroup([
          { producer: pingPongProducer(), binary: true }
        ]);

        sender.once('frame', (frame) => {
          if (frame.fragment.toString() === 'ping-demo-1') {
            console.log('[loopback] sending ping between fragments');
            ws.ping(Buffer.from('interleaved-ping'));
          }
        });

        group2.then(() => {
          console.log('[loopback] ping-demo group dispatched');
          setTimeout(() => {
            console.log('[loopback] demo complete');
            ws.on('close', () => wss.close(() => process.exit(0)));
            ws.close();
          }, 100);
        });
      });
    });
  });

  wss.on('connection', (peer) => {
    console.log('[loopback] server: client connected');
    let msgCount = 0;
    peer.on('message', (data) => {
      msgCount++;
      console.log(
        `[loopback] server: received complete message #${msgCount}: ${data.toString()}`
      );
    });
    peer.on('ping', (data) => {
      console.log(`[loopback] server: received ping: ${data.toString()}`);
    });
    peer.on('close', () => {
      console.log('[loopback] server: client disconnected');
    });
  });
}

runDemo();
