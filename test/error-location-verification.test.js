'use strict';

const assert = require('assert');
const PerMessageDeflate = require('../lib/permessage-deflate');
const Receiver = require('../lib/receiver');
const { kErrorLocation } = require('../lib/constants');

describe('Receiver error location', () => {
  it('reports consistent position for bad RSV header (one-shot vs byte-by-byte)', (done) => {
    const frame = Buffer.from([0xa2, 0x00]);

    // One-shot
    const receiver1 = new Receiver();
    receiver1.on('error', (err) => {
      assert.ok(err[kErrorLocation]);
      assert.strictEqual(err[kErrorLocation].frame, 0);
      assert.strictEqual(err[kErrorLocation].offset, 0);
      assert.strictEqual(err[kErrorLocation].field, 'rsv2,rsv3');

      // Byte-by-byte
      const receiver2 = new Receiver();
      receiver2.on('error', (err2) => {
        assert.deepStrictEqual(err2[kErrorLocation], err[kErrorLocation]);
        done();
      });
      receiver2.write(frame.slice(0, 1));
      receiver2.write(frame.slice(1));
    });
    receiver1.write(frame);
  });

  it('reports correct frame number and offset for second frame', (done) => {
    const receiver = new Receiver();

    receiver.on('error', (err) => {
      assert.ok(err[kErrorLocation]);
      assert.strictEqual(err[kErrorLocation].frame, 1);
      assert.strictEqual(err[kErrorLocation].offset, 2);
      assert.strictEqual(err[kErrorLocation].field, 'rsv2,rsv3');
      done();
    });

    // First valid frame (empty text message)
    receiver.write(Buffer.from([0x81, 0x00]));
    // Second frame with bad RSV
    receiver.write(Buffer.from([0xa2, 0x00]));
  });

  it('control frames have independent identity', (done) => {
    const receiver = new Receiver();

    receiver.on('error', (err) => {
      assert.ok(err[kErrorLocation]);
      assert.strictEqual(err[kErrorLocation].frame, 1);
      assert.strictEqual(err[kErrorLocation].controlFrame, 0);
      assert.strictEqual(err[kErrorLocation].field, 'fin');
      done();
    });

    // First valid data frame
    receiver.write(Buffer.from([0x81, 0x00]));
    // Control frame with FIN off
    receiver.write(Buffer.from([0x09, 0x00]));
  });

  it('reports payload span for decompression error', (done) => {
    const perMessageDeflate = new PerMessageDeflate();
    perMessageDeflate.accept([{}]);

    const receiver = new Receiver({
      extensions: { 'permessage-deflate': perMessageDeflate }
    });

    let messageEmitted = false;
    receiver.on('message', () => {
      messageEmitted = true;
    });

    receiver.on('error', (err) => {
      assert.ok(err[kErrorLocation]);
      assert.strictEqual(err[kErrorLocation].field, 'payload');
      assert.strictEqual(err[kErrorLocation].frame, 0);
      assert.strictEqual(err[kErrorLocation].offset, 0);
      // Payload span: header is 2 bytes, payload starts at offset 2
      assert.strictEqual(err[kErrorLocation].payloadStart, 2);
      assert.strictEqual(err[kErrorLocation].payloadEnd, 4);
      // Message should not be emitted
      assert.strictEqual(messageEmitted, false);
      done();
    });

    // Compressed frame with invalid deflate data
    receiver.write(Buffer.from([0xc1, 0x02, 0x00, 0x00]));
  });

  it('length field error is locatable across chunks', (done) => {
    const receiver = new Receiver();

    receiver.on('error', (err) => {
      assert.ok(err[kErrorLocation]);
      assert.strictEqual(err[kErrorLocation].field, 'length');
      assert.strictEqual(err[kErrorLocation].frame, 0);
      assert.strictEqual(err[kErrorLocation].offset, 0);
      done();
    });

    // 64-bit length split across chunks: 0x82 0x7f then 8 bytes
    receiver.write(Buffer.from([0x82, 0x7f]));
    receiver.write(Buffer.from([0x00, 0x20, 0x00]));
    receiver.write(Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00]));
  });

  it('reports correct offset when data precedes the error frame', (done) => {
    const receiver = new Receiver();

    receiver.on('error', (err) => {
      assert.ok(err[kErrorLocation]);
      assert.strictEqual(err[kErrorLocation].frame, 1);
      assert.strictEqual(err[kErrorLocation].offset, 4);
      assert.strictEqual(err[kErrorLocation].field, 'opcode');
      done();
    });

    // First valid frame (4 bytes): text message "hi"
    receiver.write(Buffer.from([0x81, 0x02, 0x68, 0x69]));
    // Second frame with reserved opcode 3, starts at offset 4
    receiver.write(Buffer.from([0x03, 0x00]));
  });

  it('does not store payload in error location', (done) => {
    const receiver = new Receiver();

    receiver.on('error', (err) => {
      assert.ok(err[kErrorLocation]);
      // Location should only have frame, offset, field
      const keys = Object.keys(err[kErrorLocation]);
      assert.ok(keys.includes('frame'));
      assert.ok(keys.includes('offset'));
      assert.ok(keys.includes('field'));
      // Should not have payload data
      assert.ok(!keys.includes('payload'));
      done();
    });

    receiver.write(Buffer.from([0xa2, 0x00]));
  });
});
