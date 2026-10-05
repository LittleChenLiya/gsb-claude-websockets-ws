'use strict';

/**
 * Example: explicit error location reporting for the Receiver.
 *
 * Demonstrates how to inspect the `kErrorLocation` descriptor attached to
 * errors emitted by the Receiver. The descriptor records the actual frame
 * sequence number, the original absolute byte start, and the field that
 * caused the error. For decompression errors only the encoded payload span
 * is reported; no internal bytes are fabricated and no payload is stored.
 *
 * Run with:
 *   node examples/error-location.js
 */

const PerMessageDeflate = require('../lib/permessage-deflate');
const Receiver = require('../lib/receiver');
const { kErrorLocation } = require('../lib/constants');

function showLocation(label, err) {
  console.log(label);
  console.log('  code:     ', err.code);
  console.log('  message:  ', err.message);
  console.log('  location: ', err[kErrorLocation]);
  console.log('');
}

// 1. Bad RSV header on the first frame (one-shot input).
{
  const receiver = new Receiver();
  receiver.on('error', (err) =>
    showLocation('1. bad RSV header (one-shot)', err)
  );
  receiver.write(Buffer.from([0xa2, 0x00]));
}

// 2. Bad RSV header on the second frame (offset and frame number).
{
  const receiver = new Receiver();
  receiver.on('error', (err) =>
    showLocation('2. bad RSV header (second frame)', err)
  );
  receiver.write(Buffer.from([0x81, 0x00])); // valid empty text frame
  receiver.write(Buffer.from([0xa2, 0x00])); // bad RSV, starts at offset 2
}

// 3. Control frame with FIN off (independent control frame identity).
{
  const receiver = new Receiver();
  receiver.on('error', (err) => showLocation('3. control frame FIN off', err));
  receiver.write(Buffer.from([0x81, 0x00])); // valid data frame
  receiver.write(Buffer.from([0x09, 0x00])); // ping with FIN off
}

// 4. Bad decompression (payload span only, no message event).
{
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
    showLocation('4. bad decompression (payload span)', err);
    console.log('  message emitted:', messageEmitted);
  });

  // Compressed frame with invalid deflate data.
  receiver.write(Buffer.from([0xc1, 0x02, 0x00, 0x00]));
}

// 5. Length field error located across chunks.
{
  const receiver = new Receiver();
  receiver.on('error', (err) =>
    showLocation('5. length field across chunks', err)
  );

  // 64-bit length split across chunks: 0x82 0x7f then 8 bytes.
  receiver.write(Buffer.from([0x82, 0x7f]));
  receiver.write(Buffer.from([0x00, 0x20, 0x00]));
  receiver.write(Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00]));
}
