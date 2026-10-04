'use strict';

const assert = require('assert');

const PerMessageDeflate = require('../lib/permessage-deflate');
const Receiver = require('../lib/receiver');
const ReceiverBudget = require('../lib/receiver-budget');
const { kStatusCode } = require('../lib/constants');

describe('ReceiverBudget', () => {
  it('does not debit when both budgets are disabled', () => {
    const budget = new ReceiverBudget();

    assert.strictEqual(budget.messageBudget, 0);
    assert.strictEqual(budget.connectionBudget, 0);
    assert.strictEqual(budget.debit(100), null);
    assert.strictEqual(budget.debit(100), null);
    budget.messageComplete();
    assert.strictEqual(budget.receivedBytes, 0);
  });

  it('returns a message budget error when the per-message limit is exceeded', () => {
    const budget = new ReceiverBudget({ messageBudget: 10 });

    assert.strictEqual(budget.debit(5), null);
    assert.strictEqual(budget.debit(5), null);

    const err = budget.debit(1);

    assert.ok(err instanceof RangeError);
    assert.strictEqual(err.code, 'WS_ERR_MESSAGE_BUDGET_EXCEEDED');
    assert.strictEqual(err.message, 'Message budget exceeded');
    assert.strictEqual(err[kStatusCode], 1009);
  });

  it('returns a connection budget error when the cumulative limit is exceeded', () => {
    const budget = new ReceiverBudget({ connectionBudget: 20 });

    assert.strictEqual(budget.debit(10), null);
    budget.messageComplete();
    assert.strictEqual(budget.receivedBytes, 10);

    assert.strictEqual(budget.debit(10), null);
    budget.messageComplete();
    assert.strictEqual(budget.receivedBytes, 20);

    const err = budget.debit(1);

    assert.ok(err instanceof RangeError);
    assert.strictEqual(err.code, 'WS_ERR_CONNECTION_BUDGET_EXCEEDED');
    assert.strictEqual(err.message, 'Connection budget exceeded');
    assert.strictEqual(err[kStatusCode], 1008);
  });

  it('increments the cumulative total only after a message completes', () => {
    const budget = new ReceiverBudget({ connectionBudget: 100 });

    assert.strictEqual(budget.debit(30), null);
    assert.strictEqual(budget.receivedBytes, 0);

    budget.messageComplete();
    assert.strictEqual(budget.receivedBytes, 30);

    assert.strictEqual(budget.debit(70), null);
    budget.messageComplete();
    assert.strictEqual(budget.receivedBytes, 100);
  });

  it('shares the message budget across fragments of the same message', () => {
    const budget = new ReceiverBudget({ messageBudget: 50 });

    assert.strictEqual(budget.debit(20), null);
    assert.strictEqual(budget.debit(20), null);

    const err = budget.debit(20);

    assert.ok(err instanceof RangeError);
    assert.strictEqual(err.code, 'WS_ERR_MESSAGE_BUDGET_EXCEEDED');
  });
});

describe('Receiver budget integration', () => {
  it('delivers a compressed message within the budget', (done) => {
    const perMessageDeflate = new PerMessageDeflate({ isServer: false });
    perMessageDeflate.accept([{}]);

    const receiver = new Receiver({
      extensions: { 'permessage-deflate': perMessageDeflate },
      isServer: false,
      maxPayload: 0,
      messageBudget: 1000,
      connectionBudget: 10000
    });
    const buf = Buffer.from('Hello World');

    receiver.on('message', (data) => {
      assert.deepStrictEqual(data, buf);
      assert.strictEqual(receiver._budget.receivedBytes, buf.length);
      done();
    });

    receiver.on('error', done);

    perMessageDeflate.compress(buf, true, (err, data) => {
      if (err) return done(err);

      receiver.write(Buffer.from([0xc1, data.length]));
      receiver.write(data);
    });
  });

  it('rejects a compressed message that exceeds the message budget during streaming', (done) => {
    const perMessageDeflate = new PerMessageDeflate({ isServer: false });
    perMessageDeflate.accept([{}]);

    const receiver = new Receiver({
      extensions: { 'permessage-deflate': perMessageDeflate },
      isServer: false,
      maxPayload: 0,
      messageBudget: 10
    });
    const buf = Buffer.from('A'.repeat(100));
    let messageReceived = false;

    receiver.on('message', () => {
      messageReceived = true;
    });

    receiver.on('error', (err) => {
      assert.ok(err instanceof RangeError);
      assert.strictEqual(err.code, 'WS_ERR_MESSAGE_BUDGET_EXCEEDED');
      assert.strictEqual(err[kStatusCode], 1009);
      assert.strictEqual(messageReceived, false);
      done();
    });

    perMessageDeflate.compress(buf, true, (err, data) => {
      if (err) return done(err);

      receiver.write(Buffer.from([0xc1, data.length]));
      receiver.write(data);
    });
  });

  it('rejects a compressed message that exceeds the connection budget', (done) => {
    const perMessageDeflate = new PerMessageDeflate({ isServer: false });
    perMessageDeflate.accept([{}]);

    const receiver = new Receiver({
      extensions: { 'permessage-deflate': perMessageDeflate },
      isServer: false,
      maxPayload: 0,
      connectionBudget: 50
    });
    const buf = Buffer.from('A'.repeat(100));
    let messageReceived = false;

    receiver.on('message', () => {
      messageReceived = true;
    });

    receiver.on('error', (err) => {
      assert.ok(err instanceof RangeError);
      assert.strictEqual(err.code, 'WS_ERR_CONNECTION_BUDGET_EXCEEDED');
      assert.strictEqual(err[kStatusCode], 1008);
      assert.strictEqual(messageReceived, false);
      done();
    });

    perMessageDeflate.compress(buf, true, (err, data) => {
      if (err) return done(err);

      receiver.write(Buffer.from([0xc1, data.length]));
      receiver.write(data);
    });
  });

  it('delivers an uncompressed message within the budget', (done) => {
    const receiver = new Receiver({
      isServer: true,
      maxPayload: 0,
      messageBudget: 1000
    });
    const buf = Buffer.from('Hello World');

    receiver.on('message', (data) => {
      assert.deepStrictEqual(data, buf);
      done();
    });

    receiver.on('error', done);

    receiver.write(
      Buffer.concat([Buffer.from([0x81, 0x8b]), Buffer.alloc(4), buf])
    );
  });

  it('rejects an uncompressed message that exceeds the message budget', (done) => {
    const receiver = new Receiver({
      isServer: true,
      maxPayload: 0,
      messageBudget: 10
    });
    const buf = Buffer.from('A'.repeat(100));
    let messageReceived = false;

    receiver.on('message', () => {
      messageReceived = true;
    });

    receiver.on('error', (err) => {
      assert.ok(err instanceof RangeError);
      assert.strictEqual(err.code, 'WS_ERR_MESSAGE_BUDGET_EXCEEDED');
      assert.strictEqual(err[kStatusCode], 1009);
      assert.strictEqual(messageReceived, false);
      done();
    });

    receiver.write(
      Buffer.concat([Buffer.from([0x81, 0xe4]), Buffer.alloc(4), buf])
    );
  });

  it('does not debit control frames (ping)', (done) => {
    const receiver = new Receiver({
      isServer: true,
      maxPayload: 0,
      messageBudget: 1000,
      connectionBudget: 10000
    });

    receiver.on('ping', () => {
      assert.strictEqual(receiver._budget.receivedBytes, 0);
      done();
    });

    receiver.on('error', done);

    receiver.write(
      Buffer.concat([
        Buffer.from([0x89, 0x84]),
        Buffer.alloc(4),
        Buffer.from('ping')
      ])
    );
  });

  it('shares the message budget across fragments of a compressed message', (done) => {
    const perMessageDeflate = new PerMessageDeflate({ isServer: false });
    perMessageDeflate.accept([{}]);

    const receiver = new Receiver({
      extensions: { 'permessage-deflate': perMessageDeflate },
      isServer: false,
      maxPayload: 0,
      messageBudget: 1000
    });
    const buf = Buffer.from('A'.repeat(100));
    const part1 = buf.subarray(0, 40);
    const part2 = buf.subarray(40);

    receiver.on('message', (data) => {
      assert.strictEqual(data.length, 100);
      assert.strictEqual(receiver._budget.receivedBytes, 100);
      done();
    });

    receiver.on('error', done);

    perMessageDeflate.compress(part1, false, (err, frag1) => {
      if (err) return done(err);

      receiver.write(Buffer.from([0x41, frag1.length]));
      receiver.write(frag1);

      perMessageDeflate.compress(part2, true, (err, frag2) => {
        if (err) return done(err);

        receiver.write(Buffer.from([0x80, frag2.length]));
        receiver.write(frag2);
      });
    });
  });

  it('reports a decompression failure without delivering a message', (done) => {
    const perMessageDeflate = new PerMessageDeflate({ isServer: false });
    perMessageDeflate.accept([{}]);

    const receiver = new Receiver({
      extensions: { 'permessage-deflate': perMessageDeflate },
      isServer: false,
      maxPayload: 0,
      messageBudget: 1000
    });
    let messageReceived = false;

    receiver.on('message', () => {
      messageReceived = true;
    });

    receiver.on('error', (err) => {
      assert.ok(err instanceof Error);
      assert.strictEqual(messageReceived, false);
      done();
    });

    // Invalid compressed data.
    receiver.write(Buffer.from([0xc1, 0x05, 0x00, 0x00, 0xff, 0xff, 0x00]));
  });

  it('keeps maxPayload and the budget separate: maxPayload reports its own reason', (done) => {
    const perMessageDeflate = new PerMessageDeflate({ isServer: false });
    perMessageDeflate.accept([{}]);

    const receiver = new Receiver({
      extensions: { 'permessage-deflate': perMessageDeflate },
      isServer: false,
      maxPayload: 25,
      messageBudget: 1000
    });
    const buf = Buffer.from('A'.repeat(100));

    receiver.on('error', (err) => {
      assert.ok(err instanceof RangeError);
      assert.strictEqual(err.code, 'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH');
      assert.strictEqual(err.message, 'Max payload size exceeded');
      assert.strictEqual(err[kStatusCode], 1009);
      done();
    });

    perMessageDeflate.compress(buf, true, (err, data) => {
      if (err) return done(err);

      receiver.write(Buffer.from([0xc1, data.length]));
      receiver.write(data);
    });
  });
});
