'use strict';

const EventEmitter = require('events');

const { EMPTY_BUFFER } = require('./constants');

const IDLE = 0;
const SENDING = 1;
const CLOSED = 2;

/**
 * Combines multiple abort signals into a single signal that aborts when any
 * of the input signals aborts.
 *
 * @param {...AbortSignal} signals The signals to combine
 * @return {AbortSignal} The combined signal
 * @private
 */
function combineSignals(...signals) {
  const controller = new AbortController();

  for (const signal of signals) {
    if (!signal) continue;

    if (signal.aborted) {
      controller.abort();
      break;
    }

    signal.addEventListener('abort', () => controller.abort(), { once: true });
  }

  return controller.signal;
}

/**
 * Races a promise against an abort signal. If the signal aborts before the
 * promise settles, the returned promise rejects with an error.
 *
 * @param {AbortSignal} signal The abort signal
 * @param {Promise} promise The promise to race
 * @return {Promise} A new promise
 * @private
 */
function raceSignal(signal, promise) {
  if (!signal) return promise;

  if (signal.aborted) {
    return Promise.reject(new Error('The operation was aborted'));
  }

  let onAbort;

  const abortPromise = new Promise((_, reject) => {
    onAbort = () => reject(new Error('The operation was aborted'));
    signal.addEventListener('abort', onAbort, { once: true });
  });

  return Promise.race([promise, abortPromise]).finally(() => {
    signal.removeEventListener('abort', onAbort);
  });
}

/**
 * A message-level WebSocket data sender.
 *
 * Unlike the `WebSocket#send()` method, which dispatches a single frame, a
 * `DataSender` sends whole *messages*: each message is produced by an async
 * fragment producer (an async iterable) and all of its fragments are sent
 * atomically — no other data message's frames can be interleaved. Control
 * frames (ping/pong/close) bypass the data queue and can still be sent at any
 * time through the normal `WebSocket` API.
 *
 * If a fragment producer fails or is cancelled midway through a message, the
 * connection is closed *without* sending a FIN frame for that message, so the
 * peer never sees a pseudo-complete message. Messages that have not yet been
 * started are removed and all waiting groups are explicitly rejected.
 *
 * The `send` callback only indicates that the frame has been handed to the
 * underlying socket (flushed to the kernel); it is *not* an acknowledgment
 * from the peer.
 *
 * @extends EventEmitter
 * @public
 */
class DataSender extends EventEmitter {
  /**
   * Creates a DataSender instance.
   *
   * @param {WebSocket} ws The WebSocket to send data on
   * @param {Object} [options] Options object
   * @public
   */
  constructor(ws, options = {}) {
    super();

    this._ws = ws;
    this._options = options;
    this._state = IDLE;
    this._queue = [];
    this._activeGroup = null;
    this._closed = false;
    this._closeError = null;

    this._onWsClose = this._onWsClose.bind(this);
    this._onWsError = this._onWsError.bind(this);

    ws.on('close', this._onWsClose);
    ws.on('error', this._onWsError);
  }

  /**
   * The current state of the sender.
   *
   * @type {String}
   * @public
   */
  get state() {
    return ['idle', 'sending', 'closed'][this._state];
  }

  /**
   * Indicates whether the sender has been closed.
   *
   * @type {Boolean}
   * @public
   */
  get closed() {
    return this._closed;
  }

  /**
   * Adds a group of messages to be sent.
   *
   * A group is a sequence of messages that are sent one after another. Each
   * message is produced by an async fragment producer. The producer yields
   * fragments as `{ data, fin }` objects, where `fin` indicates whether the
   * fragment is the last one of the message. The returned promise resolves
   * when *all* fragments of the group have been dispatched to the underlying
   * socket (NOT acknowledged by the peer). It rejects if the group is
   * rejected — for example because the sender is closed, the connection drops,
   * or a fragment producer fails or is cancelled.
   *
   * @param {Object[]} messages The messages to send
   * @param {AsyncIterable} messages[].producer An async iterable yielding
   *     `{ data, fin }` fragments
   * @param {Boolean} [messages[].binary=true] Specifies whether the fragments
   *     are binary or text
   * @param {AbortSignal} [messages[].signal] A signal that cancels the message
   * @param {Object} [options] Options object
   * @param {AbortSignal} [options.signal] A signal that cancels the whole group
   * @return {Promise} A promise that resolves when the group has been
   *     dispatched, or rejects if the group is rejected
   * @public
   */
  addGroup(messages, options = {}) {
    if (this._closed) {
      return Promise.reject(new Error('DataSender is closed'));
    }

    const group = {
      messages: messages.map((m) => ({
        producer: m.producer,
        binary: m.binary !== false,
        signal: m.signal
      })),
      signal: options.signal,
      resolve: null,
      reject: null,
      promise: null
    };

    group.promise = new Promise((resolve, reject) => {
      group.resolve = resolve;
      group.reject = reject;
    });

    if (group.signal) {
      if (group.signal.aborted) {
        group.reject(new Error('The operation was aborted'));
        return group.promise;
      }

      group.signal.addEventListener('abort', () => this._onGroupAbort(group), {
        once: true
      });
    }

    this._queue.push(group);
    this._pump();

    return group.promise;
  }

  /**
   * Handles the abort of a group signal.
   *
   * @param {Object} group The group whose signal aborted
   * @private
   */
  _onGroupAbort(group) {
    if (this._activeGroup === group) {
      //
      // The active message is cancelled through the combined signal, which
      // causes `_sendMessage()` to throw and the pump to call `_fail()`.
      // Nothing to do here.
      //
      return;
    }

    //
    // A waiting group is removed from the queue and explicitly rejected.
    //
    const idx = this._queue.indexOf(group);
    if (idx !== -1) this._queue.splice(idx, 1);
    group.reject(new Error('The operation was aborted'));
  }

  /**
   * Pumps the send queue.
   *
   * @private
   */
  async _pump() {
    if (this._state !== IDLE || this._closed) return;

    const group = this._queue.shift();
    if (!group) return;

    this._activeGroup = group;
    this._state = SENDING;

    try {
      for (const msg of group.messages) {
        await this._sendMessage(msg, group.signal);
      }

      group.resolve();
    } catch (err) {
      this._fail(err);
    } finally {
      this._activeGroup = null;
      this._state = IDLE;
      this._pump();
    }
  }

  /**
   * Sends a single message, producing its fragments from the async producer.
   *
   * The producer yields fragments as `{ data, fin }` objects, where `fin`
   * indicates whether the fragment is the last one of the message. If the
   * producer fails or is cancelled midway, the method throws and the
   * connection is closed without sending a FIN frame for the partial
   * message.
   *
   * @param {Object} msg The message to send
   * @param {AbortSignal} groupSignal The group's abort signal
   * @private
   */
  async _sendMessage(msg, groupSignal) {
    const signal = combineSignals(msg.signal, groupSignal);
    const iter = msg.producer[Symbol.asyncIterator]();

    let sentAny = false;

    while (true) {
      const result = await raceSignal(signal, iter.next());

      if (result.done) {
        //
        // The producer finished. If no fragments were yielded, send an empty
        // FIN frame. Otherwise the last fragment was already sent with
        // `fin=true`.
        //
        if (!sentAny) {
          await raceSignal(signal, this._write(EMPTY_BUFFER, msg.binary, true));
        }
        return;
      }

      const { data, fin = false } = result.value;

      await raceSignal(signal, this._write(data, msg.binary, fin));
      sentAny = true;

      if (fin) return;
    }
  }

  /**
   * Writes a single frame to the underlying WebSocket.
   *
   * The returned promise resolves when the frame has been flushed to the
   * socket. This is NOT an acknowledgment from the peer.
   *
   * @param {*} fragment The fragment to send
   * @param {Boolean} binary Specifies whether the fragment is binary or text
   * @param {Boolean} fin Specifies whether this is the last fragment
   * @return {Promise} A promise that resolves when the frame is flushed
   * @private
   */
  _write(fragment, binary, fin) {
    return new Promise((resolve, reject) => {
      if (this._closed) {
        reject(new Error('DataSender is closed'));
        return;
      }

      this._ws.send(fragment, { binary, fin }, (err) => {
        if (err) {
          reject(err);
        } else {
          this.emit('frame', { fragment, binary, fin });
          resolve();
        }
      });
    });
  }

  /**
   * Fails the sender: closes the connection and rejects all pending groups.
   *
   * @param {Error} err The error that caused the failure
   * @private
   */
  _fail(err) {
    if (this._closed) return;

    this._closed = true;
    this._state = CLOSED;
    this._closeError = err;

    if (this._activeGroup) {
      this._activeGroup.reject(err);
      this._activeGroup = null;
    }

    //
    // Reject all waiting groups explicitly.
    //
    while (this._queue.length) {
      const group = this._queue.shift();
      group.reject(err);
    }

    //
    // Close the connection without sending a FIN frame for the partial
    // message, so the peer never sees a pseudo-complete message.
    //
    try {
      this._ws.close(1011, 'DataSender failed');
    } catch (e) {
      // Ignore.
    }

    this.emit('close', err);
  }

  /**
   * Handles the WebSocket `'close'` event.
   *
   * @private
   */
  _onWsClose() {
    if (this._closed) return;

    this._closed = true;
    this._state = CLOSED;
    const err = new Error('WebSocket closed');
    this._closeError = err;

    if (this._activeGroup) {
      this._activeGroup.reject(err);
      this._activeGroup = null;
    }

    while (this._queue.length) {
      const group = this._queue.shift();
      group.reject(err);
    }

    this.emit('close', err);
  }

  /**
   * Handles the WebSocket `'error'` event.
   *
   * @param {Error} err The error
   * @private
   */
  _onWsError(err) {
    if (this._closed) return;

    this._fail(err);
  }

  /**
   * Closes the sender.
   *
   * Rejects all waiting groups with an explicit error. New groups are
   * rejected.
   *
   * @public
   */
  close() {
    if (this._closed) return;

    this._fail(new Error('DataSender closed'));
  }
}

module.exports = DataSender;
