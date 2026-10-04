'use strict';

const { kStatusCode } = require('./constants');

/**
 * Receive budget.
 *
 * An opt-in layer that rejects incoming messages based on their decompressed
 * size and a per-connection cumulative quota. Unlike `maxPayload`, which only
 * sees the compressed frame length for compressed messages, the budget is
 * debited with the decompressed output while the inflate stream is producing
 * it, so an over-limit message is aborted before its whole output is buffered.
 *
 * Fragmented messages share the same message budget: each decompressed chunk
 * (or each uncompressed fragment payload) accumulates into the current
 * message total. Control frames (ping/pong/close) are never debited.
 *
 * The connection cumulative total is only incremented after a message has been
 * successfully received and delivered.
 */
class ReceiverBudget {
  /**
   * @param {Object} [options] Options object
   * @param {Number} [options.messageBudget=0] The maximum allowed decompressed
   *     size of a single message. `0` disables the limit
   * @param {Number} [options.connectionBudget=0] The maximum allowed cumulative
   *     decompressed size of all messages received on the connection. `0`
   *     disables the limit
   */
  constructor(options = {}) {
    this._messageBudget = options.messageBudget | 0;
    this._connectionBudget = options.connectionBudget | 0;
    this._receivedBytes = 0;
    this._messageBytes = 0;
  }

  /**
   * @type {Number}
   */
  get messageBudget() {
    return this._messageBudget;
  }

  /**
   * @type {Number}
   */
  get connectionBudget() {
    return this._connectionBudget;
  }

  /**
   * @type {Number}
   */
  get receivedBytes() {
    return this._receivedBytes;
  }

  /**
   * Account for decompressed bytes produced while receiving the current
   * message.
   *
   * This is called synchronously from the inflate stream's `'data'` listener
   * (and for uncompressed messages, per fragment) so an over-limit message is
   * aborted before its whole output is buffered.
   *
   * @param {Number} length The number of decompressed bytes produced
   * @return {(RangeError|null} A budget error if a limit is exceeded, `null`
   *     otherwise
   */
  debit(length) {
    if (this._messageBudget === 0 && this._connectionBudget === 0) {
      return null;
    }

    this._messageBytes += length;

    if (this._messageBudget > 0 && this._messageBytes > this._messageBudget) {
      return this.error(
        'Message budget exceeded',
        'WS_ERR_MESSAGE_BUDGET_EXCEEDED',
        1009
      );
    }

    if (
      this._connectionBudget > 0 &&
      this._receivedBytes + this._messageBytes > this._connectionBudget
    ) {
      return this.error(
        'Connection budget exceeded',
        'WS_ERR_CONNECTION_BUDGET_EXCEEDED',
        1008
      );
    }

    return null;
  }

  /**
   * Increment the connection cumulative total after a message has been
   * successfully received and delivered.
   */
  messageComplete() {
    this._receivedBytes += this._messageBytes;
    this._messageBytes = 0;
  }

  /**
   * Reset the per-message total. Called when a new message starts or when the
   * current message is aborted.
   */
  reset() {
    this._messageBytes = 0;
  }

  /**
   * Builds a budget error.
   *
   * @param {String} message The error message
   * @param {String} code The exposed error code
   * @param {Number} statusCode The status code
   * @return {RangeError} The error
   * @private
   */
  error(message, code, statusCode) {
    const err = new RangeError(message);

    Error.captureStackTrace(err, this.error);
    err.code = code;
    err[kStatusCode] = statusCode;
    return err;
  }
}

module.exports = ReceiverBudget;
