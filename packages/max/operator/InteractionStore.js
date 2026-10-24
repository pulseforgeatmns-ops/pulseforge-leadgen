'use strict';

const { buildInteractionEvent } = require('./OperatorTypes');

/**
 * Append-only in-process InteractionEvent store (SPEC-012).
 * Non-durable across restarts — same posture as LiveLoop EventStore.
 */
class InteractionStore {
  constructor() {
    /** @type {object[]} */
    this._events = [];
    this._seq = 0;
  }

  /**
   * @param {object} partial
   * @returns {object} frozen InteractionEvent
   */
  append(partial) {
    this._seq += 1;
    const event = buildInteractionEvent({
      ...partial,
      seq: this._seq,
    });
    this._events.push(event);
    if (this._events.length > 8000) {
      this._events = this._events.slice(-6000);
    }
    return event;
  }

  /**
   * @param {string} tenantId
   * @param {{ afterSeq?: number, type?: string, recommendationId?: string, limit?: number }} [options]
   */
  query(tenantId, options = {}) {
    const tid = String(tenantId || '');
    const min = Number(options.afterSeq) || 0;
    const limit = Number.isFinite(Number(options.limit))
      ? Math.max(1, Number(options.limit))
      : 500;
    const type = options.type != null ? String(options.type) : null;
    const recId =
      options.recommendationId != null
        ? String(options.recommendationId)
        : null;
    const out = [];
    for (const ev of this._events) {
      if (ev.tenantId !== tid) continue;
      if (ev.seq <= min) continue;
      if (type && ev.type !== type) continue;
      if (recId && ev.recommendationId !== recId) continue;
      out.push(ev);
      if (out.length >= limit) break;
    }
    return out;
  }

  /** @returns {number} */
  get seq() {
    return this._seq;
  }

  get size() {
    return this._events.length;
  }

  clear() {
    this._events = [];
    this._seq = 0;
  }
}

module.exports = { InteractionStore };
