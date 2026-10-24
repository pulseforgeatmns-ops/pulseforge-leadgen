'use strict';

const { buildIntelligenceEvent, encodeCursor } = require('./LiveTypes');

/**
 * Append-only in-process IntelligenceEvent store (SPEC-011).
 * Non-durable across restarts — same posture as Workspace SessionStore.
 */
class EventStore {
  constructor() {
    /** @type {object[]} */
    this._events = [];
    this._seq = 0;
  }

  /**
   * @param {object} partial - fields for buildIntelligenceEvent (seq assigned)
   * @returns {object} frozen IntelligenceEvent
   */
  append(partial) {
    this._seq += 1;
    const event = buildIntelligenceEvent({
      ...partial,
      seq: this._seq,
    });
    this._events.push(event);
    if (this._events.length > 5000) {
      this._events = this._events.slice(-4000);
    }
    return event;
  }

  /**
   * @param {string} tenantId
   * @param {number} [afterSeq=0]
   * @param {{ materialOnly?: boolean, limit?: number }} [options]
   */
  since(tenantId, afterSeq = 0, options = {}) {
    const tid = String(tenantId || '');
    const min = Number(afterSeq) || 0;
    const limit = Number.isFinite(Number(options.limit))
      ? Math.max(1, Number(options.limit))
      : 200;
    const materialOnly = options.materialOnly === true;
    const out = [];
    for (const ev of this._events) {
      if (ev.tenantId !== tid) continue;
      if (ev.seq <= min) continue;
      if (materialOnly && !ev.material) continue;
      out.push(ev);
      if (out.length >= limit) break;
    }
    return out;
  }

  /**
   * Timeline for one entity (chronological).
   * @param {string} tenantId
   * @param {string} entityId
   * @param {{ kind?: string, limit?: number }} [options]
   */
  timeline(tenantId, entityId, options = {}) {
    const tid = String(tenantId || '');
    const eid = String(entityId || '');
    const kind = options.kind != null ? String(options.kind) : null;
    const limit = Number.isFinite(Number(options.limit))
      ? Math.max(1, Number(options.limit))
      : 50;
    const matched = [];
    for (const ev of this._events) {
      if (ev.tenantId !== tid) continue;
      if (String(ev.entity.id) !== eid) continue;
      if (kind && ev.entity.kind !== kind) continue;
      matched.push(ev);
    }
    return matched.slice(-limit);
  }

  /** @returns {number} */
  get seq() {
    return this._seq;
  }

  /** @returns {string} */
  cursor() {
    return encodeCursor(this._seq);
  }

  /** Test helper */
  clear() {
    this._events = [];
    this._seq = 0;
  }

  get size() {
    return this._events.length;
  }
}

module.exports = { EventStore };
