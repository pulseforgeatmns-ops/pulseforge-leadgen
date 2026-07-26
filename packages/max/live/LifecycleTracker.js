'use strict';

const {
  LIFECYCLE,
  LIFECYCLE_ORDER,
  lifecycleForEventType,
} = require('./LiveTypes');

/**
 * Per-entity lifecycle state machine (SPEC-011).
 * Records transitions; never invents intelligence.
 */
class LifecycleTracker {
  constructor() {
    /** @type {Map<string, { state: string, updatedAt: string, history: object[] }>} */
    this._states = new Map();
  }

  /**
   * @param {string} tenantId
   * @param {string} entityId
   */
  key(tenantId, entityId) {
    return `${tenantId}::${entityId}`;
  }

  /**
   * @param {string} tenantId
   * @param {string} entityId
   */
  get(tenantId, entityId) {
    return this._states.get(this.key(tenantId, entityId)) || null;
  }

  /**
   * Apply an event's implied lifecycle transition.
   * @param {object} event - IntelligenceEvent
   * @returns {{ state: string, transitioned: boolean, previous: string|null }}
   */
  apply(event) {
    if (!event || !event.tenantId || !event.entity) {
      return { state: LIFECYCLE.DETECTED, transitioned: false, previous: null };
    }
    const k = this.key(event.tenantId, event.entity.id);
    const current = this._states.get(k) || null;
    const previous = current ? current.state : null;
    const next =
      event.lifecycle ||
      lifecycleForEventType(event.type, previous) ||
      previous ||
      LIFECYCLE.DETECTED;

    if (!current) {
      const row = {
        state: next,
        updatedAt: event.timestamp,
        history: [
          {
            at: event.timestamp,
            state: next,
            eventId: event.id,
            type: event.type,
            summary: event.summary,
          },
        ],
      };
      this._states.set(k, row);
      return { state: next, transitioned: true, previous: null };
    }

    if (current.state === next) {
      return { state: current.state, transitioned: false, previous };
    }

    current.state = next;
    current.updatedAt = event.timestamp;
    current.history.push({
      at: event.timestamp,
      state: next,
      eventId: event.id,
      type: event.type,
      summary: event.summary,
    });
    if (current.history.length > 40) {
      current.history = current.history.slice(-40);
    }
    return { state: next, transitioned: true, previous };
  }

  /**
   * History of lifecycle transitions for an entity.
   * @param {string} tenantId
   * @param {string} entityId
   */
  history(tenantId, entityId) {
    const row = this.get(tenantId, entityId);
    return row ? row.history.slice() : [];
  }

  clear() {
    this._states.clear();
  }
}

/**
 * Whether moving from → to is a forward lifecycle step (informational).
 * @param {string} from
 * @param {string} to
 */
function isForwardLifecycle(from, to) {
  const a = LIFECYCLE_ORDER.indexOf(from);
  const b = LIFECYCLE_ORDER.indexOf(to);
  if (a < 0 || b < 0) return false;
  return b > a;
}

module.exports = {
  LifecycleTracker,
  isForwardLifecycle,
};
