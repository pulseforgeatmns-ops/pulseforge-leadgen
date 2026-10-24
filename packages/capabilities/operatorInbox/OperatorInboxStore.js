'use strict';

/**
 * In-memory Operator Inbox store (SPEC-037).
 * Append-only snapshots; audit log immutable once written.
 */

function newId() {
  return `oin_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

class InMemoryOperatorInboxStore {
  constructor() {
    /** @type {Map<string, object>} */
    this._byId = new Map();
    /** @type {Map<string, string[]>} client/mission key → snapshot ids */
    this._byScope = new Map();
  }

  /**
   * @param {object} input
   * @returns {object}
   */
  create(input) {
    const now = new Date().toISOString();
    const clientId = input.clientId != null ? String(input.clientId) : null;
    const missionId = input.missionId != null ? String(input.missionId) : null;
    const key = clientId || missionId || newId();
    const existing = this.listForScope(key);
    const snapshot = existing.length + 1;
    const row = {
      id: input.id || newId(),
      clientId,
      missionId,
      tenantId: String(input.tenantId || ''),
      snapshot,
      package: input.package || null,
      items: Array.isArray(input.items) ? input.items : [],
      activeItems: Array.isArray(input.activeItems) ? input.activeItems : [],
      auditLog: Array.isArray(input.auditLog) ? input.auditLog : [],
      completionEvents: Array.isArray(input.completionEvents)
        ? input.completionEvents
        : [],
      missionEvents: Array.isArray(input.missionEvents)
        ? input.missionEvents
        : [],
      timeline: Array.isArray(input.timeline) ? input.timeline : [],
      summary: input.summary || null,
      changeSummary: input.changeSummary || '',
      operator: input.operator || 'system',
      createdAt: now,
      updatedAt: now,
    };
    this._byId.set(row.id, freezeAudit(clone(row)));
    const list = this._byScope.get(key) || [];
    list.push(row.id);
    this._byScope.set(key, list);
    return clone(row);
  }

  /**
   * @param {string} id
   * @returns {object|null}
   */
  get(id) {
    const row = this._byId.get(String(id));
    return row ? clone(row) : null;
  }

  /**
   * @param {string|null} scopeKey
   * @returns {object[]}
   */
  listForScope(scopeKey) {
    if (!scopeKey) return [];
    const ids = this._byScope.get(String(scopeKey)) || [];
    return ids.map((id) => this.get(id)).filter(Boolean);
  }

  /**
   * @param {string|null} scopeKey
   * @returns {object|null}
   */
  getLatest(scopeKey) {
    const list = this.listForScope(scopeKey);
    if (!list.length) return null;
    return list[list.length - 1];
  }
}

/**
 * @param {object} [seed]
 * @returns {InMemoryOperatorInboxStore}
 */
function createInMemoryOperatorInboxStore(seed) {
  const store = new InMemoryOperatorInboxStore();
  if (seed && typeof seed === 'object') {
    store.create(seed);
  }
  return store;
}

function clone(row) {
  return JSON.parse(JSON.stringify(row));
}

function freezeAudit(row) {
  if (row.auditLog) {
    row.auditLog = row.auditLog.map((e) => Object.freeze({ ...e }));
    Object.freeze(row.auditLog);
  }
  return row;
}

module.exports = {
  InMemoryOperatorInboxStore,
  createInMemoryOperatorInboxStore,
};
