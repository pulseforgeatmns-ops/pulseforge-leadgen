'use strict';

/**
 * In-memory session → active Mission binding (SPEC-039).
 * One active Mission per operator session.
 */

class InMemoryActiveMissionBindingStore {
  constructor() {
    /** @type {Map<string, object>} */
    this._bindings = new Map();
  }

  /**
   * @param {string} sessionId
   * @returns {object|null}
   */
  async get(sessionId) {
    if (!sessionId) return null;
    const row = this._bindings.get(String(sessionId));
    return row ? { ...row } : null;
  }

  /**
   * @param {object} binding
   * @param {string} binding.sessionId
   * @param {string} binding.activeMissionId
   * @param {string|number} [binding.tenantId]
   * @param {string|number} [binding.clientId]
   * @param {string} [binding.operatorId]
   */
  async set(binding) {
    if (!binding || !binding.sessionId) {
      throw new Error('sessionId is required');
    }
    if (!binding.activeMissionId) {
      throw new Error('activeMissionId is required');
    }
    const row = {
      sessionId: String(binding.sessionId),
      activeMissionId: String(binding.activeMissionId),
      tenantId:
        binding.tenantId != null ? String(binding.tenantId) : null,
      clientId:
        binding.clientId != null ? String(binding.clientId) : null,
      operatorId: binding.operatorId != null ? String(binding.operatorId) : null,
      updatedAt: new Date().toISOString(),
    };
    this._bindings.set(row.sessionId, row);
    return { ...row };
  }

  /**
   * @param {string} sessionId
   */
  async clear(sessionId) {
    if (!sessionId) return null;
    const key = String(sessionId);
    const prev = this._bindings.get(key) || null;
    this._bindings.delete(key);
    return prev ? { ...prev } : null;
  }

  /** Test helper */
  clearAll() {
    this._bindings.clear();
  }

  get size() {
    return this._bindings.size;
  }
}

function createInMemoryActiveMissionBindingStore() {
  return new InMemoryActiveMissionBindingStore();
}

module.exports = {
  InMemoryActiveMissionBindingStore,
  createInMemoryActiveMissionBindingStore,
};
