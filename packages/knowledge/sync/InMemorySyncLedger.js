'use strict';

/**
 * Tenant-scoped idempotency ledger for sync apply/replay.
 * Storage-agnostic — in-memory for SPEC-001B; swap later without changing GraphSyncEngine.
 */
class InMemorySyncLedger {
  constructor() {
    /** @type {Map<string, Map<string, object>>} tenantId → key → record */
    this._byTenant = new Map();
  }

  _bucket(tenantId) {
    const key = String(tenantId);
    if (!this._byTenant.has(key)) {
      this._byTenant.set(key, new Map());
    }
    return this._byTenant.get(key);
  }

  async has(tenantId, idempotencyKey) {
    return this._bucket(tenantId).has(idempotencyKey);
  }

  async get(tenantId, idempotencyKey) {
    return this._bucket(tenantId).get(idempotencyKey) || null;
  }

  async mark(tenantId, idempotencyKey, record = {}) {
    const entry = {
      key: idempotencyKey,
      tenantId: String(tenantId),
      markedAt: new Date().toISOString(),
      ...record,
    };
    this._bucket(tenantId).set(idempotencyKey, entry);
    return entry;
  }

  async clearTenant(tenantId) {
    this._byTenant.delete(String(tenantId));
  }

  async size(tenantId) {
    return this._bucket(tenantId).size;
  }
}

module.exports = {
  InMemorySyncLedger,
};
