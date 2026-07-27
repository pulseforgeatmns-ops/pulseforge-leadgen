'use strict';

const { deepClone } = require('./MemoryTypes');

/**
 * In-memory append-only snapshot store.
 * Snapshots are deep-cloned on write and read — never mutated in place.
 */
class InMemorySnapshotRepository {
  constructor() {
    /** @type {Map<string, object[]>} key = `${tenantId}::${companyId}` */
    this._byCompany = new Map();
    /** @type {Map<string, object>} key = `${tenantId}::${snapshotId}` */
    this._byId = new Map();
    this._total = 0;
  }

  /**
   * @param {object} snapshot
   */
  append(snapshot) {
    if (!snapshot || !snapshot.id || !snapshot.tenantId || !snapshot.companyId) {
      throw new Error('append requires snapshot with id, tenantId, companyId');
    }
    const idKey = `${snapshot.tenantId}::${snapshot.id}`;
    if (this._byId.has(idKey)) {
      throw new Error(`Snapshot already exists (append-only): ${snapshot.id}`);
    }
    const frozen = Object.freeze(deepClone(snapshot));
    this._byId.set(idKey, frozen);
    const companyKey = `${snapshot.tenantId}::${snapshot.companyId}`;
    const list = this._byCompany.get(companyKey) || [];
    list.push(frozen);
    this._byCompany.set(companyKey, list);
    this._total += 1;
    return deepClone(frozen);
  }

  /**
   * @param {string} tenantId
   * @param {string} id
   */
  getById(tenantId, id) {
    const row = this._byId.get(`${tenantId}::${id}`);
    return row ? deepClone(row) : null;
  }

  /**
   * Chronological ascending by timestamp, then id.
   * @param {string} tenantId
   * @param {string} companyId
   * @param {{ limit?: number, before?: string, after?: string }} [options]
   */
  listByCompany(tenantId, companyId, options = {}) {
    const list = this._byCompany.get(`${tenantId}::${companyId}`) || [];
    let rows = list.map((r) => deepClone(r));
    if (options.after) {
      const after = Date.parse(options.after);
      rows = rows.filter((r) => Date.parse(r.timestamp) > after);
    }
    if (options.before) {
      const before = Date.parse(options.before);
      rows = rows.filter((r) => Date.parse(r.timestamp) < before);
    }
    rows.sort(compareSnapshots);
    if (options.limit != null) {
      rows = rows.slice(-Number(options.limit));
    }
    return rows;
  }

  /**
   * @param {string} tenantId
   * @param {string} companyId
   */
  latest(tenantId, companyId) {
    const rows = this.listByCompany(tenantId, companyId);
    return rows.length ? rows[rows.length - 1] : null;
  }

  /**
   * @param {string} [tenantId]
   */
  count(tenantId) {
    if (tenantId == null) return this._total;
    let n = 0;
    for (const key of this._byId.keys()) {
      if (key.startsWith(`${tenantId}::`)) n += 1;
    }
    return n;
  }

  clear() {
    this._byCompany.clear();
    this._byId.clear();
    this._total = 0;
  }
}

/**
 * Serializing snapshot repository — proves round-trip parity with InMemory.
 * Stores JSON strings only (simulates durable serialization without Postgres).
 */
class SerializingSnapshotRepository {
  constructor() {
    /** @type {Map<string, string>} */
    this._raw = new Map();
    /** @type {Map<string, string[]>} companyKey -> snapshot ids in append order */
    this._order = new Map();
  }

  append(snapshot) {
    if (!snapshot || !snapshot.id || !snapshot.tenantId || !snapshot.companyId) {
      throw new Error('append requires snapshot with id, tenantId, companyId');
    }
    const idKey = `${snapshot.tenantId}::${snapshot.id}`;
    if (this._raw.has(idKey)) {
      throw new Error(`Snapshot already exists (append-only): ${snapshot.id}`);
    }
    const payload = JSON.stringify(deepClone(snapshot));
    this._raw.set(idKey, payload);
    const companyKey = `${snapshot.tenantId}::${snapshot.companyId}`;
    const order = this._order.get(companyKey) || [];
    order.push(snapshot.id);
    this._order.set(companyKey, order);
    return JSON.parse(payload);
  }

  getById(tenantId, id) {
    const payload = this._raw.get(`${tenantId}::${id}`);
    return payload ? JSON.parse(payload) : null;
  }

  listByCompany(tenantId, companyId, options = {}) {
    const order = this._order.get(`${tenantId}::${companyId}`) || [];
    let rows = order
      .map((id) => this.getById(tenantId, id))
      .filter(Boolean);
    if (options.after) {
      const after = Date.parse(options.after);
      rows = rows.filter((r) => Date.parse(r.timestamp) > after);
    }
    if (options.before) {
      const before = Date.parse(options.before);
      rows = rows.filter((r) => Date.parse(r.timestamp) < before);
    }
    rows.sort(compareSnapshots);
    if (options.limit != null) {
      rows = rows.slice(-Number(options.limit));
    }
    return rows;
  }

  latest(tenantId, companyId) {
    const rows = this.listByCompany(tenantId, companyId);
    return rows.length ? rows[rows.length - 1] : null;
  }

  count(tenantId) {
    if (tenantId == null) return this._raw.size;
    let n = 0;
    for (const key of this._raw.keys()) {
      if (key.startsWith(`${tenantId}::`)) n += 1;
    }
    return n;
  }

  clear() {
    this._raw.clear();
    this._order.clear();
  }
}

/**
 * @param {object} a
 * @param {object} b
 */
function compareSnapshots(a, b) {
  const ta = Date.parse(a.timestamp);
  const tb = Date.parse(b.timestamp);
  if (ta !== tb) return ta - tb;
  return String(a.id).localeCompare(String(b.id));
}

module.exports = {
  InMemorySnapshotRepository,
  SerializingSnapshotRepository,
  compareSnapshots,
};
