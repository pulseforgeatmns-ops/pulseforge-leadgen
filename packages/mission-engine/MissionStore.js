'use strict';

/**
 * In-memory MissionStore — durable interface for tests and local runtime.
 */

const { newId, AUDIT_KINDS } = require('./types');

class InMemoryMissionStore {
  constructor() {
    /** @type {Map<string, object>} */
    this._missions = new Map();
    /** @type {object[]} */
    this._audit = [];
  }

  /**
   * @param {object} mission
   */
  async create(mission) {
    const row = { ...mission };
    this._missions.set(String(row.id), row);
    await this.appendAudit({
      missionId: row.id,
      kind: AUDIT_KINDS.REQUEST,
      payload: { objectiveText: row.objectiveText, type: row.type },
    });
    return clone(row);
  }

  /**
   * @param {string} id
   */
  async get(id) {
    const row = this._missions.get(String(id));
    return row ? clone(row) : null;
  }

  /**
   * @param {object} patch
   */
  async update(patch) {
    if (!patch || !patch.id) throw new Error('update requires id');
    const existing = this._missions.get(String(patch.id));
    if (!existing) throw new Error(`Unknown mission: ${patch.id}`);
    const next = {
      ...existing,
      ...patch,
      updatedAt: new Date().toISOString(),
    };
    this._missions.set(String(next.id), next);
    return clone(next);
  }

  /**
   * @param {object} [query]
   * @param {string|number} [query.tenantId]
   * @param {string|number} [query.clientId]
   * @param {number} [query.limit]
   */
  async list(query = {}) {
    const tenantId =
      query.tenantId != null ? String(query.tenantId) : null;
    const clientId =
      query.clientId != null ? String(query.clientId) : null;
    let rows = [...this._missions.values()];
    if (tenantId) {
      rows = rows.filter((m) => String(m.tenantId) === tenantId);
    }
    if (clientId) {
      rows = rows.filter((m) => String(m.clientId) === clientId);
    }
    rows.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
    const limit = Number(query.limit) || 50;
    return rows.slice(0, limit).map(clone);
  }

  /**
   * @param {object} event
   */
  async appendAudit(event) {
    const row = {
      id: event.id || newId('aud'),
      missionId: String(event.missionId),
      at: event.at || new Date().toISOString(),
      kind: String(event.kind),
      capabilityId: event.capabilityId || null,
      payload: event.payload || {},
    };
    this._audit.push(row);
    return clone(row);
  }

  /**
   * @param {string} missionId
   */
  async listAudit(missionId) {
    return this._audit
      .filter((e) => String(e.missionId) === String(missionId))
      .map(clone);
  }
}

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function createInMemoryMissionStore() {
  return new InMemoryMissionStore();
}

module.exports = {
  InMemoryMissionStore,
  createInMemoryMissionStore,
};
