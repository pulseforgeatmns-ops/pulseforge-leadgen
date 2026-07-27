'use strict';

/**
 * In-memory Client Playbook store (SPEC-028 / ADR-015).
 * Playbooks are immutable once used — edits create a new version.
 */

const { buildClientPlaybook, PLAYBOOK_STATUS } = require('./types');
const { seedClientPlaybooks } = require('./seedPlaybooks');

class ClientPlaybookStore {
  constructor(options = {}) {
    /** @type {Map<string, object[]>} id → versions (newest last) */
    this._byId = new Map();
    if (options.seed !== false) {
      for (const playbook of seedClientPlaybooks()) {
        this._put(playbook);
      }
    }
  }

  /**
   * @param {object} playbook
   */
  _put(playbook) {
    const built = buildClientPlaybook(playbook);
    if (!built.id) throw new Error('ClientPlaybook.id is required');
    const versions = this._byId.get(built.id) || [];
    versions.push(built);
    this._byId.set(built.id, versions);
    return built;
  }

  /**
   * @param {string} id
   * @param {string} [version]
   */
  get(id, version) {
    const versions = this._byId.get(String(id));
    if (!versions || !versions.length) return null;
    if (version != null) {
      return versions.find((p) => p.version === String(version)) || null;
    }
    for (let i = versions.length - 1; i >= 0; i -= 1) {
      if (
        versions[i].status === PLAYBOOK_STATUS.ACTIVE ||
        versions[i].status === 'approved'
      ) {
        return versions[i];
      }
    }
    return versions[versions.length - 1];
  }

  /**
   * @param {object} [query]
   * @param {string|number} [query.clientId]
   * @param {string} [query.status]
   */
  list(query = {}) {
    const status = query.status || PLAYBOOK_STATUS.ACTIVE;
    const clientId =
      query.clientId != null ? Number(query.clientId) || query.clientId : null;
    const out = [];
    for (const versions of this._byId.values()) {
      const latest = versions[versions.length - 1];
      if (status && latest.status !== status && status !== 'any') continue;
      if (
        clientId != null &&
        latest.clientId != null &&
        String(latest.clientId) !== String(clientId)
      ) {
        continue;
      }
      out.push(latest);
    }
    return out.sort((a, b) => a.name.localeCompare(b.name));
  }

  /**
   * Latest active playbook for a client.
   * @param {string|number} clientId
   */
  getForClient(clientId) {
    const list = this.list({ clientId, status: PLAYBOOK_STATUS.ACTIVE });
    return list[0] || null;
  }

  /**
   * @param {object} playbook
   */
  create(playbook) {
    const id = String(playbook.id || '');
    if (this._byId.has(id)) {
      throw new Error(`Client playbook already exists: ${id}`);
    }
    return this._put({
      ...playbook,
      version: playbook.version || '1.0',
      status: playbook.status || PLAYBOOK_STATUS.ACTIVE,
    });
  }

  /**
   * Immutable update — creates a new version.
   * @param {string} id
   * @param {object} changes
   * @param {object} [meta]
   * @param {boolean} [meta.autoActivate=false]
   */
  createVersion(id, changes, meta = {}) {
    const current = this.get(id);
    if (!current) throw new Error(`Unknown client playbook: ${id}`);
    const nextVersion = bumpVersion(current.version);
    const autoActivate = meta.autoActivate === true;
    const next = buildClientPlaybook({
      ...current,
      ...changes,
      id: current.id,
      clientId: changes.clientId != null ? changes.clientId : current.clientId,
      version: nextVersion,
      parentId: current.id,
      status: autoActivate
        ? PLAYBOOK_STATUS.ACTIVE
        : PLAYBOOK_STATUS.PENDING_REVIEW,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    if (autoActivate) {
      const versions = this._byId.get(current.id) || [];
      for (let i = 0; i < versions.length; i += 1) {
        if (versions[i].status === PLAYBOOK_STATUS.ACTIVE) {
          versions[i] = { ...versions[i], status: PLAYBOOK_STATUS.SUPERSEDED };
        }
      }
    }
    return this._put(next);
  }

  /**
   * @param {string} id
   * @param {string} version
   */
  approveVersion(id, version) {
    const versions = this._byId.get(String(id));
    if (!versions) throw new Error(`Unknown client playbook: ${id}`);
    const idx = versions.findIndex((p) => p.version === String(version));
    if (idx < 0) throw new Error(`Unknown version ${version} for ${id}`);
    for (let i = 0; i < versions.length; i += 1) {
      if (versions[i].status === PLAYBOOK_STATUS.ACTIVE) {
        versions[i] = { ...versions[i], status: PLAYBOOK_STATUS.SUPERSEDED };
      }
    }
    versions[idx] = {
      ...versions[idx],
      status: PLAYBOOK_STATUS.ACTIVE,
      updatedAt: new Date().toISOString(),
    };
    return versions[idx];
  }

  /**
   * Snapshot for mission durability (exact version pin).
   * @param {object} playbook
   */
  snapshot(playbook) {
    return buildClientPlaybook(playbook);
  }
}

function bumpVersion(version) {
  const m = /^(\d+)\.(\d+)/.exec(String(version || '1.0'));
  if (!m) return '1.1';
  return `${m[1]}.${Number(m[2]) + 1}`;
}

function createClientPlaybookStore(options) {
  return new ClientPlaybookStore(options);
}

module.exports = {
  ClientPlaybookStore,
  createClientPlaybookStore,
  bumpVersion,
};
