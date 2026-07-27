'use strict';

/**
 * In-memory Discovery Profile store (SPEC-024).
 * Profiles are immutable once used — edits create a new version.
 */

const { buildDiscoveryProfile } = require('./types');
const { seedDiscoveryProfiles } = require('./seedProfiles');

class DiscoveryProfileStore {
  constructor(options = {}) {
    /** @type {Map<string, object[]>} id → versions (newest last) */
    this._byId = new Map();
    if (options.seed !== false) {
      for (const profile of seedDiscoveryProfiles()) {
        this._put(profile);
      }
    }
  }

  /**
   * @param {object} profile
   */
  _put(profile) {
    const built = buildDiscoveryProfile(profile);
    if (!built.id) throw new Error('DiscoveryProfile.id is required');
    const versions = this._byId.get(built.id) || [];
    versions.push(built);
    this._byId.set(built.id, versions);
    return built;
  }

  /**
   * @param {string} id
   * @param {string} [version] - exact version; omit for latest active
   */
  get(id, version) {
    const versions = this._byId.get(String(id));
    if (!versions || !versions.length) return null;
    if (version != null) {
      return versions.find((p) => p.version === String(version)) || null;
    }
    for (let i = versions.length - 1; i >= 0; i -= 1) {
      if (versions[i].status === 'active' || versions[i].status === 'approved') {
        return versions[i];
      }
    }
    return versions[versions.length - 1];
  }

  /**
   * @param {object} [query]
   * @param {string|number} [query.tenantId]
   * @param {string|number} [query.clientId]
   * @param {string} [query.status]
   */
  list(query = {}) {
    const status = query.status || 'active';
    const clientId =
      query.clientId != null ? Number(query.clientId) || query.clientId : null;
    const tenantId = query.tenantId != null ? String(query.tenantId) : null;
    const out = [];
    for (const versions of this._byId.values()) {
      const latest = versions[versions.length - 1];
      if (status && latest.status !== status && status !== 'any') continue;
      if (tenantId && latest.tenantId && latest.tenantId !== tenantId) continue;
      if (
        clientId != null &&
        Array.isArray(latest.clientIds) &&
        latest.clientIds.length &&
        !latest.clientIds.some((c) => String(c) === String(clientId))
      ) {
        // Still allow global profiles (empty clientIds)
        continue;
      }
      out.push(latest);
    }
    // Also include global profiles when filtering by client
    if (clientId != null) {
      for (const versions of this._byId.values()) {
        const latest = versions[versions.length - 1];
        if (status && latest.status !== status && status !== 'any') continue;
        if (latest.clientIds && latest.clientIds.length === 0) {
          if (!out.find((p) => p.id === latest.id)) out.push(latest);
        }
      }
    }
    return out.sort((a, b) => a.name.localeCompare(b.name));
  }

  /**
   * Create a new profile (v1.0) or reject if id exists.
   * @param {object} profile
   */
  create(profile) {
    const id = String(profile.id || '');
    if (this._byId.has(id)) {
      throw new Error(`Discovery profile already exists: ${id}`);
    }
    return this._put({
      ...profile,
      version: profile.version || '1.0',
      status: profile.status || 'active',
    });
  }

  /**
   * Immutable update — creates a new version. Never mutates historical rows.
   * @param {string} id
   * @param {object} changes
   * @param {object} [meta]
   * @param {boolean} [meta.autoActivate=false] - if false, new version is pending_review
   */
  createVersion(id, changes, meta = {}) {
    const current = this.get(id);
    if (!current) throw new Error(`Unknown discovery profile: ${id}`);
    const nextVersion = bumpVersion(current.version);
    const autoActivate = meta.autoActivate === true;
    const next = buildDiscoveryProfile({
      ...current,
      ...changes,
      id: current.id,
      version: nextVersion,
      parentId: current.id,
      status: autoActivate ? 'active' : 'pending_review',
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    if (autoActivate) {
      // Mark prior active versions as superseded (new array entries only —
      // historical mission snapshots keep their own copy).
      const versions = this._byId.get(current.id) || [];
      for (let i = 0; i < versions.length; i += 1) {
        if (versions[i].status === 'active') {
          versions[i] = { ...versions[i], status: 'superseded' };
        }
      }
    }
    return this._put(next);
  }

  /**
   * Operator approval of a pending_review version.
   * @param {string} id
   * @param {string} version
   */
  approveVersion(id, version) {
    const versions = this._byId.get(String(id));
    if (!versions) throw new Error(`Unknown discovery profile: ${id}`);
    const idx = versions.findIndex((p) => p.version === String(version));
    if (idx < 0) throw new Error(`Unknown version ${version} for ${id}`);
    for (let i = 0; i < versions.length; i += 1) {
      if (versions[i].status === 'active') {
        versions[i] = { ...versions[i], status: 'superseded' };
      }
    }
    versions[idx] = {
      ...versions[idx],
      status: 'active',
      updatedAt: new Date().toISOString(),
    };
    return versions[idx];
  }

  /**
   * Snapshot a profile for mission durability (exact version pin).
   * @param {object} profile
   */
  snapshot(profile) {
    return buildDiscoveryProfile(profile);
  }
}

function bumpVersion(version) {
  const m = /^(\d+)\.(\d+)/.exec(String(version || '1.0'));
  if (!m) return '1.1';
  return `${m[1]}.${Number(m[2]) + 1}`;
}

function createDiscoveryProfileStore(options) {
  return new DiscoveryProfileStore(options);
}

module.exports = {
  DiscoveryProfileStore,
  createDiscoveryProfileStore,
  bumpVersion,
};
