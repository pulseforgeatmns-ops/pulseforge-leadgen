'use strict';

/**
 * CapabilityRegistry — discover and resolve capabilities (SPEC-023).
 * MissionPlanner never imports concrete implementations — only registry queries.
 */

class CapabilityRegistry {
  constructor() {
    /** @type {Map<string, object>} */
    this._byId = new Map();
  }

  /**
   * @param {object} capability
   */
  register(capability) {
    assertCapability(capability);
    const id = String(capability.id);
    if (this._byId.has(id)) {
      throw new Error(`Capability already registered: ${id}`);
    }
    this._byId.set(id, capability);
    return this;
  }

  /**
   * @param {string} id
   * @returns {object|null}
   */
  get(id) {
    return this._byId.get(String(id)) || null;
  }

  /**
   * @returns {object[]}
   */
  list() {
    return [...this._byId.values()].sort((a, b) =>
      String(a.id).localeCompare(String(b.id))
    );
  }

  /**
   * Discover capabilities matching required outcome tags.
   * @param {string[]|object} query - outcome tags or { outcomeTags, category }
   * @returns {object[]}
   */
  discover(query) {
    const tags = normalizeTags(query);
    const category =
      query && typeof query === 'object' && !Array.isArray(query)
        ? query.category || null
        : null;

    return this.list().filter((cap) => {
      if (category && cap.category !== category) return false;
      if (!tags.length) return true;
      const outcomeTags = Array.isArray(cap.outcomeTags)
        ? cap.outcomeTags.map(String)
        : [];
      return tags.every((t) => outcomeTags.includes(t));
    });
  }

  /**
   * Resolve ordered capability ids; throws if any missing.
   * @param {string[]} ids
   * @returns {object[]}
   */
  resolveAll(ids) {
    return (ids || []).map((id) => {
      const cap = this.get(id);
      if (!cap) throw new Error(`Unknown capability: ${id}`);
      return cap;
    });
  }

  /**
   * Lightweight schema presence check (v1: required keys on inputs).
   * @param {string} capabilityId
   * @param {object} inputs
   * @returns {{ ok: boolean, missing: string[] }}
   */
  validateInputs(capabilityId, inputs) {
    const cap = this.get(capabilityId);
    if (!cap) return { ok: false, missing: ['capability'] };
    const required =
      (cap.inputSchema && Array.isArray(cap.inputSchema.required)
        ? cap.inputSchema.required
        : []) || [];
    const missing = required.filter(
      (key) => inputs == null || inputs[key] === undefined || inputs[key] === null
    );
    return { ok: missing.length === 0, missing };
  }
}

/**
 * @param {object} capability
 */
function assertCapability(capability) {
  if (!capability || typeof capability !== 'object') {
    throw new Error('Capability must be an object');
  }
  for (const key of ['id', 'name', 'description', 'category']) {
    if (!capability[key]) throw new Error(`Capability missing ${key}`);
  }
  if (typeof capability.canRun !== 'function') {
    throw new Error(`Capability ${capability.id} missing canRun`);
  }
  if (typeof capability.estimate !== 'function') {
    throw new Error(`Capability ${capability.id} missing estimate`);
  }
  if (typeof capability.execute !== 'function') {
    throw new Error(`Capability ${capability.id} missing execute`);
  }
  // SPEC-051: optional artifact contracts (requires / produces)
  if (capability.requires != null && !Array.isArray(capability.requires)) {
    throw new Error(`Capability ${capability.id} requires must be an array`);
  }
  if (capability.produces != null && !Array.isArray(capability.produces)) {
    throw new Error(`Capability ${capability.id} produces must be an array`);
  }
}

function normalizeTags(query) {
  if (Array.isArray(query)) return query.map(String);
  if (query && typeof query === 'object' && Array.isArray(query.outcomeTags)) {
    return query.outcomeTags.map(String);
  }
  if (typeof query === 'string') return [query];
  return [];
}

/**
 * @returns {CapabilityRegistry}
 */
function createCapabilityRegistry() {
  return new CapabilityRegistry();
}

module.exports = {
  CapabilityRegistry,
  createCapabilityRegistry,
  assertCapability,
};
