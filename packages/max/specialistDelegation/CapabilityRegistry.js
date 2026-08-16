'use strict';

/**
 * SPEC-098 — lightweight registry of capabilities Max can delegate.
 * Entries represent actual callable (or declared-but-unwired) capabilities.
 * Future specialists (Penny, Emmett, Sam, Cal) are not registered until callable.
 */

const { AUTHORITY_LEVELS, asText, clone } = require('./Types');

const DEFAULT_CAPABILITIES = Object.freeze([
  {
    specialist: 'test_intelligence',
    capability: 'acquisition_assessment',
    authoritySupported: Object.freeze(['observe']),
    callable: true,
    adapter: 'test_intelligence',
    description:
      'Deterministic fixture that assesses whether Acquisition has meaningful opportunity.',
  },
  {
    specialist: 'scout',
    capability: 'acquisition_intelligence',
    authoritySupported: Object.freeze(['observe', 'recommend']),
    callable: true,
    adapter: 'scout',
    description:
      'Scout acquisition intelligence for Max (SPEC-100). Observe/recommend only.',
  },
  {
    specialist: 'scout',
    capability: 'prospect_intelligence',
    authoritySupported: Object.freeze(['observe', 'recommend']),
    callable: false,
    adapter: null,
    description:
      'Legacy Scout prospect intelligence entry. Use acquisition_intelligence.',
  },
  {
    specialist: 'paige',
    capability: 'content_strategy',
    authoritySupported: Object.freeze(['observe', 'recommend', 'draft']),
    callable: false,
    adapter: null,
    description:
      'Paige content strategy. Existing SPEC-094 path remains; SPEC-098 adapter not wired.',
  },
]);

function capabilityKey(specialist, capability) {
  return `${String(specialist || '').trim()}:${String(capability || '').trim()}`;
}

class SpecialistCapabilityRegistry {
  constructor(entries = DEFAULT_CAPABILITIES) {
    /** @type {Map<string, object>} */
    this._entries = new Map();
    for (const entry of entries) {
      this.register(entry);
    }
  }

  /**
   * @param {object} entry
   */
  register(entry) {
    const specialist = asText(entry && entry.specialist);
    const capability = asText(entry && entry.capability);
    if (!specialist || !capability) {
      throw new Error('Capability registry entry requires specialist and capability');
    }
    const authoritySupported = Array.isArray(entry.authoritySupported)
      ? entry.authoritySupported.filter((a) => AUTHORITY_LEVELS.includes(a))
      : [];
    this._entries.set(capabilityKey(specialist, capability), {
      specialist,
      capability,
      authoritySupported: Object.freeze(authoritySupported.slice()),
      callable: entry.callable === true,
      adapter: asText(entry.adapter),
      description: asText(entry.description),
    });
  }

  /**
   * @param {string} specialist
   * @param {string} capability
   * @returns {object|null}
   */
  get(specialist, capability) {
    const entry = this._entries.get(capabilityKey(specialist, capability));
    return entry ? clone(entry) : null;
  }

  /**
   * @returns {object[]}
   */
  list() {
    return [...this._entries.values()].map(clone);
  }

  /**
   * @returns {object[]}
   */
  listCallable() {
    return this.list().filter((e) => e.callable);
  }

  /**
   * @param {string} specialist
   * @param {string} capability
   * @param {string} authority
   * @returns {boolean}
   */
  supportsAuthority(specialist, capability, authority) {
    const entry = this.get(specialist, capability);
    if (!entry) return false;
    return entry.authoritySupported.includes(authority);
  }
}

function createCapabilityRegistry(entries) {
  return new SpecialistCapabilityRegistry(entries);
}

function createDefaultCapabilityRegistry() {
  return new SpecialistCapabilityRegistry();
}

module.exports = {
  DEFAULT_CAPABILITIES,
  SpecialistCapabilityRegistry,
  createCapabilityRegistry,
  createDefaultCapabilityRegistry,
  capabilityKey,
};
