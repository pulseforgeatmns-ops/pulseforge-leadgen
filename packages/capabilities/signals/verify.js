'use strict';

/**
 * Evidence verification gate (SPEC-031).
 */

const { resolveLifecycle } = require('./lifecycle');
const { withInfluence } = require('./decay');
const { buildBusinessSignal } = require('./types');

/**
 * Verify + lifecycle + decay for a batch of Detected signals.
 *
 * @param {object[]} detected
 * @param {object} [opts]
 * @param {string[]} [opts.preferredSignalTypes] - Playbook preferred → Low may activate
 * @param {Date|string|number} [opts.asOf]
 * @returns {object[]}
 */
function verifySignals(detected, opts = {}) {
  const preferred = new Set(
    (opts.preferredSignalTypes || []).map((t) => String(t).toLowerCase())
  );
  const asOf = opts.asOf;

  return (Array.isArray(detected) ? detected : [])
    .filter((s) => s && Array.isArray(s.evidence) && s.evidence.length > 0)
    .map((raw) => {
      const signal = buildBusinessSignal(raw);
      const playbookPreferred = isPreferredSignal(signal, preferred);
      const resolved = resolveLifecycle(signal, {
        playbookPreferred,
        asOf,
      });
      return withInfluence(resolved, asOf);
    });
}

/**
 * @param {object} signal
 * @param {Set<string>} preferred
 * @returns {boolean}
 */
function isPreferredSignal(signal, preferred) {
  if (!preferred || preferred.size === 0) return false;
  const type = String(signal.type || '').toLowerCase();
  const category = String(signal.category || '').toLowerCase();
  if (preferred.has(type) || preferred.has(category)) return true;
  // Discovery profile uses hiring_activity / multi_location soft-boost keys
  if (
    preferred.has('hiring_activity') &&
    (type.includes('hir') || category === 'growth' || category === 'buying')
  ) {
    return type.includes('hir');
  }
  if (
    preferred.has('multi_location') &&
    (type === 'multi_location' || type === 'commercial_footprint')
  ) {
    return true;
  }
  if (preferred.has('commercial_office') && type === 'commercial_footprint') {
    return true;
  }
  return false;
}

module.exports = {
  verifySignals,
  isPreferredSignal,
};
