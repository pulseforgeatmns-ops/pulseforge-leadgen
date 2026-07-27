'use strict';

/**
 * Temporal memory types (SPEC-003).
 * Memory stores transitions — not facts. No LLM.
 */

const CHANGE_TYPES = Object.freeze({
  SCORE_INCREASED: 'score_increased',
  SCORE_DECREASED: 'score_decreased',
  CONFIDENCE_INCREASED: 'confidence_increased',
  CONFIDENCE_DECREASED: 'confidence_decreased',
  NEW_CLAIM: 'new_claim',
  REMOVED_CLAIM: 'removed_claim',
  NEW_EVIDENCE: 'new_evidence',
  REMOVED_EVIDENCE: 'removed_evidence',
  STRATEGY_SCORE_UP: 'strategy_score_up',
  STRATEGY_SCORE_DOWN: 'strategy_score_down',
  NEW_DECISION_MAKER: 'new_decision_maker',
  NEW_HIRING_SIGNAL: 'new_hiring_signal',
  NEW_OPPORTUNITY_SIGNAL: 'new_opportunity_signal',
  NEW_CONTRADICTION: 'new_contradiction',
  PRIORITY_CHANGED: 'priority_changed',
  TYPE_CHANGED: 'type_changed',
  ACTION_CHANGED: 'action_changed',
});

const WATCH_OPS = Object.freeze({
  DELTA_ABS_GT: 'delta_abs_gt',
  DELTA_GT: 'delta_gt',
  DELTA_LT: 'delta_lt',
  VALUE_GTE: 'value_gte',
  VALUE_LTE: 'value_lte',
  VALUE_EQ: 'value_eq',
  CHANGE_TYPE: 'change_type',
});

const TREND_DIRECTIONS = Object.freeze({
  UP: 'up',
  DOWN: 'down',
  FLAT: 'flat',
  INSUFFICIENT: 'insufficient',
});

/**
 * Deep clone via JSON (snapshots are structured/JSON-safe).
 * @template T
 * @param {T} value
 * @returns {T}
 */
function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

/**
 * Stable stringify for deterministic comparison.
 * @param {unknown} value
 */
function stableStringify(value) {
  return JSON.stringify(sortKeys(value));
}

/**
 * @param {unknown} value
 * @returns {unknown}
 */
function sortKeys(value) {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = sortKeys(/** @type {any} */ (value)[key]);
    }
    return out;
  }
  return value;
}

/**
 * Set difference (a - b), sorted.
 * @param {string[]} a
 * @param {string[]} b
 */
function setDiff(a, b) {
  const bSet = new Set(b || []);
  return [...new Set(a || [])].filter((x) => !bSet.has(x)).sort();
}

/**
 * @param {string} tenantId
 * @param {string} companyId
 * @param {string} timestamp
 * @param {number} seq
 */
function snapshotId(tenantId, companyId, timestamp, seq) {
  return `snap:${tenantId}:${companyId}:${timestamp}:${String(seq).padStart(6, '0')}`;
}

module.exports = {
  CHANGE_TYPES,
  WATCH_OPS,
  TREND_DIRECTIONS,
  deepClone,
  stableStringify,
  sortKeys,
  setDiff,
  snapshotId,
};
