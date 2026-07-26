'use strict';

/**
 * Shared types and constants for the Max Reasoning Engine (SPEC-002).
 * No LLM. No invented facts. Structured data only.
 */

/** @typedef {'opportunity'|'engagement'|'relationship'|'decision_maker'|'overflow'|'technology'|'risk'} StrategyId */

/**
 * @typedef {object} EvidenceRef
 * @property {string} id
 * @property {string} kind - evidence | claim | interaction | person | company | edge | metric
 * @property {string} summary - copied from graph / template over graph fields only
 * @property {string|null} [sourceId]
 * @property {string|null} [sourceType]
 * @property {number|null} [confidence]
 */

/**
 * @typedef {object} StrategyResult
 * @property {StrategyId|string} strategy
 * @property {number} scoreDelta - signed contribution in [-100, 100]; positive favors pursuit
 * @property {number} confidence - 0–100; independent of scoreDelta
 * @property {EvidenceRef[]} supportingEvidence
 * @property {EvidenceRef[]} contradictingEvidence
 * @property {string[]} claims - claim node ids referenced
 * @property {string} summary - short structured observation (not LLM prose)
 */

/**
 * @typedef {object} ReasoningContext
 * @property {string} tenantId
 * @property {object} company
 * @property {object[]} people
 * @property {object[]} interactions
 * @property {object[]} claims
 * @property {object[]} evidence
 * @property {object[]} timeline
 * @property {object[]} relatedCompanies
 * @property {object} metrics
 * @property {object[]} neighborEdges
 * @property {string} builtAt
 * @property {string} repositoryType
 */

/**
 * @typedef {object} Recommendation
 * @property {string} id
 * @property {{ id: string, name: string|null, type: string }} subject
 * @property {string} type
 * @property {string} priority
 * @property {number} score
 * @property {number} confidence
 * @property {string} recommendedAction
 * @property {EvidenceRef[]} supportingSignals
 * @property {EvidenceRef[]} opposingSignals
 * @property {string[]} claims
 * @property {string[]} evidence
 * @property {object} reasoningSummary
 */

const STRATEGY_IDS = Object.freeze({
  OPPORTUNITY: 'opportunity',
  ENGAGEMENT: 'engagement',
  RELATIONSHIP: 'relationship',
  DECISION_MAKER: 'decision_maker',
  OVERFLOW: 'overflow',
  TECHNOLOGY: 'technology',
  RISK: 'risk',
});

/** Default aggregation weights (must sum to 1). */
const DEFAULT_STRATEGY_WEIGHTS = Object.freeze({
  [STRATEGY_IDS.OPPORTUNITY]: 0.3,
  [STRATEGY_IDS.RELATIONSHIP]: 0.2,
  [STRATEGY_IDS.ENGAGEMENT]: 0.15,
  [STRATEGY_IDS.DECISION_MAKER]: 0.1,
  [STRATEGY_IDS.TECHNOLOGY]: 0.1,
  [STRATEGY_IDS.OVERFLOW]: 0.1,
  [STRATEGY_IDS.RISK]: 0.05,
});

const RECOMMENDATION_TYPES = Object.freeze({
  PURSUE: 'pursue',
  FOLLOW_UP: 'follow_up',
  NURTURE: 'nurture',
  DEPRIORITIZE: 'deprioritize',
});

const PRIORITIES = Object.freeze({
  CRITICAL: 'critical',
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
});

const RECOMMENDED_ACTIONS = Object.freeze({
  REQUEST_INTRO: 'request_intro',
  FOLLOW_UP_OUTREACH: 'follow_up_outreach',
  NURTURE_SEQUENCE: 'nurture_sequence',
  ENRICH_CONTACTS: 'enrich_contacts',
  HOLD: 'hold',
  DEPRIORITIZE: 'deprioritize',
});

/** Target latency for a single company evaluation (ms). */
const PERFORMANCE_TARGET_MS = 500;

/**
 * Deep-freeze an object tree (best-effort; skips already frozen).
 * @template T
 * @param {T} value
 * @returns {T}
 */
function deepFreeze(value) {
  if (value === null || typeof value !== 'object') return value;
  if (Object.isFrozen(value)) return value;
  for (const key of Object.keys(value)) {
    deepFreeze(/** @type {any} */ (value)[key]);
  }
  return Object.freeze(value);
}

/**
 * Clamp a number into [min, max].
 * @param {number} n
 * @param {number} min
 * @param {number} max
 */
function clamp(n, min, max) {
  const v = Number(n);
  if (!Number.isFinite(v)) return min;
  if (v < min) return min;
  if (v > max) return max;
  return v;
}

/**
 * Round to fixed decimals for stable snapshots.
 * @param {number} n
 * @param {number} [digits=2]
 */
function round(n, digits = 2) {
  const f = 10 ** digits;
  return Math.round(Number(n) * f) / f;
}

/**
 * Deterministic recommendation id from tenant + subject.
 * @param {string} tenantId
 * @param {string} subjectId
 */
function recommendationId(tenantId, subjectId) {
  return `rec:${tenantId}:${subjectId}`;
}

/**
 * Stable sort by id (and secondary key).
 * @template {{ id?: string }} T
 * @param {T[]} rows
 * @param {(a: T, b: T) => number} [secondary]
 */
function sortById(rows, secondary) {
  return [...(rows || [])].sort((a, b) => {
    const aid = a && a.id != null ? String(a.id) : '';
    const bid = b && b.id != null ? String(b.id) : '';
    if (aid < bid) return -1;
    if (aid > bid) return 1;
    return secondary ? secondary(a, b) : 0;
  });
}

/**
 * @param {unknown} value
 * @returns {string}
 */
function asLower(value) {
  return value == null ? '' : String(value).toLowerCase();
}

/**
 * Case-insensitive includes.
 * @param {unknown} haystack
 * @param {string} needle
 */
function includesCI(haystack, needle) {
  return asLower(haystack).includes(asLower(needle));
}

/**
 * Build an evidence ref from a graph node / synthetic observation.
 * @param {Partial<EvidenceRef> & { id: string, kind: string, summary: string }} input
 * @returns {EvidenceRef}
 */
function evidenceRef(input) {
  return {
    id: String(input.id),
    kind: String(input.kind),
    summary: String(input.summary),
    sourceId: input.sourceId != null ? String(input.sourceId) : null,
    sourceType: input.sourceType != null ? String(input.sourceType) : null,
    confidence:
      input.confidence == null || !Number.isFinite(Number(input.confidence))
        ? null
        : Number(input.confidence),
  };
}

module.exports = {
  STRATEGY_IDS,
  DEFAULT_STRATEGY_WEIGHTS,
  RECOMMENDATION_TYPES,
  PRIORITIES,
  RECOMMENDED_ACTIONS,
  PERFORMANCE_TARGET_MS,
  deepFreeze,
  clamp,
  round,
  recommendationId,
  sortById,
  asLower,
  includesCI,
  evidenceRef,
};
