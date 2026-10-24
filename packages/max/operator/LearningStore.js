'use strict';

const {
  INTERACTION_TYPES,
  OUTCOMES,
  emptyLearning,
} = require('./OperatorTypes');
const { scoreTrust } = require('./TrustScorer');

/**
 * Per-recommendation learning aggregates (SPEC-012).
 * The recommendation itself never changes — engagement does.
 */
class LearningStore {
  constructor() {
    /** @type {Map<string, object>} key = tenantId::recommendationId */
    this._byKey = new Map();
  }

  /**
   * @param {string} tenantId
   * @param {string} recommendationId
   */
  get(tenantId, recommendationId) {
    if (!recommendationId) return null;
    const key = makeKey(tenantId, recommendationId);
    const row = this._byKey.get(key);
    return row ? { ...row } : emptyLearning(tenantId, recommendationId);
  }

  /**
   * Apply an interaction event to learning state.
   * @param {object} event - InteractionEvent
   * @returns {object|null} updated learning or null if no recommendationId
   */
  applyEvent(event) {
    if (!event || !event.recommendationId) return null;
    const key = makeKey(event.tenantId, event.recommendationId);
    let row = this._byKey.get(key);
    if (!row) {
      row = emptyLearning(event.tenantId, event.recommendationId);
    }

    const ts = event.timestamp || new Date().toISOString();
    row.lastEventAt = ts;

    switch (event.type) {
      case INTERACTION_TYPES.VIEWED_RECOMMENDATION:
        row.viewed += 1;
        if (!row.firstViewedAt) row.firstViewedAt = ts;
        if (row.outcome === OUTCOMES.RECOMMENDED) {
          row.outcome = OUTCOMES.REVIEWED;
        }
        break;
      case INTERACTION_TYPES.IGNORED_RECOMMENDATION:
        row.ignored += 1;
        break;
      case INTERACTION_TYPES.APPROVED_RECOMMENDATION:
        row.approved += 1;
        row.outcome = OUTCOMES.APPROVED;
        row.decidedAt = ts;
        row.timeToDecisionMs = computeTimeToDecision(row);
        break;
      case INTERACTION_TYPES.DISMISSED_CARD:
      case INTERACTION_TYPES.SNOOZED_RECOMMENDATION:
        row.dismissed += 1;
        if (
          row.outcome !== OUTCOMES.APPROVED &&
          row.outcome !== OUTCOMES.EXECUTED &&
          row.outcome !== OUTCOMES.SUCCESSFUL
        ) {
          row.outcome = OUTCOMES.DISMISSED;
          row.decidedAt = ts;
          row.timeToDecisionMs = computeTimeToDecision(row);
        }
        break;
      case INTERACTION_TYPES.ASKED_MAX:
        row.openedInMax += 1;
        if (!row.firstViewedAt) row.firstViewedAt = ts;
        if (row.outcome === OUTCOMES.RECOMMENDED) {
          row.outcome = OUTCOMES.REVIEWED;
        }
        break;
      case INTERACTION_TYPES.OPENED_EVIDENCE:
      case INTERACTION_TYPES.EXPANDED_REASONING:
      case INTERACTION_TYPES.OPENED_TIMELINE:
      case INTERACTION_TYPES.COMPARED_COMPANIES: {
        const depth = event.depth != null ? Number(event.depth) : depthForType(event.type);
        row.investigatedDepth = Math.max(row.investigatedDepth, depth);
        if (!row.firstViewedAt) row.firstViewedAt = ts;
        if (row.outcome === OUTCOMES.RECOMMENDED) {
          row.outcome = OUTCOMES.REVIEWED;
        }
        break;
      }
      default:
        break;
    }

    row.trust = scoreTrust(row);
    this._byKey.set(key, row);
    return { ...row };
  }

  /**
   * Set outcome explicitly (from OutcomeTracker).
   * @param {string} tenantId
   * @param {string} recommendationId
   * @param {string} outcome
   * @param {string} [timestamp]
   */
  setOutcome(tenantId, recommendationId, outcome, timestamp) {
    const key = makeKey(tenantId, recommendationId);
    let row = this._byKey.get(key);
    if (!row) {
      row = emptyLearning(tenantId, recommendationId);
    }
    const ts = timestamp || new Date().toISOString();
    const prev = row.outcome;
    row.outcome = outcome;
    row.lastEventAt = ts;
    if (
      outcome === OUTCOMES.APPROVED ||
      outcome === OUTCOMES.DISMISSED ||
      outcome === OUTCOMES.EXECUTED ||
      outcome === OUTCOMES.SUCCESSFUL
    ) {
      if (!row.decidedAt) row.decidedAt = ts;
      if (row.timeToDecisionMs == null) {
        row.timeToDecisionMs = computeTimeToDecision(row);
      }
    }
    // Count only on first arrival at this terminal/decision state
    if (outcome === OUTCOMES.APPROVED && prev !== OUTCOMES.APPROVED) {
      row.approved += 1;
    }
    if (outcome === OUTCOMES.DISMISSED && prev !== OUTCOMES.DISMISSED) {
      row.dismissed += 1;
    }
    row.trust = scoreTrust(row);
    this._byKey.set(key, row);
    return { ...row };
  }

  /**
   * @param {string} tenantId
   * @returns {object[]}
   */
  listForTenant(tenantId) {
    const tid = String(tenantId || '');
    const out = [];
    for (const row of this._byKey.values()) {
      if (row.tenantId === tid) out.push({ ...row });
    }
    return out;
  }

  clear() {
    this._byKey.clear();
  }

  get size() {
    return this._byKey.size;
  }
}

function makeKey(tenantId, recommendationId) {
  return `${String(tenantId)}::${String(recommendationId)}`;
}

function computeTimeToDecision(row) {
  if (!row.firstViewedAt || !row.decidedAt) return null;
  const a = Date.parse(row.firstViewedAt);
  const b = Date.parse(row.decidedAt);
  if (!Number.isFinite(a) || !Number.isFinite(b) || b < a) return null;
  return b - a;
}

function depthForType(type) {
  switch (type) {
    case INTERACTION_TYPES.EXPANDED_REASONING:
      return 1;
    case INTERACTION_TYPES.OPENED_EVIDENCE:
      return 2;
    case INTERACTION_TYPES.OPENED_TIMELINE:
      return 2;
    case INTERACTION_TYPES.COMPARED_COMPANIES:
      return 3;
    default:
      return 1;
  }
}

module.exports = { LearningStore };
