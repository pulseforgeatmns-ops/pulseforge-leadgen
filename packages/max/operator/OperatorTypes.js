'use strict';

/**
 * Operator Intelligence types (SPEC-012 / ADR-007).
 * Behavior events + learning — never business intelligence.
 */

const crypto = require('crypto');
const { deepFreeze } = require('../reasoning/ReasoningTypes');

/** Meaningful operator actions. */
const INTERACTION_TYPES = Object.freeze({
  VIEWED_RECOMMENDATION: 'ViewedRecommendation',
  OPENED_EVIDENCE: 'OpenedEvidence',
  ASKED_MAX: 'AskedMax',
  EXPANDED_REASONING: 'ExpandedReasoning',
  COMPARED_COMPANIES: 'ComparedCompanies',
  DISMISSED_CARD: 'DismissedCard',
  SNOOZED_RECOMMENDATION: 'SnoozedRecommendation',
  APPROVED_RECOMMENDATION: 'ApprovedRecommendation',
  IGNORED_RECOMMENDATION: 'IgnoredRecommendation',
  OPENED_TIMELINE: 'OpenedTimeline',
  RETURNED_TO_DECK: 'ReturnedToDeck',
  OPENED_SECTION: 'OpenedSection',
});

/** Explicit recommendation outcome lifecycle. */
const OUTCOMES = Object.freeze({
  RECOMMENDED: 'recommended',
  REVIEWED: 'reviewed',
  APPROVED: 'approved',
  EXECUTED: 'executed',
  SUCCESSFUL: 'successful',
  DISMISSED: 'dismissed',
  EXPIRED: 'expired',
  CONTRADICTED: 'contradicted',
});

/** Valid forward / terminal transitions from each outcome. */
const OUTCOME_TRANSITIONS = Object.freeze({
  [OUTCOMES.RECOMMENDED]: [
    OUTCOMES.REVIEWED,
    OUTCOMES.DISMISSED,
    OUTCOMES.EXPIRED,
    OUTCOMES.CONTRADICTED,
  ],
  [OUTCOMES.REVIEWED]: [
    OUTCOMES.APPROVED,
    OUTCOMES.DISMISSED,
    OUTCOMES.EXPIRED,
    OUTCOMES.CONTRADICTED,
  ],
  [OUTCOMES.APPROVED]: [
    OUTCOMES.EXECUTED,
    OUTCOMES.DISMISSED,
    OUTCOMES.EXPIRED,
    OUTCOMES.CONTRADICTED,
  ],
  [OUTCOMES.EXECUTED]: [
    OUTCOMES.SUCCESSFUL,
    OUTCOMES.DISMISSED,
    OUTCOMES.EXPIRED,
    OUTCOMES.CONTRADICTED,
  ],
  [OUTCOMES.SUCCESSFUL]: [],
  [OUTCOMES.DISMISSED]: [],
  [OUTCOMES.EXPIRED]: [],
  [OUTCOMES.CONTRADICTED]: [],
});

/** Deck sections that may be adaptively ordered / quieted — never hidden. */
const SECTION_IDS = Object.freeze({
  MORNING_BRIEF: 'morning_brief',
  HIGHEST_LEVERAGE: 'highest_leverage',
  WATCH_ALERTS: 'watch_alerts',
  MARKET_TRENDS: 'market_trends',
  PRIORITY_QUEUE: 'priority_queue',
});

/** Default visual section order (composer layout). */
const DEFAULT_SECTION_ORDER = Object.freeze([
  SECTION_IDS.MORNING_BRIEF,
  SECTION_IDS.HIGHEST_LEVERAGE,
  SECTION_IDS.WATCH_ALERTS,
  SECTION_IDS.MARKET_TRENDS,
  SECTION_IDS.PRIORITY_QUEUE,
]);

const DOMINANCE = Object.freeze({
  HIGH: 'high',
  NORMAL: 'normal',
  QUIET: 'quiet',
});

/** Conversational intent tags for Max preference learning. */
const INTENT_TAGS = Object.freeze({
  COMPARE: 'compare',
  CONFIDENCE: 'confidence',
  EVIDENCE: 'evidence',
  REASONING: 'reasoning',
  POLICY: 'policy',
  RISK: 'risk',
  WATCH: 'watch',
  CHANGE: 'change',
  TIMELINE: 'timeline',
});

/**
 * @typedef {object} InteractionEvent
 * @property {string} id
 * @property {string} type
 * @property {string} tenantId
 * @property {string|null} operatorId
 * @property {string|null} recommendationId
 * @property {string|null} companyId
 * @property {string|null} section
 * @property {number|null} depth
 * @property {string} timestamp
 * @property {number} seq
 * @property {object|null} payload
 */

/**
 * @param {object} input
 * @returns {InteractionEvent}
 */
function buildInteractionEvent(input) {
  if (!input || !input.type) {
    throw new Error('InteractionEvent requires type');
  }
  const type = String(input.type);
  if (!Object.values(INTERACTION_TYPES).includes(type)) {
    throw new Error(`Unknown interaction type: ${type}`);
  }
  if (input.tenantId == null || String(input.tenantId).trim() === '') {
    throw new Error('InteractionEvent requires tenantId');
  }

  const id =
    input.id != null
      ? String(input.id)
      : `oevt:${crypto.randomBytes(8).toString('hex')}`;

  return deepFreeze({
    id,
    type,
    tenantId: String(input.tenantId),
    operatorId:
      input.operatorId != null && String(input.operatorId).trim()
        ? String(input.operatorId)
        : null,
    recommendationId:
      input.recommendationId != null && String(input.recommendationId).trim()
        ? String(input.recommendationId)
        : null,
    companyId:
      input.companyId != null && String(input.companyId).trim()
        ? String(input.companyId)
        : null,
    section:
      input.section != null && String(input.section).trim()
        ? String(input.section)
        : null,
    depth:
      input.depth != null && Number.isFinite(Number(input.depth))
        ? Math.max(0, Number(input.depth))
        : null,
    timestamp: input.timestamp || new Date().toISOString(),
    seq: Number.isFinite(Number(input.seq)) ? Number(input.seq) : 0,
    payload:
      input.payload && typeof input.payload === 'object' ? input.payload : null,
  });
}

/**
 * Empty RecommendationLearning row.
 * @param {string} tenantId
 * @param {string} recommendationId
 */
function emptyLearning(tenantId, recommendationId) {
  return {
    recommendationId: String(recommendationId),
    tenantId: String(tenantId),
    viewed: 0,
    ignored: 0,
    approved: 0,
    dismissed: 0,
    openedInMax: 0,
    investigatedDepth: 0,
    timeToDecisionMs: null,
    firstViewedAt: null,
    decidedAt: null,
    lastEventAt: null,
    outcome: OUTCOMES.RECOMMENDED,
    trust: null,
  };
}

/**
 * @param {string} from
 * @param {string} to
 */
function canTransitionOutcome(from, to) {
  if (from === to) return true;
  const allowed = OUTCOME_TRANSITIONS[from];
  if (!allowed) return false;
  return allowed.includes(to);
}

module.exports = {
  INTERACTION_TYPES,
  OUTCOMES,
  OUTCOME_TRANSITIONS,
  SECTION_IDS,
  DEFAULT_SECTION_ORDER,
  DOMINANCE,
  INTENT_TAGS,
  buildInteractionEvent,
  emptyLearning,
  canTransitionOutcome,
};
