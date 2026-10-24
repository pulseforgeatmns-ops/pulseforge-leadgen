'use strict';

/**
 * Outcome Intelligence types (SPEC-036 / ADR-023).
 * Campaign/mission operational outcomes → reusable intelligence after evidence + approval.
 * Distinct from SPEC-013 / ADR-008 (Max recommendation evaluation).
 */

const OUTCOME_POLARITY = Object.freeze({
  POSITIVE: 'positive',
  NEUTRAL: 'neutral',
  NEGATIVE: 'negative',
});

const OUTCOME_TYPES = Object.freeze({
  // Positive
  DELIVERED: 'delivered',
  PHONE_CALL: 'phone_call',
  EMAIL_REPLY: 'email_reply',
  WALKTHROUGH_SCHEDULED: 'walkthrough_scheduled',
  PROPOSAL_REQUESTED: 'proposal_requested',
  PROPOSAL_SENT: 'proposal_sent',
  CLOSED_WON: 'closed_won',
  REFERRAL: 'referral',
  // Neutral
  NO_RESPONSE: 'no_response',
  FOLLOW_UP_REQUIRED: 'follow_up_required',
  DELAYED_DECISION: 'delayed_decision',
  // Negative
  RETURNED_MAIL: 'returned_mail',
  WRONG_CONTACT: 'wrong_contact',
  BUSINESS_CLOSED: 'business_closed',
  NOT_INTERESTED: 'not_interested',
  CLOSED_LOST: 'closed_lost',
});

const OUTCOME_TYPE_POLARITY = Object.freeze({
  [OUTCOME_TYPES.DELIVERED]: OUTCOME_POLARITY.POSITIVE,
  [OUTCOME_TYPES.PHONE_CALL]: OUTCOME_POLARITY.POSITIVE,
  [OUTCOME_TYPES.EMAIL_REPLY]: OUTCOME_POLARITY.POSITIVE,
  [OUTCOME_TYPES.WALKTHROUGH_SCHEDULED]: OUTCOME_POLARITY.POSITIVE,
  [OUTCOME_TYPES.PROPOSAL_REQUESTED]: OUTCOME_POLARITY.POSITIVE,
  [OUTCOME_TYPES.PROPOSAL_SENT]: OUTCOME_POLARITY.POSITIVE,
  [OUTCOME_TYPES.CLOSED_WON]: OUTCOME_POLARITY.POSITIVE,
  [OUTCOME_TYPES.REFERRAL]: OUTCOME_POLARITY.POSITIVE,
  [OUTCOME_TYPES.NO_RESPONSE]: OUTCOME_POLARITY.NEUTRAL,
  [OUTCOME_TYPES.FOLLOW_UP_REQUIRED]: OUTCOME_POLARITY.NEUTRAL,
  [OUTCOME_TYPES.DELAYED_DECISION]: OUTCOME_POLARITY.NEUTRAL,
  [OUTCOME_TYPES.RETURNED_MAIL]: OUTCOME_POLARITY.NEGATIVE,
  [OUTCOME_TYPES.WRONG_CONTACT]: OUTCOME_POLARITY.NEGATIVE,
  [OUTCOME_TYPES.BUSINESS_CLOSED]: OUTCOME_POLARITY.NEGATIVE,
  [OUTCOME_TYPES.NOT_INTERESTED]: OUTCOME_POLARITY.NEGATIVE,
  [OUTCOME_TYPES.CLOSED_LOST]: OUTCOME_POLARITY.NEGATIVE,
});

/** Map SPEC-035 Direct Mail response statuses → Outcome Types. */
const RESPONSE_STATUS_TO_OUTCOME = Object.freeze({
  no_response: OUTCOME_TYPES.NO_RESPONSE,
  returned_mail: OUTCOME_TYPES.RETURNED_MAIL,
  called: OUTCOME_TYPES.PHONE_CALL,
  emailed: OUTCOME_TYPES.EMAIL_REPLY,
  walkthrough_scheduled: OUTCOME_TYPES.WALKTHROUGH_SCHEDULED,
  proposal_sent: OUTCOME_TYPES.PROPOSAL_SENT,
  closed_won: OUTCOME_TYPES.CLOSED_WON,
  closed_lost: OUTCOME_TYPES.CLOSED_LOST,
});

/** Outcomes that count as a "response" for analytics. */
const RESPONSE_OUTCOMES = Object.freeze(
  new Set([
    OUTCOME_TYPES.PHONE_CALL,
    OUTCOME_TYPES.EMAIL_REPLY,
    OUTCOME_TYPES.WALKTHROUGH_SCHEDULED,
    OUTCOME_TYPES.PROPOSAL_REQUESTED,
    OUTCOME_TYPES.PROPOSAL_SENT,
    OUTCOME_TYPES.CLOSED_WON,
    OUTCOME_TYPES.REFERRAL,
    OUTCOME_TYPES.FOLLOW_UP_REQUIRED,
    OUTCOME_TYPES.DELAYED_DECISION,
    OUTCOME_TYPES.NOT_INTERESTED,
    OUTCOME_TYPES.CLOSED_LOST,
  ])
);

const SUCCESS_OUTCOMES = Object.freeze(
  new Set([
    OUTCOME_TYPES.WALKTHROUGH_SCHEDULED,
    OUTCOME_TYPES.PROPOSAL_REQUESTED,
    OUTCOME_TYPES.PROPOSAL_SENT,
    OUTCOME_TYPES.CLOSED_WON,
    OUTCOME_TYPES.REFERRAL,
  ])
);

const LEARNING_STATUS = Object.freeze({
  CANDIDATE: 'candidate',
  EVIDENCE_BACKED: 'evidence_backed',
  PROMOTED: 'promoted',
  REJECTED: 'rejected',
});

const RECOMMENDATION_STATUS = Object.freeze({
  PENDING: 'pending',
  APPROVED: 'approved',
  REJECTED: 'rejected',
  APPLIED: 'applied',
});

const RECOMMENDATION_TARGETS = Object.freeze({
  CLIENT_PLAYBOOK: 'client_playbook',
  RANKING_WEIGHTS: 'ranking_weights',
  DISCOVERY_STRATEGY: 'discovery_strategy',
  CAMPAIGN_TEMPLATES: 'campaign_templates',
});

const PERSONALIZATION_DIMENSIONS = Object.freeze([
  'opening_paragraph',
  'personalization_facts',
  'offer',
  'cta',
  'insert_package',
]);

const OPERATOR_ACTIONS = Object.freeze([
  'capture_outcomes',
  'generate_learnings',
  'generate_recommendations',
  'approve_recommendation',
  'reject_recommendation',
  'apply_recommendation',
  'conclude_mission',
]);

const OUTCOME_PROGRESS_STAGES = Object.freeze({
  CAPTURING: 'Capturing outcomes',
  LEARNING: 'Generating learnings',
  RECOMMENDING: 'Generating recommendations',
  ANALYTICS: 'Computing analytics',
  SUMMARY: 'Building outcome summary',
  COMPLETED: 'Completed',
});

/** Minimum samples in a segment before a learning may be evidence_backed. */
const MIN_EVIDENCE_SAMPLES = 3;

/** Minimum absolute lift vs baseline (ratio − 1) to promote a learning. */
const MIN_EVIDENCE_LIFT = 0.25;

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildOutcomeRecord(partial = {}) {
  const outcomeType = normalizeOutcomeType(partial.outcomeType || partial.type);
  const polarity =
    OUTCOME_TYPE_POLARITY[outcomeType] || OUTCOME_POLARITY.NEUTRAL;
  return {
    id: String(partial.id || ''),
    missionId: partial.missionId != null ? String(partial.missionId) : null,
    campaignId: partial.campaignId != null ? String(partial.campaignId) : null,
    campaignName:
      partial.campaignName != null ? String(partial.campaignName).trim() : null,
    prospectId: partial.prospectId != null ? String(partial.prospectId) : null,
    companyId: partial.companyId != null ? String(partial.companyId) : null,
    company:
      partial.company != null
        ? String(partial.company).trim()
        : partial.companyName != null
          ? String(partial.companyName).trim()
          : null,
    outcomeType,
    polarity,
    timestamp: partial.timestamp || new Date().toISOString(),
    operator: partial.operator != null ? String(partial.operator) : 'operator',
    notes: partial.notes != null ? String(partial.notes) : null,
    evidence: Array.isArray(partial.evidence) ? partial.evidence : [],
    confidence: clamp01(
      partial.confidence != null ? Number(partial.confidence) : 0.8
    ),
    attributes:
      partial.attributes && typeof partial.attributes === 'object'
        ? { ...partial.attributes }
        : {},
    successful: SUCCESS_OUTCOMES.has(outcomeType),
    vertical:
      partial.vertical != null
        ? String(partial.vertical).toLowerCase()
        : partial.attributes && partial.attributes.vertical
          ? String(partial.attributes.vertical).toLowerCase()
          : null,
    industry:
      partial.industry != null
        ? String(partial.industry).toLowerCase()
        : partial.attributes && partial.attributes.industry
          ? String(partial.attributes.industry).toLowerCase()
          : null,
    region:
      partial.region != null
        ? String(partial.region).toLowerCase()
        : partial.attributes && partial.attributes.region
          ? String(partial.attributes.region).toLowerCase()
          : null,
  };
}

/**
 * @param {string} raw
 * @returns {string}
 */
function normalizeOutcomeType(raw) {
  const s = String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_');
  if (OUTCOME_TYPE_POLARITY[s]) return s;
  if (RESPONSE_STATUS_TO_OUTCOME[s]) return RESPONSE_STATUS_TO_OUTCOME[s];
  return OUTCOME_TYPES.NO_RESPONSE;
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildLearning(partial = {}) {
  return {
    id: String(partial.id || ''),
    statement: String(partial.statement || '').trim(),
    dimension: partial.dimension != null ? String(partial.dimension) : null,
    segment: partial.segment != null ? String(partial.segment) : null,
    baselineRate:
      partial.baselineRate != null ? Number(partial.baselineRate) : null,
    segmentRate:
      partial.segmentRate != null ? Number(partial.segmentRate) : null,
    lift: partial.lift != null ? Number(partial.lift) : null,
    sampleSize: Number(partial.sampleSize) || 0,
    evidenceIds: Array.isArray(partial.evidenceIds)
      ? partial.evidenceIds.map(String)
      : [],
    status: partial.status || LEARNING_STATUS.CANDIDATE,
    confidence: clamp01(
      partial.confidence != null ? Number(partial.confidence) : 0
    ),
    createdAt: partial.createdAt || new Date().toISOString(),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildRecommendation(partial = {}) {
  return {
    id: String(partial.id || ''),
    summary: String(partial.summary || '').trim(),
    action: partial.action != null ? String(partial.action) : null,
    target: partial.target || RECOMMENDATION_TARGETS.CLIENT_PLAYBOOK,
    learningIds: Array.isArray(partial.learningIds)
      ? partial.learningIds.map(String)
      : [],
    status: partial.status || RECOMMENDATION_STATUS.PENDING,
    evidenceBacked: Boolean(partial.evidenceBacked),
    approvedAt: partial.approvedAt || null,
    approvedBy: partial.approvedBy != null ? String(partial.approvedBy) : null,
    rejectedAt: partial.rejectedAt || null,
    rejectedBy: partial.rejectedBy != null ? String(partial.rejectedBy) : null,
    appliedAt: partial.appliedAt || null,
    createdAt: partial.createdAt || new Date().toISOString(),
  };
}

/**
 * Structured feedback for Opportunity Ranking (SPEC-026).
 * @param {object} [partial]
 * @returns {object}
 */
function buildRankingFeedback(partial = {}) {
  return {
    kind: 'ranking_feedback',
    characteristic: String(partial.characteristic || '').trim(),
    polarity: partial.polarity || OUTCOME_POLARITY.NEUTRAL,
    scoreDelta: Number.isFinite(Number(partial.scoreDelta))
      ? Number(partial.scoreDelta)
      : 0,
    confidenceDelta: Number.isFinite(Number(partial.confidenceDelta))
      ? Number(partial.confidenceDelta)
      : 0,
    sampleSize: Number(partial.sampleSize) || 0,
    outcomeIds: Array.isArray(partial.outcomeIds)
      ? partial.outcomeIds.map(String)
      : [],
    successful: Boolean(partial.successful),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildPersonalizationFeedback(partial = {}) {
  const dims = {};
  for (const d of PERSONALIZATION_DIMENSIONS) {
    const src = (partial.dimensions && partial.dimensions[d]) || partial[d] || {};
    dims[d] = {
      exposures: Number(src.exposures) || 0,
      responses: Number(src.responses) || 0,
      wins: Number(src.wins) || 0,
      responseRate:
        src.responseRate != null
          ? Number(src.responseRate)
          : Number(src.exposures) > 0
            ? Math.round(
                ((Number(src.responses) || 0) / Number(src.exposures)) * 1000
              ) / 1000
            : 0,
    };
  }
  return {
    kind: 'personalization_feedback',
    dimensions: dims,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildCampaignAnalytics(partial = {}) {
  const mailed = Number(partial.mailed) || 0;
  const responses = Number(partial.responses) || 0;
  const walkthroughs = Number(partial.walkthroughs) || 0;
  const proposals = Number(partial.proposals) || 0;
  const wins = Number(partial.wins) || 0;
  const cost = partial.cost != null ? Number(partial.cost) : null;
  const revenue = partial.revenue != null ? Number(partial.revenue) : null;
  const rate = (n) =>
    mailed > 0 ? Math.round((n / mailed) * 1000) / 1000 : 0;
  const per = (n) =>
    cost != null && n > 0 ? Math.round((cost / n) * 100) / 100 : null;
  return {
    mailed,
    responses,
    walkthroughs,
    proposals,
    wins,
    responseRate: partial.responseRate != null ? Number(partial.responseRate) : rate(responses),
    walkthroughRate:
      partial.walkthroughRate != null
        ? Number(partial.walkthroughRate)
        : rate(walkthroughs),
    proposalRate:
      partial.proposalRate != null ? Number(partial.proposalRate) : rate(proposals),
    winRate: partial.winRate != null ? Number(partial.winRate) : rate(wins),
    cost,
    revenue,
    roi:
      cost != null && cost > 0 && revenue != null
        ? Math.round(((revenue - cost) / cost) * 1000) / 1000
        : null,
    costPerResponse: partial.costPerResponse != null
      ? Number(partial.costPerResponse)
      : per(responses),
    costPerWalkthrough: partial.costPerWalkthrough != null
      ? Number(partial.costPerWalkthrough)
      : per(walkthroughs),
    costPerCustomer: partial.costPerCustomer != null
      ? Number(partial.costPerCustomer)
      : per(wins),
  };
}

/**
 * Mission Outcome Summary (SPEC-032 contract mirror).
 * @param {object} [partial]
 * @returns {object}
 */
function buildOutcomeSummary(partial = {}) {
  return {
    kind: 'mission_outcome_summary',
    missionId: partial.missionId != null ? String(partial.missionId) : null,
    campaignId: partial.campaignId != null ? String(partial.campaignId) : null,
    campaignName:
      partial.campaignName != null ? String(partial.campaignName) : null,
    objectiveAchieved: Boolean(partial.objectiveAchieved),
    objectiveText:
      partial.objectiveText != null ? String(partial.objectiveText) : null,
    lessonsLearned: Array.isArray(partial.lessonsLearned)
      ? partial.lessonsLearned.map(String)
      : [],
    recommendationsGenerated: Number(partial.recommendationsGenerated) || 0,
    recommendationsPending: Number(partial.recommendationsPending) || 0,
    outcomeCount: Number(partial.outcomeCount) || 0,
    analytics: partial.analytics || null,
    concludedAt: partial.concludedAt || new Date().toISOString(),
  };
}

/**
 * Mission Memory event shape.
 * @param {object} [partial]
 * @returns {object}
 */
function buildMissionOutcomeEvent(partial = {}) {
  return {
    kind: 'mission_outcome_event',
    eventType: String(partial.eventType || 'outcome_captured'),
    timestamp: partial.timestamp || new Date().toISOString(),
    operator: partial.operator != null ? String(partial.operator) : 'system',
    summary: String(partial.summary || '').trim(),
    outcomeId: partial.outcomeId != null ? String(partial.outcomeId) : null,
    recommendationId:
      partial.recommendationId != null
        ? String(partial.recommendationId)
        : null,
  };
}

/**
 * Mission timeline entry shape.
 * @param {object} [partial]
 * @returns {object}
 */
function buildMissionTimelineEntry(partial = {}) {
  return {
    kind: 'mission_timeline',
    stage: String(partial.stage || 'outcome_intelligence'),
    status: partial.status != null ? String(partial.status) : null,
    timestamp: partial.timestamp || new Date().toISOString(),
    summary: String(partial.summary || '').trim(),
    operator: partial.operator != null ? String(partial.operator) : null,
  };
}

/**
 * @param {number} n
 * @returns {number}
 */
function clamp01(n) {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

module.exports = {
  OUTCOME_POLARITY,
  OUTCOME_TYPES,
  OUTCOME_TYPE_POLARITY,
  RESPONSE_STATUS_TO_OUTCOME,
  RESPONSE_OUTCOMES,
  SUCCESS_OUTCOMES,
  LEARNING_STATUS,
  RECOMMENDATION_STATUS,
  RECOMMENDATION_TARGETS,
  PERSONALIZATION_DIMENSIONS,
  OPERATOR_ACTIONS,
  OUTCOME_PROGRESS_STAGES,
  MIN_EVIDENCE_SAMPLES,
  MIN_EVIDENCE_LIFT,
  buildOutcomeRecord,
  normalizeOutcomeType,
  buildLearning,
  buildRecommendation,
  buildRankingFeedback,
  buildPersonalizationFeedback,
  buildCampaignAnalytics,
  buildOutcomeSummary,
  buildMissionOutcomeEvent,
  buildMissionTimelineEntry,
  clamp01,
};
