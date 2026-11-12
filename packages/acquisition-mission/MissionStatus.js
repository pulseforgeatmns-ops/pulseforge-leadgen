'use strict';

/**
 * P0 Max reliability — truthful mission status before discovery.
 *
 * Every acquisition mission starts in the first lifecycle stage, `discover`.
 * Planning happens inside that stage as operator decisions (clarification,
 * plan approval, discovery approval), so labelling the stage "Discovering"
 * claimed Scout work that had not been authorized or started. While one of
 * those pre-discovery decisions is pending, the status names it instead.
 * "Discovering" is reported only once no pre-discovery decision is pending,
 * i.e. after discovery approval. A locked review-only plan (research planning
 * deferred) rests with no pending decision and reads "Plan approved (review
 * only)".
 *
 * Presentation-level truth only: stage, pending decision, approvals and
 * authorization are untouched, and no terminal or custom status is rewritten.
 */

const { STAGES, STAGE_LABELS, OPERATOR_DECISION_KINDS } = require('./types');

const PRE_DISCOVERY_STATUS = Object.freeze({
  [OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION]: 'Clarification required',
  [OPERATOR_DECISION_KINDS.PLAN_EDIT]: 'Plan edit in progress',
  [OPERATOR_DECISION_KINDS.PLAN_APPROVAL]: 'Plan ready for approval',
  [OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL]: 'Awaiting discovery approval',
});

/** Review-only mission whose plan is locked: discovery is prohibited, not awaited. */
const REVIEW_ONLY_PLAN_APPROVED = 'Plan approved (review only)';

/** Labels this module owns for the discover stage; anything else is left alone. */
const DISCOVER_STAGE_LABELS = new Set([
  STAGE_LABELS[STAGES.DISCOVER],
  ...Object.values(PRE_DISCOVERY_STATUS),
  REVIEW_ONLY_PLAN_APPROVED,
]);

/**
 * @param {string} stage
 * @param {object|null} pendingOperatorDecision
 * @returns {string}
 */
function missionStatusLabel(stage, pendingOperatorDecision, mission = null) {
  if (
    stage === STAGES.DISCOVER &&
    reviewOnlyPlan(mission) &&
    (!pendingOperatorDecision || pendingOperatorDecision.kind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL)
  ) {
    return REVIEW_ONLY_PLAN_APPROVED;
  }
  if (stage === STAGES.DISCOVER && pendingOperatorDecision) {
    const label = PRE_DISCOVERY_STATUS[pendingOperatorDecision.kind];
    if (label) return label;
  }
  return STAGE_LABELS[stage] || stage;
}

/** A locked plan that deferred research planning (review-only). */
function reviewOnlyPlan(mission) {
  if (!mission || !mission.structuredMission || !mission.structuredMission.immutable) return false;
  const { isResearchPlanningDeferred } = require('./StructuredMission');
  return isResearchPlanningDeferred(mission.structuredMission);
}

/**
 * Status a stored mission should carry. Only discover-stage labels are
 * reconciled; cancelled or any other explicit status is returned unchanged.
 * @param {object} mission
 * @returns {string|undefined}
 */
function reconcileMissionStatus(mission) {
  if (!mission || typeof mission !== 'object') return undefined;
  if (mission.planCancelled === true || mission.status === 'Cancelled') return mission.status;
  if (mission.stage !== STAGES.DISCOVER) return mission.status;
  if (mission.status && !DISCOVER_STAGE_LABELS.has(mission.status)) return mission.status;
  return missionStatusLabel(mission.stage, mission.pendingOperatorDecision, mission);
}

module.exports = {
  PRE_DISCOVERY_STATUS,
  missionStatusLabel,
  reconcileMissionStatus,
};
