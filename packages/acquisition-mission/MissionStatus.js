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

/** Locked review-only plan (explicit review-only classification). */
const REVIEW_ONLY_PLAN_APPROVED = 'Plan approved (review only)';
/** Locked deferred plan whose mission is no longer review-only but still cannot research. */
const RESEARCH_DEFERRED_PLAN_APPROVED = 'Plan approved (discovery not authorized)';
/** Discovery approval is pending but the mission's authorization prohibits discovery. */
const DISCOVERY_NOT_AUTHORIZED = 'Discovery not authorized';

/** Labels this module owns for the discover stage; anything else is left alone. */
const DISCOVER_STAGE_LABELS = new Set([
  STAGE_LABELS[STAGES.DISCOVER],
  ...Object.values(PRE_DISCOVERY_STATUS),
  REVIEW_ONLY_PLAN_APPROVED,
  RESEARCH_DEFERRED_PLAN_APPROVED,
  DISCOVERY_NOT_AUTHORIZED,
]);

function researchProhibited(mission) {
  if (!mission || !mission.authorizationConstraints) return false;
  const { ACTION_CLASSES, evaluateAction } = require('./AuthorizationConstraints');
  return !evaluateAction(mission, ACTION_CLASSES.PROVIDER_RESEARCH).allowed;
}

/** A locked plan whose research planning was deferred (planned review-only). */
function restingPlan(mission) {
  if (!mission || !mission.structuredMission || !mission.structuredMission.immutable) return false;
  const { isResearchPlanningDeferred } = require('./StructuredMission');
  return isResearchPlanningDeferred(mission.structuredMission);
}

/**
 * @param {string} stage
 * @param {object|null} pendingOperatorDecision
 * @param {object|null} [mission] needed for restriction-aware labels
 * @returns {string}
 */
function missionStatusLabel(stage, pendingOperatorDecision, mission = null) {
  if (stage === STAGES.DISCOVER) {
    const pendingKind = pendingOperatorDecision ? pendingOperatorDecision.kind : null;
    if (restingPlan(mission) && (!pendingKind || pendingKind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL)) {
      return mission.authorizationConstraints && mission.authorizationConstraints.reviewOnly === true
        ? REVIEW_ONLY_PLAN_APPROVED
        : RESEARCH_DEFERRED_PLAN_APPROVED;
    }
    if (pendingKind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL && researchProhibited(mission)) {
      return DISCOVERY_NOT_AUTHORIZED;
    }
    const label = pendingKind ? PRE_DISCOVERY_STATUS[pendingKind] : null;
    if (label) return label;
  }
  return STAGE_LABELS[stage] || stage;
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
  REVIEW_ONLY_PLAN_APPROVED,
  RESEARCH_DEFERRED_PLAN_APPROVED,
  DISCOVERY_NOT_AUTHORIZED,
  missionStatusLabel,
  reconcileMissionStatus,
};
