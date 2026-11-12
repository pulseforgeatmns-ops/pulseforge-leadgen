'use strict';

/**
 * P0 Max reliability — research planning for review-only missions.
 *
 * A mission whose authorization prohibits provider research is planned
 * without Scout targeting (market segment, region) and without an
 * acquisition success metric; the plan records that research planning was
 * deferred. Two rules keep that safe if research is later authorized:
 *
 *  1. Gate — discovery never runs on a plan whose research planning was
 *     deferred. Enforced by the execution router and inside TME, next to the
 *     authorization check, so every surface is covered.
 *  2. Reopen — the governed amendment that permits research replans the
 *     mission in the same commit: the deferred questions become a pending
 *     plan clarification and the plan must be approved again before
 *     discovery approval is offered.
 */

const { amoError, STAGES } = require('./types');
const { ACTION_CLASSES, evaluateAction } = require('./AuthorizationConstraints');
const { isResearchPlanningDeferred } = require('./StructuredMission');

const RESEARCH_PLANNING_REQUIRED = 'research_planning_required';

/** @param {object|null} mission */
function researchPlanningIncomplete(mission) {
  if (!mission) return false;
  return isResearchPlanningDeferred(mission.structuredMission) ||
    isResearchPlanningDeferred(mission.missionPlanDraft);
}

/**
 * Throw when provider research is requested on a plan that never collected
 * research planning. A no-op for every other action class.
 * @param {object|null} mission
 * @param {string} actionClass
 * @param {object} [extras]
 */
function assertResearchPlanningComplete(mission, actionClass, extras = {}) {
  if (actionClass !== ACTION_CLASSES.PROVIDER_RESEARCH) return;
  if (!researchPlanningIncomplete(mission)) return;
  const plan = mission.structuredMission || mission.missionPlanDraft;
  const missing = (plan.researchPlanning && plan.researchPlanning.missing) || [];
  const err = amoError(
    RESEARCH_PLANNING_REQUIRED,
    'Discovery cannot run: this mission was planned as review-only, so research planning ' +
      (missing.length ? `(${missing.join(', ')}) ` : '') +
      'was never collected or approved. Answer the planning questions and approve the plan first.'
  );
  err.missing = missing;
  Object.assign(err, extras);
  throw err;
}

/**
 * Replan a research-deferred mission once research is permitted. Returns the
 * mission fields to commit, or null when no reopening applies. Pure: callers
 * commit the result inside their own transaction.
 * @param {object} mission current mission
 * @param {object|null} authorizationConstraints constraints after the change
 */
function reopenResearchPlanning(mission, authorizationConstraints) {
  if (!mission || mission.stage !== STAGES.DISCOVER) return null;
  if (!researchPlanningIncomplete(mission)) return null;
  if (!evaluateAction({ authorizationConstraints }, ACTION_CLASSES.PROVIDER_RESEARCH).allowed) return null;

  const { planMission, planFromObjective } = require('./MissionPlanner');
  const { buildPendingOperatorDecision } = require('./Mission');
  const opts = {
    targetSegment: mission.targetSegment || null,
    constraints: mission.constraints,
    resolutions: mission.planResolutions || null,
    authorizationConstraints,
  };
  const resolved = mission.resolvedObjective;
  const planned = resolved && resolved.objective
    ? planMission(resolved, opts)
    : planFromObjective(mission.objective, opts);
  const pendingOperatorDecision = buildPendingOperatorDecision({
    stage: STAGES.DISCOVER,
    input: {},
    missionPlanDraft: planned.draft,
    structuredMission: null,
    planAmbiguities: planned.ambiguities || [],
    planned,
  });
  return {
    missionPlanDraft: planned.draft,
    planAmbiguities: planned.ambiguities || [],
    structuredMission: null,
    structuredMissionApproved: false,
    pendingOperatorDecision,
  };
}

module.exports = {
  RESEARCH_PLANNING_REQUIRED,
  researchPlanningIncomplete,
  assertResearchPlanningComplete,
  reopenResearchPlanning,
};
