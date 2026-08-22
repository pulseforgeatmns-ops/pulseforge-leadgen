'use strict';

/**
 * SPEC-136 — Pending operator decision must match executable mission state.
 * Presentation derives from these predicates. It is never an independent source of truth.
 */

const { STAGES, OPERATOR_DECISION_KINDS, amoError } = require('./types');
const { isStructuredMissionApproved } = require('./StructuredMission');

const MISSION_STATE_INCONSISTENT = 'MISSION_STATE_INCONSISTENT';

const PLAN_KINDS = new Set([
  OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
  OPERATOR_DECISION_KINDS.PLAN_EDIT,
]);

function missionFrom(snapshotOrMission) {
  if (!snapshotOrMission) return null;
  return snapshotOrMission.mission || snapshotOrMission;
}

function pendingKind(mission) {
  const pending = mission && mission.pendingOperatorDecision;
  return pending && pending.kind ? pending.kind : null;
}

function hasPendingPlanClarification(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (isStructuredMissionApproved(mission)) return false;
  if (mission.planCancelled) return false;
  return pendingKind(mission) === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION;
}

function hasPendingPlanApproval(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (isStructuredMissionApproved(mission)) return false;
  if (hasPendingPlanClarification(snapshot)) return false;
  if (mission.planCancelled) return false;
  return PLAN_KINDS.has(pendingKind(mission));
}

function hasPendingDiscoveryApproval(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (hasPendingPlanClarification(snapshot)) return false;
  if (hasPendingPlanApproval(snapshot)) return false;
  if (!isStructuredMissionApproved(mission)) return false;
  if (mission.stage && mission.stage !== STAGES.DISCOVER) return false;
  return pendingKind(mission) === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL;
}

function hasConsumablePendingDecision(snapshot) {
  return hasPendingPlanClarification(snapshot)
    || hasPendingPlanApproval(snapshot)
    || hasPendingDiscoveryApproval(snapshot);
}

function presentableOperatorDecision(snapshot) {
  const mission = missionFrom(snapshot) || {};
  const pending = mission.pendingOperatorDecision || null;
  if (!hasConsumablePendingDecision(snapshot) || !pending) return null;
  return {
    ...pending,
    kind: pendingKind(mission),
    prompt: hasPendingPlanClarification(snapshot)
      ? (pending.clarificationPrompt || pending.prompt || null)
      : hasPendingPlanApproval(snapshot)
        ? (pending.prompt || 'Approve mission plan?')
        : (pending.prompt || 'Approve discovery?'),
    consumable: true,
  };
}

function missionStateInconsistent(message, details) {
  const err = amoError(
    MISSION_STATE_INCONSISTENT,
    message || 'Pending operator decision does not match executable mission state.'
  );
  err.spec = 'SPEC-136';
  if (details) err.details = details;
  return err;
}

function consistencyDetails(mission, snapshot) {
  return {
    kind: pendingKind(mission),
    stage: mission.stage || null,
    structuredMissionApproved: isStructuredMissionApproved(mission),
    hasPendingPlanApproval: hasPendingPlanApproval(snapshot),
    hasPendingDiscoveryApproval: hasPendingDiscoveryApproval(snapshot),
    hasPendingPlanClarification: hasPendingPlanClarification(snapshot),
  };
}

function assertMissionStateConsistent(missionOrSnapshot, extras = {}) {
  const mission = missionFrom(missionOrSnapshot);
  if (!mission) return mission;
  const snapshot = extras.snapshot || {
    mission,
    contributions: extras.contributions
      || (missionOrSnapshot && missionOrSnapshot.contributions)
      || [],
  };
  const kind = pendingKind(mission);
  const approved = isStructuredMissionApproved(mission);
  const stage = mission.stage || null;
  const details = consistencyDetails(mission, snapshot);

  if (mission.planCancelled === true && kind) {
    throw missionStateInconsistent(
      'Cancelled missions cannot advertise an operator decision.',
      details
    );
  }

  if (approved && (PLAN_KINDS.has(kind) || kind === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION)) {
    throw missionStateInconsistent(
      'Plan is approved but pendingOperatorDecision still advertises a plan decision.',
      details
    );
  }

  if (!approved && kind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL) {
    throw missionStateInconsistent(
      'Discovery approval is advertised before the mission plan is locked.',
      details
    );
  }

  if (kind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL && stage && stage !== STAGES.DISCOVER) {
    throw missionStateInconsistent(
      'Discovery approval is advertised outside the discover stage.',
      details
    );
  }

  if (
    (PLAN_KINDS.has(kind) || kind === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION)
    && stage
    && stage !== STAGES.DISCOVER
  ) {
    throw missionStateInconsistent(
      'Plan decision is advertised outside the discover stage.',
      details
    );
  }

  if (PLAN_KINDS.has(kind) && !hasPendingPlanApproval(snapshot)) {
    throw missionStateInconsistent(
      'pendingOperatorDecision.kind is a plan approval that the execution engine cannot consume.',
      details
    );
  }

  if (kind === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION && !hasPendingPlanClarification(snapshot)) {
    throw missionStateInconsistent(
      'pendingOperatorDecision.kind is a plan clarification that the execution engine cannot consume.',
      details
    );
  }

  if (kind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL && !hasPendingDiscoveryApproval(snapshot)) {
    throw missionStateInconsistent(
      'pendingOperatorDecision.kind is a discovery approval that the execution engine cannot consume.',
      details
    );
  }

  if (hasPendingPlanApproval(snapshot) && !PLAN_KINDS.has(kind)) {
    throw missionStateInconsistent(
      'hasPendingPlanApproval is true but pendingOperatorDecision does not match.',
      details
    );
  }

  if (hasPendingDiscoveryApproval(snapshot) && kind !== OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL) {
    throw missionStateInconsistent(
      'hasPendingDiscoveryApproval is true but pendingOperatorDecision does not match.',
      details
    );
  }

  if (hasPendingPlanClarification(snapshot) && kind !== OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION) {
    throw missionStateInconsistent(
      'hasPendingPlanClarification is true but pendingOperatorDecision does not match.',
      details
    );
  }

  return mission;
}

function isMissionStateInconsistent(err) {
  return Boolean(err && err.code === MISSION_STATE_INCONSISTENT);
}

const DISCOVER_DECISION_KINDS = new Set([
  OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION,
  OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
  OPERATOR_DECISION_KINDS.PLAN_EDIT,
  OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
]);

/**
 * Leaving Discover makes plan/discovery decisions unconsumable.
 * Clear them in the same mutation so the resulting state stays consistent.
 */
function applyStageToPendingDecision(mission, targetStage) {
  if (!mission) return mission;
  if (targetStage === STAGES.DISCOVER) return mission;
  if (DISCOVER_DECISION_KINDS.has(pendingKind(mission))) {
    mission.pendingOperatorDecision = null;
  }
  return mission;
}

module.exports = {
  MISSION_STATE_INCONSISTENT,
  hasPendingPlanClarification,
  hasPendingPlanApproval,
  hasPendingDiscoveryApproval,
  hasConsumablePendingDecision,
  presentableOperatorDecision,
  pendingKind,
  assertMissionStateConsistent,
  missionStateInconsistent,
  isMissionStateInconsistent,
  applyStageToPendingDecision,
};
