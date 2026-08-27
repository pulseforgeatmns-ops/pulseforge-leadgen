'use strict';

/**
 * SPEC-136 — Pending operator decision must match executable mission state.
 * SPEC-141 — Discovery review gate: prioritization_approval after Scout completes.
 * Presentation derives from these predicates. It is never an independent source of truth.
 */

const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  amoError,
} = require('./types');
const { isStructuredMissionApproved } = require('./StructuredMission');
const {
  isExecutionApproved,
  canAdvertiseExecutionApproval,
} = require('./ExecutionApproval');

const MISSION_STATE_INCONSISTENT = 'MISSION_STATE_INCONSISTENT';

const PLAN_KINDS = new Set([
  OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
  OPERATOR_DECISION_KINDS.PLAN_EDIT,
]);

function missionFrom(snapshotOrMission) {
  if (!snapshotOrMission) return null;
  return snapshotOrMission.mission || snapshotOrMission;
}

function contributionsFrom(snapshotOrMission, extras = {}) {
  if (extras.contributions) return extras.contributions;
  if (snapshotOrMission && snapshotOrMission.contributions) return snapshotOrMission.contributions;
  return [];
}

function pendingKind(mission) {
  const pending = mission && mission.pendingOperatorDecision;
  return pending && pending.kind ? pending.kind : null;
}

function hasDiscoveryArtifact(snapshotOrMission, extras = {}) {
  const contributions = contributionsFrom(snapshotOrMission, extras);
  return contributions.some(
    (row) => row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
  );
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
  if (hasDiscoveryArtifact(snapshot)) return false;
  return pendingKind(mission) === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL;
}

function hasConsumedPrioritizationApproval(snapshotOrMission, extras = {}) {
  const contributions = contributionsFrom(snapshotOrMission, extras);
  return contributions.some(
    (row) =>
      row.specialist === SPECIALISTS.OPERATOR &&
      row.kind === CONTRIBUTION_KINDS.APPROVAL &&
      (row.payload.action === 'prioritization_approved' ||
        row.payload.kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL)
  );
}

function hasPendingPrioritizationApproval(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (hasPendingPlanClarification(snapshot)) return false;
  if (hasPendingPlanApproval(snapshot)) return false;
  if (hasPendingDiscoveryApproval(snapshot)) return false;
  if (hasPendingDiscoveryInvestigation(snapshot)) return false;
  if (!isStructuredMissionApproved(mission)) return false;
  if (mission.stage && mission.stage !== STAGES.DISCOVER) return false;
  if (!hasDiscoveryArtifact(snapshot)) return false;
  return pendingKind(mission) === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL;
}

function hasPendingDiscoveryInvestigation(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (hasPendingPlanClarification(snapshot)) return false;
  if (hasPendingPlanApproval(snapshot)) return false;
  if (hasPendingDiscoveryApproval(snapshot)) return false;
  if (!isStructuredMissionApproved(mission)) return false;
  if (mission.stage && mission.stage !== STAGES.DISCOVER) return false;
  if (!hasDiscoveryArtifact(snapshot)) return false;
  return pendingKind(mission) === OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION;
}

function hasPendingExecutionApproval(snapshot, extras = {}) {
  const mission = missionFrom(snapshot) || {};
  const contributions = contributionsFrom(snapshot, extras);
  if (!canAdvertiseExecutionApproval(mission, contributions, extras)) return false;
  return pendingKind(mission) === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL;
}

function hasConsumedExecutionApproval(snapshotOrMission, extras = {}) {
  const mission = missionFrom(snapshotOrMission) || {};
  const contributions = contributionsFrom(snapshotOrMission, extras);
  return isExecutionApproved(contributions, mission.id, extras);
}

function hasConsumablePendingDecision(snapshot) {
  return hasPendingPlanClarification(snapshot)
    || hasPendingPlanApproval(snapshot)
    || hasPendingDiscoveryApproval(snapshot)
    || hasPendingDiscoveryInvestigation(snapshot)
    || hasPendingPrioritizationApproval(snapshot)
    || hasPendingExecutionApproval(snapshot);
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
        : hasPendingPrioritizationApproval(snapshot)
          ? (pending.prompt || 'Approve prioritization?')
          : hasPendingDiscoveryInvestigation(snapshot)
            ? (pending.prompt || 'Continue investigation?')
            : hasPendingExecutionApproval(snapshot)
            ? (pending.prompt || 'Authorize external execution of prepared outreach?')
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
    hasPendingDiscoveryInvestigation: hasPendingDiscoveryInvestigation(snapshot),
    hasPendingPrioritizationApproval: hasPendingPrioritizationApproval(snapshot),
    hasPendingExecutionApproval: hasPendingExecutionApproval(snapshot),
    hasPendingPlanClarification: hasPendingPlanClarification(snapshot),
    hasDiscoveryArtifact: hasDiscoveryArtifact(snapshot),
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
  const pending = mission.pendingOperatorDecision;
  const details = consistencyDetails(mission, snapshot);

  if (pending && pending.stage && stage && pending.stage !== stage) {
    throw missionStateInconsistent(
      'pendingOperatorDecision.stage does not match mission.stage.',
      details
    );
  }

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

  if (!approved && kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL) {
    throw missionStateInconsistent(
      'Prioritization approval is advertised before the mission plan is locked.',
      details
    );
  }

  if (kind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL && stage && stage !== STAGES.DISCOVER) {
    throw missionStateInconsistent(
      'Discovery approval is advertised outside the discover stage.',
      details
    );
  }

  if (kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL && stage && stage !== STAGES.DISCOVER) {
    throw missionStateInconsistent(
      'Prioritization approval is advertised outside the discover stage.',
      details
    );
  }

  if (kind === OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION && stage && stage !== STAGES.DISCOVER) {
    throw missionStateInconsistent(
      'Discovery investigation is advertised outside the discover stage.',
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

  if (kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL && !hasPendingPrioritizationApproval(snapshot)) {
    throw missionStateInconsistent(
      'pendingOperatorDecision.kind is a prioritization approval that the execution engine cannot consume.',
      details
    );
  }

  if (kind === OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION && !hasPendingDiscoveryInvestigation(snapshot)) {
    throw missionStateInconsistent(
      'pendingOperatorDecision.kind is a discovery investigation that the execution engine cannot consume.',
      details
    );
  }

  if (kind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL && !hasPendingExecutionApproval(snapshot)) {
    throw missionStateInconsistent(
      'pendingOperatorDecision.kind is an execution approval that the execution engine cannot consume.',
      details
    );
  }

  if (hasPendingExecutionApproval(snapshot) && kind !== OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL) {
    throw missionStateInconsistent(
      'hasPendingExecutionApproval is true but pendingOperatorDecision does not match.',
      details
    );
  }

  if (
    stage === STAGES.READY &&
    canAdvertiseExecutionApproval(mission, snapshot.contributions || contributionsFrom(snapshot, extras), extras)
    && kind !== OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
  ) {
    throw missionStateInconsistent(
      'Prepared outreach is ready but execution approval is not pending.',
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

  if (hasPendingPrioritizationApproval(snapshot) && kind !== OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL) {
    throw missionStateInconsistent(
      'hasPendingPrioritizationApproval is true but pendingOperatorDecision does not match.',
      details
    );
  }

  if (hasPendingDiscoveryInvestigation(snapshot) && kind !== OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION) {
    throw missionStateInconsistent(
      'hasPendingDiscoveryInvestigation is true but pendingOperatorDecision does not match.',
      details
    );
  }

  if (hasPendingPlanClarification(snapshot) && kind !== OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION) {
    throw missionStateInconsistent(
      'hasPendingPlanClarification is true but pendingOperatorDecision does not match.',
      details
    );
  }

  if (
    stage === STAGES.DISCOVER &&
    isStructuredMissionApproved(mission) &&
    hasDiscoveryArtifact(snapshot)
  ) {
    const consumed = hasConsumedPrioritizationApproval(snapshot);
    if (
      !consumed
      && kind !== OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL
      && kind !== OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION
    ) {
      throw missionStateInconsistent(
        'Discovery artifact exists but post-discovery operator decision is not pending.',
        details
      );
    }
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
  OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
  OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
]);

/**
 * Leaving Discover makes plan/discovery/prioritization decisions unconsumable.
 * Prefer applyStageTransition — this helper remains for legacy callers.
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
  hasDiscoveryArtifact,
  hasPendingPlanClarification,
  hasPendingPlanApproval,
  hasPendingDiscoveryApproval,
  hasPendingDiscoveryInvestigation,
  hasPendingPrioritizationApproval,
  hasPendingExecutionApproval,
  hasConsumedExecutionApproval,
  hasConsumedPrioritizationApproval,
  hasConsumablePendingDecision,
  presentableOperatorDecision,
  pendingKind,
  assertMissionStateConsistent,
  missionStateInconsistent,
  isMissionStateInconsistent,
  applyStageToPendingDecision,
};
