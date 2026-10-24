'use strict';

/**
 * SPEC-157 — Autonomous Discovery Approval Policy (ADR-074 companion).
 * When executionPolicy = autonomous, consumable operator decisions that do not
 * require human judgment under the mission contract are auto-consumed.
 */

const { OPERATOR_DECISION_KINDS } = require('./types');
const { hasPendingDiscoveryApproval } = require('./PendingOperatorDecision');
const { MISSION_STAGE_CONTRACTS, PROGRESSION_STAGES } = require('./MissionProgression');

const AUTONOMOUS_EXECUTION_POLICY = 'autonomous';

function discoveryRequiresOperatorJudgment(snapshot = {}) {
  const mission = snapshot.mission || snapshot;
  const contract = MISSION_STAGE_CONTRACTS[PROGRESSION_STAGES.DISCOVERY];
  if (contract && contract.requiresHumanDecision) {
    return true;
  }
  const structured = mission && mission.structuredMission;
  const execution = structured && structured.execution;
  if (execution && execution.requireDiscoveryApproval === true) {
    return true;
  }
  return false;
}

function operatorDecisionRequiresHuman(kind, snapshot = {}) {
  if (!kind) return false;
  if (kind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL) {
    return discoveryRequiresOperatorJudgment(snapshot);
  }
  if (kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL) {
    const contract = MISSION_STAGE_CONTRACTS[PROGRESSION_STAGES.DISCOVERY_REVIEW];
    return contract ? contract.requiresHumanDecision !== false : true;
  }
  return true;
}

function shouldAutoConsumeDiscoveryApproval(snapshot = {}, executionPolicy = null) {
  if (!hasPendingDiscoveryApproval(snapshot)) return false;
  if (executionPolicy !== AUTONOMOUS_EXECUTION_POLICY) return false;
  if (discoveryRequiresOperatorJudgment(snapshot)) return false;
  return true;
}

module.exports = {
  AUTONOMOUS_EXECUTION_POLICY,
  discoveryRequiresOperatorJudgment,
  operatorDecisionRequiresHuman,
  shouldAutoConsumeDiscoveryApproval,
};
