'use strict';

/**
 * SPEC-118 — Max answers mission questions from evidence, not opinion.
 * SPEC-121 — Mission-oriented communication with progressive reasoning disclosure.
 * SPEC-122 — Mission inspection precedes durable knowledge retrieval.
 * AUDIT-005 — Delegates to WorkspaceMissionInspection for workspace-owned routing.
 */

const {
  maybeHandleWorkspaceMissionInspection,
  looksLikeAcquisitionMissionQuestion,
  referencesMissionState,
  shouldInspectActiveMission,
  resolveTenantId,
} = require('./WorkspaceMissionInspection');

async function maybeHandleAcquisitionMissionTurn(input = {}) {
  return maybeHandleWorkspaceMissionInspection(input);
}

module.exports = {
  looksLikeAcquisitionMissionQuestion,
  referencesMissionState,
  shouldInspectActiveMission,
  resolveTenantId,
  maybeHandleAcquisitionMissionTurn,
};
