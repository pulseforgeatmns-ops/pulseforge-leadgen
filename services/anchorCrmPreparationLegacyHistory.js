'use strict';

// A legacy-format approval is NOT execution authority. SPEC-252 added cadence
// to the prepared revision; an older consumed approval may consequently leave
// an unrelated READY mission non-executable with no pending decision. Only the
// exact Anchor preparation hydrator may leave that history unloaded. Never
// repair the mission, recognize the old revision as approval, or alter its rows.
const crypto = require('crypto');
const { assertMissionStateConsistent } = require('../packages/acquisition-mission/PendingOperatorDecision');
const { isStructuredMissionApproved } = require('../packages/acquisition-mission/StructuredMission');
const { isUpstreamArtifactChainCoherent } = require('../packages/acquisition-mission/UpstreamArtifactCoherence');
const {
  computePreparedArtifactBinding,
  findValidExecutionApproval,
  isExecutionApprovalContribution,
  canAdvertiseExecutionApproval,
} = require('../packages/acquisition-mission/ExecutionApproval');
const { isSupersededContribution } = require('../packages/acquisition-mission/ContributionSupersession');
const { isRolledBackContribution } = require('../packages/acquisition-mission/CanonicalContributionSelection');

function validAnchorPreparationContext(context, tenantId, preinitializedSchema) {
  return Boolean(preinitializedSchema === true && String(tenantId) === '10'
    && context && typeof context === 'object' && !Array.isArray(context)
    && Object.keys(context).sort().join(',') === 'childMissionId,sourceMissionId'
    && typeof context.sourceMissionId === 'string'
    && context.sourceMissionId.trim() === context.sourceMissionId && context.sourceMissionId.length > 0
    && typeof context.childMissionId === 'string' && /^mission_crm_[a-f0-9]{32}$/.test(context.childMissionId)
    && context.sourceMissionId !== context.childMissionId);
}

function legacyPreparedHistoryMatches({ mission, contributions, context }) {
  if (!validAnchorPreparationContext(context, mission?.tenantId, true)
    || !mission || typeof mission.id !== 'string'
    || mission.id === context.sourceMissionId || mission.id === context.childMissionId
    || mission.stage !== 'ready' || mission.pendingOperatorDecision !== null
    || mission.planCancelled || !isStructuredMissionApproved(mission)
    || !Array.isArray(contributions)
    || !contributions.every(row => row && row.missionId === mission.id)) return false;

  // Use the unmodified current canonical predicates. Other inconsistencies,
  // incomplete chains, current approvals and thrown shape errors are not a
  // compatibility case. The source and newly created child are never omitted.
  if (!isUpstreamArtifactChainCoherent(mission, contributions)
    || !canAdvertiseExecutionApproval(mission, contributions)
    || findValidExecutionApproval(contributions, mission.id)) return false;
  const binding = computePreparedArtifactBinding(mission.id, contributions);
  const missionBinding = computePreparedArtifactBinding(mission.id, contributions, mission);
  if (!binding.maxContributionId || !binding.paigeContributionId || !binding.emmettContributionId
    || JSON.stringify(binding) !== JSON.stringify(missionBinding)) return false;
  const legacyBinding = { ...binding };
  delete legacyBinding.outreachSequenceSteps;
  const legacyRevision = crypto.createHash('sha256').update(JSON.stringify(legacyBinding)).digest('hex').slice(0, 16);
  const approvals = contributions.filter(isExecutionApprovalContribution);
  if (approvals.length !== 1) return false;
  return approvals.every(row => {
    const approval = row.payload;
    return isExecutionApprovalContribution(row) && !isSupersededContribution(row) && !isRolledBackContribution(row)
      && row.invalidated !== true && row.superseded !== true && row.rolledBack !== true && row.commitStatus !== 'rolled_back'
      && approval.approved === true && approval.consumed === true && approval.stage === 'ready'
      && approval.missionId === mission.id && approval.preparedArtifactRevision === legacyRevision
      && approval.maxContributionId === binding.maxContributionId
      && approval.paigeContributionId === binding.paigeContributionId
      && approval.emmettContributionId === binding.emmettContributionId
      && approval.queueCount === binding.queueCount && approval.governorOutcome === binding.governorOutcome;
  });
}

function mayOmitLegacyPreparedHistory({ mission, contributions, error, context, cachedMission }) {
  return cachedMission === null
    && error?.code === 'MISSION_STATE_INCONSISTENT' && error.spec === 'SPEC-136'
    && error.message === 'Prepared outreach is ready but execution approval is not pending.'
    && legacyPreparedHistoryMatches({ mission, contributions, context });
}

function assertNoCachedLegacyPreparedHistory(loaded, store, context) {
  // addContribution deliberately keeps the first row for an ID. Inspect the
  // loaded history before that merge so a cached current-hash row cannot hide
  // a subsequently persisted legacy mismatch. This never refreshes the cache.
  const seen = new Set();
  const contributions = loaded.contributions.filter(row => {
    if (row && row.id) {
      if (seen.has(row.id)) return false;
      seen.add(row.id);
    }
    return Boolean(row);
  });
  for (const mission of loaded.missions) {
    if (!store.getMission(mission.id)) continue;
    const rows = contributions.filter(row => row.missionId === mission.id);
    if (legacyPreparedHistoryMatches({ mission, contributions: rows, context })) {
      // Keep the native exception. A cached record is not safely "unloaded";
      // omit-only compatibility is unavailable and no cache repair is allowed.
      assertMissionStateConsistent(mission, { contributions: rows });
    }
  }
}

module.exports = { validAnchorPreparationContext, mayOmitLegacyPreparedHistory, assertNoCachedLegacyPreparedHistory };
