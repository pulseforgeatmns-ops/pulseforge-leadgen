'use strict';

/**
 * Anchor tenant 10 — Paige copy revision eligibility, customer-send detection,
 * and doctrine validation for regenerateAnchorPaigeCopyRevision.js.
 */

const amo = require('../../packages/acquisition-mission');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  isSupersededContribution,
} = amo;
const {
  canAdvertiseExecutionApproval,
  buildPendingExecutionDecision,
  findValidExecutionApproval,
} = require('../../packages/acquisition-mission/ExecutionApproval');
const { listOutboundExecutionsForMission } = require('../../services/acquisitionMissionOutboundPersistence');
const { loadTenantMissions } = require('../../services/acquisitionMissionPersistence');
const { unwrapContributionPayload } = require('../validateAnchorCanonicalMission');
const {
  validateAnchorCopyDoctrine,
  DOCTRINE_BLOCKER,
  ANCHOR_COPY_OWNER,
} = require('../../utils/anchorCopyDoctrine');

const TENANT_ID = '10';
const CLIENT_ID = 10;

const CUSTOMER_SEND_STATUSES = new Set(['sent']);

function activeContribution(contributions, specialist, kind) {
  const rows = (contributions || []).filter(
    (row) => row.specialist === specialist
      && row.kind === kind
      && !isSupersededContribution(row)
  );
  return rows.at(-1) || null;
}

function activePaigePayload(contributions) {
  const row = activeContribution(contributions, SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS);
  if (!row) return null;
  return unwrapContributionPayload(row.payload || row);
}

function hasPreparedOutreach(contributions) {
  return Boolean(
    activeContribution(contributions, SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS)
    && activeContribution(contributions, SPECIALISTS.EMMETT, CONTRIBUTION_KINDS.CAPACITY)
  );
}

async function detectCustomerSend(missionId, pool) {
  const records = await listOutboundExecutionsForMission(missionId, pool, { skipEnsure: true });
  const sent = (records || []).filter((row) => CUSTOMER_SEND_STATUSES.has(String(row.status || '').toLowerCase()));
  return {
    detected: sent.length > 0,
    records: sent,
    total: (records || []).length,
  };
}

function detectInconsistentApprovalState(mission, contributions) {
  if (!mission || mission.stage !== STAGES.READY) return null;
  if (!hasPreparedOutreach(contributions)) return null;
  if (!canAdvertiseExecutionApproval(mission, contributions)) return null;
  if (mission.pendingOperatorDecision?.kind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL) {
    return null;
  }
  return 'prepared_outreach_ready_but_approval_not_pending';
}

function paigePayloadFailsDoctrine(paigePayload) {
  const variants = Array.isArray(paigePayload?.variants) ? paigePayload.variants : [];
  const violations = [];
  for (const variant of variants) {
    const result = validateAnchorCopyDoctrine({
      subject: variant.subject,
      body: variant.body,
      cta: variant.cta,
    });
    if (!result.ok) {
      violations.push({
        candidateId: variant.candidateId || variant.companyId || variant.variantId || null,
        blocker: result.blocker,
        violations: result.violations,
      });
    }
  }
  return {
    fails: violations.length > 0,
    violations,
  };
}

function validatePaigeVariantsDoctrine(paigePayload) {
  const check = paigePayloadFailsDoctrine(paigePayload);
  return {
    ok: !check.fails,
    blocker: check.fails ? DOCTRINE_BLOCKER : null,
    violations: check.violations,
  };
}

function hasApprovedExecutionInProgress(mission, contributions) {
  if (findValidExecutionApproval(contributions, mission?.id)) return true;
  if (mission?.stage === STAGES.EXECUTE && mission?.executionSummary?.sent > 0) return true;
  return false;
}

/**
 * @returns {import('./anchorPaigeCopyRevision').PaigeCopyRevisionEligibility}
 */
function classifyMissionEligibility({
  mission,
  contributions = [],
  customerSend = { detected: false },
}) {
  const stage = mission?.stage || null;
  const inconsistentReason = detectInconsistentApprovalState(mission, contributions);

  if (customerSend.detected) {
    return {
      status: 'skip_sent',
      reason: 'customer_send_detected',
      message: 'Mission is already in execute and has customer sends. Existing sent copy was not mutated.',
      sentRecords: customerSend.records || [],
    };
  }

  if (inconsistentReason) {
    const paigePayload = activePaigePayload(contributions);
    const doctrine = paigePayload ? paigePayloadFailsDoctrine(paigePayload) : { fails: true };
    const safetyFailures = [];
    if (!hasPreparedOutreach(contributions)) {
      safetyFailures.push('prepared_outreach_missing');
    }
    if (hasApprovedExecutionInProgress(mission, contributions)) {
      safetyFailures.push('approved_execution_in_progress');
    }
    if (!doctrine.fails && paigePayload) {
      safetyFailures.push('copy_already_passes_doctrine');
    }
    if (safetyFailures.length) {
      return {
        status: 'skip_inconsistent_approval',
        reason: 'inconsistent_state_not_safe_to_repair',
        inconsistentReason,
        safetyFailures,
      };
    }
    return {
      status: 'repairable_inconsistent_approval',
      reason: inconsistentReason,
    };
  }

  if (stage === STAGES.READY && hasPreparedOutreach(contributions)) {
    return {
      status: 'ready_revision_allowed',
      reason: 'mission_ready',
    };
  }

  if (stage === STAGES.EXECUTE && hasPreparedOutreach(contributions)) {
    return {
      status: 'execute_revision_allowed',
      reason: 'execute_no_customer_send_detected',
    };
  }

  return {
    status: 'skip_wrong_stage',
    reason: 'unsupported_stage',
    stage,
  };
}

async function listAnchorCandidateMissions(pool, missionIdFilter = null) {
  const loaded = await loadTenantMissions(TENANT_ID, pool);
  const byMission = new Map();
  for (const row of loaded.contributions || []) {
    if (!row?.missionId) continue;
    if (!byMission.has(row.missionId)) byMission.set(row.missionId, []);
    byMission.get(row.missionId).push(row);
  }

  let missions = (loaded.missions || []).filter(
    (row) => row && String(row.tenantId || row.clientId || TENANT_ID) === TENANT_ID
  );
  if (missionIdFilter) {
    missions = missions.filter((row) => row.id === missionIdFilter);
  }

  return missions.map((mission) => ({
    mission,
    contributions: byMission.get(mission.id) || [],
  }));
}

function seedMissionIntoEngine(engine, { mission, contributions }) {
  for (const row of contributions) {
    if (row) engine.store.addContribution(row);
  }
  const repairMission = { ...mission };
  const inconsistent = detectInconsistentApprovalState(repairMission, contributions);
  if (inconsistent) {
    repairMission.pendingOperatorDecision = buildPendingExecutionDecision(repairMission, contributions);
  }
  engine.store.putMission(repairMission);
  return engine.get(mission.id, TENANT_ID);
}

function buildAuditEvent(kind, missionId, extras = {}) {
  return {
    kind: 'audit',
    specialist: SPECIALISTS.OPERATOR,
    label: kind,
    payload: {
      event: kind,
      mission_id: missionId,
      client_id: CLIENT_ID,
      doctrine_owner: ANCHOR_COPY_OWNER,
      ...extras,
    },
    at: new Date().toISOString(),
  };
}

module.exports = {
  TENANT_ID,
  CLIENT_ID,
  DOCTRINE_BLOCKER,
  CUSTOMER_SEND_STATUSES,
  activeContribution,
  activePaigePayload,
  hasPreparedOutreach,
  detectCustomerSend,
  detectInconsistentApprovalState,
  paigePayloadFailsDoctrine,
  validatePaigeVariantsDoctrine,
  classifyMissionEligibility,
  listAnchorCandidateMissions,
  seedMissionIntoEngine,
  buildAuditEvent,
};
