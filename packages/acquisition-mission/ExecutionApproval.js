'use strict';

/**
 * Canonical READY transition & artifact-bound execution approval.
 * Approval authorizes the exact prepared Paige + Emmett + Max state at READY.
 */

const crypto = require('crypto');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  asText,
  nowIso,
} = require('./types');

const EXECUTION_APPROVAL_ACTION = 'execution_approved';

function findLatestContribution(contributions = [], specialist, kind) {
  return [...contributions]
    .reverse()
    .find((row) => row.specialist === specialist && row.kind === kind) || null;
}

function findMaxPrioritization(contributions = []) {
  return findLatestContribution(contributions, SPECIALISTS.MAX, CONTRIBUTION_KINDS.PRIORITIZATION);
}

function findPaigeVariants(contributions = []) {
  return findLatestContribution(contributions, SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS);
}

function findEmmettCapacity(contributions = []) {
  return findLatestContribution(contributions, SPECIALISTS.EMMETT, CONTRIBUTION_KINDS.CAPACITY);
}

function findLatestScoutDiscovery(contributions = []) {
  return findLatestContribution(contributions, SPECIALISTS.SCOUT, CONTRIBUTION_KINDS.DISCOVERY);
}

/**
 * Deterministic revision from canonical contribution IDs and prepared queue state.
 */
function computePreparedArtifactBinding(missionId, contributions = []) {
  const max = findMaxPrioritization(contributions);
  const paige = findPaigeVariants(contributions);
  const emmett = findEmmettCapacity(contributions);
  const emmettPayload = (emmett && emmett.payload) || {};
  const queue = emmettPayload.queue || {};
  const queueItems = Array.isArray(queue.items) ? queue.items : [];
  const governor = emmettPayload.governor || {};
  const {
    capacitySenderIdentity,
  } = require('../../utils/canonicalSenderIdentity');
  const sender = capacitySenderIdentity(emmettPayload);

  return {
    missionId,
    maxContributionId: max?.id || null,
    paigeContributionId: paige?.id || null,
    emmettContributionId: emmett?.id || null,
    queueCount: queueItems.length,
    queueTargetIds: queueItems
      .slice(0, 25)
      .map((item) => asText(item.prospectId || item.id || item.companyId || item.company))
      .filter(Boolean)
      .sort(),
    governorOutcome: governor.outcome || null,
    // Identity drift in senderEmail/sendingDomain invalidates execution authorization.
    senderEmail: sender.senderEmail || null,
    sendingDomain: sender.sendingDomain || null,
  };
}

function computePreparedArtifactRevision(missionId, contributions = []) {
  const binding = computePreparedArtifactBinding(missionId, contributions);
  return crypto
    .createHash('sha256')
    .update(JSON.stringify(binding))
    .digest('hex')
    .slice(0, 16);
}

function isExecutionApprovalContribution(row) {
  if (!row || row.specialist !== SPECIALISTS.OPERATOR || row.kind !== CONTRIBUTION_KINDS.APPROVAL) {
    return false;
  }
  const payload = row.payload || {};
  return (
    payload.decisionKind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
    || payload.kind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
    || payload.action === EXECUTION_APPROVAL_ACTION
  );
}

function findExecutionApprovals(contributions = []) {
  return contributions.filter(isExecutionApprovalContribution);
}

function findValidExecutionApproval(contributions = [], missionId) {
  const revision = computePreparedArtifactRevision(missionId, contributions);
  const approvals = findExecutionApprovals(contributions);
  for (let i = approvals.length - 1; i >= 0; i -= 1) {
    const approval = approvals[i];
    if (approval.payload && approval.payload.preparedArtifactRevision === revision) {
      return approval;
    }
  }
  return null;
}

function isExecutionApproved(contributions = [], missionId, extras = {}) {
  if (extras.executionApproved === true) return true;
  if (!missionId) return false;
  return Boolean(findValidExecutionApproval(contributions, missionId));
}

function buildExecutionApprovalPayload(mission, contributions = [], input = {}) {
  const binding = computePreparedArtifactBinding(mission.id, contributions);
  const revision = computePreparedArtifactRevision(mission.id, contributions);
  const emmett = findEmmettCapacity(contributions);
  const emmettPayload = (emmett && emmett.payload) || {};
  const queueItems = Array.isArray(emmettPayload.queue?.items) ? emmettPayload.queue.items : [];

  return {
    approved: true,
    consumed: true,
    decisionKind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
    kind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
    action: EXECUTION_APPROVAL_ACTION,
    stage: STAGES.READY,
    missionId: mission.id,
    preparedArtifactRevision: revision,
    maxContributionId: binding.maxContributionId,
    paigeContributionId: binding.paigeContributionId,
    emmettContributionId: binding.emmettContributionId,
    queueCount: binding.queueCount,
    governorOutcome: binding.governorOutcome,
    senderEmail: binding.senderEmail,
    sendingDomain: binding.sendingDomain,
    plannedSendCount: queueItems.length,
    approvedAt: input.approvedAt || nowIso(),
    approvedBy: asText(input.approvedBy || input.operatorId) || 'operator',
    command: asText(input.command || input.question) || null,
    executionRequestId: input.executionRequestId || null,
    transactionId: input.transactionId || null,
  };
}

function buildExecutionReview(mission, contributions = []) {
  const max = findMaxPrioritization(contributions);
  const paige = findPaigeVariants(contributions);
  const emmett = findEmmettCapacity(contributions);
  const scout = findLatestScoutDiscovery(contributions);
  const maxPayload = (max && max.payload) || {};
  const paigePayload = (paige && paige.payload) || {};
  const emmettPayload = (emmett && emmett.payload) || {};
  const scoutPayload = (scout && scout.payload) || {};
  const variant = (Array.isArray(paigePayload.variants) && paigePayload.variants[0]) || {};
  const queue = emmettPayload.queue || {};
  const queueItems = Array.isArray(queue.items) ? queue.items : [];
  const capacity = emmettPayload.capacity || {};
  const deliverability = emmettPayload.deliverability || emmettPayload.warmup || {};
  const governor = emmettPayload.governor || {};
  const rankedTargets = Array.isArray(maxPayload.rankedTargets)
    ? maxPayload.rankedTargets
    : Array.isArray(maxPayload.priorities)
      ? maxPayload.priorities
      : [];

  const targets = rankedTargets.map((row, index) => ({
    rank: row.rank != null ? row.rank : index + 1,
    company: row.name || row.company || row.segment || null,
    priorityReason: row.reason || row.fit || maxPayload.objectiveReason || null,
    buyingSignals: row.signals || scoutPayload.buyingSignals || scoutPayload.signals || [],
    scoutEvidence: scoutPayload.evidence || [],
  }));

  const blockers = [];
  if (governor.outcome === 'pause' || governor.outcome === 'emergency') {
    blockers.push(governor.reason || 'Deliverability governor paused sending.');
  }
  if (deliverability.status === 'warming' || deliverability.warmup === true) {
    blockers.push('Domain warmup in progress.');
  }

  return {
    spec: 'execution_review',
    missionId: mission.id,
    stage: mission.stage,
    preparedArtifactRevision: computePreparedArtifactRevision(mission.id, contributions),
    artifactBinding: computePreparedArtifactBinding(mission.id, contributions),
    targets,
    communication: {
      subject: variant.subject || paigePayload.subjects?.[0] || null,
      body: variant.body || null,
      cta: variant.cta || paigePayload.cta || null,
      selectedVariant: variant.label || 'Primary',
      variantCount: Array.isArray(paigePayload.variants) ? paigePayload.variants.length : 0,
    },
    infrastructure: {
      queue: queueItems,
      safeCapacity: capacity.recommended != null ? capacity.recommended : capacity.available,
      timingRecommendation: emmettPayload.sendRecommendations || [],
      deliverabilityStatus: deliverability.status || deliverability.warmupStatus || null,
      governorOutcome: governor.outcome || null,
      governorReason: governor.reason || null,
      reputationWarnings: emmettPayload.reputationWarnings || deliverability.warnings || [],
    },
    decision: {
      summary: 'Authorize external execution of the prepared outreach queue.',
      plannedSendCount: queueItems.length,
      onApproval: `Up to ${queueItems.length} prepared send(s) become eligible for provider execution.`,
      blockers,
      unknowns: blockers.length ? [] : (queueItems.length ? [] : ['No queue items prepared.']),
    },
  };
}

function buildPendingExecutionDecision(mission, contributions = []) {
  return {
    stage: STAGES.READY,
    kind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
    prompt: 'Authorize external execution of prepared outreach?',
    description: 'Review prepared variants, queue, and deliverability before authorizing send.',
    executionReview: buildExecutionReview(mission, contributions),
  };
}

function canAdvertiseExecutionApproval(mission, contributions = [], extras = {}) {
  if (!mission || mission.stage !== STAGES.READY) return false;
  const by = (specialist, kind) =>
    contributions.some((row) => row.specialist === specialist && row.kind === kind);
  const paigeComplete = by(SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS) || extras.paigeComplete;
  const emmettComplete = by(SPECIALISTS.EMMETT, CONTRIBUTION_KINDS.CAPACITY) || extras.emmettComplete;
  if (!paigeComplete || !emmettComplete) return false;
  const emmett = findEmmettCapacity(contributions);
  const governor = emmett && emmett.payload && emmett.payload.governor;
  const deliverabilityPaused = extras.deliverabilityPaused === true
    || Boolean(governor && (governor.outcome === 'pause' || governor.outcome === 'emergency'));
  if (deliverabilityPaused) return false;
  if (isExecutionApproved(contributions, mission.id, extras)) return false;
  return true;
}

module.exports = {
  EXECUTION_APPROVAL_ACTION,
  findLatestContribution,
  findMaxPrioritization,
  findPaigeVariants,
  findEmmettCapacity,
  computePreparedArtifactBinding,
  computePreparedArtifactRevision,
  isExecutionApprovalContribution,
  findExecutionApprovals,
  findValidExecutionApproval,
  isExecutionApproved,
  buildExecutionApprovalPayload,
  buildExecutionReview,
  buildPendingExecutionDecision,
  canAdvertiseExecutionApproval,
};
