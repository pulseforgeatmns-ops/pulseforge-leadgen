'use strict';

/**
 * Canonical EXECUTE outbound — frozen execution bundle, artifact integrity, idempotency.
 */

const crypto = require('crypto');
const {
  STAGES,
  SPECIALISTS,
  asText,
  nowIso,
  newId,
  amoError,
} = require('./types');
const {
  findValidExecutionApproval,
  findPaigeVariants,
  findEmmettCapacity,
  findMaxPrioritization,
  computePreparedArtifactRevision,
} = require('./ExecutionApproval');
const { unwrapSpecialistPayload } = require('./ContributionSupersession');
const { specialistContext } = require('./Lifecycle');
const {
  BLOCK_CODES,
  normalizeCanonicalSender,
  extractCapacitySenderIdentity,
  assertCapacityMatchesCanonical,
} = require('../../utils/canonicalSenderIdentity');

const EXECUTION_RECORD_STATUS = Object.freeze({
  QUEUED: 'queued',
  ATTEMPTED: 'attempted',
  SENT: 'sent',
  FAILED: 'failed',
  BLOCKED: 'blocked',
});

const GOVERNOR_BLOCK_OUTCOMES = new Set(['pause', 'emergency', 'halt']);

function deriveExecutionIdentity({ missionId, prospectId, preparedArtifactRevision }) {
  const key = [
    asText(missionId),
    asText(prospectId),
    asText(preparedArtifactRevision),
  ].join(':');
  return crypto.createHash('sha256').update(key).digest('hex');
}

function deriveIdempotencyKey(executionIdentity) {
  return `exec_${String(executionIdentity || '').slice(0, 32)}`;
}

function persistableExecutionIdentityError(code) {
  return Object.assign(new Error(code), { code });
}

function ensureExecutionIdentityFields(record = {}) {
  const missionId = asText(record.missionId);
  const prospectId = asText(record.prospectId);
  const preparedArtifactRevision = asText(record.preparedArtifactRevision);
  let executionIdentity = asText(record.executionIdentity) || null;
  if (!executionIdentity && missionId && prospectId && preparedArtifactRevision) {
    executionIdentity = deriveExecutionIdentity({ missionId, prospectId, preparedArtifactRevision });
  }
  return {
    ...record,
    executionIdentity,
    idempotencyKey: asText(record.idempotencyKey) || (executionIdentity ? deriveIdempotencyKey(executionIdentity) : null),
  };
}

function assertPersistableExecutionRecord(record = {}) {
  const filled = ensureExecutionIdentityFields(record);
  if (!asText(filled.id)) throw persistableExecutionIdentityError('execution_record_id_required');
  if (!asText(filled.missionId)) throw persistableExecutionIdentityError('execution_mission_id_required');
  if (!asText(filled.prospectId)) throw persistableExecutionIdentityError('execution_prospect_id_required');
  if (!asText(filled.preparedArtifactRevision)) throw persistableExecutionIdentityError('execution_revision_required');
  if (!asText(filled.status)) throw persistableExecutionIdentityError('execution_status_required');
  if (!asText(filled.executionIdentity)) throw persistableExecutionIdentityError('execution_identity_required');
  return filled;
}

function bindGovernedRefillSend(bundle, refillItem, approvalMeta = {}) {
  const snapshot = refillItem?.snapshot || refillItem || {};
  const prospectId = asText(refillItem?.candidate_id || snapshot.candidateId || snapshot.prospectId);
  const preparedArtifactRevision = asText(
    approvalMeta.preparedArtifactRevision || bundle?.executionApproval?.preparedArtifactRevision || snapshot.revision
  );
  return ensureExecutionIdentityFields({
    prospectId,
    companyId: String(snapshot.companyId || refillItem?.company_id || ''),
    email: String(snapshot.email || refillItem?.email || ''),
    toName: snapshot.toName || null,
    queuePosition: (bundle?.sends || []).length + 1,
    message: snapshot.message,
    status: EXECUTION_RECORD_STATUS.QUEUED,
    blockReason: null,
    missionId: bundle?.missionId,
    preparedArtifactRevision,
  });
}

function resolvePaigeVariant(paigePayload = {}, variantLabelOrOpts = 'Primary') {
  const variants = Array.isArray(paigePayload.variants) ? paigePayload.variants : [];
  const opts = variantLabelOrOpts && typeof variantLabelOrOpts === 'object'
    ? variantLabelOrOpts
    : { variantLabel: variantLabelOrOpts };
  const identityKeys = [
    opts.candidateId,
    opts.companyId,
    opts.placeId,
    opts.id,
  ].map((value) => asText(value)).filter(Boolean);
  if (identityKeys.length) {
    const { findBoundVariant } = require('../max/workspace/EmmettMissionCandidates');
    const bound = findBoundVariant(variants, identityKeys);
    if (bound && asText(bound.subject) && asText(bound.body)) {
      return {
        variantLabel: bound.label || opts.variantLabel || 'Primary',
        subject: bound.subject,
        body: bound.body,
        cta: bound.cta || paigePayload.cta || null,
        candidateId: bound.candidateId || identityKeys[0],
      };
    }
  }
  const label = asText(opts.variantLabel || variantLabelOrOpts) || 'Primary';
  const match = variants.find((row) => asText(row.label) === label) || null;
  if (!match || !asText(match.subject) || !asText(match.body)) return null;
  return {
    variantLabel: match.label || label,
    subject: match.subject,
    body: match.body,
    cta: match.cta || paigePayload.cta || null,
    candidateId: match.candidateId || null,
  };
}

function verifyArtifactRevision(missionId, contributions = [], approval) {
  const currentRevision = computePreparedArtifactRevision(missionId, contributions);
  const approvedRevision = approval?.payload?.preparedArtifactRevision || null;
  if (!approvedRevision) {
    return { ok: false, reason: 'Execution approval is missing preparedArtifactRevision.', currentRevision, approvedRevision };
  }
  if (currentRevision !== approvedRevision) {
    return {
      ok: false,
      reason: 'Prepared artifacts changed since execution approval. Re-approve before sending.',
      currentRevision,
      approvedRevision,
    };
  }
  return { ok: true, currentRevision, approvedRevision };
}

function isGovernorBlocked(emmettPayload = {}) {
  const governor = emmettPayload.governor || {};
  const outcome = asText(governor.outcome).toLowerCase();
  if (GOVERNOR_BLOCK_OUTCOMES.has(outcome) || governor.halt === true) {
    return { blocked: true, reason: governor.reason || `Governor outcome: ${outcome || 'blocked'}` };
  }
  return { blocked: false, reason: null };
}

/**
 * Build canonical frozen execution bundle from approved artifacts.
 * @param {object} input
 * @returns {{ ok: boolean, bundle?: object, blockReason?: string, status?: string }}
 */
function buildExecutionBundle(input = {}) {
  const {
    mission,
    contributions = [],
    approval,
    tenantId,
    resolveProspectAttributes,
    canonicalSender,
    senderIdentity,
  } = input;

  if (!mission?.id) {
    return { ok: false, blockReason: 'Mission is required.', status: EXECUTION_RECORD_STATUS.BLOCKED };
  }

  const ctx = specialistContext(contributions, { missionId: mission.id });
  if (!ctx.executionApproved) {
    return { ok: false, blockReason: 'Execution approval is required.', status: EXECUTION_RECORD_STATUS.BLOCKED };
  }
  if (ctx.deliverabilityPaused) {
    return { ok: false, blockReason: 'Deliverability risk blocks execution.', status: EXECUTION_RECORD_STATUS.BLOCKED };
  }

  const validApproval = approval || findValidExecutionApproval(contributions, mission.id);
  if (!validApproval) {
    return {
      ok: false,
      blockReason: 'No valid execution approval matches current prepared artifacts.',
      status: EXECUTION_RECORD_STATUS.BLOCKED,
    };
  }

  const revisionCheck = verifyArtifactRevision(mission.id, contributions, validApproval);
  if (!revisionCheck.ok) {
    return { ok: false, blockReason: revisionCheck.reason, status: EXECUTION_RECORD_STATUS.BLOCKED };
  }

  const resolvedSender = normalizeCanonicalSender(canonicalSender || senderIdentity);
  if (!resolvedSender.ok) {
    return {
      ok: false,
      blockReason: resolvedSender.blockReason,
      status: EXECUTION_RECORD_STATUS.BLOCKED,
      blockCode: resolvedSender.code || BLOCK_CODES.REQUIRED,
    };
  }

  const max = findMaxPrioritization(contributions);
  const paige = findPaigeVariants(contributions);
  const emmett = findEmmettCapacity(contributions);
  const emmettPayload = unwrapSpecialistPayload(emmett);
  const paigePayload = unwrapSpecialistPayload(paige);
  const capacityIdentity = extractCapacitySenderIdentity(emmettPayload);
  const capacityBind = assertCapacityMatchesCanonical(emmettPayload, resolvedSender.identity);
  if (!capacityBind.ok) {
    return {
      ok: false,
      blockReason: capacityBind.blockReason,
      status: EXECUTION_RECORD_STATUS.BLOCKED,
      blockCode: capacityBind.code,
    };
  }
  const queueItems = Array.isArray(emmettPayload.queue?.items) ? emmettPayload.queue.items : [];

  if (validApproval.payload?.emmettContributionId && emmett?.id !== validApproval.payload.emmettContributionId) {
    return {
      ok: false,
      blockReason: 'Emmett capacity artifact does not match approved execution binding.',
      status: EXECUTION_RECORD_STATUS.BLOCKED,
    };
  }
  if (validApproval.payload?.paigeContributionId && paige?.id !== validApproval.payload.paigeContributionId) {
    return {
      ok: false,
      blockReason: 'Paige variants artifact does not match approved execution binding.',
      status: EXECUTION_RECORD_STATUS.BLOCKED,
    };
  }

  const governorBlock = isGovernorBlocked(emmettPayload);
  if (governorBlock.blocked) {
    return { ok: false, blockReason: governorBlock.reason, status: EXECUTION_RECORD_STATUS.BLOCKED };
  }

  const approvedTargetIds = new Set(
    queueItems.map((item) => asText(item.prospectId || item.id)).filter(Boolean)
  );

  const sends = [];
  for (const item of queueItems) {
    const prospectId = asText(item.prospectId || item.id) || null;
    const companyId = asText(item.companyId || item.company) || null;
    let email = asText(item.email) || null;
    let toName = asText(item.name || item.company) || null;

    if (!email && prospectId && typeof resolveProspectAttributes === 'function') {
      const attrs = resolveProspectAttributes(prospectId, { missionId: mission.id, queueItem: item });
      if (attrs?.email) email = asText(attrs.email);
      if (attrs?.name && !toName) toName = asText(attrs.name);
    }

    const variantLabel = item.paige?.variantLabel || 'Primary';
    const message = resolvePaigeVariant(paigePayload, {
      variantLabel,
      candidateId: item.paige?.candidateId || item.candidateId || item.id,
      companyId: item.companyId,
      placeId: item.placeId,
      id: item.id,
    });

    const governor = {
      outcome: emmettPayload.governor?.outcome || 'proceed',
      reason: emmettPayload.governor?.reason || null,
    };

    let status = EXECUTION_RECORD_STATUS.QUEUED;
    let blockReason = null;

    if (!prospectId || !approvedTargetIds.has(prospectId)) {
      status = EXECUTION_RECORD_STATUS.BLOCKED;
      blockReason = 'Recipient is not in the approved mission queue.';
    } else if (!message) {
      status = EXECUTION_RECORD_STATUS.BLOCKED;
      blockReason = 'No approved Paige copy could be resolved for target.';
    } else if (!email) {
      status = EXECUTION_RECORD_STATUS.BLOCKED;
      blockReason = 'Approved target is missing a deliverable email address.';
    } else if (item.sendable === false || item.dnc === true) {
      status = EXECUTION_RECORD_STATUS.BLOCKED;
      blockReason = item.dnc ? 'Target is do-not-contact.' : 'Target is not sendable per Emmett queue.';
    }

    sends.push({
      prospectId,
      companyId,
      email,
      toName,
      queuePosition: item.position != null ? item.position : sends.length + 1,
      maxPriority: item.maxPriority != null ? item.maxPriority : null,
      message,
      governor,
      timing: {
        recommendedAt: item.recommendedAt || nowIso(),
      },
      status,
      blockReason,
      executionIdentity: prospectId && revisionCheck.approvedRevision
        ? deriveExecutionIdentity({
          missionId: mission.id,
          prospectId,
          preparedArtifactRevision: revisionCheck.approvedRevision,
        })
        : null,
      idempotencyKey: null,
    });
  }

  for (const send of sends) {
    if (send.executionIdentity) {
      send.idempotencyKey = deriveIdempotencyKey(send.executionIdentity);
    }
  }

  const bundle = {
    missionId: mission.id,
    tenantId: tenantId != null ? String(tenantId) : String(mission.tenantId || ''),
    executionApproval: {
      contributionId: validApproval.id,
      preparedArtifactRevision: revisionCheck.approvedRevision,
      approvedAt: validApproval.payload?.approvedAt || validApproval.createdAt || null,
      approvedBy: validApproval.payload?.approvedBy || null,
    },
    preparedArtifacts: {
      maxContributionId: max?.id || validApproval.payload?.maxContributionId || null,
      paigeContributionId: paige?.id || validApproval.payload?.paigeContributionId || null,
      emmettContributionId: emmett?.id || validApproval.payload?.emmettContributionId || null,
    },
    sends,
    capacity: {
      recommended: emmettPayload.capacity?.recommended ?? null,
      remaining: emmettPayload.capacity?.remaining ?? emmettPayload.capacity?.recommended ?? null,
    },
    provider: {
      channel: 'email',
      provider: 'brevo',
      senderIdentity: resolvedSender.identity.senderEmail,
      senderName: resolvedSender.identity.senderName,
      sendingDomain: resolvedSender.identity.sendingDomain,
    },
    senderIdentity: resolvedSender.identity,
    capacitySenderIdentity: capacityIdentity,
  };

  return { ok: true, bundle, currentRevision: revisionCheck.currentRevision };
}

function buildExecutionRecord(input = {}) {
  const at = input.attemptedAt || input.sentAt || nowIso();
  const identified = ensureExecutionIdentityFields(input);
  return {
    id: identified.id || newId('amo_send'),
    missionId: identified.missionId,
    tenantId: identified.tenantId != null ? String(identified.tenantId) : null,
    prospectId: identified.prospectId,
    preparedArtifactRevision: identified.preparedArtifactRevision,
    executionApprovalContributionId: identified.executionApprovalContributionId || null,
    provider: identified.provider || 'brevo',
    providerMessageId: identified.providerMessageId || null,
    status: identified.status || EXECUTION_RECORD_STATUS.QUEUED,
    providerErrorCode: identified.providerErrorCode || null,
    providerErrorMessage: identified.providerErrorMessage || null,
    executionRequestId: identified.executionRequestId || null,
    transactionId: identified.transactionId || null,
    executionIdentity: identified.executionIdentity || null,
    idempotencyKey: identified.idempotencyKey || null,
    attemptedAt: identified.attemptedAt || at,
    sentAt: identified.sentAt || (identified.status === EXECUTION_RECORD_STATUS.SENT ? at : null),
    createdAt: identified.createdAt || at,
    updatedAt: identified.updatedAt || at,
    payload: identified.payload || {},
  };
}

function summarizeExecutionRecords(records = []) {
  const summary = {
    total: records.length,
    queued: 0,
    attempted: 0,
    sent: 0,
    failed: 0,
    blocked: 0,
    complete: false,
  };
  for (const row of records) {
    const status = asText(row.status).toLowerCase();
    if (status === EXECUTION_RECORD_STATUS.SENT) summary.sent += 1;
    else if (status === EXECUTION_RECORD_STATUS.FAILED) summary.failed += 1;
    else if (status === EXECUTION_RECORD_STATUS.BLOCKED) summary.blocked += 1;
    else if (status === EXECUTION_RECORD_STATUS.ATTEMPTED) summary.attempted += 1;
    else summary.queued += 1;
  }
  const terminal = summary.sent + summary.failed + summary.blocked;
  summary.complete = records.length > 0 && terminal === records.length;
  return summary;
}

function findSuccessfulExecutionRecord(records = [], executionIdentity) {
  return records.find(
    (row) => row.executionIdentity === executionIdentity
      && row.status === EXECUTION_RECORD_STATUS.SENT
  ) || null;
}

function outboundExecutionError(code, message) {
  return amoError(code, message);
}

module.exports = {
  EXECUTION_RECORD_STATUS,
  deriveExecutionIdentity,
  deriveIdempotencyKey,
  ensureExecutionIdentityFields,
  assertPersistableExecutionRecord,
  bindGovernedRefillSend,
  resolvePaigeVariant,
  verifyArtifactRevision,
  isGovernorBlocked,
  buildExecutionBundle,
  buildExecutionRecord,
  summarizeExecutionRecords,
  findSuccessfulExecutionRecord,
  outboundExecutionError,
};
