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
const { specialistContext } = require('./Lifecycle');

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

function resolvePaigeVariant(paigePayload = {}, variantLabel = 'Primary') {
  const variants = Array.isArray(paigePayload.variants) ? paigePayload.variants : [];
  const label = asText(variantLabel) || 'Primary';
  const match = variants.find((row) => asText(row.label) === label) || variants[0] || null;
  if (!match || !asText(match.subject) || !asText(match.body)) return null;
  return {
    variantLabel: match.label || label,
    subject: match.subject,
    body: match.body,
    cta: match.cta || paigePayload.cta || null,
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
    // Legacy alias — string-only senderIdentity is no longer sufficient for tenant AMO sends.
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

  const max = findMaxPrioritization(contributions);
  const paige = findPaigeVariants(contributions);
  const emmett = findEmmettCapacity(contributions);
  const emmettPayload = emmett?.payload || {};
  const paigePayload = paige?.payload || {};
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

  const {
    resolveCanonicalSenderIdentity,
    validateCanonicalSenderConfiguration,
    assertCapacitySenderBinding,
  } = require('../../utils/canonicalSenderIdentity');

  let resolvedSender = null;
  if (canonicalSender && typeof canonicalSender === 'object') {
    resolvedSender = resolveCanonicalSenderIdentity({
      tenantId: tenantId != null ? tenantId : mission.tenantId,
      clientId: canonicalSender.clientId != null ? canonicalSender.clientId : tenantId,
      client: {
        id: canonicalSender.clientId != null ? canonicalSender.clientId : tenantId,
        sender_email: canonicalSender.senderEmail,
        sender_name: canonicalSender.senderName,
        sending_domain: canonicalSender.sendingDomain,
      },
    });
  } else if (senderIdentity && typeof senderIdentity === 'object') {
    resolvedSender = resolveCanonicalSenderIdentity({
      tenantId: tenantId != null ? tenantId : mission.tenantId,
      clientId: senderIdentity.clientId != null ? senderIdentity.clientId : tenantId,
      client: {
        id: senderIdentity.clientId != null ? senderIdentity.clientId : tenantId,
        sender_email: senderIdentity.senderEmail || senderIdentity.email,
        sender_name: senderIdentity.senderName || senderIdentity.name,
        sending_domain: senderIdentity.sendingDomain || senderIdentity.domain,
      },
    });
  }

  // No FROM_EMAIL / BREVO_SENDER_* / hello@gopulseforge.com fallback for tenant AMO sends.
  if (!resolvedSender) {
    return {
      ok: false,
      blockReason: 'Canonical sender identity is required for tenant acquisition sends.',
      status: EXECUTION_RECORD_STATUS.BLOCKED,
    };
  }

  const configCheck = validateCanonicalSenderConfiguration(resolvedSender);
  if (!configCheck.ok) {
    return {
      ok: false,
      blockReason: configCheck.reason || 'Canonical sender configuration is incomplete.',
      status: EXECUTION_RECORD_STATUS.BLOCKED,
    };
  }

  const bindingCheck = assertCapacitySenderBinding({
    capacityPayload: emmettPayload,
    canonicalSender: configCheck.identity,
  });
  if (!bindingCheck.ok) {
    return {
      ok: false,
      blockReason: bindingCheck.reason,
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
    const message = resolvePaigeVariant(paigePayload, variantLabel);

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

  const sender = configCheck.identity;
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
      senderIdentity: sender.senderEmail,
      senderName: sender.senderName,
      sendingDomain: sender.sendingDomain,
    },
    canonicalSender: sender,
  };

  return { ok: true, bundle, currentRevision: revisionCheck.currentRevision };
}

function buildExecutionRecord(input = {}) {
  const at = input.attemptedAt || input.sentAt || nowIso();
  return {
    id: input.id || newId('amo_send'),
    missionId: input.missionId,
    tenantId: input.tenantId != null ? String(input.tenantId) : null,
    prospectId: input.prospectId,
    preparedArtifactRevision: input.preparedArtifactRevision,
    executionApprovalContributionId: input.executionApprovalContributionId || null,
    provider: input.provider || 'brevo',
    providerMessageId: input.providerMessageId || null,
    status: input.status || EXECUTION_RECORD_STATUS.QUEUED,
    providerErrorCode: input.providerErrorCode || null,
    providerErrorMessage: input.providerErrorMessage || null,
    executionRequestId: input.executionRequestId || null,
    transactionId: input.transactionId || null,
    executionIdentity: input.executionIdentity || null,
    idempotencyKey: input.idempotencyKey || null,
    attemptedAt: input.attemptedAt || at,
    sentAt: input.sentAt || (input.status === EXECUTION_RECORD_STATUS.SENT ? at : null),
    createdAt: input.createdAt || at,
    updatedAt: at,
    payload: input.payload || {},
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
  resolvePaigeVariant,
  verifyArtifactRevision,
  isGovernorBlocked,
  buildExecutionBundle,
  buildExecutionRecord,
  summarizeExecutionRecords,
  findSuccessfulExecutionRecord,
  outboundExecutionError,
};
