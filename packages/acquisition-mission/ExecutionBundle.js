'use strict';

/**
 * Frozen execution bundle from artifact-bound EXECUTION_APPROVAL state.
 */

const { asText, amoError } = require('./types');
const {
  computePreparedArtifactRevision,
  findValidExecutionApproval,
  findMaxPrioritization,
  findPaigeVariants,
  findEmmettCapacity,
} = require('./ExecutionApproval');
const { specialistContext } = require('./Lifecycle');

function resolvePaigeVariant(paigePayload = {}, variantLabel = null) {
  const variants = Array.isArray(paigePayload.variants) ? paigePayload.variants : [];
  if (!variants.length) return null;
  if (variantLabel) {
    const match = variants.find((row) => asText(row.label) === asText(variantLabel));
    if (match) return match;
  }
  return variants[0];
}

function resolveQueueItemEmail(item = {}, resolveEmail) {
  if (item.email) return Promise.resolve(String(item.email).trim());
  if (typeof resolveEmail === 'function' && item.prospectId) {
    return resolveEmail(item.prospectId);
  }
  return Promise.resolve(null);
}

/**
 * Verify current artifacts match the approved revision. Fail closed on drift.
 */
function assertArtifactRevisionValid(missionId, contributions = [], approval = null) {
  const currentRevision = computePreparedArtifactRevision(missionId, contributions);
  const validApproval = approval || findValidExecutionApproval(contributions, missionId);
  if (!validApproval) {
    throw amoError(
      'amo_execution_not_approved',
      'Valid artifact-bound execution approval is required.'
    );
  }
  const approvedRevision = validApproval.payload?.preparedArtifactRevision;
  if (!approvedRevision || approvedRevision !== currentRevision) {
    throw amoError(
      'amo_execution_artifact_stale',
      'Prepared artifacts changed since execution approval. Re-authorize before sending.'
    );
  }
  return { currentRevision, approval: validApproval };
}

/**
 * Build canonical frozen execution bundle.
 * @param {object} input
 * @param {object} input.mission
 * @param {object[]} input.contributions
 * @param {object} [input.approval]
 * @param {Function} [input.resolveEmail] - optional CRM email lookup by prospectId only
 * @param {object} [input.senderIdentity]
 * @returns {Promise<object>}
 */
async function buildExecutionBundle(input = {}) {
  const mission = input.mission;
  if (!mission) throw amoError('amo_mission_required', 'Mission is required.');
  const contributions = input.contributions || [];
  const { currentRevision, approval } = assertArtifactRevisionValid(
    mission.id,
    contributions,
    input.approval
  );

  const ctx = specialistContext(contributions, { missionId: mission.id });
  if (!ctx.executionApproved) {
    throw amoError('amo_execution_not_approved', 'Execution is not approved.');
  }

  const maxRow = findMaxPrioritization(contributions);
  const paigeRow = findPaigeVariants(contributions);
  const emmettRow = findEmmettCapacity(contributions);
  if (!maxRow || !paigeRow || !emmettRow) {
    throw amoError('amo_execution_artifacts_missing', 'Max, Paige, and Emmett contributions are required.');
  }

  const emmettPayload = emmettRow.payload || {};
  const paigePayload = paigeRow.payload || {};
  const governor = emmettPayload.governor || {};
  const governorOutcome = asText(governor.outcome).toLowerCase();
  if (governorOutcome === 'pause' || governorOutcome === 'emergency' || governor.halt === true) {
    throw amoError(
      'amo_governor_blocked',
      governor.reason || 'Safe Send Governor blocked outbound execution.'
    );
  }
  if (ctx.deliverabilityPaused) {
    throw amoError('amo_deliverability_paused', 'Deliverability risk blocks execution.');
  }

  const queueItems = Array.isArray(emmettPayload.queue?.items) ? emmettPayload.queue.items : [];
  const capacity = emmettPayload.capacity || {};
  const maxPayload = maxRow.payload || {};
  const rankedByProspect = new Map();
  for (const row of maxPayload.rankedTargets || maxPayload.priorities || []) {
    const pid = row.prospectId || row.id;
    if (pid) rankedByProspect.set(String(pid), row);
  }

  const sends = [];
  for (let index = 0; index < queueItems.length; index += 1) {
    const item = queueItems[index];
    const prospectId = asText(item.prospectId || item.id);
    if (!prospectId) continue;

    const variantLabel = item.paige?.variantLabel || item.variantLabel || 'Primary';
    const variant = resolvePaigeVariant(paigePayload, variantLabel);
    if (!variant || !variant.subject || !variant.body) {
      sends.push({
        prospectId,
        companyId: item.companyId || null,
        email: item.email || null,
        queuePosition: item.position != null ? item.position : index + 1,
        maxPriority: item.maxPriority ?? item.ranking?.priority ?? null,
        status: 'blocked',
        blockReason: 'Paige copy unavailable for approved variant.',
      });
      continue;
    }

    const email = await resolveQueueItemEmail(item, input.resolveEmail);
    if (!email) {
      sends.push({
        prospectId,
        companyId: item.companyId || null,
        email: null,
        queuePosition: item.position != null ? item.position : index + 1,
        maxPriority: item.maxPriority ?? item.ranking?.priority ?? null,
        status: 'blocked',
        blockReason: 'Recipient email unavailable for approved queue target.',
      });
      continue;
    }

    const maxTarget = rankedByProspect.get(prospectId) || {};
    sends.push({
      prospectId,
      companyId: item.companyId || maxTarget.companyId || null,
      email,
      queuePosition: item.position != null ? item.position : index + 1,
      maxPriority: item.maxPriority ?? maxTarget.rank ?? null,
      message: {
        variantLabel: variant.label || variantLabel,
        subject: variant.subject,
        body: variant.body,
        cta: variant.cta || paigePayload.cta || null,
      },
      governor: {
        outcome: governor.outcome || null,
        reason: governor.reason || null,
      },
      timing: {
        recommendedAt: emmettPayload.recommendedAt || emmettRow.at || null,
      },
      status: 'queued',
    });
  }

  const sender = input.senderIdentity || {};
  return {
    missionId: mission.id,
    tenantId: String(mission.tenantId || mission.clientId || ''),
    executionApproval: {
      contributionId: approval.id,
      preparedArtifactRevision: currentRevision,
      approvedAt: approval.payload?.approvedAt || approval.at || null,
      approvedBy: approval.payload?.approvedBy || null,
    },
    preparedArtifacts: {
      maxContributionId: maxRow.id,
      paigeContributionId: paigeRow.id,
      emmettContributionId: emmettRow.id,
    },
    sends,
    capacity: {
      recommended: capacity.recommended != null ? capacity.recommended : null,
      remaining: capacity.remaining != null ? capacity.remaining : capacity.recommended,
    },
    provider: {
      channel: 'email',
      provider: 'brevo',
      senderIdentity: {
        name: sender.name || sender.fromName || null,
        email: sender.email || sender.fromEmail || null,
      },
    },
  };
}

module.exports = {
  assertArtifactRevisionValid,
  buildExecutionBundle,
  resolvePaigeVariant,
};
