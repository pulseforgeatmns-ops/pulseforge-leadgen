'use strict';

/**
 * Canonical outbound execution adapter — executes approved mission bundle via provider transport.
 * Emmett does not regenerate copy or select recipients during EXECUTE.
 */

const { sendEmail: brevoSendEmail } = require('../../providers/brevo/sendEmail');
const {
  buildExecutionBundle,
  buildExecutionRecord,
  findSuccessfulExecutionRecord,
  EXECUTION_RECORD_STATUS,
  summarizeExecutionRecords,
} = require('../../acquisition-mission/OutboundExecution');
const { nowIso } = require('../../acquisition-mission/types');
const {
  BLOCK_CODES,
  resolveCanonicalSenderIdentity,
  normalizeCanonicalSender,
  evaluateCanonicalSenderReadiness,
} = require('../../../utils/canonicalSenderIdentity');

async function resolveExecuteSender(input = {}) {
  if (input.canonicalSender) {
    return normalizeCanonicalSender(input.canonicalSender);
  }
  if (input.senderIdentity && typeof input.senderIdentity === 'object') {
    return normalizeCanonicalSender(input.senderIdentity);
  }
  return resolveCanonicalSenderIdentity({
    tenantId: input.tenantId,
    clientId: input.clientId || input.tenantId,
    client: input.client,
    pool: input.pool,
    loadClient: input.loadClient,
  });
}

/**
 * Execute one canonical approved outbound bundle.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function executeOutboundBundle(input = {}) {
  const {
    mission,
    contributions = [],
    approval,
    tenantId,
    transactionId,
    executionRequestId,
    sendEmail = brevoSendEmail,
    resolveProspectAttributes,
    existingRecords = [],
    persistExecutionRecord,
  } = input;

  const resolvedSender = await resolveExecuteSender(input);
  if (!resolvedSender.ok) {
    return {
      blocked: true,
      blockReason: resolvedSender.blockReason,
      blockCode: resolvedSender.code,
      bundle: null,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  if (input.senderReadiness && input.senderReadiness.ready === false) {
    return {
      blocked: true,
      blockReason: input.senderReadiness.blockReason || 'Canonical sender is not ready to send.',
      blockCode: input.senderReadiness.code || BLOCK_CODES.NOT_READY,
      bundle: null,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  const mustEvaluateProvider = input.requireProviderReadiness === true
    || Boolean(input.brevoState)
    || (Boolean(input.pool) && input.senderReadiness == null && input.requireProviderReadiness !== false);
  if (mustEvaluateProvider && !(input.senderReadiness && input.senderReadiness.ready === true)) {
    const readiness = await evaluateCanonicalSenderReadiness({
      identity: resolvedSender.identity,
      client: input.client,
      brevoState: input.brevoState,
      brevoApiKey: input.brevoApiKey,
      http: input.http,
      pool: input.pool,
    });
    if (!readiness.ready) {
      return {
        blocked: true,
        blockReason: readiness.blockReason || 'Canonical sender failed sending readiness.',
        blockCode: readiness.code || BLOCK_CODES.NOT_READY,
        bundle: null,
        records: [],
        summary: summarizeExecutionRecords([]),
      };
    }
  }

  const bundleResult = buildExecutionBundle({
    mission,
    contributions,
    approval,
    tenantId,
    resolveProspectAttributes,
    canonicalSender: resolvedSender.identity,
  });

  if (!bundleResult.ok) {
    return {
      blocked: true,
      blockReason: bundleResult.blockReason,
      blockCode: bundleResult.blockCode || null,
      bundle: null,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  const { bundle } = bundleResult;
  const records = [];
  const approvalMeta = bundle.executionApproval;
  const explicitSender = {
    email: bundle.provider.senderIdentity,
    name: bundle.provider.senderName,
  };
  if (!explicitSender.email || !explicitSender.name) {
    return {
      blocked: true,
      blockReason: 'Canonical AMO execution requires an explicit provider sender object.',
      blockCode: BLOCK_CODES.REQUIRED,
      bundle,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  for (const send of bundle.sends) {
    if (send.status === EXECUTION_RECORD_STATUS.BLOCKED) {
      const blockedRecord = buildExecutionRecord({
        missionId: bundle.missionId,
        tenantId: bundle.tenantId,
        prospectId: send.prospectId,
        preparedArtifactRevision: approvalMeta.preparedArtifactRevision,
        executionApprovalContributionId: approvalMeta.contributionId,
        provider: bundle.provider.provider,
        status: EXECUTION_RECORD_STATUS.BLOCKED,
        providerErrorCode: 'blocked',
        providerErrorMessage: send.blockReason,
        executionRequestId,
        transactionId,
        executionIdentity: send.executionIdentity,
        idempotencyKey: send.idempotencyKey,
        payload: { blockReason: send.blockReason },
      });
      records.push(blockedRecord);
      if (typeof persistExecutionRecord === 'function') {
        await persistExecutionRecord(blockedRecord);
      }
      continue;
    }

    const priorSuccess = findSuccessfulExecutionRecord(
      [...existingRecords, ...records],
      send.executionIdentity
    );
    if (priorSuccess) {
      records.push({ ...priorSuccess, deduplicated: true });
      continue;
    }

    const attemptedAt = nowIso();
    const pendingRecord = buildExecutionRecord({
      missionId: bundle.missionId,
      tenantId: bundle.tenantId,
      prospectId: send.prospectId,
      preparedArtifactRevision: approvalMeta.preparedArtifactRevision,
      executionApprovalContributionId: approvalMeta.contributionId,
      provider: bundle.provider.provider,
      status: EXECUTION_RECORD_STATUS.ATTEMPTED,
      executionRequestId,
      transactionId,
      executionIdentity: send.executionIdentity,
      idempotencyKey: send.idempotencyKey,
      attemptedAt,
      payload: {
        email: send.email,
        subject: send.message.subject,
        queuePosition: send.queuePosition,
        senderEmail: explicitSender.email,
        sendingDomain: bundle.provider.sendingDomain,
      },
    });
    if (typeof persistExecutionRecord === 'function') {
      await persistExecutionRecord(pendingRecord);
    }

    const providerResult = await sendEmail({
      toEmail: send.email,
      toName: send.toName,
      subject: send.message.subject,
      body: send.message.body,
      tags: [
        `mission:${bundle.missionId}`,
        `prospect:${send.prospectId}`,
        `revision:${approvalMeta.preparedArtifactRevision}`,
      ],
      idempotencyKey: send.idempotencyKey,
      sender: explicitSender,
      requireExplicitSender: true,
    });

    const sentAt = nowIso();
    const finalRecord = buildExecutionRecord({
      ...pendingRecord,
      status: providerResult.success ? EXECUTION_RECORD_STATUS.SENT : EXECUTION_RECORD_STATUS.FAILED,
      providerMessageId: providerResult.providerMessageId || providerResult.messageId || null,
      providerErrorCode: providerResult.providerErrorCode || null,
      providerErrorMessage: providerResult.providerErrorMessage || providerResult.error || null,
      sentAt: providerResult.success ? sentAt : null,
      attemptedAt,
      updatedAt: sentAt,
      payload: {
        ...pendingRecord.payload,
        providerResponse: providerResult.brevoResponse || null,
      },
    });
    records.push(finalRecord);
    if (typeof persistExecutionRecord === 'function') {
      await persistExecutionRecord(finalRecord);
    }
  }

  return {
    blocked: false,
    bundle,
    records,
    summary: summarizeExecutionRecords(records),
  };
}

module.exports = {
  executeOutboundBundle,
  resolveExecuteSender,
};
