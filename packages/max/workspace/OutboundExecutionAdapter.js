'use strict';

/**
 * Canonical outbound execution adapter — executes approved mission bundle via provider transport.
 * Emmett does not regenerate copy or select recipients during EXECUTE.
 * Provider adapter does not decide tenant identity — sender must be explicit.
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
  evaluateCanonicalSenderReadiness,
} = require('../../../utils/canonicalSenderIdentity');

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
    canonicalSender,
    senderIdentity,
    existingRecords = [],
    persistExecutionRecord,
    brevoState,
    skipProviderReadiness = false,
  } = input;

  const bundleResult = buildExecutionBundle({
    mission,
    contributions,
    approval,
    tenantId,
    resolveProspectAttributes,
    canonicalSender,
    senderIdentity,
  });

  if (!bundleResult.ok) {
    return {
      blocked: true,
      blockReason: bundleResult.blockReason,
      bundle: null,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  const { bundle } = bundleResult;
  const sender = bundle.canonicalSender;
  if (!sender?.senderEmail || !sender?.senderName) {
    return {
      blocked: true,
      blockReason: 'Canonical AMO execution requires an explicit provider sender identity.',
      bundle,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  if (!skipProviderReadiness) {
    const readiness = await evaluateCanonicalSenderReadiness({
      identity: sender,
      client: {
        id: sender.clientId,
        sender_email: sender.senderEmail,
        sender_name: sender.senderName,
        sending_domain: sender.sendingDomain,
      },
      brevoState,
    });
    if (!readiness.sendable) {
      return {
        blocked: true,
        blockReason: readiness.reason || 'Canonical sender is not ready for provider delivery.',
        bundle,
        records: [],
        summary: summarizeExecutionRecords([]),
        readiness,
      };
    }
  }

  const records = [];
  const approvalMeta = bundle.executionApproval;
  const explicitSender = {
    email: sender.senderEmail,
    name: sender.senderName,
  };

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
        sendingDomain: sender.sendingDomain,
      },
    });
    if (typeof persistExecutionRecord === 'function') {
      await persistExecutionRecord(pendingRecord);
    }

    // Omission of explicit sender is an error for canonical AMO — never rely on provider env defaults.
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
};
