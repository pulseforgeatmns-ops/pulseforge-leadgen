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
    senderIdentity,
    existingRecords = [],
    persistExecutionRecord,
  } = input;

  const bundleResult = buildExecutionBundle({
    mission,
    contributions,
    approval,
    tenantId,
    resolveProspectAttributes,
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
  const records = [];
  const approvalMeta = bundle.executionApproval;

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
      sender: senderIdentity
        ? { email: senderIdentity, name: process.env.FROM_NAME || 'Pulseforge' }
        : undefined,
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
