'use strict';

/**
 * Mission-bound outbound execution evidence (canonical EXECUTE path).
 */

const crypto = require('crypto');
const { asText, nowIso, newId, clone } = require('./types');

const EXECUTION_RECORD_STATUS = Object.freeze({
  PENDING: 'pending',
  ATTEMPTED: 'attempted',
  SENT: 'sent',
  FAILED: 'failed',
  BLOCKED: 'blocked',
  SKIPPED: 'skipped',
});

function deriveExecutionIdempotencyKey(missionId, prospectId, preparedArtifactRevision) {
  return crypto
    .createHash('sha256')
    .update(`${asText(missionId)}:${asText(prospectId)}:${asText(preparedArtifactRevision)}`)
    .digest('hex')
    .slice(0, 32);
}

function createExecutionRecord(input = {}) {
  const missionId = asText(input.missionId);
  const prospectId = asText(input.prospectId);
  const revision = asText(input.preparedArtifactRevision);
  const idempotencyKey = input.idempotencyKey
    || deriveExecutionIdempotencyKey(missionId, prospectId, revision);

  return {
    id: asText(input.id) || newId('exec'),
    idempotencyKey,
    missionId,
    tenantId: asText(input.tenantId) || null,
    prospectId,
    companyId: input.companyId != null ? asText(input.companyId) : null,
    preparedArtifactRevision: revision,
    executionApprovalContributionId: asText(input.executionApprovalContributionId) || null,
    provider: asText(input.provider) || 'brevo',
    providerMessageId: input.providerMessageId != null ? asText(input.providerMessageId) : null,
    status: asText(input.status) || EXECUTION_RECORD_STATUS.PENDING,
    attemptedAt: input.attemptedAt || null,
    sentAt: input.sentAt || null,
    executionRequestId: asText(input.executionRequestId) || null,
    transactionId: asText(input.transactionId) || null,
    providerErrorCode: input.providerErrorCode != null ? asText(input.providerErrorCode) : null,
    providerErrorMessage: input.providerErrorMessage != null ? String(input.providerErrorMessage) : null,
    queuePosition: input.queuePosition != null ? Number(input.queuePosition) : null,
    subjectArtifactRef: input.subjectArtifactRef || null,
    queueArtifactRef: input.queueArtifactRef || null,
    payload: input.payload && typeof input.payload === 'object' ? clone(input.payload) : {},
    createdAt: input.createdAt || nowIso(),
    updatedAt: input.updatedAt || nowIso(),
  };
}

function isTerminalSendStatus(status) {
  return status === EXECUTION_RECORD_STATUS.SENT
    || status === EXECUTION_RECORD_STATUS.FAILED
    || status === EXECUTION_RECORD_STATUS.BLOCKED;
}

module.exports = {
  EXECUTION_RECORD_STATUS,
  deriveExecutionIdempotencyKey,
  createExecutionRecord,
  isTerminalSendStatus,
};
