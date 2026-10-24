'use strict';

/**
 * Direct Mail Execution validation gates (SPEC-035 / ADR-022).
 */

const { CAMPAIGN_REVIEW_STATUS } = require('../campaignReview/types');
const { EXECUTION_STATUS } = require('./types');
const { isLockedStatus } = require('./transitions');

/**
 * Require an approved / ready-to-print campaign revision.
 * @param {object} inputs
 * @returns {{ ok: boolean, errors: string[], revision: number|null, approved: boolean }}
 */
function validateApprovedRevision(inputs = {}) {
  const errors = [];
  const campaign = inputs.campaign || null;
  const approvedRevision =
    inputs.approvedRevision != null
      ? inputs.approvedRevision
      : inputs.campaignRevision != null
        ? inputs.campaignRevision
        : campaign && campaign.revision != null
          ? campaign.revision
          : inputs.reviewRevision != null
            ? inputs.reviewRevision
            : null;

  const status =
    (inputs.campaignStatus && String(inputs.campaignStatus)) ||
    (campaign && campaign.status) ||
    (inputs.reviewSummary && inputs.reviewSummary.status) ||
    null;

  const campaignApproved =
    inputs.campaignApproved === true ||
    status === CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT ||
    status === 'approved' ||
    status === EXECUTION_STATUS.READY_TO_PRINT;

  if (!campaignApproved) {
    errors.push('approved_revision_required');
  }

  if (approvedRevision == null || !Number.isFinite(Number(approvedRevision))) {
    // Allow status-only approval when revision is implied as 1 from review
    if (campaignApproved && (inputs.executionPackage || inputs.mailBatch)) {
      // revision can default later
    } else if (!campaignApproved) {
      // already recorded
    } else {
      errors.push('approved_revision_missing');
    }
  }

  const hasExecutionPackage = Boolean(
    inputs.executionPackage ||
      (inputs.priorOutputs && inputs.priorOutputs.executionPackage)
  );
  const packages =
    inputs.packages ||
    (inputs.mailBatch && inputs.mailBatch.packages) ||
    [];
  const hasMailBatch =
    Boolean(inputs.mailBatch) ||
    (Array.isArray(packages) && packages.length > 0);

  if (!hasExecutionPackage && !hasMailBatch) {
    errors.push('execution_artifacts_missing');
  }

  return {
    ok: errors.length === 0,
    errors,
    revision:
      approvedRevision != null && Number.isFinite(Number(approvedRevision))
        ? Number(approvedRevision)
        : campaignApproved
          ? 1
          : null,
    approved: campaignApproved,
  };
}

/**
 * Block mutating pinned artifacts when locked.
 * @param {object} execution
 * @param {object} [attempt]
 * @returns {{ ok: boolean, errors: string[] }}
 */
function validateArtifactMutation(execution, attempt = {}) {
  const errors = [];
  const locked =
    (execution && execution.lock && execution.lock.locked) ||
    isLockedStatus(execution && execution.summary && execution.summary.status);

  if (!locked) return { ok: true, errors: [] };

  if (attempt.replaceRevision === true) {
    errors.push('campaign_revision_locked');
  }
  if (attempt.replaceMailBatch === true) {
    errors.push('mail_package_batch_pinned');
  }
  if (attempt.replaceExecutionPackage === true) {
    errors.push('execution_package_pinned');
  }
  if (attempt.generateContent === true) {
    errors.push('execution_must_not_generate_content');
  }

  return { ok: errors.length === 0, errors };
}

/**
 * @param {string} responseStatus
 * @param {object} RESPONSE_STATUS
 * @returns {boolean}
 */
function isValidResponseStatus(responseStatus, RESPONSE_STATUS) {
  return Object.values(RESPONSE_STATUS).includes(responseStatus);
}

module.exports = {
  validateApprovedRevision,
  validateArtifactMutation,
  isValidResponseStatus,
};
