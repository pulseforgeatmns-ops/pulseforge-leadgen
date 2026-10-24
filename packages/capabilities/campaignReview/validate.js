'use strict';

/**
 * Campaign Review validation gates (SPEC-034 / ADR-021).
 */

const {
  PROSPECT_REVIEW_STATUS,
  CAMPAIGN_REVIEW_STATUS,
  DEFAULT_CONFIDENCE_THRESHOLD,
} = require('./types');

/**
 * Validate whether a prospect row may be approved.
 * @param {object} row
 * @param {object} [opts]
 * @returns {{ ok: boolean, errors: string[], status: string }}
 */
function validateProspectForApproval(row = {}, opts = {}) {
  const threshold =
    Number.isFinite(Number(opts.confidenceThreshold))
      ? Number(opts.confidenceThreshold)
      : DEFAULT_CONFIDENCE_THRESHOLD;

  const errors = [];

  if (row.skipped || row.status === PROSPECT_REVIEW_STATUS.SKIPPED) {
    return {
      ok: false,
      errors: ['skipped'],
      status: PROSPECT_REVIEW_STATUS.SKIPPED,
    };
  }

  const company = String(row.company || '').trim();
  const recipient = String(row.recipient || '').trim();
  const address = String(row.address || '').trim();
  const confidence = Number.isFinite(Number(row.confidence))
    ? Number(row.confidence)
    : 0;

  if (!address) errors.push('missing_address');
  if (!company) errors.push('missing_company');
  if (!recipient) errors.push('missing_recipient');

  if (Array.isArray(row.validationErrors) && row.validationErrors.length) {
    for (const e of row.validationErrors) {
      if (e && !errors.includes(String(e))) errors.push(String(e));
    }
  }

  if (row.mailValidationFailed === true) {
    errors.push('validation_failed');
  }

  if (confidence < threshold) {
    errors.push('confidence_below_threshold');
  }

  if (errors.length) {
    return {
      ok: false,
      errors,
      status: PROSPECT_REVIEW_STATUS.BLOCKED,
    };
  }

  return {
    ok: true,
    errors: [],
    status: PROSPECT_REVIEW_STATUS.APPROVED,
  };
}

/**
 * Aggregate queue counts for campaign summary.
 * @param {object[]} rows
 * @returns {{ prospectCount: number, readyCount: number, needsReviewCount: number, blockedCount: number }}
 */
function summarizeQueue(rows = []) {
  let readyCount = 0;
  let needsReviewCount = 0;
  let blockedCount = 0;
  let prospectCount = 0;

  for (const row of rows) {
    if (row.status === PROSPECT_REVIEW_STATUS.SKIPPED) continue;
    prospectCount += 1;
    if (row.status === PROSPECT_REVIEW_STATUS.APPROVED) {
      readyCount += 1;
    } else if (row.status === PROSPECT_REVIEW_STATUS.BLOCKED) {
      blockedCount += 1;
    } else if (row.status === PROSPECT_REVIEW_STATUS.REJECTED) {
      // Rejected still counts toward in-scope prospects needing resolution
      needsReviewCount += 1;
    } else {
      needsReviewCount += 1;
    }
  }

  return { prospectCount, readyCount, needsReviewCount, blockedCount };
}

/**
 * Campaign Ready-to-Print gates (ADR-021).
 * @param {object} workspace
 * @param {object} [opts]
 * @returns {{ ok: boolean, errors: string[], status: string }}
 */
function validateCampaignApproval(workspace = {}, opts = {}) {
  const errors = [];
  const rows = Array.isArray(workspace.queue) ? workspace.queue : [];
  const summary = workspace.summary || {};
  const revision = Number(summary.revision) || 1;
  const activeRevision =
    summary.activeRevision != null ? Number(summary.activeRevision) : revision;

  const required = rows.filter(
    (r) =>
      r.status !== PROSPECT_REVIEW_STATUS.SKIPPED &&
      r.required !== false
  );

  const unapproved = required.filter(
    (r) => r.status !== PROSPECT_REVIEW_STATUS.APPROVED
  );
  if (unapproved.length) {
    errors.push('required_prospects_not_approved');
  }

  const blocking = required.filter(
    (r) =>
      r.status === PROSPECT_REVIEW_STATUS.BLOCKED ||
      (Array.isArray(r.validationErrors) && r.validationErrors.length > 0)
  );
  // Only count blocked/validation when still unapproved — approved rows cleared errors
  const blockingUnapproved = blocking.filter(
    (r) => r.status !== PROSPECT_REVIEW_STATUS.APPROVED
  );
  if (blockingUnapproved.length) {
    errors.push('blocking_validation_errors');
  }

  const mailOk =
    workspace.mailPackageGenerated === true ||
    summary.mailPackageGenerated === true ||
    (workspace.mailBatch && workspace.mailBatch.id) ||
    opts.mailPackageGenerated === true;
  if (!mailOk) {
    errors.push('mail_package_not_generated');
  }

  if (revision !== activeRevision) {
    errors.push('current_revision_not_active');
  }

  if (errors.length) {
    return {
      ok: false,
      errors,
      status: CAMPAIGN_REVIEW_STATUS.BLOCKED,
    };
  }

  return {
    ok: true,
    errors: [],
    status: CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT,
  };
}

module.exports = {
  validateProspectForApproval,
  validateCampaignApproval,
  summarizeQueue,
};
