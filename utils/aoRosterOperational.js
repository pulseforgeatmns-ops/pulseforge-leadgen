'use strict';

const AO_REVIEW_NEEDS_REASSIGNMENT = 'needs_reassignment';
const AO_REASSIGNMENT_REASON_AO_INACTIVE = 'ao_inactive';
const AO_REVIEW_TRANSFERRED_INACTIVE = 'transferred_from_inactive_ao';

function normalizeAoOperationalStatus(value) {
  const status = String(value || 'active').trim().toLowerCase();
  if (status === 'paused' || status === 'inactive') return status;
  return 'active';
}

function isAoEligibleForAssignment(userRow) {
  if (!userRow || userRow.active === false) return false;
  return normalizeAoOperationalStatus(userRow.ao_operational_status) === 'active';
}

function sqlEligibleAoUsers(alias = 'u') {
  const a = alias;
  return `AND ${a}.active = true AND COALESCE(${a}.ao_operational_status, 'active') = 'active'`;
}

function isTransferredReviewAccount(row) {
  const bucket = row?.ao_review_bucket || null;
  return bucket === AO_REVIEW_NEEDS_REASSIGNMENT || bucket === AO_REVIEW_TRANSFERRED_INACTIVE;
}

function transferredReviewLabel(bucket) {
  if (bucket === AO_REVIEW_NEEDS_REASSIGNMENT) return 'Needs reassignment';
  if (bucket === AO_REVIEW_TRANSFERRED_INACTIVE) return 'Transferred from inactive AO';
  return null;
}

function shouldExcludeFromTodayQueue(account) {
  if (isTransferredReviewAccount(account)) return true;
  if (account?.ao_paused && isTransferredReviewAccount(account)) return true;
  return false;
}

module.exports = {
  AO_REVIEW_NEEDS_REASSIGNMENT,
  AO_REVIEW_TRANSFERRED_INACTIVE,
  AO_REASSIGNMENT_REASON_AO_INACTIVE,
  normalizeAoOperationalStatus,
  isAoEligibleForAssignment,
  sqlEligibleAoUsers,
  isTransferredReviewAccount,
  transferredReviewLabel,
  shouldExcludeFromTodayQueue,
};
