'use strict';

// Reserved for manual existing-AO-help reviews. Either persisted marker fails
// closed if another writer drops/corrupts the other. Presence is protective even
// for false/null/malformed values: this reserved key is not an execution opt-out.
// No ordinary OPERATOR_VISIBLE / MANUAL behavior or old fingerprints change.
const MANUAL_REVIEW_NAMESPACE = 'ao-help-review:';
const MANUAL_REVIEW_PREFIX = `${MANUAL_REVIEW_NAMESPACE}v1:`;
function isManualReviewOnlyAttention(item) {
  const priority = item?.priority;
  return String(item?.dedup_fingerprint || '').startsWith(MANUAL_REVIEW_NAMESPACE)
    || (priority != null && Object.hasOwn(priority, 'manual_review_only'))
    || (Array.isArray(priority) && priority.includes('manual_review_only'))
    || priority === 'manual_review_only';
}
module.exports = { MANUAL_REVIEW_NAMESPACE, MANUAL_REVIEW_PREFIX, isManualReviewOnlyAttention };
