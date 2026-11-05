'use strict';

const { anchorOutreachReviewSql } = require('./anchorOutreachReview');

// Shared baseline for AO working surfaces. Channel-specific permissions
// (including call suppression) remain separate from account visibility.
function aoWorkingEligibilitySql(alias = 'p') {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) throw new Error('Invalid prospect SQL alias');
  return `(
    ${anchorOutreachReviewSql(alias)}
    AND COALESCE(${alias}.do_not_contact, false) = false
    AND COALESCE(${alias}.prospect_motion, '') NOT IN ('SUPPRESS', 'EMAIL_LED')
    AND COALESCE(${alias}.disposition_status, 'active') = 'active'
    AND COALESCE(${alias}.ao_current_status, '') NOT IN ('not_a_fit', 'dead')
    AND COALESCE(${alias}.ao_next_action, '') <> 'disqualify'
    AND NULLIF(TRIM(COALESCE(${alias}.ao_disqualification_reason, '')), '') IS NULL
  )`;
}

module.exports = { aoWorkingEligibilitySql };
