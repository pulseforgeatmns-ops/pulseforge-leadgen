'use strict';

/**
 * Operator Inbox validation (SPEC-037 / ADR-024).
 */

const { INBOX_STATUS, ACTIVE_STATUSES, OPERATOR_ACTIONS } = require('./types');

/**
 * Inbox must never claim to process workflows.
 * @param {object} inputs
 * @returns {{ ok: boolean, errors: string[] }}
 */
function validateCoordinationOnly(inputs) {
  const errors = [];
  if (!inputs || typeof inputs !== 'object') return { ok: true, errors };
  if (inputs.executeWorkflow === true || inputs.runCapability) {
    errors.push('inbox_must_not_perform_workflow');
  }
  if (inputs.printCampaign === true || inputs.assembleMail === true) {
    errors.push('inbox_must_not_perform_workflow');
  }
  return { ok: errors.length === 0, errors };
}

/**
 * @param {object} item
 * @param {string} action
 * @returns {{ ok: boolean, errors: string[] }}
 */
function validateInboxAction(item, action) {
  const errors = [];
  if (!item) return { ok: false, errors: ['inbox_item_not_found'] };
  const a = String(action || '').toLowerCase();
  if (!OPERATOR_ACTIONS.includes(a) && a !== 'ingest') {
    errors.push('unknown_inbox_action');
  }
  if (
    (a === 'complete' ||
      a === 'approve' ||
      a === 'reject' ||
      a === 'open' ||
      a === 'review' ||
      a === 'snooze' ||
      a === 'assign') &&
    !ACTIVE_STATUSES.has(item.status) &&
    item.status !== INBOX_STATUS.REJECTED
  ) {
    if (item.status === INBOX_STATUS.COMPLETED || item.status === INBOX_STATUS.ARCHIVED) {
      errors.push('inbox_item_not_active');
    }
  }
  if (a === 'snooze' && !item && false) {
    errors.push('snooze_requires_until');
  }
  return { ok: errors.length === 0, errors };
}

module.exports = {
  validateCoordinationOnly,
  validateInboxAction,
};
