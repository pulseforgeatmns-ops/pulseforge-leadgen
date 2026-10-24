'use strict';

/**
 * Operator Approval Rate tracking stub (SPEC-048).
 * Primary quality metric: share of outreach approved without substantive edits.
 */

const ACTIONS = Object.freeze({
  APPROVE_UNCHANGED: 'approve_unchanged',
  APPROVE_WITH_EDITS: 'approve_with_edits',
  REJECT: 'reject',
  REGENERATE: 'regenerate',
});

/** @type {object[]} */
const _events = [];

/**
 * Record an operator review outcome for outreach.
 * @param {object} event
 * @returns {object}
 */
function recordApprovalEvent(event = {}) {
  const row = {
    id: `oar_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`,
    at: new Date().toISOString(),
    action: event.action || ACTIONS.APPROVE_UNCHANGED,
    prospectId: event.prospectId != null ? String(event.prospectId) : null,
    packageId: event.packageId != null ? String(event.packageId) : null,
    missionId: event.missionId != null ? String(event.missionId) : null,
    channel: event.channel || 'direct_mail',
    operatorConfidence:
      event.operatorConfidence != null ? Number(event.operatorConfidence) : null,
    substantiveEdit: Boolean(event.substantiveEdit),
    notes: event.notes != null ? String(event.notes) : '',
  };
  _events.push(row);
  return row;
}

/**
 * @param {object} [filter]
 * @returns {object}
 */
function computeOperatorApprovalRate(filter = {}) {
  let rows = [..._events];
  if (filter.missionId) {
    rows = rows.filter((e) => e.missionId === String(filter.missionId));
  }
  if (filter.channel) {
    rows = rows.filter((e) => e.channel === filter.channel);
  }
  const total = rows.length;
  const unchanged = rows.filter(
    (e) => e.action === ACTIONS.APPROVE_UNCHANGED && !e.substantiveEdit
  ).length;
  const rate = total === 0 ? null : Number((unchanged / total).toFixed(4));
  return {
    total,
    approvedUnchanged: unchanged,
    approvedWithEdits: rows.filter((e) => e.action === ACTIONS.APPROVE_WITH_EDITS)
      .length,
    rejected: rows.filter((e) => e.action === ACTIONS.REJECT).length,
    regenerated: rows.filter((e) => e.action === ACTIONS.REGENERATE).length,
    operatorApprovalRate: rate,
    primaryMetric: 'operator_approval_rate',
  };
}

function listApprovalEvents() {
  return [..._events];
}

function resetApprovalEvents() {
  _events.length = 0;
}

module.exports = {
  ACTIONS,
  recordApprovalEvent,
  computeOperatorApprovalRate,
  listApprovalEvents,
  resetApprovalEvents,
};
