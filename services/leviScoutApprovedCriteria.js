'use strict';

const { TENANT_ID, approvedCriteria, PROPOSED_MIN_AMOUNT, PROPOSED_MAX_AMOUNT } = require('./leviScoutProductionContract');
const { tenantCriteria } = require('../packages/max/workspace/ProspectInvestigationPriority');

function fromUnderstanding(understanding, tenantId = TENANT_ID) {
  const parsed = tenantCriteria(understanding, { tenantId: String(tenantId), clientId: Number(tenantId) });
  if (!parsed || !parsed.range) return null;
  const criteria = {
    approved: true,
    source: 'approved_blueprint',
    minAmount: parsed.range.min,
    maxAmount: parsed.range.max,
  };
  return approvedCriteria(criteria) ? criteria : null;
}

function proposedRangeIsNotAuthority() {
  return Object.freeze({
    minAmount: PROPOSED_MIN_AMOUNT,
    maxAmount: PROPOSED_MAX_AMOUNT,
    invented: false,
    authorized: false,
  });
}

async function loadApprovedCriteria({ tenantId = TENANT_ID, loadUnderstanding, understanding } = {}) {
  const resolved = understanding !== undefined
    ? understanding
    : (typeof loadUnderstanding === 'function' ? await loadUnderstanding({ tenantId }) : null);
  return fromUnderstanding(resolved, tenantId);
}

module.exports = {
  fromUnderstanding,
  loadApprovedCriteria,
  proposedRangeIsNotAuthority,
};
