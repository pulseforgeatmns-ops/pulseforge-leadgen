'use strict';

/**
 * SPEC-116 — in-memory Operator Scorecard store.
 * Tenant-scoped. Cross-tenant reads fail closed.
 */

const { clone, asText, nowIso, osiError } = require('./types');

function createMemoryOsiStore(opts = {}) {
  const scorecards = new Map();
  const learning = [];

  function putScorecard(scorecard) {
    const copy = clone(scorecard);
    copy.updatedAt = copy.updatedAt || nowIso();
    scorecards.set(copy.id, copy);
    return clone(copy);
  }

  function getScorecard(id) {
    const found = scorecards.get(asText(id));
    return found ? clone(found) : null;
  }

  function requireScorecard(id) {
    const scorecard = getScorecard(id);
    if (!scorecard) throw osiError('osi_not_found', `Scorecard not found: ${id}`);
    return scorecard;
  }

  function listScorecards(tenantId) {
    const rows = [...scorecards.values()].map(clone);
    if (tenantId == null || tenantId === '') return rows;
    const key = String(tenantId);
    return rows.filter((row) => String(row.tenantId || row.tenant_id || '') === key);
  }

  function getDraft(tenantId) {
    const rows = listScorecards(tenantId)
      .filter((row) => row.status === 'draft' || row.status === 'in_review')
      .sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));
    return rows[0] || null;
  }

  function getApproved(tenantId) {
    const rows = listScorecards(tenantId)
      .filter((row) => row.status === 'approved')
      .sort((a, b) => String(b.approvedAt || b.updatedAt).localeCompare(String(a.approvedAt || a.updatedAt)));
    return rows[0] || null;
  }

  function addLearning(row) {
    learning.push(clone(row));
    return clone(row);
  }

  function listLearning(tenantId) {
    if (tenantId == null || tenantId === '') return learning.map(clone);
    const key = String(tenantId);
    return learning.filter((row) => String(row.tenantId || row.tenant_id || '') === key).map(clone);
  }

  for (const extra of opts.seeds || []) putScorecard(extra);

  return {
    putScorecard,
    getScorecard,
    requireScorecard,
    listScorecards,
    getDraft,
    getApproved,
    addLearning,
    listLearning,
  };
}

module.exports = {
  createMemoryOsiStore,
};
