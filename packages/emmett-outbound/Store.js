'use strict';

/**
 * SPEC-117 — in-memory store. Tenant-scoped. Cross-tenant reads fail closed.
 */

const { clone, asText, nowIso, eoiError } = require('./types');

function createMemoryEoiStore(opts = {}) {
  const plans = new Map();
  const snapshots = new Map();
  const acks = [];
  const outcomes = [];
  const learning = [];

  function putPlan(plan) {
    const copy = clone(plan);
    copy.updatedAt = copy.updatedAt || nowIso();
    plans.set(copy.id, copy);
    return clone(copy);
  }

  function getPlan(id) {
    const found = plans.get(asText(id));
    return found ? clone(found) : null;
  }

  function requirePlan(id) {
    const plan = getPlan(id);
    if (!plan) throw eoiError('eoi_plan_not_found', `Send plan not found: ${id}`);
    return plan;
  }

  function listPlans(tenantId) {
    const rows = [...plans.values()].map(clone);
    if (tenantId == null || tenantId === '') return rows;
    const key = String(tenantId);
    return rows.filter((row) => String(row.tenantId || row.clientId || '') === key);
  }

  function getApprovedPlan(tenantId, localDate) {
    return listPlans(tenantId)
      .filter((row) => row.status === 'approved' && (!localDate || row.localDate === localDate))
      .sort((a, b) => String(b.approvedAt || b.updatedAt).localeCompare(String(a.approvedAt || a.updatedAt)))[0] || null;
  }

  function putSnapshot(snapshot) {
    const copy = clone(snapshot);
    snapshots.set(`${copy.tenantId}:${copy.localDate || 'latest'}`, copy);
    return clone(copy);
  }

  function getSnapshot(tenantId, localDate) {
    return snapshots.get(`${tenantId}:${localDate}`) || snapshots.get(`${tenantId}:latest`) || null;
  }

  function addAck(row) {
    acks.push(clone(row));
    return clone(row);
  }

  function addOutcome(row) {
    outcomes.push(clone(row));
    return clone(row);
  }

  function addLearning(row) {
    learning.push(clone(row));
    return clone(row);
  }

  function listOutcomes(tenantId) {
    const key = String(tenantId);
    return outcomes.filter((row) => String(row.tenantId || row.clientId || '') === key).map(clone);
  }

  function listLearning(tenantId) {
    if (tenantId == null || tenantId === '') return learning.map(clone);
    const key = String(tenantId);
    return learning.filter((row) => String(row.tenantId || row.clientId || '') === key).map(clone);
  }

  function listAcks(tenantId) {
    const key = String(tenantId);
    return acks.filter((row) => String(row.tenantId || '') === key).map(clone);
  }

  for (const extra of opts.seeds || []) putPlan(extra);

  return {
    putPlan,
    getPlan,
    requirePlan,
    listPlans,
    getApprovedPlan,
    putSnapshot,
    getSnapshot,
    addAck,
    addOutcome,
    addLearning,
    listOutcomes,
    listLearning,
    listAcks,
  };
}

module.exports = {
  createMemoryEoiStore,
};
