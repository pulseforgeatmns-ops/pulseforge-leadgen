'use strict';

const { EPISTEMIC } = require('./types');
const { EXPECTATION_STATUS } = require('../stateIngestion/types');
const { resolvesExpectation } = require('./activityEvidence');
const { expectationScope } = require('./expectationScope');

function findUser(users, aoId) {
  return users.find(u => u.id === aoId) || null;
}

function relationshipFlags(prospect) {
  const meta = prospect?.acquisition_metadata?.maxStateIngestion || {};
  return {
    relationship_active: Boolean(prospect?.relationship_active || meta.relationship_active),
    suppress_cold_outreach: Boolean(prospect?.suppress_cold_outreach || meta.suppress_cold_outreach),
    status: prospect?.status || 'unknown',
  };
}

function activitySince(activities, sinceIso, now) {
  const since = new Date(sinceIso).getTime();
  const until = new Date(now).getTime();
  if (!sinceIso || !Number.isFinite(since) || !Number.isFinite(until)) return [];
  return activities.filter(a => {
    const at = new Date(a.at || a.created_at).getTime();
    return Number.isFinite(at) && at >= since && at <= until;
  });
}

/**
 * Build minimum relevant canonical context for a decision trigger.
 * @param {object} input
 * @param {object} input.stateStore - MemoryStateStore or compatible
 * @param {object} input.trigger
 * @param {object} [input.expectation]
 * @param {object} [input.prospect]
 */
async function buildUnderstanding({ stateStore, clientId = stateStore.clientId, trigger, expectation,
  prospect: prospectInput, now = new Date() }) {
  const users = stateStore.users || [];
  let prospect = null;
  const requestedExpectationId = expectation?.id || trigger?.payload?.expectation_id || null;
  let expectationRow = null;
  let contextStatus = 'complete';
  try {
    // Trigger payloads and ingestion snapshots identify a subject; they cannot
    // supply canonical ownership, activity or durable expectation state.
    if (requestedExpectationId) {
      expectationRow = await stateStore.readExpectation(requestedExpectationId, { clientId });
      if (!expectationRow) contextStatus = 'unavailable';
    }
    const prospectId = expectationRow?.prospect_id || prospectInput?.id || trigger?.payload?.prospect_id
      || expectation?.prospect_id;
    if (prospectId) prospect = await stateStore.readProspect(prospectId);
    if ((expectation?.id && trigger?.payload?.expectation_id
      && expectation.id !== trigger.payload.expectation_id)
      || !prospect || String(prospect.client_id) !== String(clientId)
      || (prospectInput?.id && prospectInput.id !== prospect.id)
      || (trigger?.payload?.prospect_id && trigger.payload.prospect_id !== prospect.id)) {
      contextStatus = 'unavailable';
      prospect = null;
    }
  } catch {
    contextStatus = 'unavailable';
  }

  const ownerId = prospect?.assigned_ao_id || null;
  const owner = ownerId ? findUser(users, ownerId) : null;
  const rel = relationshipFlags(prospect);

  const windowEnd = expectationRow?.expected_window?.ends_at
    || expectationRow?.expected_window?.end
    || null;

  const windowStart = expectationRow?.expected_window?.starts_at || expectationRow?.expected_window?.start;
  const validWindow = Boolean(windowEnd && Number.isFinite(new Date(windowEnd).getTime())
    && (!windowStart || (Number.isFinite(new Date(windowStart).getTime())
      && new Date(windowStart) <= new Date(windowEnd))));
  if (expectationRow && (!['OPEN', 'WAITING', 'OVERDUE', 'RESOLVED'].includes(expectationRow.status)
    || !expectationScope(clientId, {
    expectation: expectationRow,
    account: { id: prospect?.id, client_id: prospect?.client_id, owner_id: ownerId },
  }))) contextStatus = 'unavailable';

  let activityRead = { status: 'unavailable', activities: [], source: null };
  if (contextStatus === 'complete' && typeof stateStore.readProspectActivities === 'function') {
    try {
      activityRead = await stateStore.readProspectActivities(prospect.id, { clientId, aoId: ownerId });
    } catch {
      activityRead = { status: 'unavailable', activities: [], source: null };
    }
  }
  const activityComplete = activityRead?.status === 'complete' && Array.isArray(activityRead.activities);
  const subsequent = activityComplete ? activitySince(activityRead.activities, windowEnd, now) : [];
  const resolving = subsequent.filter(a => resolvesExpectation(a, expectationRow));
  const hasResolvingActivity = contextStatus === 'complete' && validWindow && activityComplete && resolving.length > 0;

  const epistemic = {
    account_owner: owner ? EPISTEMIC.KNOWN : ownerId ? EPISTEMIC.INFERRED : EPISTEMIC.UNKNOWN,
    expectation_window: validWindow ? EPISTEMIC.KNOWN : EPISTEMIC.UNKNOWN,
    subsequent_activity: activityComplete && validWindow ? EPISTEMIC.KNOWN : EPISTEMIC.UNKNOWN,
    relationship_state: rel.relationship_active ? EPISTEMIC.KNOWN : EPISTEMIC.INFERRED,
  };

  if (stateStore.conflicts?.length && prospect) {
    const conflict = stateStore.conflicts.find(c => c.entity_id === prospect.id);
    if (conflict) epistemic.ownership = EPISTEMIC.CONFLICTING;
  }

  let assignedAo = null;
  if (contextStatus === 'complete' && ownerId && stateStore.loadAssignedAoContext) {
    try { assignedAo = await stateStore.loadAssignedAoContext(ownerId); } catch {
      contextStatus = 'unavailable';
    }
  } else if (owner) {
    assignedAo = {
      id: owner.id,
      name: owner.name,
      role: 'Acquisition Operator',
      email: owner.email || null,
      phone: null,
      mailboxStatus: 'not_configured',
    };
  }

  const snapshot = {
    account: {
      id: prospect?.id || null,
      client_id: prospect?.client_id ?? null,
      company_name: prospect?.company_name || expectationRow?.source_evidence?.account_name || null,
      owner_id: ownerId,
      owner_name: owner?.name || expectationRow?.source_evidence?.ao_name || null,
      assignedAo,
    },
    relationship: rel,
    expectation: expectationRow
      ? {
        id: expectationRow.id,
        client_id: expectationRow.client_id,
        prospect_id: expectationRow.prospect_id,
        ao_id: expectationRow.ao_id,
        type: expectationRow.expectation_type,
        expectation_type: expectationRow.expectation_type,
        status: expectationRow.status,
        expected_window: expectationRow.expected_window || {},
        description: expectationRow.description,
        source_evidence: expectationRow.source_evidence || {},
      }
      : null,
    context_status: contextStatus,
    activity_read_status: activityComplete ? 'complete' : 'unavailable',
    subsequent_activity_count: activityComplete && validWindow ? subsequent.length : null,
    resolving_activity: resolving,
    has_resolving_activity: hasResolvingActivity,
    epistemic,
    trigger,
  };

  const supportingEvidence = [];
  if (expectationRow?.source_ingestion_id) {
    supportingEvidence.push({
      kind: 'expectation',
      id: expectationRow.id,
      status: expectationRow.status,
      epistemic: EPISTEMIC.KNOWN,
    });
  }
  if (expectationRow?.source_evidence) {
    supportingEvidence.push({
      kind: 'expectation_source',
      payload: expectationRow.source_evidence,
      epistemic: EPISTEMIC.KNOWN,
    });
  }
  if (owner) {
    supportingEvidence.push({
      kind: 'ownership',
      owner_id: owner.id,
      owner_name: owner.name,
      epistemic: EPISTEMIC.KNOWN,
    });
  }
  supportingEvidence.push({
    kind: 'activity_query',
    since: windowEnd,
    count: activityComplete && validWindow ? subsequent.length : null,
    status: activityComplete ? 'complete' : 'unavailable',
    source: activityRead?.source || null,
    client_id: clientId,
    prospect_id: prospect?.id || null,
    ao_id: ownerId,
    until: now.toISOString(),
    epistemic: activityComplete && validWindow ? EPISTEMIC.KNOWN : EPISTEMIC.UNKNOWN,
  });

  supportingEvidence.push(...resolving.map(activity => ({ ...activity, epistemic: EPISTEMIC.KNOWN })));

  return { snapshot, supportingEvidence, prospect, expectation: expectationRow, owner, requestedExpectationId };
}

function expectationStillOpen(expectation, now = new Date()) {
  if (!expectation) return false;
  if (!['OPEN', 'WAITING'].includes(expectation.status)) return false;
  const end = expectation.expected_window?.ends_at || expectation.expected_window?.end;
  if (!end) return true;
  return new Date(end) >= now;
}

function isOverdue(expectation, now = new Date()) {
  if (!expectation) return false;
  if (expectation.status === EXPECTATION_STATUS.OVERDUE) return true;
  if (!['OPEN', 'WAITING'].includes(expectation.status)) return false;
  const end = expectation.expected_window?.ends_at || expectation.expected_window?.end;
  if (!end) return false;
  return new Date(end) < now;
}

module.exports = {
  buildUnderstanding,
  expectationStillOpen,
  isOverdue,
  relationshipFlags,
};
