'use strict';

const { EPISTEMIC } = require('./types');
const { EXPECTATION_STATUS } = require('../stateIngestion/types');

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

function activitySince(prospect, sinceIso) {
  const activities = prospect?.activities || [];
  if (!sinceIso) return activities;
  const since = new Date(sinceIso).getTime();
  return activities.filter(a => new Date(a.at || a.created_at || 0).getTime() >= since);
}

/**
 * Build minimum relevant canonical context for a decision trigger.
 * @param {object} input
 * @param {object} input.stateStore - MemoryStateStore or compatible
 * @param {object} input.trigger
 * @param {object} [input.expectation]
 * @param {object} [input.prospect]
 */
async function buildUnderstanding({ stateStore, trigger, expectation, prospect: prospectInput }) {
  const users = stateStore.users || [];
  let prospect = prospectInput;
  let expectationRow = expectation;

  if (expectationRow && !prospect) {
    prospect = await stateStore.readProspect(expectationRow.prospect_id);
  }
  if (!prospect && trigger?.payload?.prospect_id) {
    prospect = await stateStore.readProspect(trigger.payload.prospect_id);
  }

  const ownerId = prospect?.assigned_ao_id || expectationRow?.ao_id || null;
  const owner = ownerId ? findUser(users, ownerId) : null;
  const rel = relationshipFlags(prospect);

  const windowEnd = expectationRow?.expected_window?.ends_at
    || expectationRow?.expected_window?.end
    || null;

  const subsequent = activitySince(prospect, windowEnd);
  const hasResolvingActivity = subsequent.some(a =>
    ['inbound_call', 'meeting_scheduled', 'call_completed', 'reply_received'].includes(a.kind)
  );

  const epistemic = {
    account_owner: owner ? EPISTEMIC.KNOWN : ownerId ? EPISTEMIC.INFERRED : EPISTEMIC.UNKNOWN,
    expectation_window: windowEnd ? EPISTEMIC.KNOWN : EPISTEMIC.UNKNOWN,
    subsequent_activity: hasResolvingActivity
      ? EPISTEMIC.KNOWN
      : (subsequent.length === 0 && windowEnd ? EPISTEMIC.KNOWN : EPISTEMIC.UNKNOWN),
    relationship_state: rel.relationship_active ? EPISTEMIC.KNOWN : EPISTEMIC.INFERRED,
  };

  if (stateStore.conflicts?.length && prospect) {
    const conflict = stateStore.conflicts.find(c => c.entity_id === prospect.id);
    if (conflict) epistemic.ownership = EPISTEMIC.CONFLICTING;
  }

  let assignedAo = null;
  if (ownerId && stateStore.loadAssignedAoContext) {
    assignedAo = await stateStore.loadAssignedAoContext(ownerId);
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
      company_name: prospect?.company_name || expectationRow?.source_evidence?.account_name || null,
      owner_id: ownerId,
      owner_name: owner?.name || expectationRow?.source_evidence?.ao_name || null,
      assignedAo,
    },
    relationship: rel,
    expectation: expectationRow
      ? {
        id: expectationRow.id,
        type: expectationRow.expectation_type,
        expectation_type: expectationRow.expectation_type,
        status: expectationRow.status,
        expected_window: expectationRow.expected_window || {},
        description: expectationRow.description,
        source_evidence: expectationRow.source_evidence || {},
      }
      : null,
    subsequent_activity_count: subsequent.length,
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
    count: subsequent.length,
    epistemic: EPISTEMIC.KNOWN,
  });

  return { snapshot, supportingEvidence, prospect, expectation: expectationRow, owner };
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
