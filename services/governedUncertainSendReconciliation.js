'use strict';

const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { GovernedOutboundStore } = require('./governedOutboundStore');
const { isPreProviderOutboundFailure } = require('./governedOutboundProviderBoundary');

const PRE_PROVIDER_UNCERTAIN_REASONS = new Set([
  'ak_observed_provenance_required',
  'governed_paige_lineage_missing',
  'governed_paige_lineage_revision_mismatch',
  'governed_paige_lineage_copy_mismatch',
  'provider_or_persistence_error',
]);

async function loadItemWithEnvelope(pool, tenantId, itemId) {
  const row = (await pool.query(
    `SELECT i.*, e.mission_id, e.local_day, e.program_id
       FROM acquisition_outbound_items i
       JOIN acquisition_outbound_envelopes e ON e.id = i.envelope_id
      WHERE i.id = $1 AND i.tenant_id = $2`,
    [itemId, String(tenantId)],
  )).rows[0] || null;
  return row;
}

async function gatherUncertainSendEvidence(pool, item) {
  const tenantId = String(item.tenant_id);
  const prospectId = String(item.prospect_id || item.candidate_id || '');
  const email = String(item.email || '').toLowerCase();
  const missionId = String(item.mission_id || '');

  const schedules = (await pool.query(
    `SELECT id, status, outbound_message_id, prospect_id, mission_id, created_at
       FROM tenant_outreach_scheduled_sends
      WHERE tenant_id = $1
        AND (prospect_id = $2 OR lower(recipient_email) = $3)
      ORDER BY created_at DESC
      LIMIT 20`,
    [tenantId, prospectId, email],
  )).rows;

  const mailboxMessages = (await pool.query(
    `SELECT id, status, provider_message_id, rfc_message_id, sent_at, prospect_id
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND direction = 'OUTBOUND'
        AND (prospect_id = $2 OR lower(recipients::text) LIKE $3)
      ORDER BY created_at DESC
      LIMIT 20`,
    [tenantId, prospectId, `%${email}%`],
  )).rows;

  const executions = (await pool.query(
    `SELECT id, status, provider_message_id, sent_at, attempted_at, mission_id,
            prospect_id, payload, provider_error_code
       FROM acquisition_mission_outbound_executions
      WHERE tenant_id = $1
        AND (prospect_id = $2 OR prospect_id = $4 OR lower(payload->>'email') = $3)
      ORDER BY attempted_at DESC NULLS LAST
      LIMIT 20`,
    [tenantId, prospectId, email, String(item.candidate_id || '')],
  )).rows;

  const events = (await pool.query(
    `SELECT event_type, payload, created_at
       FROM acquisition_outbound_events
      WHERE tenant_id = $1 AND item_id = $2
      ORDER BY created_at DESC
      LIMIT 50`,
    [tenantId, item.id],
  )).rows;

  const emmettReservation = (await pool.query(
    `SELECT id, action, status, payload
       FROM agent_log
      WHERE client_id = $1::int
        AND agent_name = 'emmett'
        AND prospect_id = $2
        AND ran_at >= $3::timestamptz - interval '1 day'
      ORDER BY ran_at DESC
      LIMIT 10`,
    [tenantId, prospectId, item.attempted_at || new Date().toISOString()],
  )).rows;

  const tickBlocks = (await pool.query(
    `SELECT payload, created_at
       FROM acquisition_outbound_events
      WHERE tenant_id = $1
        AND event_type = 'tick_blocked'
        AND created_at >= $2::timestamptz - interval '1 hour'
        AND created_at <= $2::timestamptz + interval '1 hour'
      ORDER BY created_at DESC
      LIMIT 20`,
    [tenantId, item.attempted_at || new Date().toISOString()],
  )).rows;

  return {
    schedules,
    mailboxMessages,
    executions,
    events,
    emmettReservation,
    tickBlocks,
  };
}

function extractPreProviderFailureCode(item, evidence) {
  const reason = String(item.reason || '').trim();
  if (PRE_PROVIDER_UNCERTAIN_REASONS.has(reason)) return reason;
  for (const row of evidence.tickBlocks) {
    const code = String(row.payload?.reason || '').trim();
    if (PRE_PROVIDER_UNCERTAIN_REASONS.has(code) || isPreProviderOutboundFailure({ code })) return code;
  }
  for (const row of evidence.events) {
    const code = String(row.payload?.reason || row.reason || '').trim();
    if (PRE_PROVIDER_UNCERTAIN_REASONS.has(code)) return code;
  }
  return null;
}

function classifyProvenSent(item, evidence) {
  if (!item || !['uncertain', 'attempted'].includes(item.status)) {
    return { outcome: 'SKIP', reason: 'item_not_reconcilable' };
  }
  if (item.status === 'sent' && item.provider_message_id) {
    return { outcome: 'SKIP', reason: 'already_sent' };
  }
  const email = String(item.email || '').toLowerCase();
  const sentExecution = evidence.executions.find((row) => row.status === 'sent'
    && row.provider_message_id
    && (
      String(row.payload?.email || '').toLowerCase() === email
      || String(row.prospect_id) === String(item.candidate_id)
      || String(row.prospect_id) === String(item.prospect_id)
    ));
  if (!sentExecution) {
    return { outcome: 'UNKNOWN', reason: 'canonical_execution_not_sent' };
  }
  return {
    outcome: 'PROVEN_SENT',
    reason: 'canonical_execution_record',
    providerMessageId: sentExecution.provider_message_id,
    providerOutcome: 'PROVIDER_CONFIRMED_SENT',
    evidenceSummary: {
      executionId: sentExecution.id,
      providerMessageId: sentExecution.provider_message_id,
      executionAttemptedAt: sentExecution.attempted_at || sentExecution.sent_at || null,
    },
  };
}

function classifyProvenUnsent(item, evidence) {
  if (!item || !['uncertain', 'attempted'].includes(item.status)) {
    return { outcome: 'SKIP', reason: 'item_not_reconcilable' };
  }
  if (item.provider_message_id) {
    return { outcome: 'SKIP', reason: 'provider_message_id_present' };
  }
  const sentSchedule = evidence.schedules.find((row) => String(row.status) === 'SENT' && row.outbound_message_id);
  if (sentSchedule) return { outcome: 'SKIP', reason: 'schedule_shows_send_path' };

  const durableSchedule = evidence.schedules.find((row) => ['SCHEDULED', 'EXECUTING'].includes(String(row.status)));
  if (durableSchedule) return { outcome: 'SKIP', reason: 'spec252_schedule_exists' };

  const sentMailbox = evidence.mailboxMessages.find((row) => row.status === 'sent' && (row.provider_message_id || row.sent_at));
  if (sentMailbox) return { outcome: 'SKIP', reason: 'mailbox_message_sent' };

  const sentExecution = evidence.executions.find((row) => row.status === 'sent' && row.provider_message_id);
  if (sentExecution) return { outcome: 'SKIP', reason: 'canonical_execution_sent' };

  const preProviderCode = extractPreProviderFailureCode(item, evidence);
  if (!preProviderCode) {
    return { outcome: 'UNKNOWN', reason: 'pre_provider_failure_not_proven' };
  }

  if (evidence.executions.some((row) => row.status === 'sent' || row.provider_message_id)) {
    return { outcome: 'SKIP', reason: 'execution_provider_id' };
  }

  if (evidence.emmettReservation.some((row) => row.action === 'email_sent')) {
    return { outcome: 'SKIP', reason: 'emmett_reservation_present' };
  }

  return {
    outcome: 'PROVEN_UNSENT',
    reason: preProviderCode,
    providerOutcome: 'PROVIDER_CONFIRMED_NOT_SENT',
    evidenceSummary: {
      scheduleCount: evidence.schedules.length,
      mailboxMessageCount: evidence.mailboxMessages.length,
      executionCount: evidence.executions.length,
      emmettReservationCount: evidence.emmettReservation.length,
      preProviderFailureCode: preProviderCode,
    },
  };
}

async function persistReconciliationEvidence(pool, item, classification) {
  const id = hash(['send_evidence_reconciliation', item.id, classification.outcome, classification.reason]);
  await pool.query(
    `INSERT INTO acquisition_outbound_events(id,tenant_id,program_id,envelope_id,item_id,event_type,payload)
     VALUES ($1,$2,$3,$4,$5,'send_evidence_reconciliation',$6)
     ON CONFLICT DO NOTHING`,
    [
      id,
      item.tenant_id,
      item.program_id || null,
      item.envelope_id,
      item.id,
      {
        itemId: item.id,
        outcome: classification.outcome,
        reason: classification.reason,
        providerOutcome: classification.providerOutcome || null,
        evidenceSummary: classification.evidenceSummary || null,
        reconciledBy: 'canonical_evidence_reconciler',
      },
    ],
  );
  return id;
}

async function reconcileUncertainItemFromEvidence(pool, tenantId, itemId, opts = {}) {
  const item = await loadItemWithEnvelope(pool, tenantId, itemId);
  if (!item) return { itemId, skipped: true, reason: 'item_not_found' };
  if (item.status === 'pending' && item.reason === 'reconciled_not_sent') {
    return { itemId, skipped: true, reason: 'already_reconciled' };
  }
  const evidence = await gatherUncertainSendEvidence(pool, item);
  const sentClassification = classifyProvenSent(item, evidence);
  if (sentClassification.outcome === 'PROVEN_SENT') {
    await persistReconciliationEvidence(pool, item, sentClassification);
    if (opts.dryRun) {
      return { itemId, classification: sentClassification, applied: false, dryRun: true };
    }
    const store = new GovernedOutboundStore(pool, tenantId);
    const finalized = await store.lock(async () => {
      const live = await loadItemWithEnvelope(pool, tenantId, itemId);
      if (!live || live.status === 'sent') return { skipped: true, reason: 'already_sent' };
      if (!['uncertain', 'attempted'].includes(live.status)) {
        return { skipped: true, reason: 'item_not_reconcilable' };
      }
      await store.finish(live, 'sent', 'evidence_reconciled', sentClassification.providerMessageId);
      await store.event('send_reconciled', [itemId, 'accepted'], {
        itemId,
        outcome: 'accepted',
        providerMessageId: sentClassification.providerMessageId,
        evidence: JSON.stringify(sentClassification.evidenceSummary || {}),
        actor: 'canonical_evidence_reconciler',
        providerOutcome: sentClassification.providerOutcome,
      });
      return { status: 'sent' };
    });
    if (finalized?.skipped) {
      return { itemId, skipped: true, reason: finalized.reason, classification: sentClassification, applied: false };
    }
    return {
      itemId,
      classification: sentClassification,
      applied: true,
      status: finalized?.status || 'sent',
      retryAllowed: false,
    };
  }

  const classification = classifyProvenUnsent(item, evidence);
  await persistReconciliationEvidence(pool, item, classification);
  if (classification.outcome !== 'PROVEN_UNSENT') {
    return { itemId, skipped: true, classification, applied: false };
  }
  if (opts.dryRun) {
    return { itemId, classification, applied: false, dryRun: true };
  }
  const store = new GovernedOutboundStore(pool, tenantId);
  const released = await store.lock(async () => store.releaseUnsent(item, 'reconciled_not_sent', {
    reconciled: true,
    evidence: JSON.stringify(classification.evidenceSummary || {}),
    actor: 'canonical_evidence_reconciler',
    providerOutcome: classification.providerOutcome,
  }));
  return {
    itemId,
    classification,
    applied: true,
    status: released?.status || null,
    retryAllowed: true,
  };
}

module.exports = {
  gatherUncertainSendEvidence,
  classifyProvenSent,
  classifyProvenUnsent,
  reconcileUncertainItemFromEvidence,
};
