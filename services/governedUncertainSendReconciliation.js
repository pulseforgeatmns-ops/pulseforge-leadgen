'use strict';

const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { GovernedOutboundStore } = require('./governedOutboundStore');
const { withTenantOutreachExecutorLock } = require('./tenantOutreachExecutorLock');
const {
  PRE_PROVIDER_FAILURE_CODES,
  isRetryablePreProviderScheduleRejection,
} = require('./governedOutboundProviderBoundary');

const PRE_PROVIDER_UNCERTAIN_REASONS = new Set([
  'ak_observed_provenance_required',
  'governed_paige_lineage_missing',
  'governed_paige_lineage_revision_mismatch',
  'governed_paige_lineage_copy_mismatch',
  'emmett_capacity_exhausted',
  'emmett_envelope_missing',
  'emmett_envelope_expired',
  'emmett_outside_send_window',
  'emmett_governor_halted',
  'emmett_governor_pause',
  'emmett_governor_emergency',
  'dispatch_unavailable_now',
]);

async function loadItemWithEnvelope(pool, tenantId, itemId) {
  const row = (await pool.query(
    `SELECT i.*, i.attempted_at::text AS attempt_token, e.mission_id, e.local_day, e.program_id
       FROM acquisition_outbound_items i
       JOIN acquisition_outbound_envelopes e ON e.id = i.envelope_id AND e.tenant_id = i.tenant_id
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
    `SELECT id, status, outbound_message_id, prospect_id, mission_id, skip_reason,
            failure_code, failure_message, executed_at, created_at
       FROM tenant_outreach_scheduled_sends
      WHERE tenant_id = $1
        AND (prospect_id = $2 OR lower(recipient_email) = $3)
      ORDER BY (status IN ('SCHEDULED','EXECUTING','SENT') OR outbound_message_id IS NOT NULL) DESC,
               created_at DESC
      LIMIT 20`,
    [tenantId, prospectId, email],
  )).rows;

  const mailboxMessages = (await pool.query(
    `SELECT id, status, provider_message_id, rfc_message_id, sent_at, prospect_id
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND direction = 'OUTBOUND'
        AND (prospect_id = $2 OR lower(recipients::text) LIKE $3)
      ORDER BY (provider_message_id IS NOT NULL OR rfc_message_id IS NOT NULL OR sent_at IS NOT NULL) DESC,
               created_at DESC
      LIMIT 20`,
    [tenantId, prospectId, `%${email}%`],
  )).rows;

  const executions = (await pool.query(
    `SELECT id, status, provider_message_id, sent_at, mission_id, provider_error_code
       FROM acquisition_mission_outbound_executions
      WHERE tenant_id = $1
        AND (prospect_id = $2 OR lower(payload->>'email') = $3)
      ORDER BY (status = 'sent' OR provider_message_id IS NOT NULL OR sent_at IS NOT NULL) DESC,
               attempted_at DESC NULLS LAST
      LIMIT 20`,
    [tenantId, prospectId, email],
  )).rows;

  const events = (await pool.query(
    `SELECT event_type, payload, created_at
       FROM acquisition_outbound_events
      WHERE tenant_id = $1 AND item_id = $2
      ORDER BY (COALESCE(payload->>'providerMessageId',payload->>'provider_message_id','') <> ''
        OR payload->>'providerOutcome' = 'PROVIDER_CONFIRMED_SENT'
        OR payload->>'providerBoundaryCrossed' = 'true') DESC NULLS LAST, created_at DESC
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
  // An ambiguous provider result cannot be cleared using an unrelated failure
  // elsewhere in the tenant's one-hour diagnostic window. Missing local receipt
  // rows are not proof that the provider did not accept this attempt.
  if (['provider_or_persistence_error', 'provider_acceptance_unknown'].includes(reason)) return null;
  // Item IDs are reused across retries. A reason string or same-item historical
  // event cannot prove this attempt stayed before the provider boundary. Only
  // exact current-attempt evidence is eligible; legacy unversioned events hold.
  if (!item.attempt_token) return null;
  for (const row of [...evidence.tickBlocks, ...evidence.events]) {
    if (String(row.payload?.itemId || '') !== String(item.id)
      || String(row.payload?.envelopeId || '') !== String(item.envelope_id)
      || row.payload?.attemptToken !== item.attempt_token
      || row.payload?.providerBoundaryCrossed !== false) continue;
    const code = String(row.payload?.reason || '').trim();
    if (PRE_PROVIDER_UNCERTAIN_REASONS.has(code) || PRE_PROVIDER_FAILURE_CODES.has(code)) return code;
  }
  return null;
}

function classifyProvenUnsent(item, evidence) {
  const terminalPreProvider = item?.status === 'suppressed'
    && isRetryablePreProviderScheduleRejection(item.reason);
  if (!item || (!['uncertain', 'attempted'].includes(item.status) && !terminalPreProvider)) {
    return { outcome: 'SKIP', reason: 'item_not_reconcilable' };
  }
  if (item.provider_message_id) {
    return { outcome: 'SKIP', reason: 'provider_message_id_present' };
  }
  if (evidence.events.some((row) => row.payload?.providerMessageId
    || row.payload?.provider_message_id
    || row.payload?.providerOutcome === 'PROVIDER_CONFIRMED_SENT')) {
    return { outcome: 'SKIP', reason: 'item_event_provider_receipt' };
  }
  if (evidence.events.some((row) => row.payload?.providerBoundaryCrossed === true)) {
    return { outcome: 'UNKNOWN', reason: 'item_event_provider_boundary_crossed' };
  }
  const sentSchedule = evidence.schedules.find((row) => String(row.status) === 'SENT' || row.outbound_message_id);
  if (sentSchedule) return { outcome: 'SKIP', reason: 'schedule_shows_send_path' };

  const durableSchedule = evidence.schedules.find((row) => ['SCHEDULED', 'EXECUTING'].includes(String(row.status)));
  if (durableSchedule) return { outcome: 'SKIP', reason: 'spec252_schedule_exists' };

  if (terminalPreProvider) {
    const matchingSkip = evidence.schedules.find((row) =>
      String(row.status) === 'SKIPPED'
      && String(row.skip_reason || '') === String(item.reason || '')
      && !row.outbound_message_id
    );
    if (!matchingSkip) return { outcome: 'UNKNOWN', reason: 'pre_provider_schedule_rejection_not_proven' };
  }

  const sentMailbox = evidence.mailboxMessages.find((row) => row.provider_message_id || row.rfc_message_id || row.sent_at);
  if (sentMailbox) return { outcome: 'SKIP', reason: 'mailbox_message_sent' };

  const sentExecution = evidence.executions.find((row) => row.status === 'sent' || row.provider_message_id || row.sent_at);
  if (sentExecution) return { outcome: 'SKIP', reason: 'canonical_execution_sent' };

  const preProviderCode = extractPreProviderFailureCode(item, evidence);
  if (!preProviderCode) {
    return { outcome: 'UNKNOWN', reason: 'pre_provider_failure_not_proven' };
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
  const store = opts.dryRun ? null : new GovernedOutboundStore(pool, tenantId);
  async function inspectAndReconcile() {
    // Apply reads fresh evidence only after acquiring the tenant's send lock.
    // Inspection deliberately avoids the lock and every mutation path.
    const item = await loadItemWithEnvelope(pool, tenantId, itemId);
    if (!item) return { itemId, skipped: true, reason: 'item_not_found', applied: false, retryAllowed: false };
    if (item.status === 'pending' && item.reason === 'reconciled_not_sent') {
      return { itemId, skipped: true, reason: 'already_reconciled', applied: false, retryAllowed: false };
    }
    try {
      await require('./anchorEmailRecoveryIntegration').assertNotQuarantinedWhenInstalled(pool, {
        tenantId: String(tenantId), itemId: item.id, prospectId: item.prospect_id,
        companyId: item.company_id, email: item.email,
      });
    } catch (error) {
      // Quarantine is a permanent operational hold, not proof of non-delivery.
      // No diagnostic write or reset is permitted on this path, including dry-run.
      return { itemId, classification: { outcome: 'UNKNOWN', reason: error.code || 'recovery_guard_unavailable' },
        applied: false, retryAllowed: false, skipped: true, dryRun: Boolean(opts.dryRun) };
    }
    const evidence = await gatherUncertainSendEvidence(pool, item);
    const classification = classifyProvenUnsent(item, evidence);
    // Inspection must return before every write, including diagnostic evidence.
    if (opts.dryRun) {
      return { itemId, classification, applied: false, dryRun: true,
        ...(classification.outcome !== 'PROVEN_UNSENT' ? { skipped: true } : {}) };
    }
    await persistReconciliationEvidence(pool, item, classification);
    if (classification.outcome !== 'PROVEN_UNSENT') {
      return { itemId, skipped: true, classification, applied: false, retryAllowed: false };
    }
    const released = await store.releaseUnsent(item, 'reconciled_not_sent', {
      reconciled: true,
      evidence: JSON.stringify(classification.evidenceSummary || {}),
      actor: 'canonical_evidence_reconciler',
      providerOutcome: classification.providerOutcome,
      canonicalPreProviderRejection: true,
      missionId: item.mission_id,
      expectedAttempt: { status: item.status, reason: item.reason, attemptedAt: item.attempt_token },
    });
    return {
      itemId,
      classification,
      applied: true,
      status: released.status,
      retryAllowed: true,
    };
  }
  if (opts.dryRun) return inspectAndReconcile();
  const result = await store.lock(() => withTenantOutreachExecutorLock(pool, inspectAndReconcile));
  if (result?.halted) {
    return { itemId, skipped: true, reason: result.halted, halted: result.halted, applied: false, retryAllowed: false };
  }
  return result;
}

module.exports = {
  gatherUncertainSendEvidence,
  classifyProvenUnsent,
  reconcileUncertainItemFromEvidence,
};
