#!/usr/bin/env node
'use strict';

/**
 * Inspect-only verification that Anchor governed outbound resumed after the
 * 2026-09-29 Sacramento PMG reconciliation. Does not tick, send, or apply.
 *
 *   node scripts/verifyGovernedOutboundResumption.js --confirm-production
 */

require('dotenv').config({ quiet: true });

const fs = require('fs');
const path = require('path');
const { INCIDENT } = require('./reconcileAnchorUncertainSend');

const REMAINING_REFILL_IDS = Object.freeze([
  'daily_b6405a6360afa44baaf23c2a_4',
  'daily_b6405a6360afa44baaf23c2a_5',
  'daily_b6405a6360afa44baaf23c2a_6',
  'daily_b6405a6360afa44baaf23c2a_7',
]);

const OPERATIONAL_HALTS = new Set([
  null,
  'spacing',
  'outside_business_hours',
  'weekend',
  'cap_reached',
  'not_started',
  'provider_rejected',
]);

function atOrAfter(value, iso) {
  return value && +new Date(value) >= +new Date(iso);
}

function evaluateResumption(snapshot) {
  const program = snapshot.program || {};
  const item = snapshot.item || {};
  const events = snapshot.events || [];
  const envelopeItems = snapshot.envelopeItems || [];
  const executions = snapshot.executions || [];
  const uncertainItems = snapshot.uncertainItems || [];
  const reconcileAt = snapshot.reconcileAt
    || events.find(event => event.event_type === 'send_reconciled')?.created_at;
  const afterReconcile = events.filter(event => reconcileAt && atOrAfter(event.created_at, reconcileAt));
  const reasons = afterReconcile.map(event => event.reason || event.payload?.reason || null);
  const remaining = REMAINING_REFILL_IDS.map(id => envelopeItems.find(row => row.id === id) || { id, status: null });
  const refillRetryExecution = executions.find(row =>
    String(row.prospect_id) === INCIDENT.prospectId && row.execution_identity);

  const criteria = {
    notBlockedByUncertainReconciliation: {
      pass: uncertainItems.length === 0
        && program.last_error !== 'uncertain_send_requires_reconciliation'
        && !reasons.includes('uncertain_send_requires_reconciliation'),
      detail: {
        lastError: program.last_error || null,
        uncertainCount: uncertainItems.length,
      },
    },
    emmettGovernorProceedSubjectToNormalGates: {
      pass: program.mode === 'active'
        && OPERATIONAL_HALTS.has(program.last_error || null)
        && program.last_error !== 'emmett_governor_halted'
        && snapshot.dispatchUnavailableNow !== true,
      detail: {
        mode: program.mode || null,
        lastError: program.last_error || null,
        lastTickAt: program.last_tick_at || null,
        emmettCapacity: snapshot.emmettCapacity ?? null,
        dispatchUnavailableNow: snapshot.dispatchUnavailableNow ?? null,
      },
    },
    reconciledItemRetriedOnlyThroughGovernedEligibility: {
      pass: Boolean(reconcileAt)
        && item.status !== 'uncertain'
        && atOrAfter(item.attempted_at, reconcileAt)
        && !snapshot.forceSend,
      detail: {
        status: item.status || null,
        reason: item.reason || null,
        attemptedAt: item.attempted_at || null,
        reconcileAt: reconcileAt || null,
      },
    },
    remainingItemsOnCorrectedPersistencePath: {
      pass: remaining.every(row => row.status === 'pending' && row.reason == null)
        && Boolean(refillRetryExecution?.execution_identity)
        && remaining.every(row => String(row.refill) === 'true' || row.refill === true),
      detail: {
        remaining: remaining.map(row => ({ id: row.id, status: row.status, refill: row.refill || null })),
        liveExecutionIdentity: refillRetryExecution?.execution_identity || null,
      },
    },
    noSqlstate23502: {
      pass: !reasons.includes('23502')
        && afterReconcile.every(event => event.sqlstate !== '23502' && event.payload?.sqlstate !== '23502'),
      detail: { postReconcileReasons: reasons.filter(Boolean) },
    },
    noNewAbandonedOrUncertainSend: {
      pass: afterReconcile.every(event =>
        event.event_type !== 'send_uncertain' && event.reason !== 'abandoned_attempt')
        && item.status !== 'uncertain'
        && item.reason !== 'abandoned_attempt',
      detail: { itemStatus: item.status || null, itemReason: item.reason || null },
    },
    noForceSendOrManualAdvancement: {
      pass: !snapshot.forceSend
        && remaining.every(row => row.status === 'pending' && !row.attempted_at),
      detail: { remainingPending: remaining.every(row => row.status === 'pending') },
    },
  };

  const failed = Object.entries(criteria).filter(([, value]) => !value.pass).map(([name]) => name);
  return {
    verdict: failed.length ? 'FAIL' : 'PASS',
    failed,
    criteria,
    programOperational: program.mode === 'active' && uncertainItems.length === 0,
  };
}

async function loadSnapshot(pool) {
  const program = (await pool.query(
    `SELECT id, tenant_id, mode, last_tick_at, last_error
       FROM acquisition_outbound_programs
      WHERE tenant_id = $1 AND mode <> 'revoked'
      ORDER BY authorized_at DESC LIMIT 1`,
    [INCIDENT.tenantId],
  )).rows[0] || null;
  const item = (await pool.query(
    `SELECT id, status, reason, email, attempted_at, provider_message_id,
            snapshot->>'refill' AS refill, candidate_id, company_id
       FROM acquisition_outbound_items WHERE id = $1`,
    [INCIDENT.itemId],
  )).rows[0] || null;
  const envelopeItems = (await pool.query(
    `SELECT id, status, reason, email, attempted_at, provider_message_id,
            snapshot->>'refill' AS refill
       FROM acquisition_outbound_items WHERE envelope_id = $1 ORDER BY id`,
    [INCIDENT.envelopeId],
  )).rows;
  const events = (await pool.query(
    `SELECT created_at, event_type, item_id,
            payload->>'reason' AS reason,
            payload->>'outcome' AS outcome,
            payload->>'providerOutcome' AS provider_outcome,
            payload->>'sqlstate' AS sqlstate,
            payload
       FROM acquisition_outbound_events
      WHERE tenant_id = $1 AND created_at >= $2
      ORDER BY created_at`,
    [INCIDENT.tenantId, INCIDENT.attemptedAt],
  )).rows;
  const executions = (await pool.query(
    `SELECT id, status, provider_message_id, prospect_id, execution_identity, attempted_at, sent_at
       FROM acquisition_mission_outbound_executions
      WHERE tenant_id = $1 AND prospect_id = $2`,
    [INCIDENT.tenantId, INCIDENT.prospectId],
  )).rows;
  const uncertainItems = (await pool.query(
    `SELECT id, status, reason FROM acquisition_outbound_items
      WHERE tenant_id = $1 AND status IN ('attempted','uncertain')`,
    [INCIDENT.tenantId],
  )).rows;
  const control = (await pool.query(
    `SELECT created_at,
            payload->>'emmettCapacity' AS emmett_capacity,
            payload->>'dispatchUnavailableNow' AS dispatch_unavailable,
            payload->>'limitingFactor' AS limiting_factor,
            payload->>'capacityReason' AS capacity_reason
       FROM acquisition_outbound_events
      WHERE tenant_id = $1 AND event_type = 'max_outbound_control'
      ORDER BY created_at DESC LIMIT 1`,
    [INCIDENT.tenantId],
  )).rows[0] || null;
  return {
    inspectedAt: new Date().toISOString(),
    program,
    item,
    envelopeItems,
    events,
    executions,
    uncertainItems,
    reconcileAt: events.find(event => event.event_type === 'send_reconciled')?.created_at || null,
    emmettCapacity: control?.emmett_capacity != null ? Number(control.emmett_capacity) : null,
    dispatchUnavailableNow: control?.dispatch_unavailable === 'true',
    limitingFactor: control?.limiting_factor || null,
    capacityReason: control?.capacity_reason || null,
    forceSend: false,
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const options = { confirmProduction: false, out: null, help: false };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--confirm-production') options.confirmProduction = true;
    else if (argv[i] === '--out') options.out = argv[++i];
    else if (argv[i] === '--help' || argv[i] === '-h') options.help = true;
    else throw new Error(`Unknown argument: ${argv[i]}`);
  }
  return options;
}

async function run(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    console.log('Usage: node scripts/verifyGovernedOutboundResumption.js --confirm-production [--out file.json]');
    return { help: true };
  }
  if (!options.confirmProduction) {
    throw Object.assign(new Error('Refusing to run without --confirm-production.'), {
      code: 'confirm_production_required',
    });
  }
  const pool = require('../db');
  try {
    const snapshot = await loadSnapshot(pool);
    const evaluation = evaluateResumption(snapshot);
    const result = {
      itemId: INCIDENT.itemId,
      providerOutcome: 'PROVIDER_CONFIRMED_NOT_SENT',
      ...evaluation,
      snapshot: {
        inspectedAt: snapshot.inspectedAt,
        program: snapshot.program,
        item: snapshot.item,
        envelopeItems: snapshot.envelopeItems,
        executions: snapshot.executions,
        uncertainItems: snapshot.uncertainItems,
        reconcileAt: snapshot.reconcileAt,
        emmettCapacity: snapshot.emmettCapacity,
        dispatchUnavailableNow: snapshot.dispatchUnavailableNow,
        limitingFactor: snapshot.limitingFactor,
        lastTickAt: snapshot.program?.last_tick_at || null,
        lastError: snapshot.program?.last_error || null,
        postReconcileEvents: (snapshot.events || []).filter(event =>
          snapshot.reconcileAt && atOrAfter(event.created_at, snapshot.reconcileAt))
          .map(event => ({
            created_at: event.created_at,
            event_type: event.event_type,
            item_id: event.item_id,
            reason: event.reason,
            outcome: event.outcome,
            provider_outcome: event.provider_outcome,
          })),
      },
    };
    if (options.out) {
      fs.writeFileSync(path.resolve(options.out), `${JSON.stringify(result, null, 2)}\n`);
    }
    return result;
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  run().then((result) => {
    console.log(JSON.stringify(result, null, 2));
    if (result.verdict === 'FAIL') process.exitCode = 1;
  }).catch((error) => {
    console.error(JSON.stringify({ error: error.code || error.message }));
    process.exitCode = 1;
  });
}

module.exports = {
  INCIDENT,
  REMAINING_REFILL_IDS,
  OPERATIONAL_HALTS,
  evaluateResumption,
  parseArgs,
  run,
};
