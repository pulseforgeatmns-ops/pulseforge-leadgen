#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — recover the STR operator AMO mission to READY.
 *
 * Canonical path only:
 *   inspect → reuse existing STR mission (or create if none)
 *   → Scout DISCOVERY → Max PRIORITIZATION → Paige VARIANTS
 *   → Emmett CAPACITY → READY
 *
 * Stops before APPROVE_EXECUTION / EXECUTE_OUTBOUND.
 * Never enables autosend. Never issues APPROVE_PRIORITIZATION unless
 * prioritization approval is actually pending. When healthy Scout candidates
 * exist but discovery readiness is still insufficient, stops with the exact
 * canonical blocker instead of looping destructive investigation or forcing Max.
 * If discovery is already approved and Scout has zero candidates, continues
 * investigation (canonical Scout path) instead of re-issuing APPROVE_DISCOVERY.
 *
 *   node scripts/recoverAnchorCanonicalOutbound.js --confirm-production
 */

require('dotenv').config();

const pool = require('../db');
const {
  createMission,
  executeCanonical,
  inspectMission,
  listMissions,
} = require('../services/acquisitionMission');
const inspectScript = require('./inspectAnchorCanonicalOutbound');
const {
  TENANT_ID,
  CLIENT_ID,
  STR_OBJECTIVE,
  FORBIDDEN_SEND_INTENTS,
  summarizeMissionSnapshot,
  pickCanonicalStrMission,
  chooseNextRecoveryIntent,
  payloadForIntent,
  questionForIntent,
  assertNotSendingIntent,
} = require('./lib/anchorCanonicalOutbound');

const OPERATOR_ID = 'anchor-str-outbound-recovery';
const MAX_STEPS = 8;

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const help = argv.includes('--help') || argv.includes('-h');
  const inspectOnly = argv.includes('--inspect-only');
  const unknown = argv.filter(
    (arg) =>
      arg !== '--confirm-production'
      && arg !== '--help'
      && arg !== '-h'
      && arg !== '--inspect-only'
  );
  if (unknown.length) {
    throw Object.assign(
      new Error(`Unknown argument(s): ${unknown.join(', ')}.`),
      { code: 'unknown_args' }
    );
  }
  return { confirmProduction, help, inspectOnly };
}

function printUsage() {
  console.log(`Anchor STR canonical outbound recovery (tenant ${TENANT_ID})

Usage:
  node scripts/recoverAnchorCanonicalOutbound.js --confirm-production
  node scripts/recoverAnchorCanonicalOutbound.js --confirm-production --inspect-only

Safety:
  Refuses without --confirm-production.
  Never APPROVE_EXECUTION or EXECUTE_OUTBOUND.
  Never APPROVE_PRIORITIZATION unless prioritization approval is pending.
  Never loops destructive Scout continuation when healthy candidates exist but discovery is not prioritization-ready.
  If discovery is already approved and candidates are empty, runs Scout continuation instead of re-approving discovery.
  Never enables autosend. Never uses fixtures.
`);
}

function assertRuntimeEnv(inspectOnly) {
  const missing = [];
  if (!process.env.DATABASE_URL) missing.push('DATABASE_URL');
  if (!inspectOnly && !process.env.GOOGLE_PLACES_KEY) missing.push('GOOGLE_PLACES_KEY');
  if (missing.length) {
    const err = new Error(`Missing required runtime env: ${missing.join(', ')}`);
    err.code = 'runtime_env_missing';
    throw err;
  }
  if (
    process.env.ALLOW_FIXTURE_FALLBACK === 'true'
    || process.env.allowFixtureFallback === 'true'
  ) {
    const err = new Error('Refusing to run with ALLOW_FIXTURE_FALLBACK enabled.');
    err.code = 'fixture_fallback_env';
    throw err;
  }
}

async function assertAutosendOff(db) {
  const { rows } = await db.query(
    `SELECT id, autosend_enabled, enabled_agents
       FROM clients
      WHERE id = $1`,
    [CLIENT_ID]
  );
  const client = rows[0];
  if (!client) {
    const err = new Error('Anchor client 10 was not found.');
    err.code = 'anchor_client_missing';
    throw err;
  }
  if (client.autosend_enabled === true) {
    const err = new Error('Anchor autosend_enabled is true. Aborting.');
    err.code = 'autosend_enabled';
    throw err;
  }
  return client;
}

async function inspectSummary(db, missionId) {
  const snapshot = await inspectMission(missionId, { tenantId: TENANT_ID, pool: db });
  return summarizeMissionSnapshot(snapshot, { autosendEnabled: false });
}

async function resolveCanonicalMission(db, inspectReport) {
  const existing = pickCanonicalStrMission(inspectReport.missions || []);
  if (existing) {
    return { missionId: existing.id, created: false, summary: existing };
  }
  const created = await createMission({
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    objective: STR_OBJECTIVE,
    targetSegment: 'Short-term rental operators',
    createdBy: OPERATOR_ID,
    owner: 'Operator',
    title: 'Anchor STR operator — Greater Manchester recurring commercial cleaning',
  }, { pool: db, production: true });
  return {
    missionId: created.id,
    created: true,
    summary: await inspectSummary(db, created.id),
  };
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    const err = new Error('Refusing to run without --confirm-production.');
    err.code = 'confirm_production_required';
    throw err;
  }

  const inspectOnly = options.inspectOnly === true;
  assertRuntimeEnv(inspectOnly);
  const db = options.pool || pool;
  const client = await assertAutosendOff(db);

  const inspectReport = await inspectScript.run({
    confirmProduction: true,
    pool: db,
  });

  if (inspectOnly) {
    return {
      ...inspectReport,
      mode: 'inspect-only',
      recovered: false,
      sent: false,
    };
  }

  const resolved = await resolveCanonicalMission(db, inspectReport);
  const steps = [];
  const seenIntents = [];
  let summary = await inspectSummary(db, resolved.missionId);
  let revisedOnce = false;

  for (let i = 0; i < MAX_STEPS; i += 1) {
    const chosen = chooseNextRecoveryIntent(summary);
    if (chosen.stop || !chosen.intent) {
      steps.push({
        label: 'STOP',
        reason: chosen.reason,
        operatorAction: chosen.operatorAction || null,
      });
      break;
    }
    if (chosen.intent === 'REVISE_PREPARED_OUTREACH') {
      if (revisedOnce) {
        steps.push({
          label: 'STOP',
          reason: 'capacity_queue_blocked',
          operatorAction:
            'Resolve blocked recipient/copy requirements before execution approval.',
        });
        break;
      }
      revisedOnce = true;
    }
    if (seenIntents.filter((intent) => intent === chosen.intent).length >= 2) {
      steps.push({
        label: 'STOP',
        reason: `intent_loop:${chosen.intent}`,
        operatorAction: chosen.operatorAction || summary.waitingReason,
      });
      break;
    }

    assertNotSendingIntent(chosen.intent);
    seenIntents.push(chosen.intent);
    const stepRecord = {
      label: chosen.intent,
      reason: chosen.reason,
      startedAt: new Date().toISOString(),
    };
    steps.push(stepRecord);

    try {
      const routed = await executeCanonical({
        tenantId: TENANT_ID,
        missionId: resolved.missionId,
        intent: chosen.intent,
        operatorId: OPERATOR_ID,
        question: questionForIntent(chosen.intent),
        payload: payloadForIntent(chosen.intent, chosen),
        allowFixtureFallback: false,
      }, { pool: db, production: true });
      stepRecord.completedAt = new Date().toISOString();
      stepRecord.action = routed.action || null;
      stepRecord.executionOutcome = routed.executionResult?.executionOutcome
        || routed.audit?.outcome
        || routed.executionOutcome
        || null;
      stepRecord.alreadyExecuted = routed.executionResult?.alreadyExecuted === true
        || routed.audit?.outcome === 'already_executed';
      stepRecord.rolledBack = routed.executionResult?.rolledBack === true;
      if (
        stepRecord.alreadyExecuted
        && chosen.intent === 'APPROVE_DISCOVERY'
      ) {
        stepRecord.note = 'Discovery approval already executed; next step uses Scout continuation, not another approval.';
      }
    } catch (err) {
      stepRecord.error = { code: err.code || null, message: err.message };
      summary = await inspectSummary(db, resolved.missionId).catch(() => summary);
      return {
        tenantId: TENANT_ID,
        missionId: resolved.missionId,
        created: resolved.created,
        mode: 'recover-to-ready',
        success: false,
        sent: false,
        autosendEnabled: client.autosend_enabled === true,
        steps,
        final: summary,
        error: { code: err.code || null, message: err.message },
        operatorAction: summary.waitingReason || err.message,
      };
    }

    summary = await inspectSummary(db, resolved.missionId);
  }

  const finalChoice = chooseNextRecoveryIntent(summary);
  const ready = summary.stage === 'ready';
  const sendable = Number(summary.sendableCount || 0) > 0;
  let stopReason = finalChoice.reason;
  let operatorAction = finalChoice.operatorAction || null;
  if (!sendable) {
    if (
      stopReason === 'ready_awaiting_execution_approval'
      || stopReason === 'execution_approval_without_sendable_queue'
      || stopReason === 'capacity_not_sendable'
      || stopReason === 'revise_did_not_create_sendable_queue'
    ) {
      stopReason = 'capacity_queue_blocked';
    }
    if (
      !operatorAction
      || /execution authorization required before send/i.test(String(operatorAction))
      || /APPROVE_EXECUTION/i.test(String(operatorAction))
    ) {
      operatorAction = 'Resolve blocked recipient/copy requirements before execution approval.';
    }
  } else {
    operatorAction = operatorAction
      || (ready
        ? 'APPROVE_EXECUTION for the current prepared artifacts, then EXECUTE_OUTBOUND. Autosend stays off.'
        : summary.waitingReason);
  }
  return {
    tenantId: TENANT_ID,
    missionId: resolved.missionId,
    created: resolved.created,
    mode: 'recover-to-ready',
    success: ready && sendable,
    sent: false,
    forbiddenIntentsUntouched: FORBIDDEN_SEND_INTENTS,
    autosendEnabled: client.autosend_enabled === true,
    steps,
    final: summary,
    stopReason,
    operatorAction,
  };
}

module.exports = {
  run,
  parseArgs,
  printUsage,
  TENANT_ID,
  STR_OBJECTIVE,
  OPERATOR_ID,
  MAX_STEPS,
};

if (require.main === module) {
  run(parseArgs())
    .then((report) => {
      console.log(JSON.stringify(report, null, 2));
    })
    .catch((err) => {
      console.error(JSON.stringify({
        error: { code: err.code || null, message: err.message, sent: false },
        completedAt: new Date().toISOString(),
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
