#!/usr/bin/env node
'use strict';

/**
 * MAX-P0-003 — operator-run interrupted-execution recovery for ONE tenant.
 *
 *   node scripts/max-reliability/recover.cjs --tenant 10                 # plan (read-only)
 *   node scripts/max-reliability/recover.cjs --tenant 10 --apply         # apply the plan
 *   node scripts/max-reliability/recover.cjs --tenant 10 --unresolved    # list unknown outcomes
 *   [--stale-minutes 15]
 *
 * Never scheduled. Never dispatches, retries or reconciles anything: it only
 * moves stale claims to abandoned_not_dispatched, stale dispatches to
 * outcome_unknown (escalation), and stale request receipts to interrupted.
 * Output is redacted (no recipients, keys are hashed).
 */

const { recoverInterruptedExecution } = require('../../services/maxReliability/executionRecovery');
const { createExternalActionLedger } = require('../../services/maxReliability/externalActionLedger');
const { shortHash } = require('../../services/maxReliability/redaction');

function arg(name) {
  const i = process.argv.indexOf(name);
  return i === -1 ? null : (process.argv[i + 1] || null);
}

async function main() {
  const tenantId = arg('--tenant');
  if (!tenantId) {
    console.error('usage: recover.cjs --tenant <id> [--apply] [--unresolved] [--stale-minutes N]');
    process.exit(2);
  }
  const pool = require('../../db');
  try {
    if (process.argv.includes('--unresolved')) {
      const rows = await createExternalActionLedger(pool).listUnresolved(tenantId);
      console.log(JSON.stringify(rows.map((r) => ({ ...r, idempotencyKey: shortHash(r.idempotencyKey) })), null, 2));
      return;
    }
    const staleMinutes = Number(arg('--stale-minutes')) || 15;
    const result = await recoverInterruptedExecution(pool, {
      tenantId, staleAfterMs: staleMinutes * 60 * 1000, apply: process.argv.includes('--apply'),
    });
    const { plan, ...summary } = result;
    console.log(JSON.stringify(summary, (k, v) => (k === 'unknown' || k === 'abandoned' ? (v || []).map(shortHash) : v), 2));
    if (result.escalationRequired) process.exitCode = 3;
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error(`[max-reliability] ${err.code || 'error'}: ${err.message}`);
  process.exit(1);
});
