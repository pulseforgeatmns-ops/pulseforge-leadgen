'use strict';

/**
 * MAX-P0-003 — independent worker process for PostgreSQL reliability tests.
 * Each worker owns its own pg.Pool, so contention crosses processes AND pooled
 * connections exactly as two production web processes would.
 *
 * argv[2] = JSON { op, connectionString, goFile?, providerLog?, ... }
 * stdout  = one JSON line with the outcome.
 */

const fs = require('fs');
const { Pool } = require('pg');
const { persistStageCommit } = require('../../services/acquisitionMissionPersistence');
const { createExternalActionLedger } = require('../../services/maxReliability/externalActionLedger');
const { guardedDispatch } = require('../../services/maxReliability/guardedDispatch');
const { acquireMissionLease } = require('../../services/missionLease');

const args = JSON.parse(process.argv[2]);
const out = (value) => process.stdout.write(`${JSON.stringify(value)}\n`);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForGo() {
  if (!args.goFile) return;
  const deadline = Date.now() + 20000;
  while (!fs.existsSync(args.goFile)) {
    if (Date.now() > deadline) throw new Error('go barrier timeout');
    await sleep(5);
  }
}

function providerCall(tag) {
  // Append-only, one line per real provider invocation across all processes.
  fs.appendFileSync(args.providerLog, `${tag}:${process.pid}\n`);
}

async function main() {
  const pool = new Pool({ connectionString: args.connectionString, max: 3 });
  // Warm several pooled connections so lock/unlock could land on different ones.
  const warm = await Promise.all([pool.connect(), pool.connect(), pool.connect()]);
  warm.forEach((c) => c.release());
  try {
    await waitForGo();
    if (args.op === 'stage-commit') {
      try {
        await persistStageCommit({ mission: args.mission }, pool, {
          skipEnsure: true, tryLock: false, lockTimeoutMs: 15000, expectedFingerprint: args.expectedFingerprint,
        });
        out({ ok: true, objective: args.mission.objective });
      } catch (err) {
        out({ ok: false, code: err.code });
      }
    } else if (args.op === 'claim-dispatch') {
      const ledger = createExternalActionLedger(pool);
      try {
        const result = await guardedDispatch({
          ledger,
          claim: args.claim,
          dispatch: async () => {
            providerCall(args.tag || 'send');
            if (args.dispatchDelayMs) await sleep(args.dispatchDelayMs);
            if (args.dispatchThrows) throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
            return { success: true, providerMessageId: `msg-${process.pid}` };
          },
        });
        out({ ok: true, outcome: result.outcome, code: result.code || null, state: result.ledgerState || (result.receipt && result.receipt.state) });
      } catch (err) {
        out({ ok: false, code: err.code || null, message: err.message });
      }
    } else if (args.op === 'dispatch-then-crash') {
      // Crash AFTER the provider accepted and BEFORE the acknowledgement is persisted.
      const ledger = createExternalActionLedger(pool);
      const claimed = await ledger.claim(args.claim);
      if (claimed.outcome !== 'claimed') { out({ ok: false, outcome: claimed.outcome, code: claimed.code }); return; }
      await ledger.markDispatchAttempted({ tenantId: args.claim.tenantId, idempotencyKey: args.claim.idempotencyKey, ownerToken: claimed.ownerToken });
      providerCall('crash-send');
      out({ ok: true, crashed: true });
      process.exit(137);
    } else if (args.op === 'claim-then-crash') {
      // Crash after claiming and BEFORE the dispatch marker: provider never invoked.
      const ledger = createExternalActionLedger(pool);
      const claimed = await ledger.claim(args.claim);
      out({ ok: true, outcome: claimed.outcome, crashed: true });
      process.exit(137);
    } else if (args.op === 'lease-write-then-crash') {
      // Hold a mission lease, write inside it, then die without COMMIT.
      const lease = await acquireMissionLease(pool, { missionId: args.mission.id, tenantId: args.mission.tenantId, mode: 'try' });
      await persistStageCommit({ mission: args.mission }, pool, { skipEnsure: true, lease });
      out({ ok: true, held: true, pid: (await lease.client.query('SELECT pg_backend_pid() AS pid')).rows[0].pid });
      process.exit(137);
    } else {
      throw new Error(`unknown op ${args.op}`);
    }
  } finally {
    await pool.end().catch(() => {});
  }
}

main().catch((err) => {
  out({ ok: false, fatal: true, code: err.code || null, message: err.message });
  process.exitCode = 1;
});
