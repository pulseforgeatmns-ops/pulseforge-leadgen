'use strict';

/**
 * P0-MAX-REASONING-001 PR 3 — ConversationLedger durable recovery across
 * separate Node processes on disposable PostgreSQL, using the existing
 * max_conversation_context table (no new migration).
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const WORKER = path.join(__dirname, 'helpers', 'maxLedgerRecoveryWorker.cjs');
const LEVI = '21';
const FIVE = ['p101', 'p102', 'p103', 'p104', 'p105'];

function worker(config) {
  const env = { PATH: process.env.PATH, HOME: process.env.HOME, MAX_LEDGER_WORKER_CONFIG: JSON.stringify(config) };
  const r = spawnSync(process.execPath, [WORKER], { cwd: path.resolve(__dirname, '..'), env, encoding: 'utf8', timeout: 60000 });
  assert.equal(r.status, 0, r.stderr);
  const line = r.stdout.trim().split('\n').pop();
  return JSON.parse(line);
}

test('PostgreSQL: ledger recovery across processes, tenant boundary, tampering, expiry, unavailability', { timeout: 180000 }, async (t) => {
  const instance = await startDisposablePostgres('max-ledger-');
  const db = new Pool({ connectionString: instance.connectionString });
  t.after(async () => { await db.end(); await instance.stop(); });
  const migration = fs.readFileSync('migrations/2026-11-09-max-conversation-context.sql', 'utf8');
  await db.query(migration);
  const cs = instance.connectionString;

  const written = worker({ mode: 'write', connectionString: cs, tenantId: LEVI, prospects: FIVE });
  const sid = written.sessionId;
  assert.ok(sid);
  assert.deepEqual(written.ledger.focus.referent.memberIds, FIVE);

  const row = (await db.query('SELECT tenant_id, context FROM max_conversation_context WHERE session_id = $1', [sid])).rows;
  assert.equal(row.length, 1);
  assert.equal(row[0].tenant_id, LEVI);
  assert.equal(row[0].context.version, 2);
  const stored = JSON.stringify(row[0].context);
  for (const secret of ['Jane', 'Borrower', 'private notes', 'freshest', "Levi's"]) assert.ok(!stored.includes(secret), `persisted: ${secret}`);
  assert.ok(Buffer.byteLength(stored) <= 16384);

  await t.test('a separate process recovers the same authenticated conversation', () => {
    const r = worker({ mode: 'recover', connectionString: cs, tenantId: LEVI, sessionId: sid });
    assert.notEqual(r.pid, written.pid);
    assert.equal(r.recovery.recovered, true);
    assert.equal(r.sessionRestored, true);
    const { focus, view } = r.ledger;
    assert.deepEqual(focus.referent.memberIds, FIVE);
    assert.equal(focus.requiresRevalidation, true);
    assert.equal(focus.evidenceRefreshRequired, true);
    assert.equal(focus.verified, false);
    assert.ok(view.remembered.recovered.droppedReferents >= 1, 'the deleted mission is not restored');
    assert.ok(!JSON.stringify(view).includes('m_gone') || view.remembered.decisions.some((d) => d.missionId === 'm_gone'), 'm_gone survives only as decision history');
    assert.ok(view.remembered.constraints.some((c) => c.kind === 'prohibition' && c.action === 'any_change'));
    assert.ok(view.remembered.constraints.some((c) => c.kind === 'prohibition' && c.action === 'send'));
    const approval = view.remembered.decisions.find((d) => d.outcome === 'operator_affirmed');
    assert.equal(approval.decisionKind, 'execution_approval');
    assert.equal(approval.authority, 'none');
    assert.deepEqual(view.authority, { grantsAuthority: false, executionPermitted: false, consumableApprovals: [], source: 'mission runtime and pending-decision resolver only' });
  });

  await t.test('another tenant cannot recover it', () => {
    const r = worker({ mode: 'recover', connectionString: cs, tenantId: '10', sessionId: sid });
    assert.equal(r.recovery.recovered, false);
    assert.equal(r.sessionRestored, false);
    assert.equal(r.ledger, null);
  });

  await t.test('a tampered row that claims authority is refused, with no referents', async () => {
    await db.query(`UPDATE max_conversation_context SET context = jsonb_set(context, '{ledger,decisions,0,authority}', '"granted"') WHERE session_id = $1`, [sid]);
    const r = worker({ mode: 'recover', connectionString: cs, tenantId: LEVI, sessionId: sid });
    assert.equal(r.recovery.recovered, false);
    assert.equal(r.sessionRestored, false);
    assert.equal(r.ledger, null);
    await db.query(`UPDATE max_conversation_context SET context = jsonb_set(context, '{ledger,decisions,0,authority}', '"none"') WHERE session_id = $1`, [sid]);
  });

  await t.test('an expired row is not silently reused', async () => {
    await db.query("UPDATE max_conversation_context SET expires_at = now() - interval '1 second' WHERE session_id = $1", [sid]);
    const r = worker({ mode: 'recover', connectionString: cs, tenantId: LEVI, sessionId: sid });
    assert.equal(r.recovery.recovered, false);
    assert.equal(r.ledger, null);
  });

  await t.test('an unreachable database degrades to a fresh conversation', () => {
    const r = worker({ mode: 'recover', connectionString: 'postgresql://postgres@127.0.0.1:1/none', tenantId: LEVI, sessionId: sid });
    assert.deepEqual(r.recovery, { recovered: false, reason: 'context_unavailable' });
    assert.equal(r.sessionRestored, false);
  });
});
