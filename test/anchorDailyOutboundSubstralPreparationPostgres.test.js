'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { GovernedOutboundStore } = require('../services/governedOutboundStore');

test('Substral preparation commit locks and lifetime budgets on PostgreSQL', {
  skip: process.env.ANCHOR_OUTBOUND_TEST_POSTGRES !== 'true', timeout: 90000,
}, async t => {
  const pg = await startDisposablePostgres('substral-prep-');
  const pool = new Pool({ connectionString: pg.connectionString });
  t.after(async () => { await pool.end(); await pg.stop(); });
  await pool.query(`
    CREATE TABLE acquisition_outbound_programs(id TEXT PRIMARY KEY, tenant_id TEXT,
      mode TEXT, policy_hash TEXT, scope_hash TEXT);
    CREATE TABLE acquisition_outbound_envelopes(id TEXT PRIMARY KEY, program_id TEXT,
      tenant_id TEXT, local_day DATE, mission_id TEXT, revision TEXT, manifest JSONB,
      manifest_hash TEXT, status TEXT, approval_id TEXT);
    CREATE TABLE acquisition_outbound_items(id TEXT PRIMARY KEY, envelope_id TEXT,
      tenant_id TEXT, candidate_id TEXT, prospect_id TEXT, company_id TEXT, email TEXT,
      snapshot JSONB, status TEXT DEFAULT 'pending');
    INSERT INTO acquisition_outbound_programs VALUES('program','17','shadow','policy','scope');
  `);
  const store = new GovernedOutboundStore(pool, '17');
  store.event = async () => {}; // Isolate commit SQL; no provider or notification work.
  const program = { id: 'program', tenant_id: '17', policy_hash: 'policy', scope_hash: 'scope',
    policy: { preparationOnly: true, dailyCap: 2, totalCap: 3 } };
  const item = n => ({ candidateId: `candidate-${n}`, prospectId: `prospect-${n}`,
    companyId: `company-${n}`, email: `synthetic-${n}@example.invalid` });

  let envelope = await store.freeze(program, '2026-10-01', 'mission-1', 'r1', [item(1)]);
  assert.equal(envelope.status, 'frozen');
  envelope = await store.appendToEnvelope(envelope, [item(2)], 'r2', { shadowProgram: program });
  assert.equal(envelope.manifest.length, 2);
  await pool.query("UPDATE acquisition_outbound_items SET status='expired'");
  await assert.rejects(store.appendToEnvelope(envelope, [item(3)], 'r3', { shadowProgram: program }),
    /preparation_budget_exhausted/);

  // Two stale callers compete for one remaining lifetime slot. The program
  // and envelope locks allow one commit; the other must reject its stale view.
  let nextDay = await store.freeze(program, '2026-10-02', 'mission-2', 'r1', []);
  const results = await Promise.allSettled([
    store.appendToEnvelope(nextDay, [item(3)], 'r3', { shadowProgram: program }),
    store.appendToEnvelope(nextDay, [item(4)], 'r4', { shadowProgram: program }),
  ]);
  assert.equal(results.filter(result => result.status === 'fulfilled').length, 1);
  assert.match(results.find(result => result.status === 'rejected').reason.message,
    /replenishment_grant_changed|preparation_budget_exhausted/);
  assert.equal((await store.preparationCounts(program, '2026-10-02')).total, 3);

  nextDay = await store.envelope('2026-10-02');
  await pool.query("UPDATE acquisition_outbound_envelopes SET status='authorized',approval_id='approval' WHERE id=$1", [nextDay.id]);
  await assert.rejects(store.appendToEnvelope(nextDay, [item(5)], 'r5', { shadowProgram: program }),
    /replenishment_grant_changed/);
  assert.equal((await store.preparationCounts(program, '2026-10-02')).total, 3);
});
