'use strict';
// SPEC264 wiring proof: actual default wrapper, pg.Pool and bridge. Synthetic local DB only.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const { Pool, Client } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const persistence = require('../services/acquisitionMissionPersistence');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
// Reuse only the native synthetic seed. No replacement require is supplied to
// the wrapper or bridge; their normal module imports and pool creation execute.
function fixture() {
  const filename = path.join(__dirname, 'anchorScopedResumeCrmPreparationPostgres.test.js');
  const local = createRequire(filename), module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + '\nmodule.exports={setup,install};', {
    module, exports: module.exports, __dirname, process, console,
    require(id) { return id === 'node:test' ? () => {} : local(id); },
  }, { filename });
  return module.exports;
}

test('native default wrapper apply calls actual bridge with its private pool, consumes once and cannot dispatch', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('anchor-crm-invocation-apply-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 12 });
  t.after(async () => { resetAcquisitionMissionRuntime(); await pool.end(); await pg.stop(); });
  t.mock.timers.enable({ apis: ['Date'], now: +new Date('2026-10-19T16:00:00Z') });
  const envKeys = ['DATABASE_URL', 'DATABASE_SSL', 'RAILWAY_GIT_COMMIT_SHA', 'ANCHOR_EMAIL_SCOPED_RESUME_ID',
    'ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID'];
  const oldEnv = Object.fromEntries(envKeys.map(k => [k, process.env[k]]));
  t.after(() => envKeys.forEach(k => oldEnv[k] === undefined ? delete process.env[k] : process.env[k] = oldEnv[k]));
  envKeys.forEach(k => delete process.env[k]);
  const h = fixture(), f = await h.setup(pool); await h.install(pool, f);
  Object.assign(process.env, { DATABASE_URL: pg.connectionString, DATABASE_SSL: 'false',
    RAILWAY_GIT_COMMIT_SHA: 'a'.repeat(40), ANCHOR_EMAIL_SCOPED_RESUME_ID: f.manifest.id });
  const argv = ['--tenant', '10', '--revision', process.env.RAILWAY_GIT_COMMIT_SHA,
    '--manifest-sha256', hash(f.manifest), '--source-sha256', hash(f.source)];
  const installedEnv = Object.fromEntries(envKeys.map(k => [k, process.env[k]]));
  const count = async table => (await pool.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n;
  const held = async () => (await pool.query('SELECT row_to_json(i) AS row FROM acquisition_outbound_items i WHERE id=$1', [f.quarantine.item_id])).rows[0].row;
  const beforeHeld = await held();
  async function snapshot() {
    const names = (await pool.query("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")).rows;
    const result = {};
    for (const { tablename } of names) result[tablename] = (await pool.query(`SELECT row_to_json(t) AS row FROM "${tablename}" t`)).rows.map(r => JSON.stringify(r.row)).sort();
    return result;
  }
  const before = await snapshot();
  assert.equal(await count('acquisition_mission_contributions'), 0);
  let externalCalls = 0;
  const forbidden = () => { externalCalls++; throw new Error('synthetic provider transport forbidden'); };
  for (const method of ['get', 'post', 'request']) t.mock.method(require('axios'), method, forbidden);
  for (const protocol of ['node:http', 'node:https']) for (const method of ['get', 'request']) t.mock.method(require(protocol), method, forbidden);
  t.mock.method(globalThis, 'fetch', forbidden);
  const calls = [], privatePools = new Set(), ended = new Set();
  const originalQuery = Client.prototype.query, originalConnect = Pool.prototype.connect, originalEnd = Pool.prototype.end;
  t.mock.method(Client.prototype, 'query', function (...args) {
    const sql = typeof args[0] === 'string' ? args[0] : args[0].text;
    calls.push(sql);
    assert.doesNotMatch(sql, /\b(?:CREATE|ALTER|DROP|TRUNCATE)\b/i, 'no bootstrap DDL after fixture setup');
    return originalQuery.apply(this, args);
  });
  t.mock.method(Pool.prototype, 'connect', function (...args) {
    if (this !== pool) { privatePools.add(this); assert.equal(this.options.connectionString, pg.connectionString); assert.equal(this.options.ssl, false); }
    return originalConnect.apply(this, args);
  });
  t.mock.method(Pool.prototype, 'end', function (...args) { if (this !== pool) ended.add(this); return originalEnd.apply(this, args); });
  const { run } = require('../scripts/anchorCrmPreparationOnce');
  // No env argument, clock argument, pool injection or bridge replacement.
  const preview = await run(argv);
  assert.equal(preview.mode, 'read_only'); assert.equal(preview.writesAttempted, false);
  assert.equal(preview.completeEligibilityChecked, false);
  assert.ok(calls.includes('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY'));
  assert.ok(!calls.some(sql => /FOR SHARE|FOR UPDATE|advisory|INSERT |UPDATE |DELETE /i.test(sql)));
  assert.deepEqual(await snapshot(), before);
  const result = await run([...argv, '--apply']);
  assert.deepEqual(result, { mode: 'applied', tenantId: '10', manifestHash: hash(f.manifest), sourceHash: hash(f.source),
    candidateCount: 1, manifestAndSourceValidated: true, completeEligibilityChecked: true, providerReadiness: 'not_checked',
    writesAttempted: true, missionId: result.missionId, readyForReview: true, executionApproved: false,
    envelopeAuthorized: false, sent: 0, providerCalls: 0 });
  assert.match(result.missionId, /^mission_crm_[a-f0-9]{32}$/);
  const saved = await persistence.loadMissionSnapshot(result.missionId, '10', pool, { skipEnsure: true });
  assert.equal(saved.mission.stage, 'ready'); assert.equal(saved.mission.orchestrationMissionId, f.source.id);
  assert.equal(saved.mission.pendingOperatorDecision.kind, 'execution_approval');
  assert.equal(saved.mission.crmPreparation.providerReadiness, 'not_checked');
  for (const [specialist, kind] of [['scout', 'discovery'], ['max', 'prioritization'], ['max', 'acquisition_approach'], ['paige', 'variants'], ['emmett', 'capacity']]) {
    assert.ok(saved.contributions.some(c => c.specialist === specialist && c.kind === kind));
  }
  assert.equal(require('../packages/acquisition-mission/ExecutionApproval').findExecutionApprovals(saved.contributions).length, 0);
  assert.deepEqual((await pool.query('SELECT operation,item_count FROM anchor_scoped_resume_consumption')).rows,
    [{ operation: 'prepare_existing_crm', item_count: 1 }]);
  assert.equal(await count('anchor_email_recovery_reservations'), 0);
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_outbound_envelopes WHERE local_day='2026-10-19'")).rows[0].n, 0);
  assert.equal(hash((await pool.query('SELECT payload FROM acquisition_missions WHERE id=$1', [f.source.id])).rows[0].payload), hash(f.source));
  assert.deepEqual(await held(), beforeHeld);
  const after = await snapshot();
  await assert.rejects(run([...argv, '--apply']), error => error.code === 'anchor_crm_invocation_rejected' && error.writesMayHaveOccurred === true);
  assert.deepEqual(await snapshot(), after); // Replay retains budget and original UNKNOWN; no second child.
  assert.equal(externalCalls, 0); assert.equal(privatePools.size, 3); assert.equal(ended.size, 3);
  assert.deepEqual(Object.fromEntries(envKeys.map(k => [k, process.env[k]])), installedEnv);
  assert.doesNotMatch(JSON.stringify(result), /DATABASE_URL|postgresql:|recipientEmail|senderEmail|subject|body/);
});
