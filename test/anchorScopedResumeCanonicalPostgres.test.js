'use strict';
// Independent SPEC-262 proof: real scoped service -> real canonical writer.
// Synthetic loopback database only; no provider, discovery or paid callbacks.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { applyScopedSchema } = require('./helpers/anchorScopedResumePostgresFixture');
const { scopedResumeFixture } = require('./helpers/anchorScopedResumeFixture');
const { insert } = require('./helpers/anchorRecoveryPostgresFixture');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const scope = require('../services/anchorScopedResume');
const control = require('../services/anchorScopedResumeControl');
const prepare = require('../services/anchorEmailRecoveryPrepareExisting');
const { adapters } = require('../services/governedOutboundAdapters');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');

function existingPreparationHarness() {
  // Reuse the mature native canonical fixture without registering its tests or
  // exporting test-only helpers from production. Its setup explicitly installs
  // synthetic prerequisites before the audited no-DDL boundary starts.
  const filename = path.join(__dirname, 'anchorDailyOutboundRecoveryApprovalPostgres.test.js');
  const local = createRequire(filename), module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + '\nmodule.exports={setup,auditPool};', {
    module, exports: module.exports, __dirname: path.dirname(filename), process, console,
    require(id) { return id === 'node:test' ? () => {} : local(id); },
  }, { filename });
  return module.exports;
}

function scopedService(pool, f) {
  const filename = path.join(__dirname, '../services/governedOutbound.js');
  const local = createRequire(filename), module = { exports: {} };
  const source = fs.readFileSync(filename, 'utf8');
  assert.ok(source.includes('    authorize,\n'));
  vm.runInNewContext(source.replace('    authorize,\n', '    authorize, __testBindApproval: bindApproval,\n'), {
    module, exports: module.exports, require: local, process, Date, console,
  }, { filename });
  return module.exports.service({ pool, tenantId: '10', adapters: adapters(pool, { tenantId: '10' }),
    now: () => f.now, enabled: () => true });
}

test('native scoped approval invokes actual canonical writer without bootstrap or dispatch', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('anchor-scoped-real-canonical-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 14 });
  t.after(async () => { resetAcquisitionMissionRuntime(); await pool.end(); await pg.stop(); });
  const harness = existingPreparationHarness();
  const f = await harness.setup(pool);
  // The actual adapter intentionally rechecks runtime wall-clock authority.
  // Freeze only Date so the finite synthetic grant also works on future CI days.
  t.mock.timers.enable({ apis: ['Date'], now: +f.now });
  f.missionRow.payload.structuredMission.immutable = true;
  f.config.missionHash = hash(f.missionRow.payload);
  f.program.scope_hash = hash(missionScope(f.missionRow.payload));
  f.config.scopeHash = f.program.scope_hash;
  await pool.query('UPDATE acquisition_missions SET payload=$1 WHERE id=$2', [f.missionRow.payload, f.missionRow.id]);
  await pool.query('UPDATE acquisition_outbound_programs SET scope_hash=$1 WHERE id=$2', [f.program.scope_hash, f.program.id]);
  const sourceMission = { ...f.missionRow.payload, id: f.program.source_mission_id, stage: 'plan' };
  delete sourceMission.pendingOperatorDecision;
  await insert(pool, 'acquisition_missions', { ...f.missionRow, id: f.program.source_mission_id, stage: 'plan',
    payload: sourceMission });
  await applyScopedSchema(pool);
  const frozen = await prepare.prepareExistingApproved(pool, f.config, { now: f.now, env: f.env });
  const envelope = (await pool.query('SELECT * FROM acquisition_outbound_envelopes WHERE id=$1', [frozen.envelopeId])).rows[0];
  const grant = scopedResumeFixture().manifest;
  grant.scopeHash = f.program.scope_hash;
  grant.scopes.email.approvalMode = 'delegated_existing_program';
  grant.scopes.preparation = { enabled: true, approvalReference: 'local-explicit-canonical-delegation',
    dailyCap: 1, totalCap: 1, dailyItemCap: 1, totalItemCap: 1, operations: ['approve_canonical_envelope'] };
  const bound = await scope.readScopedIdentity(pool, { tenantId: '10', ...grant.admissions[0] }, { lock: false });
  grant.admissions[0].identityFingerprint = bound.identityFingerprint;
  const env = { ANCHOR_EMAIL_SCOPED_RESUME_ID: grant.id };
  const keys = ['ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID'];
  const beforeEnv = Object.fromEntries(keys.map(key => [key, process.env[key]]));
  for (const key of keys) delete process.env[key];
  process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = grant.id;
  t.after(() => { for (const key of keys) { if (beforeEnv[key] === undefined) delete process.env[key]; else process.env[key] = beforeEnv[key]; } });
  await control.installScopedResume(pool, { manifest: grant, approvalReference: grant.approvalReference },
    { id: f.operator.id, role: f.operator.role }, { now: f.now, env });
  const oldBefore = (await pool.query('SELECT row_to_json(i) AS row FROM acquisition_outbound_items i WHERE id=$1', [f.quarantine.item_id])).rows[0].row;
  const calls = [], audited = harness.auditPool(pool, calls);
  const service = scopedService(audited, f);
  const result = await service.__testBindApproval(f.program, envelope);
  assert.equal(result.status, 'authorized');
  const approvals = (await pool.query("SELECT payload FROM acquisition_mission_contributions WHERE mission_id=$1 AND specialist='operator' AND kind='approval'", [envelope.mission_id])).rows;
  assert.equal(approvals.length, 1);
  assert.equal(approvals[0].payload.payload.approvedBy, f.operator.id);
  assert.equal(approvals[0].payload.payload.preparedArtifactRevision, envelope.revision);
  assert.equal(approvals[0].payload.payload.dailyEnvelope.id, envelope.id);
  assert.ok(calls.some(sql => /INSERT INTO acquisition_mission_execution_audit/.test(sql)));
  assert.ok(calls.some(sql => /INSERT INTO acquisition_missions/.test(sql)));
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM anchor_email_recovery_reservations')).rows[0].n, 0);
  assert.deepEqual((await pool.query('SELECT operation,item_count FROM anchor_scoped_resume_consumption')).rows,
    [{ operation: 'approve_canonical_envelope', item_count: 0 }]);
  await assert.rejects(service.__testBindApproval(f.program, envelope));
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_mission_contributions WHERE mission_id=$1 AND specialist='operator' AND kind='approval'", [envelope.mission_id])).rows[0].n, 1);
  assert.deepEqual((await pool.query('SELECT row_to_json(i) AS row FROM acquisition_outbound_items i WHERE id=$1', [f.quarantine.item_id])).rows[0].row, oldBefore);
});
