'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { scopedResumeFixture } = require('./helpers/anchorScopedResumeFixture');
const filename = path.join(__dirname, '../scripts/anchorCrmPreparationOnce.js');
const localRequire = createRequire(filename), code = fs.readFileSync(filename, 'utf8');
function fixture() {
  const f = scopedResumeFixture();
  f.sourceMission.stage = 'discover'; f.sourceMission.status = 'Discovering';
  f.manifest.scopes.email = { enabled: false };
  f.manifest.scopes.preparation = { enabled: true, approvalReference: 'local-synthetic-prepare',
    dailyCap: 2, totalCap: 2, dailyItemCap: 2, totalItemCap: 2, operations: ['prepare_existing_crm'] };
  f.env.RAILWAY_GIT_COMMIT_SHA = 'a'.repeat(40); f.env.DATABASE_URL = 'local-synthetic-db-not-a-secret';
  f.argv = ['--tenant','10','--revision',f.env.RAILWAY_GIT_COMMIT_SHA,'--manifest-sha256',hash(f.manifest),'--source-sha256',hash(f.sourceMission)];
  return f;
}
function load(f, { poolOverride, apply } = {}) {
  const calls = [], events = [], output = [], opened = [];
  const original = f.db.query;
  const db = { async query(q, values = []) {
    const sql = typeof q === 'string' ? q : q.text; calls.push(sql);
    if (/^(BEGIN|SET LOCAL|COMMIT|ROLLBACK)/.test(sql)) return { rows: [] };
    return original.call(f.db, q, values);
  }, release() { events.push('release'); } };
  const pool = poolOverride || { on() {}, connect: async () => db, end: async () => events.push('end') };
  const contextProcess = { env: f.env, argv: [], stdout: { write: s => output.push(s) }, stderr: { write: s => output.push(s) } };
  const module = { exports: {} };
  const FakeDate = class extends Date { constructor(...args) { super(...(args.length ? args : [f.now])); } static now() { return +f.now; } };
  const requireFn = id => {
    if (id === 'pg') return { Pool: function(options) { opened.push(options); return pool; } };
    if (id === '../services/anchorScopedResumeCrmPreparation') {
      events.push('bridge-import');
      return { prepareExistingCrm: async (...args) => { events.push('apply');
        if (apply) return apply(...args);
        return { readyForReview: true, candidateCount: 1, missionId: 'mission_crm_'+'b'.repeat(32),
          executionApproved: false, envelopeAuthorized: false, sent: 0, providerCalls: 0 }; } };
    }
    return localRequire(id);
  };
  requireFn.main = null;
  vm.runInNewContext(code, { require: requireFn, module, exports: module.exports, process: contextProcess,
    Date: FakeDate, Set, Proxy, Reflect, Object, Error, JSON, console }, { filename });
  return { ...module.exports, calls, events, output, opened, contextProcess };
}
test('one-shot default read-only validates exact installed manifest/source without locks, writes or loading bridge', async () => {
  const f = fixture(), h = load(f);
  const before = JSON.stringify(f.env), result = await h.run(f.argv, f.env);
  assert.equal(result.mode, 'read_only'); assert.equal(result.completeEligibilityChecked, false);
  assert.equal(result.writesAttempted, false); assert.equal(result.candidateCount, 1);
  assert.equal(h.calls[0], 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
  assert.ok(!h.calls.some(sql => /FOR SHARE|FOR UPDATE|advisory|INSERT |UPDATE |DELETE |ALTER |CREATE /i.test(sql)));
  assert.deepEqual(h.events, ['release','end']); assert.equal(JSON.stringify(f.env), before);
});
test('missing, duplicate, unknown, other-tenant and malformed approval flags reject before DB', async () => {
  const f = fixture();
  for (const argv of [[],f.argv.slice(2),[...f.argv,'--apply','--apply'],[...f.argv,'--secret','x'],
    f.argv.map(x => x === '10' ? '13' : x),f.argv.map(x => x === hash(f.manifest) ? 'placeholder' : x)]) {
    const h=load(f); await assert.rejects(h.run(argv,f.env), /invocation_rejected/); assert.equal(h.opened.length,0);
  }
  const h=load(f);await assert.rejects(h.run(f.argv,{...f.env,RAILWAY_GIT_COMMIT_SHA:'c'.repeat(40)}),/invocation_rejected/);assert.equal(h.opened.length,0);
});
test('invalid/changed/revoked manifest, activation, source and operator fail closed without invoking bridge', async () => {
  for (const change of ['hash','invalid','revoked','activation','maintenance','source','operator','email','overbatch']) {
    const f=fixture();
    if(change==='hash') f.manifest.approvalReference='changed';
    if(change==='invalid') f.manifest.tenantId='13';
    if(change==='revoked') f.installation={revoked_at:f.now.toISOString()};
    if(change==='activation') delete f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;
    if(change==='maintenance') f.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE='true';
    if(change==='source') f.sourceMission.objective='changed';
    if(change==='operator') f.operator={active:false};
    if(change==='email') {f.manifest.scopes.email={enabled:true};f.argv[5]=hash(f.manifest);}
    if(change==='overbatch') f.manifest.admissions=Array(6).fill(f.manifest.admissions[0]);
    const h=load(f);await assert.rejects(h.run([...f.argv,'--apply'],f.env),/invocation_rejected/);
    assert.ok(!h.events.includes('bridge-import'));assert.ok(h.calls.includes('ROLLBACK'));
  }
});
test('explicit apply releases preview transaction, pins existing activation, preserves live kill fields and calls only fixed API', async () => {
  const f=fixture(), h=load(f,{apply:async (_pool,input,{env})=>{
    assert.equal(h.events[0],'release');assert.equal(input.programId,f.program.id);assert.equal(input.sourceMissionHash,hash(f.sourceMission));
    assert.equal(hash(input.admissions),hash(f.manifest.admissions));assert.equal(env.ANCHOR_EMAIL_SCOPED_RESUME_ID,f.manifest.id);
    f.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE='true';assert.equal(env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE,'true');
    f.env.ANCHOR_EMAIL_SCOPED_RESUME_ID='replacement';assert.equal(env.ANCHOR_EMAIL_SCOPED_RESUME_ID,undefined);
    throw Object.assign(new Error('secret-person@example.test SQL payload'),{code:'raw-secret'});
  }});
  await assert.rejects(h.run([...f.argv,'--apply'],f.env),e=>e.code==='anchor_crm_invocation_rejected'&&e.writesMayHaveOccurred===true&&!e.message.includes('@'));
  assert.deepEqual(h.events,['release','bridge-import','apply','end']);
});
test('successful explicit apply returns only bounded non-PII summary and closure never edits environment', async () => {
  const f=fixture(),h=load(f),before=JSON.stringify(f.env),result=await h.run([...f.argv,'--apply'],f.env);
  assert.equal(result.mode,'applied');assert.equal(result.sent,0);assert.equal(result.executionApproved,false);
  assert.equal(JSON.stringify(f.env),before);assert.ok(!JSON.stringify(result).includes('@'));assert.ok(!JSON.stringify(result).includes(f.env.DATABASE_URL));
});
test('CLI output suppresses nested raw diagnostics and emits sanitized possibly-written failure only', async () => {
  const f=fixture();let h;
  h=load(f,{apply:async()=>{h.contextProcess.stdout.write('private-person@example.test');h.contextProcess.stderr.write('secret-connection');throw new Error('secret-payload');}});
  await h.cli([...f.argv,'--apply']);const output=h.output.join('');
  assert.equal(h.contextProcess.exitCode,1);assert.equal(output,JSON.stringify({status:'rejected',code:'anchor_crm_invocation_rejected',writesMayHaveOccurred:true})+'\n');
});
module.exports = { load, fixture };
