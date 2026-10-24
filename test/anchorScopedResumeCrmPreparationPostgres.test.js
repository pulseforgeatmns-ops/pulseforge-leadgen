'use strict';
// Real PostgreSQL and native CER/TME/specialists. Only synthetic local records.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { createRequire } = require('node:module');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { applyScopedSchema } = require('./helpers/anchorScopedResumePostgresFixture');
const { applyCrmPreparationSchema } = require('./helpers/anchorCrmPreparationSchema');
const { scopedResumeFixture } = require('./helpers/anchorScopedResumeFixture');
const amo = require('../packages/acquisition-mission');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const persistence = require('../services/acquisitionMissionPersistence');
const scope = require('../services/anchorScopedResume');
const control = require('../services/anchorScopedResumeControl');
const { prepareExistingCrm } = require('../services/anchorScopedResumeCrmPreparation');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
function harness() {
  const filename = path.join(__dirname, 'anchorDailyOutboundRecoveryApprovalPostgres.test.js');
  const local = createRequire(filename), module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + '\nmodule.exports={setup,auditPool};', {
    module, exports: module.exports, __dirname: path.dirname(filename), process, console,
    require(id) { return id === 'node:test' ? () => {} : local(id); },
  }, { filename });
  return module.exports;
}
const h = harness();
async function setup(pool) {
  const f = await h.setup(pool);
  await pool.query('DELETE FROM acquisition_mission_contributions; DELETE FROM acquisition_mission_events; DELETE FROM acquisition_missions');
  const plan = amo.freezeStructuredMission(amo.createStructuredMission({ missionType: 'acquisition',
    objective: 'Prepare first-touch cleaning outreach for property management firms', successMetric: { type: 'replies', target: 1 },
    market: { segment: 'property_management', industry: 'property_management', buyer: 'property manager', label: 'Property managers' },
    geography: { region: 'United States', scope: 'nationwide', cities: [] }, constraints: ['email_only'], priority: 1,
    evidence: { minimumConfidence: 0.5 } }), { approvedBy: f.operator.id });
  const source = amo.createMission({ id: f.program.source_mission_id, tenantId: '10', clientId: 10,
    objective: plan.objective, targetSegment: 'property_management', structuredMission: plan, constraints: ['email_only'] });
  await persistence.persistStageCommit({ mission: source }, pool, { skipEnsure: true });
  f.program.scope_hash = hash(missionScope(source));
  await pool.query('UPDATE acquisition_outbound_programs SET scope_hash=$1 WHERE id=$2', [f.program.scope_hash, f.program.id]);
  await pool.query(`ALTER TABLE companies ADD COLUMN location text,ADD COLUMN industry text;
    ALTER TABLE prospects ADD COLUMN vertical text,ADD COLUMN service_area_match text;
    ALTER TABLE clients ADD COLUMN created_at timestamptz,ADD COLUMN warmup_start_date timestamptz;
    CREATE TABLE email_events(client_id integer,event_type text,event_at timestamptz,sender_identity_status text);
    CREATE TABLE agent_log(client_id integer,agent_name text,action text,ran_at timestamptz);
    CREATE TABLE acquisition_outbound_inbox_health(tenant_id text,integration_id text,last_success_at timestamptz);
    UPDATE prospects SET vertical='property_management',service_area_match='United States';
    UPDATE companies SET location='New York, United States',industry='property_management';
    UPDATE clients SET created_at='2026-08-01',warmup_start_date='2026-08-01';`);
  await pool.query("INSERT INTO acquisition_outbound_inbox_health VALUES('10',$1,$2)", [f.integration.id, f.now]);
  await pool.query(`INSERT INTO agent_log SELECT 10,'emmett','email_sent',$1::timestamptz - d * interval '1 day' FROM generate_series(1,20) d`, [f.now]);
  await pool.query("INSERT INTO email_events VALUES(10,'sent','2026-08-01T12:00:00Z','match'),(10,'sent','2026-10-18T12:00:00Z','match')");
  await applyScopedSchema(pool);
  await applyCrmPreparationSchema(pool);
  f.manifest = scopedResumeFixture().manifest;
  f.manifest.scopeHash = f.program.scope_hash;
  f.manifest.scopes.email = { enabled: false };
  f.manifest.scopes.preparation = { enabled: true, approvalReference: 'local-explicit-crm-only-preparation',
    dailyCap: 2, totalCap: 2, dailyItemCap: 5, totalItemCap: 5, operations: ['prepare_existing_crm'] };
  f.manifest.admissions[0].identityFingerprint = (await scope.readScopedIdentity(pool,
    { tenantId: '10', ...f.manifest.admissions[0] }, { lock: false })).identityFingerprint;
  f.env = { ANCHOR_EMAIL_SCOPED_RESUME_ID: f.manifest.id };
  f.input = { programId: f.program.id, sourceMissionHash: hash(source), admissions: f.manifest.admissions };
  f.source = source;
  return f;
}
const install = (pool, f) => control.installScopedResume(pool, { manifest: f.manifest, approvalReference: f.manifest.approvalReference },
  { id: f.operator.id, role: f.operator.role }, { env: f.env, now: f.now });
const run = (pool, f, input = f.input) => prepareExistingCrm(pool, input, { env: f.env, now: f.now });
async function count(pool, table) { return (await pool.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n; }

async function extraAdmission(pool, f, n) {
  const prospectId = `30000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
  const companyId = `40000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
  const domain = `synthetic-extra-${n}.test`, email = `fresh@${domain}`;
  await pool.query(`INSERT INTO companies(id,client_id,name,domain,website,location,industry)
    VALUES($1,10,$2,$3,$4,'New York, United States','property_management')`, [companyId, `Synthetic Extra ${n}`, domain, `https://${domain}`]);
  await pool.query(`INSERT INTO prospects(id,client_id,company_id,email,do_not_contact,is_synthetic,ao_outreach_review_required,
    email_verified,email_status,enrichment_provenance,vertical,service_area_match)
    VALUES($1,10,$2,$3,false,false,false,true,'verified','{"email":{"source":"website_email"}}','property_management','United States')`, [prospectId,companyId,email]);
  const bound = await scope.readScopedIdentity(pool, {tenantId:'10',prospectId,companyId,recipientEmail:email}, {lock:false});
  return {prospectId,companyId,recipientEmail:email,companyDomain:bound.companyDomain,companyNameKey:bound.companyNameKey,
    identityFingerprint:bound.identityFingerprint,approvalReference:'local-explicit-extra-synthetic-review'};
}
function afterConsumption(pool, action) {
  let acted = false;
  return {query:pool.query.bind(pool),async connect(){const db=await pool.connect();let consumed=false;
    return {release:db.release.bind(db),async query(q,v){const sql=typeof q==='string'?q:q.text;
      if (/INSERT INTO anchor_scoped_resume_consumption/.test(sql)) consumed=true;
      const result=await db.query(q,v);
      if(sql==='COMMIT' && consumed && !acted){acted=true;await action();}
      return result;}};
  }};
}

test('native CRM-only scoped preparation from discover without providers or bootstrap', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('anchor-crm-prep-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 12 });
  t.after(async () => { resetAcquisitionMissionRuntime(); await pool.end(); await pg.stop(); });
  const keys = ['ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID'];
  const previous = Object.fromEntries(keys.map(k => [k, process.env[k]]));
  keys.forEach(k => delete process.env[k]); process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = 'local-scoped-resume';
  t.after(() => keys.forEach(k => previous[k] === undefined ? delete process.env[k] : process.env[k] = previous[k]));
  t.mock.timers.enable({ apis: ['Date'], now: +new Date('2026-10-19T16:00:00Z') });
  let externalCalls = 0;
  const forbidden = () => { externalCalls++; throw new Error('external provider forbidden'); };
  for (const method of ['get','post','request']) t.mock.method(require('axios'), method, forbidden);
  t.mock.method(require('node:https'), 'request', forbidden);
  t.mock.method(globalThis, 'fetch', forbidden);

  await t.test('real discover source becomes a linked review-only READY child with fresh native artifacts', async () => {
    const f = await setup(pool); await install(pool, f);
    assert.equal(await count(pool, 'acquisition_mission_contributions'), 0);
    const calls = [], audited = h.auditPool(pool, calls);
    const result = await run(audited, f);
    assert.equal(result.readyForReview, true); assert.equal(result.providerCalls, 0);
    assert.equal(result.executionApproved, false); assert.equal(result.envelopeAuthorized, false); assert.equal(result.sent, 0);
    const saved = await persistence.loadMissionSnapshot(result.missionId, '10', pool, { skipEnsure: true });
    assert.equal(saved.mission.stage, 'ready'); assert.equal(saved.mission.orchestrationMissionId, f.source.id);
    assert.equal(saved.mission.pendingOperatorDecision.kind, 'execution_approval');
    assert.equal(saved.mission.crmPreparation.providerReadiness, 'not_checked');
    const capacity = require('../packages/acquisition-mission/ContributionSupersession').unwrapSpecialistPayload(amo.findEmmettCapacity(saved.contributions, saved.mission));
    assert.equal(capacity.preparationEvidence.authentication.spf.state, 'UNKNOWN');
    assert.equal(capacity.preparationEvidence.authentication.dkim.state, 'UNKNOWN');
    assert.equal(capacity.preparationEvidence.blacklist.listed, null);
    assert.equal(capacity.capacity.recommended, 1);
    assert.doesNotMatch(JSON.stringify(saved), /No blacklist concerns|Healthy\. Safe\. Send\./);
    assert.match(JSON.stringify(saved), /Blacklist not checked/);
    for (const [specialist, kind] of [['scout','discovery'],['max','prioritization'],['max','acquisition_approach'],['paige','variants'],['emmett','capacity']]) {
      assert.ok(saved.contributions.some(c => c.specialist === specialist && c.kind === kind), `${specialist}/${kind}`);
    }
    const scout = saved.contributions.find(c => c.specialist === 'scout' && c.kind === 'discovery');
    assert.match(JSON.stringify(scout), /existing canonical CRM|existing_crm|Existing canonical CRM/);
    const variants = amo.findPaigeVariants(saved.contributions, saved.mission);
    const copy = require('../packages/acquisition-mission/ContributionSupersession').unwrapSpecialistPayload(variants);
    assert.equal(copy.variants.length, 1); assert.notEqual(copy.variants[0].subject, 'Synthetic approved subject');
    assert.ok(calls.some(sql => /INSERT INTO acquisition_mission_execution_audit/.test(sql)));
    assert.deepEqual((await pool.query('SELECT payload FROM acquisition_missions WHERE id=$1', [f.source.id])).rows[0].payload, f.source);
    assert.deepEqual((await pool.query('SELECT operation,item_count FROM anchor_scoped_resume_consumption')).rows,
      [{ operation: 'prepare_existing_crm', item_count: 1 }]);
    const before = await count(pool, 'acquisition_mission_contributions');
    await assert.rejects(run(audited, f), /operation_consumed/);
    await assert.rejects(run(audited, f, { ...f.input, operationId: 'different-id' }), /input_invalid/);
    assert.equal(await count(pool, 'acquisition_mission_contributions'), before);
    assert.equal(await count(pool, 'anchor_email_recovery_reservations'), 0);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_outbound_envelopes WHERE local_day='2026-10-19'")).rows[0].n, 0);
    assert.equal(externalCalls, 0);
  });

  await t.test('default closed, exact admission and source/hash/tenant checks precede canonical writes', async () => {
    const f = await setup(pool);
    await assert.rejects(run(pool, f));
    await install(pool, f);
    for (const input of [{ ...f.input, admissions: [] }, { ...f.input, programId: 'different' },
      { ...f.input, sourceMissionHash: 'a'.repeat(64) }, { ...f.input, admissions: [{ ...f.input.admissions[0], recipientEmail: 'other@synthetic-rejection.test' }] }]) {
      await assert.rejects(run(pool, f, input));
    }
    assert.equal(await count(pool, 'anchor_scoped_resume_consumption'), 0);
    assert.equal(await count(pool, 'acquisition_mission_contributions'), 0);
  });

  await t.test('previously attempted inputs, missing review, provenance drift and stale telemetry reject without effects', async () => {
    for (const mutation of [
      "INSERT INTO acquisition_mission_outbound_executions(id,tenant_id,mission_id,prospect_id,status,payload) SELECT 'prior','10','prior-mission',company_id,'attempted',jsonb_build_object('email',email) FROM prospects WHERE ao_outreach_review_required=false",
      'UPDATE prospects SET ao_outreach_review_required=NULL WHERE ao_outreach_review_required=false',
      "UPDATE prospects SET enrichment_provenance='{}' WHERE ao_outreach_review_required=false",
      "UPDATE acquisition_outbound_inbox_health SET last_success_at='2026-10-18T00:00:00Z'",
      'ALTER TABLE prospects DROP COLUMN ao_outreach_review_required',
      'DROP TABLE email_events',
    ]) {
      const f = await setup(pool); await install(pool, f); await pool.query(mutation);
      const calls = []; await assert.rejects(run(h.auditPool(pool, calls), f));
      assert.equal(await count(pool, 'anchor_scoped_resume_consumption'), 0);
      assert.equal(await count(pool, 'acquisition_mission_contributions'), 0);
      assert.equal(externalCalls, 0);
    }
  });
  await t.test('fixed batch limit and independent item cap reject before canonical effects', async () => {
    const f = await setup(pool);
    for(let n=2;n<=6;n++) f.manifest.admissions.push(await extraAdmission(pool,f,n));
    f.input.admissions = f.manifest.admissions;
    await install(pool,f);
    await assert.rejects(run(pool,f), /input_invalid/);
    assert.equal(await count(pool,'anchor_scoped_resume_consumption'),0);
    assert.equal(await count(pool,'acquisition_mission_contributions'),0);
    const g=await setup(pool);g.manifest.admissions.push(await extraAdmission(pool,g,2));
    g.manifest.scopes.preparation.dailyItemCap=1;g.manifest.scopes.preparation.totalItemCap=1;
    g.input.admissions=g.manifest.admissions;await install(pool,g);
    // Native capacity may itself reject a two-row batch earlier; independently
    // prove the operation ledger refuses the two admitted draft identities.
    await assert.rejects(control.consumeScopedOperation(pool,{manifest:g.manifest,capability:'preparation',operation:'prepare_existing_crm',
      operationId:'local-two-draft-cap-check',identities:g.manifest.admissions},{env:g.env,now:g.now}),/cap_reached/);
    assert.equal(await count(pool,'anchor_scoped_resume_consumption'),0);
  });
  await t.test('concurrent identical batch has one permanent operation and one canonical child', async () => {
    const f=await setup(pool);await install(pool,f);
    const results=await Promise.allSettled([run(pool,f),run(pool,f)]);
    assert.equal(results.filter(r=>r.status==='fulfilled').length,1);
    assert.match(results.find(r=>r.status==='rejected').reason.message,/operation_consumed/);
    assert.equal(await count(pool,'anchor_scoped_resume_consumption'),1);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_missions WHERE id LIKE 'mission_crm_%'")).rows[0].n,1);
  });
  await t.test('late review/revoke/schema changes after permanent reservation cannot start canonical writes or refund the operation', async () => {
    for(const change of ['review','revoke','schema','source']) {
      const f=await setup(pool);await install(pool,f);
      const hooked=afterConsumption(pool,async()=>{
        if(change==='review') await pool.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1',[f.prospect.id]);
        else if(change==='revoke') await control.revokeScopedResume(pool,{manifestId:f.manifest.id,approvalReference:'local-explicit-revoke'},
          {id:f.operator.id,role:f.operator.role},{env:f.env,now:f.now});
        else if(change==='schema') await pool.query('DROP TABLE acquisition_mission_observations');
        else await pool.query("UPDATE acquisition_missions SET payload=jsonb_set(payload,'{objective}','\"Changed source\"') WHERE id=$1",[f.source.id]);
      });
      await assert.rejects(run(hooked,f));
      assert.equal(await count(pool,'anchor_scoped_resume_consumption'),1);
      assert.equal(await count(pool,'acquisition_mission_contributions'),0);
      assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_missions WHERE id LIKE 'mission_crm_%'")).rows[0].n,0);
      if(change==='review') {
        await pool.query('UPDATE prospects SET ao_outreach_review_required=false WHERE id=$1',[f.prospect.id]);
        await assert.rejects(run(pool,f),/operation_consumed/);
      }
      assert.equal(externalCalls,0);
    }
  });

  await t.test('native READY drafts freeze through supported prepared/store APIs only with both operations and 2N item budget', async () => {
    for(const sufficient of [true,false]) {
      const f=await setup(pool);
      f.manifest.scopes.preparation.operations.push('reuse_existing_artifacts','freeze');
      f.manifest.scopes.preparation.dailyItemCap=sufficient?2:1;
      f.manifest.scopes.preparation.totalItemCap=sufficient?2:1;
      await install(pool,f);
      const calls=[],audited=h.auditPool(pool,calls);
      const result=await run(audited,f);
      const saved=await persistence.loadMissionSnapshot(result.missionId,'10',audited,{skipEnsure:true});
      const prepared=await require('../services/governedOutboundAdapters').adapters(audited,{tenantId:'10'}).prepared(saved,f.program,{preinitializedSchema:true});
      const entries=prepared.candidates.map(candidate=>{
        const admitted=f.manifest.admissions.find(a=>a.companyId===String(candidate.item.companyId) && a.recipientEmail===candidate.item.email);
        assert.ok(admitted);
        assert.equal(candidate.candidateId,admitted.companyId);
        assert.equal(candidate.item.prospectId,admitted.companyId);
        // Native queue uses the company candidate key; the exact reviewed CRM
        // contact is carried separately by admissions and the frozen item.
        assert.equal(candidate.item.crmProspectId,null);
        assert.equal(candidate.item.paige.candidateId,admitted.companyId);
        assert.notEqual(candidate.candidateId,admitted.prospectId);
        return {candidateId:candidate.candidateId,prospectId:admitted.prospectId,companyId:admitted.companyId,
          email:admitted.recipientEmail,message:candidate.message,sender:prepared.sender,revision:prepared.revision};
      });
      const store=new (require('../services/governedOutboundStore').GovernedOutboundStore)(audited,'10');
      const freezing=()=>store.freeze(f.program,'2026-10-19',result.missionId,prepared.revision,entries,{env:f.env,now:f.now});
      if(!sufficient) {
        await assert.rejects(freezing(),/cap_reached/);
        assert.equal(await count(pool,'anchor_scoped_resume_consumption'),1);
        assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_outbound_envelopes WHERE local_day='2026-10-19'")).rows[0].n,0);
      } else {
        const envelope=await freezing();
        assert.equal(envelope.status,'frozen');assert.equal(envelope.approval_id,null);assert.equal(envelope.mission_id,result.missionId);
        assert.equal(envelope.revision,result.preparedArtifactRevision);assert.deepEqual(envelope.manifest,entries);
        const budget=(await pool.query('SELECT count(*)::int AS operations,sum(item_count)::int AS items FROM anchor_scoped_resume_consumption')).rows[0];
        assert.deepEqual(budget,{operations:2,items:2});
        const item=(await store.items(envelope.id))[0];assert.equal(item.status,'pending');assert.equal(item.attempted_at,null);assert.equal(item.provider_message_id,null);
        assert.equal(item.candidate_id,f.manifest.admissions[0].companyId);
        assert.equal(item.prospect_id,f.manifest.admissions[0].prospectId);
        assert.equal(item.company_id,f.manifest.admissions[0].companyId);
        assert.deepEqual(item.snapshot.message,prepared.candidates[0].message);
      }
      assert.equal(require('../packages/acquisition-mission/ExecutionApproval').findExecutionApprovals(saved.contributions).length,0);
      assert.equal(await count(pool,'anchor_email_recovery_reservations'),0);
      assert.equal(externalCalls,0);
    }
  });
  await t.test('an entirely previously attempted exact batch cannot become fresh draft inventory',async()=>{
    const f=await setup(pool);f.manifest.admissions.push(await extraAdmission(pool,f,2));f.input.admissions=f.manifest.admissions;
    await install(pool,f);
    await pool.query(`INSERT INTO acquisition_mission_outbound_executions(id,tenant_id,mission_id,prospect_id,status,payload)
      SELECT 'previous_'||id,'10','previous-mission',company_id,'sent',jsonb_build_object('email',email)
      FROM prospects WHERE ao_outreach_review_required=false`);
    await assert.rejects(run(pool,f));
    assert.equal(await count(pool,'anchor_scoped_resume_consumption'),0);
    assert.equal(await count(pool,'acquisition_mission_contributions'),0);
    assert.equal(externalCalls,0);
  });

});
