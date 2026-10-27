'use strict';
// Actual canonical APPROVE_EXECUTION and disposable PostgreSQL; fake provider only.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {createHash}=require('node:crypto');
const {createTrackedPostgresPool,closePostgresFixture}=require('./helpers/closePostgresPool');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {seedPrepareExisting}=require('./helpers/anchorRecoveryPrepareExistingPostgresFixture');
const {hash}=require('../packages/acquisition-mission/DailyOutboundPolicy');
const prepare=require('../services/anchorEmailRecoveryPrepareExisting');
const recovery=require('../services/anchorEmailRecovery');
const integration=require('../services/anchorEmailRecoveryIntegration');
const {bindRecoveryApproval}=require('../services/anchorEmailRecoveryApproval');
const persistence=require('../services/acquisitionMissionPersistence');
const {adapters}=require('../services/governedOutboundAdapters');
const {resetAcquisitionMissionRuntime}=require('../services/acquisitionMissionRuntime');
const {GovernedOutboundStore}=require('../services/governedOutboundStore');
const options=f=>({now:f.now,env:f.env});
function auditPool(pool,calls){
  const query=(db,q,v)=>{const sql=typeof q==='string'?q:q.text;calls.push(sql);
    assert.doesNotMatch(sql,/\b(?:CREATE|ALTER|DROP|TRUNCATE)\b/i,'recovery approval must never initialize or migrate schema');
    return db.query(q,v);};
  return {query:(q,v)=>query(pool,q,v),async connect(){const c=await pool.connect();return{query:(q,v)=>query(c,q,v),release:c.release.bind(c)};}};
}
async function setup(pool){
  resetAcquisitionMissionRuntime();
  await pool.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public');
  const f=await seedPrepareExisting(pool);
  await pool.query(`ALTER TABLE acquisition_missions ADD COLUMN campaign text,ADD COLUMN title text,
    ADD COLUMN priority text DEFAULT 'normal',ADD COLUMN confidence float8,ADD COLUMN owner text,ADD COLUMN created_by text;`);
  // These schema prerequisites are installed explicitly by test setup, never by recovery.
  await persistence.ensureAcquisitionMissionSchema(pool);await persistence.ensureExecutionAuditSchema(pool);
  await pool.query(`INSERT INTO users(id,client_id,role,active) VALUES('1',10,'manager',true);
    CREATE FUNCTION local_observe_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;`);
  for(const table of ['clients','users','prospects','companies','ao_leads','ao_contacts'])await pool.query(`CREATE TRIGGER acquisition_outbound_observe BEFORE UPDATE ON ${table} FOR EACH ROW EXECUTE FUNCTION local_observe_noop()`);
  Object.assign(f.missionRow.payload,{status:'active',priority:'normal',structuredMissionApproved:true,
    pendingOperatorDecision:{stage:'ready',kind:'execution_approval',prompt:'Synthetic explicit approval pending'}});
  Object.assign(f.artifactRows[0].payload.payload,{summary:'Synthetic current-day evidence for local canonical test only.',
    qualifiedCount:1,rankedProspects:[{id:f.config.candidate.candidateId,name:'Fresh Synthetic Enterprise',readinessState:'unknown'}],
    evidence:[{source:'synthetic-local-evidence',label:'Synthetic attributed local evidence'}]});
  f.config.missionHash=hash(f.missionRow.payload);
  f.config.artifacts=Object.fromEntries(f.artifactRows.map(row=>[row.specialist,{id:row.id,hash:hash(row.payload)}]));
  f.config.contributionSetHash=prepare.contributionSetHash(f.artifactRows);
  f.config.preparedRevision=prepare.preparedRevision(f.missionRow.id,f.artifactRows.map(row=>row.payload));
  await pool.query('UPDATE acquisition_missions SET payload=$1 WHERE id=$2',[f.missionRow.payload,f.missionRow.id]);
  for(const row of f.artifactRows)await pool.query('UPDATE acquisition_mission_contributions SET payload=$1 WHERE id=$2',[row.payload,row.id]);
  return f;
}
async function frozen(pool,f){
  const result=await prepare.prepareExistingApproved(pool,f.config,options(f));
  f.approvalInput={operatorId:f.operator.id,manifest:{...result.proposedManifest,id:'local-final-one-shot-grant',approvalReference:'synthetic-final-reviewed-recipient-copy-sender'}};
  f.env.ANCHOR_EMAIL_RECOVERY_APPROVAL_ID=f.approvalInput.manifest.id;
  return result;
}
function authorize(pool,f,input,dispatch){
  const module={exports:{}};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../utils/callEligibility.js'),'utf8'),{module,exports:module.exports,require(id){
    if(id==='./anchorOutreachReview')return require('../utils/anchorOutreachReview');
    if(id==='../services/anchorEmailRecovery')return{loadRecoveryState:db=>recovery.loadRecoveryState(db,options(f)),assertRecoveryLeaf:(db,i)=>recovery.assertRecoveryLeaf(db,i,options(f))};
    if(id==='../services/anchorEmailRecoveryIntegration')return{...integration,consumeRecoveryDispatch:(p,auth)=>integration.consumeRecoveryDispatch(p,auth,{now:()=>f.now,env:f.env})};
    if(id==='../services/anchorScopedResume')return require('../services/anchorScopedResume');
    throw Error('Unexpected guard dependency '+id);
  }});
  return module.exports.withEmailAuthorization(pool,input.identities,dispatch,{context:input.context,command:input.command});
}
test('native recovery freeze to canonical approval and one-shot leaf',{timeout:120000},async t=>{
  const pg=await startDisposablePostgres('anchor-recovery-approval-');
  const pool=createTrackedPostgresPool({connectionString:pg.connectionString,max:12});
  t.after(async()=>{resetAcquisitionMissionRuntime();await closePostgresFixture(pool,pg);});
  await t.test('actual canonical approval creates no DDL and remains closed until separately activated',async()=>{
    const f=await setup(pool);const before=(await pool.query('SELECT row_to_json(i) AS row FROM acquisition_outbound_items i WHERE id=$1',[f.quarantine.item_id])).rows[0].row;
    const frozenResult=await frozen(pool,f);const calls=[];const audited=auditPool(pool,calls);const adapter=adapters(audited,{tenantId:'10'});
    const result=await bindRecoveryApproval(audited,f.approvalInput,{id:f.operator.id,role:f.operator.role},adapter,options(f));
    assert.equal(result.bound,true);assert.equal(result.providerAuthorized,false);assert.equal(result.activationInstalled,false);
    const approval=(await pool.query('SELECT payload FROM acquisition_mission_contributions WHERE id=$1',[result.canonicalApprovalId])).rows[0].payload;
    assert.equal(approval.specialist,'operator');assert.equal(approval.kind,'approval');assert.equal(approval.payload.approved,true);
    assert.equal(approval.payload.approvedBy,f.operator.id);assert.equal(approval.payload.dailyEnvelope.id,frozenResult.envelopeId);
    assert.ok(calls.some(sql=>/INSERT INTO acquisition_mission_execution_audit/.test(sql)));
    assert.ok(calls.some(sql=>/INSERT INTO acquisition_missions/.test(sql)));
    assert.equal((await recovery.loadRecoveryState(pool,options(f))).mode,'maintenance');
    assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest,null);
    // Activation is an explicitly separate test-only action, never a side effect of approval.
    // SPEC-262 tightens tenant-10 activation: explicit maintenance always wins.
    f.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE='false';
    f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID=result.proposedManifest.id;
    await pool.query('UPDATE anchor_email_recovery_control SET manifest=$1',[result.proposedManifest]);
    const previous=process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID;process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID=result.proposedManifest.id;
    try{
      const item=(await pool.query('SELECT * FROM acquisition_outbound_items WHERE id=$1',[frozenResult.itemId])).rows[0];
      const claimed=await new GovernedOutboundStore(pool,'10').claim(item,f.program,f.config.localDay,f.now);
      const key=`exec_${createHash('sha256').update([f.missionRow.id,claimed.candidate_id,f.config.preparedRevision].join(':')).digest('hex').slice(0,32)}`;
      const input={identities:[{clientId:10,prospectId:f.prospect.id}],context:{programId:f.program.id,envelopeId:item.envelope_id,itemId:item.id,provider:'brevo',transport:'governed_brevo'},
        command:{toEmail:item.email,subject:item.snapshot.message.subject,body:item.snapshot.message.body,sender:{email:f.client.sender_email,name:f.client.sender_name},idempotencyKey:key,credentialOverride:false}};
      input.command.actualPayload={sender:{...input.command.sender},to:[{email:input.command.toEmail,name:''}],
        subject:input.command.subject,textContent:input.command.body,htmlContent:require('../services/anchorScopedResume').brevoHtml(input.command.body),
        headers:{'Idempotency-Key':key}};
      let sends=0;await authorize(pool,f,input,async()=>{sends++;return{messageId:'synthetic-provider-only'};});
      await assert.rejects(authorize(pool,f,input,async()=>{sends++;}));assert.equal(sends,1);
      assert.equal((await pool.query('SELECT count(*)::int AS n FROM anchor_email_recovery_reservations')).rows[0].n,1);
      assert.ok((await pool.query('SELECT dispatch_started_at FROM anchor_email_recovery_reservations')).rows[0].dispatch_started_at);
    }finally{if(previous===undefined)delete process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID;else process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID=previous;}
    assert.deepEqual((await pool.query('SELECT row_to_json(i) AS row FROM acquisition_outbound_items i WHERE id=$1',[f.quarantine.item_id])).rows[0].row,before);
  });
  await t.test('independent canonical commit followed by envelope CAS loss stays closed and exact retry reuses approval',async()=>{
    const f=await setup(pool);await frozen(pool,f);const calls=[];const audited=auditPool(pool,calls);
    let sabotaged=false;
    const casLoss={query:audited.query,async connect(){const c=await audited.connect();return{release:c.release,query(q,v){
      const sql=typeof q==='string'?q:q.text;
      if(!sabotaged && /UPDATE acquisition_outbound_envelopes SET status='authorized'/.test(sql)){
        sabotaged=true;const values=[...(typeof q==='string'?v:q.values)];values[2]='no-match-synthetic-CAS';
        return c.query({...(typeof q==='string'?{text:q}:q),values});
      }
      return c.query(q,v);
    }};}};
    await assert.rejects(bindRecoveryApproval(casLoss,f.approvalInput,{id:f.operator.id,role:f.operator.role},adapters(casLoss,{tenantId:'10'}),options(f)),{code:'anchor_recovery_approval_binding_changed'});
    const approvals=await pool.query("SELECT id FROM acquisition_mission_contributions WHERE specialist='operator' AND kind='approval'");
    assert.equal(approvals.rows.length,1);assert.equal((await recovery.loadRecoveryState(pool,options(f))).mode,'maintenance');
    assert.equal((await pool.query("SELECT status FROM acquisition_outbound_envelopes WHERE local_day='2026-10-19'")).rows[0].status,'frozen');
    const result=await bindRecoveryApproval(audited,f.approvalInput,{id:f.operator.id,role:f.operator.role},adapters(audited,{tenantId:'10'}),options(f));
    assert.equal(result.canonicalApprovalId,approvals.rows[0].id);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_mission_contributions WHERE specialist='operator' AND kind='approval'")).rows[0].n,1);
    assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest,null);
  });
  await t.test('lost outer COMMIT preserves canonical approval but leaves activation closed and permits exact binding retry',async()=>{
    const f=await setup(pool);await frozen(pool,f);const calls=[],audited=auditPool(pool,calls);
    let lost=false;
    const wrapped={query:audited.query,async connect(){const c=await audited.connect();let outer=false;return{release:c.release,query(q,v){
      const sql=typeof q==='string'?q:q.text;
      if(/FROM anchor_email_recovery_control.*FOR UPDATE/.test(sql))outer=true;
      if(outer && sql==='COMMIT' && !lost){lost=true;throw Object.assign(new Error('synthetic lost outer commit'),{code:'08006'});}
      return c.query(q,v);
    }};}};
    await assert.rejects(bindRecoveryApproval(wrapped,f.approvalInput,{id:f.operator.id,role:f.operator.role},adapters(wrapped,{tenantId:'10'}),options(f)),{code:'08006'});
    const approvals=(await pool.query("SELECT id FROM acquisition_mission_contributions WHERE specialist='operator' AND kind='approval'")).rows;
    assert.equal(approvals.length,1);assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest,null);
    assert.equal((await pool.query("SELECT status FROM acquisition_outbound_envelopes WHERE local_day='2026-10-19'")).rows[0].status,'frozen');
    const result=await bindRecoveryApproval(audited,f.approvalInput,{id:f.operator.id,role:f.operator.role},adapters(audited,{tenantId:'10'}),options(f));
    assert.equal(result.canonicalApprovalId,approvals[0].id);assert.equal(result.activationInstalled,false);
  });
  await t.test('wrong actor, held contact, copy drift and changed sender cannot mint canonical approval',async()=>{
    for(const kind of ['actor','hold','copy','sender']){
      const f=await setup(pool);await frozen(pool,f);const actor={id:f.operator.id,role:f.operator.role};
      if(kind==='actor')actor.id='different-operator';
      if(kind==='hold')await pool.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1',[f.prospect.id]);
      if(kind==='copy')f.approvalInput.manifest.admissions[0].body+=' unapproved';
      if(kind==='sender')await pool.query("UPDATE clients SET sender_name='Changed Synthetic Sender' WHERE id=10");
      const calls=[],audited=auditPool(pool,calls);
      await assert.rejects(bindRecoveryApproval(audited,f.approvalInput,actor,adapters(audited,{tenantId:'10'}),options(f)));
      assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_mission_contributions WHERE specialist='operator' AND kind='approval'")).rows[0].n,0);
      assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest,null);
    }
  });
  await t.test('mission or contribution changed before canonical lock cannot mint stale approval',async()=>{
    for(const kind of ['mission','contribution']){
      const f=await setup(pool);await frozen(pool,f);const calls=[],audited=auditPool(pool,calls);let changed=false;
      const racing={query:audited.query,async connect(){const c=await audited.connect();return{release:c.release,async query(q,v){
        const sql=typeof q==='string'?q:q.text,values=typeof q==='string'?v:q.values;
        if(!changed && /SELECT pg_try_advisory_lock/.test(sql) && values?.[0]===750825){
          changed=true;
          if(kind==='mission')await pool.query("UPDATE acquisition_missions SET payload=jsonb_set(payload,'{syntheticChanged}','true') WHERE id=$1",[f.missionRow.id]);
          else await pool.query("UPDATE acquisition_mission_contributions SET payload=jsonb_set(payload,'{payload,syntheticChanged}','true') WHERE id=$1",[f.artifactRows[2].id]);
        }
        return c.query(q,v);
      }};}};
      await assert.rejects(bindRecoveryApproval(racing,f.approvalInput,{id:f.operator.id,role:f.operator.role},adapters(racing,{tenantId:'10'}),options(f)),{code:'anchor_recovery_artifacts_changed'});
      assert.equal(changed,true);assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_mission_contributions WHERE specialist='operator' AND kind='approval'")).rows[0].n,0);
      assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest,null);
    }
  });
  await t.test('missing preinitialized canonical schema fails closed without attempting bootstrap',async()=>{
    const f=await setup(pool);await frozen(pool,f);await pool.query('DROP TABLE acquisition_mission_observations');
    const calls=[],audited=auditPool(pool,calls);
    await assert.rejects(bindRecoveryApproval(audited,f.approvalInput,{id:f.operator.id,role:f.operator.role},adapters(audited,{tenantId:'10'}),options(f)));
    assert.equal(calls.some(sql=>/INSERT INTO acquisition_mission_contributions/.test(sql)),false);
    assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest,null);
    assert.equal((await pool.query("SELECT status FROM acquisition_outbound_envelopes WHERE local_day='2026-10-19'")).rows[0].status,'frozen');
  });
});
