'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {createRequire}=require('node:module'),{createHash}=require('node:crypto');
const {Pool}=require('pg');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {seedScopedResume}=require('./helpers/anchorScopedResumePostgresFixture');
const {insert}=require('./helpers/anchorRecoveryPostgresFixture');
const control=require('../services/anchorScopedResumeControl'),recovery=require('../services/anchorEmailRecovery'),integration=require('../services/anchorEmailRecoveryIntegration'),scope=require('../services/anchorScopedResume');
const {GovernedOutboundStore}=require('../services/governedOutboundStore');
const {hash,clock}=require('../packages/acquisition-mission/DailyOutboundPolicy');
const clone=x=>JSON.parse(JSON.stringify(x));
function guardAt(f){const filename=path.join(__dirname,'../utils/callEligibility.js'),local=createRequire(filename),module={exports:{}};
 vm.runInNewContext(fs.readFileSync(filename,'utf8'),{module,exports:module.exports,require(id){
  if(id==='../services/anchorEmailRecovery')return {...recovery,loadRecoveryState:db=>recovery.loadRecoveryState(db,{env:f.env,now:f.now}),assertRecoveryLeaf:(db,input)=>recovery.assertRecoveryLeaf(db,input,{env:f.env,now:f.now})};
  if(id==='../services/anchorEmailRecoveryIntegration')return {...integration,consumeRecoveryDispatch:(p,a)=>integration.consumeRecoveryDispatch(p,a,{env:f.env,now:()=>f.now})};
  return local(id);
 }});return module.exports;}
function commandFor(f,item,envelope){const s=item.snapshot,key=`exec_${createHash('sha256').update([envelope.mission_id,item.candidate_id,envelope.revision].join(':')).digest('hex').slice(0,32)}`;
 const command={toEmail:item.email,subject:s.message.subject,body:s.message.body,sender:{email:s.sender.senderEmail,name:s.sender.senderName},idempotencyKey:key,credentialOverride:false};
 command.actualPayload={sender:clone(command.sender),to:[{email:item.email,name:''}],subject:command.subject,textContent:command.body,htmlContent:scope.brevoHtml(command.body),headers:{'Idempotency-Key':key}};
 return {identities:[{clientId:10,prospectId:item.prospect_id}],context:{provider:'brevo',transport:'governed_brevo',programId:f.program.id,envelopeId:envelope.id,itemId:item.id},command};}
async function send(pool,f,item,envelope,cb){const input=commandFor(f,item,envelope);return guardAt(f).withEmailAuthorization(pool,input.identities,cb,{context:input.context,command:input.command});}
async function addIdentity(pool,f,n){const prospect=clone(f.prospect),company={...clone(f.companies[0]),id:`company-fresh-${n}`,name:`Distinct Synthetic Enterprise ${n}`,domain:`fresh-${n}.test`,website:`https://fresh-${n}.test`};
 Object.assign(prospect,{id:`contact-fresh-${n}`,company_id:company.id,email:`recipient@fresh-${n}.test`,company_name:company.name,company_domain:company.domain,company_website:company.website});
 const {company_name,company_domain,company_website,...row}=prospect;await insert(pool,'companies',company);await insert(pool,'prospects',row);
 const ci=require('../services/anchorEmailQuarantine').companyIdentity(company);const a={prospectId:prospect.id,companyId:company.id,recipientEmail:prospect.email,...ci,identityFingerprint:scope.identityFingerprint(prospect,company),approvalReference:`local-review-${n}`};f.manifest.admissions.push(a);
 const entry=clone(f.item.snapshot);Object.assign(entry,{candidateId:`candidate-fresh-${n}`,prospectId:prospect.id,companyId:company.id,email:prospect.email});entry.message.candidateId=entry.candidateId;return entry;}
async function canonicalApprove(pool,f,envelope){const approval=clone(f.approval);approval.id=`approval-${envelope.id}`;approval.mission_id=envelope.mission_id;approval.payload.id=approval.id;approval.payload.missionId=envelope.mission_id;
 approval.payload.payload.preparedArtifactRevision=envelope.revision;approval.payload.payload.dailyEnvelope={id:envelope.id,programId:f.program.id,policyHash:f.program.policy_hash,manifestHash:envelope.manifest_hash,localDay:clock(f.now).day,candidateIds:envelope.manifest.map(i=>i.candidateId)};
 await insert(pool,'acquisition_mission_contributions',approval);await pool.query("UPDATE acquisition_outbound_envelopes SET status='authorized',approval_id=$2 WHERE id=$1",[envelope.id,approval.id]);return {...envelope,status:'authorized',approval_id:approval.id};}
test('native scoped lifecycle: tenant10 maintenance, approved batch, fresh days, durable caps, UNKNOWN and revoke',{timeout:120000},async t=>{
 const pg=await startDisposablePostgres('anchor-scoped-lifecycle-'),pool=new Pool({connectionString:pg.connectionString,max:14});t.after(async()=>{await pool.end();await pg.stop();});
 const f=await seedScopedResume(pool,{preparation:true});
 f.program.policy.expiresAt='2026-10-22T00:00:00.000Z';f.program.policy_hash=hash(f.program.policy);f.manifest.policyHash=f.program.policy_hash;f.manifest.expiresAt='2026-10-21T20:30:00.000Z';
 f.manifest.scopes.email.dailyCap=1;f.manifest.scopes.email.totalCap=2;f.manifest.scopes.preparation.dailyCap=3;f.manifest.scopes.preparation.totalCap=6;f.manifest.scopes.preparation.dailyItemCap=1;f.manifest.scopes.preparation.totalItemCap=3;
 await pool.query('UPDATE acquisition_outbound_programs SET policy=$1,policy_hash=$2 WHERE id=$3',[f.program.policy,f.program.policy_hash,f.program.id]);
 f.approval.payload.payload.dailyEnvelope.policyHash=f.program.policy_hash;await pool.query('UPDATE acquisition_mission_contributions SET payload=$1 WHERE id=$2',[f.approval.payload,f.approval.id]);
 const second=await addIdentity(pool,f,2),third=await addIdentity(pool,f,3);
 f.envelope.manifest.push(third);f.envelope.manifest_hash=hash(f.envelope.manifest);
 f.approval.payload.payload.dailyEnvelope.manifestHash=f.envelope.manifest_hash;f.approval.payload.payload.dailyEnvelope.candidateIds=f.envelope.manifest.map(i=>i.candidateId);
 await pool.query('UPDATE acquisition_outbound_envelopes SET manifest=$1,manifest_hash=$2 WHERE id=$3',[JSON.stringify(f.envelope.manifest),f.envelope.manifest_hash,f.envelope.id]);
 await pool.query('UPDATE acquisition_mission_contributions SET payload=$1 WHERE id=$2',[f.approval.payload,f.approval.id]);
 const competitor={...clone(f.item),id:'local-competitor-item',candidate_id:third.candidateId,prospect_id:third.prospectId,company_id:third.companyId,email:third.email,snapshot:third,status:'pending',attempted_at:null};delete competitor.attempt_token;await insert(pool,'acquisition_outbound_items',competitor);
 const before=(await pool.query('SELECT row_to_json(i) AS row FROM acquisition_outbound_items i WHERE id=$1',[f.quarantine.item_id])).rows[0].row;
 const store=new GovernedOutboundStore(pool,'10');let calls=0;
 await assert.rejects(store.claim(f.item,f.program,'2026-10-19',f.now),{code:'anchor_recovery_maintenance'});
 const prev=process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID=f.manifest.id;t.after(()=>{if(prev===undefined)delete process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;else process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID=prev;});
 await control.installScopedResume(pool,{manifest:f.manifest,approvalReference:f.manifest.approvalReference},f.actor,{now:f.now,env:f.env});
 assert.equal((await recovery.loadRecoveryState(pool,{now:f.now,env:f.env})).mode,'scoped_resume');
 await t.test('concurrent claims consume one daily slot and first handoff is irreversible',async()=>{
  const attempts=await Promise.allSettled([store.claim(f.item,f.program,'2026-10-19',f.now),store.claim(competitor,f.program,'2026-10-19',f.now)]);assert.equal(attempts.filter(r=>r.status==='fulfilled').length,1,attempts.map(r=>r.reason?.stack || 'ok').join('\n'));
  const item=attempts.find(r=>r.status==='fulfilled').value;await send(pool,f,item,f.envelope,async()=>{calls++;return {messageId:'fake-first'};});
  await assert.rejects(send(pool,f,item,f.envelope,async()=>{calls++;}));assert.equal(calls,1);
  await store.finish(item,'sent',null,'fake-first');
 });
 await t.test('separate preparation scope freezes next day without any provider or research callback',async()=>{
  f.now=new Date('2026-10-20T16:00:00.000Z');
  let envelope=await store.freeze(f.program,clock(f.now).day,'daily-mission-two',second.revision,[second],{now:f.now,env:f.env});
  assert.equal(envelope.status,'frozen');assert.equal(calls,1);
  // Explicit synthetic canonical approval fixture; actual canonical writer is tested separately.
  envelope=await canonicalApprove(pool,f,envelope);const item=(await store.items(envelope.id))[0];
  const claimed=await store.claim(item,f.program,clock(f.now).day,f.now);await send(pool,f,claimed,envelope,async()=>{calls++;});assert.equal(calls,2);
  await store.finish(claimed,'uncertain','fake_lost_response');
  const c=await integration.uncertaintyState(pool,'10',{env:f.env,now:f.now,programId:f.program.id,ownAttempt:{tenantId:'10',programId:f.program.id,envelopeId:envelope.id,itemId:claimed.id,attemptToken:claimed.attempt_token}});
  assert.equal(c.quarantined_uncertain,1);assert.equal(c.blocking_uncertain,1);assert.equal(c.own_attempt_uncertain,0);
  await assert.rejects(store.freeze(f.program,'2026-10-21','daily-three',third.revision,[third],{now:new Date('2026-10-21T16:00:00Z'),env:f.env}),/unresolved/);
  const budget=await integration.scopedAttemptBudget(pool,f.program.id,'2026-10-20');assert.deepEqual(budget,{total:2,today:1});
 });
 await t.test('revoke closes Anchor only and retains exact quarantine, markers and original UNKNOWN',async()=>{
  await control.revokeScopedResume(pool,{manifestId:f.manifest.id,approvalReference:'local-revoke-explicit'},f.actor,{now:f.now,env:f.env});
  assert.equal((await recovery.loadRecoveryState(pool,{now:f.now,env:f.env})).mode,'maintenance');
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM anchor_email_recovery_reservations WHERE dispatch_started_at IS NOT NULL')).rows[0].n,2);
  assert.deepEqual((await pool.query('SELECT row_to_json(i) AS row FROM acquisition_outbound_items i WHERE id=$1',[f.quarantine.item_id])).rows[0].row,before);
  const other={query(){throw Error('nonAnchor recovery query forbidden');}};assert.equal(await integration.uncertaintyState(other,'13'),null);
  await assert.rejects(control.installScopedResume(pool,{manifest:f.manifest,approvalReference:f.manifest.approvalReference},f.actor,{now:f.now,env:f.env}));
 });
});

test('native scoped scheduler approval bridge consumes authority and keeps canonical writer preinitialized',{timeout:120000},async t=>{
 const pg=await startDisposablePostgres('anchor-scoped-approval-'),pool=new Pool({connectionString:pg.connectionString,max:12});t.after(async()=>{await pool.end();await pg.stop();});
 const f=await seedScopedResume(pool,{preparation:true});f.manifest.scopes.email.approvalMode='delegated_existing_program';f.manifest.scopes.preparation.operations.push('approve_canonical_envelope');
 t.mock.timers.enable({ apis: ['Date'], now: +f.now });
 await pool.query("UPDATE acquisition_outbound_envelopes SET status='frozen',approval_id=NULL WHERE id=$1",[f.envelope.id]);f.envelope.status='frozen';f.envelope.approval_id=null;
 const prev=process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID=f.manifest.id;t.after(()=>{if(prev===undefined)delete process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;else process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID=prev;});
 await control.installScopedResume(pool,{manifest:f.manifest,approvalReference:f.manifest.approvalReference},f.actor,{now:f.now,env:f.env});
 const filename=path.join(__dirname,'../services/governedOutbound.js'),local=createRequire(filename),module={exports:{}};
 // Expose only the private function to this test; production has no extra API.
 const source=fs.readFileSync(filename,'utf8').replace('    authorize,\n','    authorize, __testBindApproval: bindApproval,\n');
 vm.runInNewContext(source,{module,exports:module.exports,require:local,process,Date,console});
 let writes=0;const adapters={
  async loadMission(id,options){assert.equal(options.preinitializedSchema,true);return {mission:{id,tenantId:'10'},contributions:[]};},
  async prepared(_snapshot,_program,options){assert.equal(options.preinitializedSchema,true);return {revision:f.envelope.revision};},
  async approve(snapshot,program,binding,options){assert.equal(options.preinitializedSchema,true);assert.equal(typeof options.persistencePool.query,'function');
   const row=clone(f.approval);row.payload.payload.approvedBy=f.manifest.operatorId;row.payload.payload.approved=true;row.payload.payload.preparedArtifactRevision=f.envelope.revision;row.payload.payload.dailyEnvelope=binding;
   await options.persistencePool.query('UPDATE acquisition_mission_contributions SET payload=$1 WHERE id=$2',[row.payload,row.id]);writes++;return row.payload;
  }
 };
 const service=module.exports.service({pool,adapters,tenantId:'10',now:()=>f.now,enabled:()=>true});
 const result=await service.__testBindApproval(f.program,f.envelope);assert.equal(result.status,'authorized');assert.equal(writes,1);
 const ledger=(await pool.query('SELECT operation,item_count FROM anchor_scoped_resume_consumption')).rows;assert.deepEqual(ledger,[{operation:'approve_canonical_envelope',item_count:0}]);
 await assert.rejects(service.__testBindApproval(f.program,f.envelope),/operation_consumed/);assert.equal(writes,1);
 assert.equal((await pool.query('SELECT count(*)::int n FROM anchor_email_recovery_reservations')).rows[0].n,0);
});
