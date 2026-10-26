'use strict';
const deny=require('./helpers/denyExternalProviders.cjs');
const test=require('node:test'),assert=require('node:assert/strict');
const {startResearchHarness,ENV}=require('./helpers/scoutBoundedResearchFixture');
const authority=require('../services/scoutBoundedResearchAuthority');
const runner=require('../services/scoutBoundedAcquisition');
const {hash}=require('../services/scoutBoundedAcquisitionBudget');
const pause=require('../utils/anchorResearchProviderPause');
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const response=data=>({ok:true,status:200,json:async()=>data});

test('installed bounded research uses actual PG/native provider/page/ticket/admission while global gates stay closed',{timeout:180000},async t=>{
 const h=await startResearchHarness();t.after(()=>h.close());
 const {pool}=h;const envBefore=Object.fromEntries(Object.keys(ENV).map(k=>[k,process.env[k]]));
 async function count(table,ref){return (await pool.query(`SELECT count(*)::int n FROM ${table}${ref?' WHERE approval_ref=$1':''}`,ref?[ref]:[])).rows[0].n;}
 async function install(f){const p=await authority.previewResearchAuthority(pool,f.manifest);assert.equal(p.previewOnly,true);assert.equal(f.calls.length,0);return authority.installResearchAuthority(pool,f.manifest,p.manifestHash);}
 async function closeOpen(){for(const row of (await pool.query('SELECT manifest,manifest_hash FROM scout_bounded_research_grants WHERE revoked_at IS NULL')).rows)await authority.revokeResearchAuthority(pool,row.manifest,row.manifest_hash);}
 async function noWork(f){assert.equal(f.calls.length,0);assert.equal(await count('scout_bounded_acquisition_runs',f.manifest.approvalRef),0);assert.equal(await count('scout_bounded_acquisition_attempts',f.manifest.approvalRef),0);}
 async function scenario(name,fn){await t.test(name,async()=>{const f=await h.newManifest(),history=await h.historySnapshot();try{await fn(f);}finally{await closeOpen();}
  assert.deepEqual(await h.historySnapshot(),history,'research must not mutate historical recovery/quarantine/preparation/mission records');assert.deepEqual(Object.fromEntries(Object.keys(ENV).map(k=>[k,process.env[k]])),envBefore,'research never changes environment');assert.equal(deny.attempts(),0);});}
 const readonly={async connect(){const db=await pool.connect();return {release:db.release.bind(db),async query(sql,values){assert.match(typeof sql==='string'?sql:sql.text,/^(SELECT|BEGIN (?:ISOLATION LEVEL REPEATABLE READ )?READ ONLY|SET LOCAL|COMMIT|ROLLBACK)/);return db.query(sql,values);}};}};
 await scenario('preview/receipt are SELECT-only; missing grant never installs or dispatches',async f=>{
  const preview=await authority.previewResearchAuthority(readonly,f.manifest);assert.equal(preview.status,'not_installed');assert.equal(preview.installedAuthority,false);
  assert.equal((await authority.inspectResearchAuthority(readonly,f.manifest)).outcome,'not_started');
  await assert.rejects(f.run(),/bounded_research_grant_required/);await noWork(f);assert.equal(await count('scout_bounded_research_grants'),0);
 });
 await scenario('explicit exact install then actual page queues19new candidates, keeps old20untouched and never hydrates history',async f=>{
  const originalHistory=await h.historySnapshot();const p=await install(f);assert.equal(p.installed,true);
  const r=await f.run();assert.equal(r.status,'completed');assert.equal(r.rawSearchResults,20);assert.equal(r.queuedForEnrichment,19);
  assert.equal(r.claimedNewPageOnlyCandidates,19);assert.equal(r.sharedWithOlderQueries,1);assert.equal(r.cleanVerifiedContacts,0);assert.equal(r.otherProviderAttempts,0);assert.equal(r.modeledReservedUsdMicros,540000);assert.equal(f.calls.length,21);
  const old=(await pool.query('SELECT w.attempts FROM scout_candidate_memberships m JOIN scout_candidate_work w USING(tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key) WHERE m.search_fingerprint=$1 AND m.query_key=$2',[f.manifest.fingerprint,f.manifest.rootQueryKey])).rows;assert.equal(old.length,20);assert.ok(old.every(r=>Number(r.attempts)===0));
  assert.deepEqual((await pool.query('SELECT candidates FROM scout_query_checkpoints WHERE search_fingerprint=$1 AND query_key=$2',[f.manifest.fingerprint,f.manifest.rootQueryKey])).rows[0].candidates,f.raw);
  assert.deepEqual(await h.historySnapshot(),originalHistory);assert.equal((await pool.query("SELECT sum(item_count)::int n FROM anchor_scoped_resume_consumption")).rows[0].n,2);
  assert.equal(h.hydrationGuard.attempts(),0,'transitive module loading must never enter runtime/hydration APIs');assert.equal(h.historicalMissionIds.length,45);assert.equal(originalHistory.legacyContributions.length,298);
  const receipt=await authority.inspectResearchAuthority(readonly,f.manifest);assert.equal(receipt.outcome,'completed');assert.equal(receipt.modeledReservedUsdMicros,540000);assert.equal(receipt.cleanVerifiedContacts,0);assert.equal(receipt.executionAuthorizationNotRechecked,true);
  await assert.rejects(f.run());assert.equal(f.calls.length,21);assert.equal(await count('scout_bounded_acquisition_runs',f.manifest.approvalRef),1);
 });
 await scenario('wrong explicit install/revoke confirmation cannot create grant or tombstone',async f=>{
  await assert.rejects(authority.installResearchAuthority(pool,f.manifest,'0'.repeat(64)),/exact_confirmation/);
  await assert.rejects(authority.revokeResearchAuthority(pool,f.manifest,'0'.repeat(64)),/exact_confirmation/);await noWork(f);
  assert.equal((await authority.inspectResearchAuthority(pool,f.manifest)).status,'not_installed');
 });
 for(const [name,mutate] of [
  ['wrong tenant',m=>m.tenantId='11'],['source pin',m=>m.sourceHash='0'.repeat(64)],['program pin',m=>m.programHash='0'.repeat(64)],
  ['revision',m=>m.researchAuthority.expectedRevision='b'.repeat(40)],['quarantine pin',m=>m.researchAuthority.quarantineDecisionHash='0'.repeat(64)],
  ['recovery pin',m=>m.researchAuthority.recoveryControlHash='0'.repeat(64)],['another provider',m=>m.researchAuthority.provider='hunter'],
  ['search cap widening',m=>m.researchAuthority.maxSearchAttempts=4],['details cap widening',m=>m.researchAuthority.maxDetailsAttempts=21],
  ['modeled cap widening',m=>m.maxModeledUsdMicros=620001],['enrichment right',m=>m.researchAuthority.enrichment=true],
  ['verification right',m=>m.researchAuthority.verification=true],['preparation right',m=>m.researchAuthority.preparation=true],['email right',m=>m.researchAuthority.email=true],
  ['unknown authority field',m=>m.researchAuthority.future=true],['second root',m=>m.grant.rootQueryKeys.push('b'.repeat(64))],
  ['future window',m=>{m.grant.notBefore=new Date(Date.now()+60000).toISOString();m.grant.expiresAt=new Date(Date.now()+120000).toISOString();}],
  ['expired window',m=>{m.grant.notBefore=new Date(Date.now()-120000).toISOString();m.grant.expiresAt=new Date(Date.now()-60000).toISOString();}],
  ['over60minute window',m=>m.grant.expiresAt=new Date(Date.parse(m.grant.notBefore)+3600001).toISOString()]
 ])await scenario(name+' denies preview/install/run without any allowance or HTTP',async f=>{
  mutate(f.manifest);await assert.rejects(authority.previewResearchAuthority(pool,f.manifest));await assert.rejects(authority.installResearchAuthority(pool,f.manifest,hash(f.manifest)));await assert.rejects(f.run());await noWork(f);
 });
 for(const [key,value] of [['ANCHOR_RESEARCH_PROVIDERS_PAUSED','false'],['ANCHOR_EMAIL_RECOVERY_MAINTENANCE','false'],['ANCHOR_GOVERNED_OUTBOUND_ENABLED','true'],['ANCHOR_EMAIL_SCOPED_RESUME_ID','synthetic-other'],['ANCHOR_EMAIL_RECOVERY_MANIFEST_ID','synthetic-other']])
  await scenario('current '+key+' drift blocks installed execution',async f=>{await install(f);const old=process.env[key];process.env[key]=value;try{await assert.rejects(f.run(),/closed_environment/);await noWork(f);}finally{old===undefined?delete process.env[key]:process.env[key]=old;}});
 await scenario('changed source after install denies work; safe revoke still works after revision and window drift',async f=>{
  await install(f);await pool.query("UPDATE acquisition_outbound_programs SET authorized_at=authorized_at+interval '1 second' WHERE id=$1",[f.manifest.programId]);await assert.rejects(f.run(),/bounded_source_changed/);await noWork(f);
  const old=process.env.RAILWAY_GIT_COMMIT_SHA;process.env.RAILWAY_GIT_COMMIT_SHA='f'.repeat(40);try{assert.equal((await authority.revokeResearchAuthority(pool,f.manifest,hash(f.manifest))).status,'revoked');}finally{process.env.RAILWAY_GIT_COMMIT_SHA=old;}
 });
 await scenario('nonempty recovery control denies research even with the installed grant',async f=>{
  await install(f);await pool.query("UPDATE anchor_email_recovery_control SET manifest='{}'");try{await assert.rejects(f.run(),/closed_control/);}finally{await pool.query('UPDATE anchor_email_recovery_control SET manifest=NULL');}await noWork(f);
 });
 await scenario('private scope is not forgeable and direct internal runner cannot mint authority',async f=>{
  await install(f);assert.equal(authority.withResearchAuthority,undefined);assert.equal(authority.allowsBoundedPlacesResearch('google_places','10'),false);
  await assert.rejects(Promise.resolve().then(()=>pause.withResearchProviderScope('10',()=>f.run())),/anchor_research_provider_paused/);
  await assert.rejects(runner.runBoundedAcquisitionInScope({pool,manifest:f.manifest,apiKey:'synthetic',fetchImpl:f.fetchImpl}),/anchor_research_provider_paused|bounded_research_execution_scope_required/);await noWork(f);
  assert.equal(authority.allowsBoundedPlacesResearch.call({active:true,tenantId:'10'},'google_places','10'),false);
 });
 await scenario('only Google/tenant10 is permitted in native transport; detached async descendant loses authority',async f=>{
  await install(f);let release,leak,checks=0;
  await f.run({fetchImpl:async(url,opts)=>{if(!checks++){
   assert.equal(authority.allowsBoundedPlacesResearch('google_places','10'),true);assert.equal(authority.allowsBoundedPlacesResearch('google_places','11'),false);
   const other=structuredClone(f.manifest);other.approvalRef+='-other';other.grant.approvalRef=other.approvalRef;
   await assert.rejects(runner.runBoundedAcquisitionInScope({pool,manifest:other,apiKey:'synthetic',fetchImpl:f.fetchImpl}),/bounded_research_execution_scope_required/);
   for(const p of ['hunter','prospeo','serpapi','brevo','smtp']){assert.equal(authority.allowsBoundedPlacesResearch(p,'10'),false);assert.throws(()=>pause.assertResearchProviderAllowed(p,'10'),/anchor_research_provider_paused/);}
   assert.throws(()=>pause.withResearchProviderScope('11',()=>{}),/tenant_mismatch/);
   leak=new Promise(r=>{release=r;}).then(()=>authority.allowsBoundedPlacesResearch('google_places','10'));
  }return f.fetchImpl(url,opts);}});
  release();assert.equal(await leak,false);assert.equal(authority.allowsBoundedPlacesResearch('google_places','10'),false);
 });
 await scenario('two concurrent exact installs are idempotent; different hash cannot borrow approval reference',async f=>{
  const outcomes=await Promise.all([authority.installResearchAuthority(pool,f.manifest,hash(f.manifest)),authority.installResearchAuthority(pool,f.manifest,hash(f.manifest))]);
  assert.equal(outcomes.filter(r=>r.installed).length,1);assert.equal(outcomes.filter(r=>r.idempotent).length,1);
  const changed=structuredClone(f.manifest);changed.maxModeledUsdMicros=40000;await assert.rejects(authority.installResearchAuthority(pool,changed,hash(changed)),/grant_conflict/);await noWork(f);
 });
 await scenario('open research grantA blocks preview/install/run of grantB before any mutation',async f=>{
  await install(f);const other=await h.newManifest();await assert.rejects(authority.previewResearchAuthority(pool,other.manifest),/other_authority_open/);await assert.rejects(authority.installResearchAuthority(pool,other.manifest,hash(other.manifest)),/other_authority_open/);await assert.rejects(other.run(),/other_authority_open/);await noWork(f);await noWork(other);
  assert.equal((await authority.inspectResearchAuthority(pool,other.manifest)).status,'not_installed');
 });
 await scenario('revocation-before-install tombstone permanently prevents delayed install',async f=>{
  const first=await authority.revokeResearchAuthority(pool,f.manifest,hash(f.manifest));assert.equal(first.tombstone,true);
  await assert.rejects(authority.installResearchAuthority(pool,f.manifest,hash(f.manifest)),/grant_revoked/);await assert.rejects(f.run(),/grant_revoked/);
  const read=await authority.inspectResearchAuthority(readonly,f.manifest);assert.equal(read.status,'revoked');assert.equal(read.tombstone,true);await noWork(f);
 });
 await scenario('concurrent install/revoke always leaves closed durable authority without dispatch',async f=>{
  await Promise.allSettled([authority.installResearchAuthority(pool,f.manifest,hash(f.manifest)),authority.revokeResearchAuthority(pool,f.manifest,hash(f.manifest))]);
  assert.equal((await authority.inspectResearchAuthority(pool,f.manifest)).status,'revoked');await assert.rejects(f.run(),/grant_revoked/);await noWork(f);
 });
 await scenario('database rejects permanent grant edits/delete/truncate and tombstone reopening',async f=>{
  await install(f);
  for(const sql of ["UPDATE scout_bounded_research_grants SET manifest=manifest||'{\"changed\":true}' WHERE approval_ref=$1","UPDATE scout_bounded_research_grants SET installed_at=installed_at+interval '1 second' WHERE approval_ref=$1","DELETE FROM scout_bounded_research_grants WHERE approval_ref=$1"])
   await assert.rejects(pool.query(sql,[f.manifest.approvalRef]),/permanent/);
  await assert.rejects(pool.query('TRUNCATE scout_bounded_research_grants'),/permanent/);
  await authority.revokeResearchAuthority(pool,f.manifest,hash(f.manifest));await assert.rejects(pool.query('UPDATE scout_bounded_research_grants SET revoked_at=NULL,revoke_approval_ref=NULL WHERE approval_ref=$1',[f.manifest.approvalRef]),/permanent/);await noWork(f);
 });
 await scenario('concurrent actual runners cannot double debit or queue',async f=>{
  await install(f);const outcomes=await Promise.allSettled([f.run(),f.run()]);assert.equal(outcomes.filter(r=>r.status==='fulfilled').length,1);
  assert.equal(f.calls.length,21);assert.equal(await count('scout_bounded_acquisition_runs',f.manifest.approvalRef),1);assert.equal(await count('scout_bounded_acquisition_attempts',f.manifest.approvalRef),21);
 });
 await scenario('three searches and20details consume exact620000 upper bound, never page2',async f=>{
  await install(f);let searches=0;await f.run({fetchImpl:async(url,opts)=>{if(url.includes('textsearch')&&++searches<3){f.calls.push('retry-search');return response({status:'UNKNOWN_ERROR'});}return f.fetchImpl(url,opts);}});
  assert.equal(f.calls.length,23);assert.equal((await authority.inspectResearchAuthority(pool,f.manifest)).modeledReservedUsdMicros,620000);await assert.rejects(f.run());assert.equal(f.calls.length,23);
 });
 await scenario('lower monetary cap is consumed, not refunded/reopened by new approval reference',async f=>{
  f.manifest.maxModeledUsdMicros=40000;await install(f);await assert.rejects(f.run());assert.equal(f.calls.length,1);
  assert.equal((await authority.inspectResearchAuthority(pool,f.manifest)).modeledReservedUsdMicros,40000);await authority.revokeResearchAuthority(pool,f.manifest,hash(f.manifest));
  const next=structuredClone(f.manifest);next.approvalRef+='-again';next.grant.approvalRef=next.approvalRef;await assert.rejects(authority.installResearchAuthority(pool,next,hash(next)));assert.equal(f.calls.length,1);
 });
 await scenario('lost HTTP response remains uncertain charged and never retried or leaked into async descendants',async f=>{
  await install(f);let release,leak;
  await assert.rejects(f.run({fetchImpl:async()=>{f.calls.push('lost-response');leak=new Promise(r=>{release=r;}).then(()=>authority.allowsBoundedPlacesResearch('google_places','10'));throw Error('synthetic lost response');}}));
  release();assert.equal(await leak,false);const r=await authority.inspectResearchAuthority(pool,f.manifest);assert.equal(r.modeledReservedUsdMicros,40000);assert.equal(r.outcome,'consumed_outcome_not_confirmed');assert.equal(r.attempts[0].state,'uncertain');
  await assert.rejects(f.run());assert.equal(f.calls.length,1);
 });
 await scenario('lost install COMMIT acknowledgement reads installed state without automatic execution',async f=>{
  let lost=false;const faultPool={options:pool.options,query:pool.query.bind(pool),async connect(){const db=await pool.connect();return {release:db.release.bind(db),async query(sql,args){const r=await db.query(sql,args);if(sql==='COMMIT'&&!lost){lost=true;throw Error('synthetic commit acknowledgement lost');}return r;}};}};
  await assert.rejects(authority.installResearchAuthority(faultPool,f.manifest,hash(f.manifest)),/commit_uncertain/);assert.equal(lost,true);
  assert.equal((await authority.inspectResearchAuthority(readonly,f.manifest)).status,'installed');await noWork(f);
  lost=false;await assert.rejects(authority.revokeResearchAuthority(faultPool,f.manifest,hash(f.manifest)),/commit_uncertain/);assert.equal((await authority.inspectResearchAuthority(readonly,f.manifest)).status,'revoked');await noWork(f);
 });
 await scenario('durable response survives lost transport COMMIT acknowledgement without replay',async f=>{
  await install(f);let bodySaved=false,lost=false;const faultPool={options:pool.options,async query(sql,args){const r=await pool.query(sql,args);if(/SET response_evidence=/.test(sql))bodySaved=true;return r;},async connect(){const db=await pool.connect();return {release:db.release.bind(db),async query(sql,args){const r=await db.query(sql,args);if(sql==='COMMIT'&&bodySaved&&!lost){lost=true;throw Error('synthetic transport commit acknowledgement lost');}return r;}};}};
  await assert.rejects(f.run({pool:faultPool}));assert.equal(lost,true);assert.equal(f.calls.length,1);const row=(await pool.query('SELECT response_evidence,response_hash,state,reserved_usd_micros FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1',[f.manifest.approvalRef])).rows[0];assert.equal(row.response_hash,hash(row.response_evidence));assert.equal(row.response_evidence.results.length,20);assert.equal(row.reserved_usd_micros,40000);assert.equal(row.state,'uncertain');await assert.rejects(f.run());assert.equal(f.calls.length,1);
 });
 async function requestRevokeDuringTransport(f){const pending=authority.revokeResearchAuthority(pool,f.manifest,hash(f.manifest));pending.catch(()=>{});let waiting=false;
  for(let i=0;i<100;i++){waiting=(await pool.query("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND mode='ExclusiveLock') AS waiting")).rows[0].waiting;if(waiting)break;await sleep(5);}
  assert.equal(waiting,true,'revoke queued behind the actual transport recovery lock');return {pending};}
 for(const stopAfter of [1,21])await scenario(`revoke during actual transport${stopAfter} prevents later HTTP/admission without refunds`,async f=>{
  await install(f);const queueBefore=await count('scout_unenriched');let revoke;
  await assert.rejects(f.run({fetchImpl:async(url,opts)=>{const r=await f.fetchImpl(url,opts);if(f.calls.length===stopAfter)revoke=(await requestRevokeDuringTransport(f)).pending;return r;}}));
  assert.equal((await revoke).status,'revoked');assert.equal(f.calls.length,stopAfter);assert.equal(await count('scout_unenriched'),queueBefore);
  const r=await authority.inspectResearchAuthority(readonly,f.manifest);assert.equal(r.modeledReservedUsdMicros,stopAfter===1?40000:540000);await assert.rejects(f.run(),/grant_revoked/);assert.equal(f.calls.length,stopAfter);
 });
 await scenario('expiry during row-lock wait is sampled after lock, before any provider call',async f=>{
  f.manifest.grant.notBefore=new Date(Date.now()-1000).toISOString();f.manifest.grant.expiresAt=new Date(Date.now()+900).toISOString();await install(f);
  const holder=await pool.connect();await holder.query('BEGIN');await holder.query('SELECT * FROM scout_bounded_research_grants WHERE approval_ref=$1 FOR UPDATE',[f.manifest.approvalRef]);
  const pending=f.run().then(()=>null,e=>e);try{await sleep(1000);await holder.query('ROLLBACK');const failure=await pending;assert.match(failure?.code||'',/window_inactive/);await noWork(f);}finally{await holder.query('ROLLBACK');holder.release();}
 });
 await scenario('expiry during consumed response body denies subsequent calls and admission',async f=>{
  f.manifest.grant.notBefore=new Date(Date.now()-1000).toISOString();f.manifest.grant.expiresAt=new Date(Date.now()+900).toISOString();await install(f);const before=await count('scout_unenriched');
  await assert.rejects(f.run({fetchImpl:async(url,opts)=>{const r=await f.fetchImpl(url,opts);if(f.calls.length===1)await sleep(Math.max(0,Date.parse(f.manifest.grant.expiresAt)-Date.now()+50));return r;}}));
  assert.equal(f.calls.length,1);assert.equal(await count('scout_unenriched'),before);assert.equal((await authority.inspectResearchAuthority(pool,f.manifest)).modeledReservedUsdMicros,40000);
 });
 for(const trigger of ['scout_bounded_research_no_delete','scout_bounded_research_revoke_only'])await scenario('disabled '+trigger+' fails closed without installing or dispatching',async f=>{
  await pool.query(`ALTER TABLE scout_bounded_research_grants DISABLE TRIGGER ${trigger}`);try{await assert.rejects(authority.previewResearchAuthority(pool,f.manifest),/schema_guards_missing/);await assert.rejects(authority.installResearchAuthority(pool,f.manifest,hash(f.manifest)),/schema_guards_missing/);await assert.rejects(f.run(),/schema_guards_missing/);await noWork(f);}finally{await pool.query(`ALTER TABLE scout_bounded_research_grants ENABLE TRIGGER ${trigger}`);}
 });
 await scenario('missing named uniqueness/index proof fails closed despite intact data',async f=>{
  await pool.query('ALTER INDEX scout_bounded_research_one_open_tenant RENAME TO synthetic_hidden_open_index');try{await assert.rejects(authority.previewResearchAuthority(pool,f.manifest),/schema_identity_missing/);await assert.rejects(authority.installResearchAuthority(pool,f.manifest,hash(f.manifest)),/schema_identity_missing/);await noWork(f);}finally{await pool.query('ALTER INDEX synthetic_hidden_open_index RENAME TO scout_bounded_research_one_open_tenant');}
 });
 await scenario('unhandled original checkpoint cannot receive a research grant',async f=>{
  await pool.query("UPDATE scout_query_checkpoints SET state='fetched',handled_at=NULL WHERE search_fingerprint=$1 AND query_key=$2",[f.manifest.fingerprint,f.manifest.rootQueryKey]);
  await assert.rejects(authority.previewResearchAuthority(pool,f.manifest));await assert.rejects(authority.installResearchAuthority(pool,f.manifest,hash(f.manifest)));await assert.rejects(f.run());await noWork(f);
 });
 await scenario('existing native first-page reservation cannot be silently adopted or reopened',async f=>{
  const {createPageCheckpointStore}=require('../services/scoutQueryContinuation');const pages=createPageCheckpointStore({db:pool,tenantId:'10',fingerprint:f.manifest.fingerprint,coverageEpoch:f.manifest.grant});
  const plan=await pages.plan(f.manifest.rootQueryKey),id=await pages.reserve(plan);assert.ok(id);
  const before=(await pool.query('SELECT * FROM scout_query_coverage_grant_pages WHERE search_fingerprint=$1',[f.manifest.fingerprint])).rows;
  await assert.rejects(authority.previewResearchAuthority(pool,f.manifest));await assert.rejects(authority.installResearchAuthority(pool,f.manifest,hash(f.manifest)));await noWork(f);
  assert.deepEqual((await pool.query('SELECT * FROM scout_query_coverage_grant_pages WHERE search_fingerprint=$1',[f.manifest.fingerprint])).rows,before);
 });
 await scenario('legacy v1 coverage receipt requires reconciliation, not adoption into research authority',async f=>{
  const {createPageCheckpointStore}=require('../services/scoutQueryContinuation');const old={...f.manifest.grant,version:1};delete old.maxNewPages;
  await createPageCheckpointStore({db:pool,tenantId:'10',fingerprint:f.manifest.fingerprint,coverageEpoch:old}).plan(f.manifest.rootQueryKey);
  await assert.rejects(authority.previewResearchAuthority(pool,f.manifest));await assert.rejects(authority.installResearchAuthority(pool,f.manifest,hash(f.manifest)));await noWork(f);
 });
 for(const failure of ['body','settlement'])await scenario('unconfirmed '+failure+' persistence stops further calls and permanently retains paid debit',async f=>{
  await install(f);const faultPool={options:pool.options,connect:pool.connect.bind(pool),async query(sql,args){
   if(failure==='body'&&/SET response_evidence=/.test(sql))throw Error('synthetic body persistence loss');
   if(failure==='settlement'&&/UPDATE api_usage_events SET status/.test(sql))return {rows:[],rowCount:0};return pool.query(sql,args);}};
  await assert.rejects(f.run({pool:faultPool}));assert.equal(f.calls.length,1);const receipt=await authority.inspectResearchAuthority(pool,f.manifest);assert.equal(receipt.modeledReservedUsdMicros,40000);assert.equal(receipt.outcome,'consumed_outcome_not_confirmed');await assert.rejects(f.run());assert.equal(f.calls.length,1);
 });
 await scenario('runtime path performs no schema creation or alteration',async f=>{
  const seen=[];function audit(sql){const text=typeof sql==='string'?sql:sql.text;seen.push(text);assert.doesNotMatch(text,/\b(CREATE|ALTER|DROP)\s/i);}
  const audited={options:pool.options,async query(sql,args){audit(sql);return pool.query(sql,args);},async connect(){const db=await pool.connect();return {release:db.release.bind(db),async query(sql,args){audit(sql);return db.query(sql,args);}};}};
  await authority.installResearchAuthority(audited,f.manifest,hash(f.manifest));assert.equal((await f.run({pool:audited})).queuedForEnrichment,19);assert.ok(seen.length>100);
 });
 await t.test('an already open preparation grant blocks research with all historical rows unchanged',async()=>{
  const second=await startResearchHarness();try{const f=await second.newManifest();const row=(await second.pool.query("SELECT * FROM anchor_scoped_resume_grants WHERE manifest_id='history-prep-1'")).rows[0];
   const m={...row.manifest,id:'synthetic-open-preparation'};await require('./helpers/anchorRecoveryPostgresFixture').insert(second.pool,'anchor_scoped_resume_grants',{...row,manifest_id:m.id,manifest:m,manifest_hash:hash(m),revoked_at:null,revoke_approval_ref:null,revoked_by:null,revoked_role:null});
   const before=await second.historySnapshot();await assert.rejects(authority.previewResearchAuthority(second.pool,f.manifest),/other_authority_open/);await assert.rejects(f.run(),/other_authority_open/);assert.equal(f.calls.length,0);assert.deepEqual(await second.historySnapshot(),before);
  }finally{await second.close();}
 });
 await scenario('second migration application fails atomically without resetting grants or history',async f=>{
  await install(f);const before=(await pool.query('SELECT to_jsonb(g) AS row FROM scout_bounded_research_grants g ORDER BY manifest_hash')).rows;
  const db=await pool.connect();try{const sql=require('node:fs').readFileSync(require('node:path').join(__dirname,'../migrations/2026-10-26-scout-bounded-research-authority.sql'),'utf8');await assert.rejects(db.query(sql),{code:'42P07'});}finally{await db.query('ROLLBACK');db.release();}
  assert.deepEqual((await pool.query('SELECT to_jsonb(g) AS row FROM scout_bounded_research_grants g ORDER BY manifest_hash')).rows,before);await noWork(f);
 });
 await t.test('opaque JSON contract rejects prototype/accessor/symbol/function/cycle before side effects',()=>{
  let accessed=0;const getter={};Object.defineProperty(getter,'version',{enumerable:true,get(){accessed++;return 2;}});
  const cycle={};cycle.x=cycle;
  let traps=0;const proxy=new Proxy({version:2},{get(){traps++;return 2;},ownKeys(){traps++;return [];},getOwnPropertyDescriptor(){traps++;return undefined;}});
  for(const v of [proxy,getter,Object.create({version:2}),{x:undefined},{x:NaN},{x:1n},{x:()=>{}},{x:new Date()},cycle,{[Symbol('x')]:1},new Array(2)])assert.throws(()=>authority.validateJsonDocument(v),/bounded_manifest_invalid/);
  assert.throws(()=>runner.validateManifest(getter),/bounded_manifest_invalid/);assert.throws(()=>runner.validateManifest(proxy),/bounded_manifest_invalid/);assert.equal(accessed,0);assert.equal(traps,0);
 });
 assert.equal(deny.attempts(),0);assert.equal(h.hydrationGuard.attempts(),0);
});
