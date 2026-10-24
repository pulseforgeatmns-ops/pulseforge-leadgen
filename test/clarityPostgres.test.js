'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { ClarityStore } = require('../packages/max/clarity/store');
const { prepareRecommendations,readClarityBrief } = require('../services/clarityRecommendations');
const { createClarityRouter } = require('../routes/clarity');
const { createExecutionRequest,EXECUTION_INTENTS } = require('../packages/acquisition-mission/ExecutionRequest');
const { routeExecutionRequest } = require('../packages/acquisition-mission/ExecutionRouter');
const express=require('express');
const { once }=require('node:events');
const env={CLARITY_ENABLED:'true'};
const fixture={project_id:'synthetic976',window_start:'2026-10-11',window_end:'2026-10-17',synthetic:true,
  metrics:{sessions:232,pages_per_session:1.22,scroll_depth:8.65,session_duration:51.5,dead_clicks:3.88,rage_clicks:0,quick_backs:0.43,js_errors:0},
  deltas_pct:{sessions:180,scroll_depth:-68,session_duration:-34.1},
  provenance:{gmail_message_id:'fixture976',gmail_thread_id:'fixture-thread',parser_version:'synthetic-v1',source_type:'synthetic',received_at:'2026-10-18T12:00:00Z',content_sha256:'1'.repeat(64)}};
// Non-skipping, disposable DB only. Never process.env.DATABASE_URL.
test('Clarity PostgreSQL tenant isolation, atomic dedup, provenance, review concurrency and HTTP routing', {timeout:90000},async t=>{
 const instance=await startDisposablePostgres('clarity-976-');
 const db=new Pool({connectionString:instance.connectionString});
 t.after(async()=>{await db.end();await instance.stop();});
 await db.query(`CREATE TABLE clients(id INTEGER PRIMARY KEY); INSERT INTO clients VALUES(10),(11);
 CREATE TABLE users(id INTEGER PRIMARY KEY,role TEXT,client_id INTEGER,active BOOLEAN); INSERT INTO users VALUES(1,'admin',NULL,TRUE),(2,'manager',11,TRUE),(3,'viewer',10,TRUE);`);
 await db.query(fs.readFileSync(path.join(__dirname,'../migrations/2026-10-20-clarity-digest-recommendations.sql'),'utf8'));
 const store=new ClarityStore(db);
 await store.saveIntegration({clientId:10,integration:{client_id:10,mailbox_email:'jacob@goanchorcleaning.com',digest_sender:'synthetic@microsoft.com',project_id:'synthetic976',
 enabled:true,consent_status:'authorized',oauth_scopes:['https://www.googleapis.com/auth/gmail.readonly'],oauth_client_id:'synthetic-client',
 refresh_token_encrypted:'synthetic-encrypted-non-secret',token_key_id:'v1',consented_at:'2026-10-20T00:00:00Z'}});
 assert.equal((await store.getIntegration({clientId:10})).enabled,false,'OAuth enrollment must not activate ingestion');
 const grant=await store.getIntegration({clientId:10});
 await assert.rejects(store.saveIntegration({clientId:10,integration:{...grant,project_id:'different-project'}}),/project_change_forbidden/);
 await assert.rejects(store.saveSnapshot({clientId:10,snapshot:fixture,expectedConsentAt:grant.consented_at}),/grant_changed/);
 await db.query('UPDATE clarity_integrations SET enabled=TRUE WHERE client_id=10');
 await assert.rejects(store.saveSnapshot({clientId:10,snapshot:fixture,expectedConsentAt:'2026-10-19T00:00:00Z'}),/grant_changed/);
 const saved=await Promise.all(Array.from({length:4},()=>store.saveSnapshot({clientId:10,snapshot:fixture})));
 assert.equal(saved.filter(x=>x.inserted).length,1);
 const report=saved[0].snapshot;
 await store.saveSnapshot({clientId:10,snapshot:{...fixture,provenance:{...fixture.provenance,gmail_message_id:'duplicate-new-id'}}});
 assert.equal((await db.query('SELECT count(*)::int n FROM clarity_snapshots')).rows[0].n,1);
 assert.equal((await db.query('SELECT count(*)::int n FROM clarity_digest_deliveries')).rows[0].n,2);
 await store.saveSnapshot({clientId:11,snapshot:fixture});
 assert.equal((await store.listSnapshots({clientId:11})).length,1);
 assert.equal(await store.getSnapshot({clientId:11,snapshotId:report.id}),null);
 const previous=await store.saveSnapshot({clientId:10,snapshot:{...fixture,window_start:'2026-10-04',window_end:'2026-10-10',
 metrics:{...fixture.metrics,scroll_depth:27},provenance:{...fixture.provenance,gmail_message_id:'history'}}});
 assert.ok(previous.inserted);
 const recommendations=await prepareRecommendations({clientId:10,snapshot:report,store,pool:db,
 loadAds:async()=>({live:{available:false}}),loadLeads:async()=>({availability:'UNAVAILABLE'})});
 assert.equal(recommendations.length,3);
 await assert.rejects(store.saveRecommendations({clientId:10,snapshotId:report.id,recommendations:[],expectedConsentAt:'2020-01-01T00:00:00Z'}),/grant_changed/);
 assert.equal(recommendations[0].evidence.find(e=>e.label==='Previous observed scroll depth').value,'27%');
 assert.equal(await store.getRecommendation({clientId:11,recommendationId:recommendations[0].id}),null);
 const brief=await readClarityBrief({clientId:10,pool:db,store,env,now:new Date('2026-10-20')});
 assert.equal(brief.report.synthetic,true);assert.equal(brief.recommendations.length,3);
 const rec=recommendations[0];
 const decide=(overrides={})=>store.decide({clientId:10,recommendationId:rec.id,operatorId:1,decision:'approve',expectedDigest:rec.digest,idempotencyKey:'approval-fixture-976',requestId:'cer_fixture',...overrides});
 await assert.rejects(decide({operatorId:2}),/forbidden/);
 await assert.rejects(decide({clientId:11}),/disabled|not_found/);
 await assert.rejects(decide({expectedDigest:'f'.repeat(64)}),/stale/);
 const concurrent=await Promise.all([decide(),decide()]);
 assert.equal(concurrent.filter(r=>!r.replay).length,1);assert.equal(concurrent[0].decision.website_changes_authorized,false);
 assert.equal((await db.query('SELECT count(*)::int n FROM clarity_review_decisions')).rows[0].n,1);
 await assert.rejects(decide({decision:'reject'}),/idempotency_conflict/);
 await assert.rejects(decide({idempotencyKey:'different-key-976'}),/already_reviewed/);
 const request=createExecutionRequest({source:'command_deck',intent:EXECUTION_INTENTS.REVIEW_CLARITY_RECOMMENDATION,
 operatorId:1,permissions:{canExecute:true,role:'admin'},runtimeOwner:'clarity_review',executionMode:'manual',payload:{client_id:10,
 recommendation_id:recommendations[1].id,decision:'reject',expected_digest:recommendations[1].digest,idempotency_key:'reject-fixture-976'}});
 const routed=await routeExecutionRequest(request,{tenantId:10,env,pool:db,clarityStore:store});
 assert.equal(routed.executionResult.recommendation.status,'rejected');assert.equal(routed.executionResult.decision.website_changes_authorized,false);
 const lock=await store.withIngestionLock({clientId:10},async()=>store.withIngestionLock({clientId:10},()=>assert.fail('double lock')));
 assert.equal(lock.reason,'already_running');
 await store.recordIngestionRun({clientId:10,status:'failed',counts:{rejected:1,body:'must not persist'},errorCode:'bad raw body secret',
 rejections:[{code:'clarity_missing_window',provenance:{...fixture.provenance,body:'never'}}],nextRunAt:'2026-10-21T00:00:00Z'});
 const run=(await db.query('SELECT * FROM clarity_ingestion_runs')).rows[0];
 assert.equal(run.error_code,null);assert.deepEqual(run.counts,{rejected:1});assert.equal(run.rejections[0].provenance.body,undefined);
 const before=await store.getIntegration({clientId:10});
 await store.recordIngestionRun({clientId:10,status:'failed',nextRunAt:'2099-01-01T00:00:00Z',expectedConsentAt:'2020-01-01T00:00:00Z'});
 assert.equal(new Date((await store.getIntegration({clientId:10})).next_run_at).getTime(),new Date(before.next_run_at).getTime(),'old run cannot overwrite new grant schedule');
 // Real authenticated HTTP route + CSRF + canonical router + durable DB.
 const app=express(),session={user:{id:1,role:'admin',active:true},active_client_id:10};
 app.use('/shared',express.static(path.join(__dirname,'../public/shared')));app.use(express.static(path.join(__dirname,'..')));
 app.use(express.json());app.use((req,res,next)=>{req.session=session;next();});app.use(createClarityRouter({pool:db,env,store}));
 const server=app.listen(0,'127.0.0.1');await once(server,'listening');t.after(()=>new Promise(resolve=>server.close(resolve)));
 const origin=`http://127.0.0.1:${server.address().port}`;
 const htmlResponse=await fetch(`${origin}/clarity-review?client_id=10&id=${rec.id}`);
 assert.equal(htmlResponse.status,200);const html=await htmlResponse.text();
 for(const asset of ['/public/clarity-review.js','/public/clarity-review.css']) {
   assert.ok(html.includes(asset));assert.equal((await fetch(origin+asset)).status,200);
 }
 const detail=await(await fetch(`${origin}/api/v1/clarity/recommendations/${recommendations[2].id}?client_id=10`)).json();
 assert.ok(detail.csrf_token);assert.equal(detail.permissions.can_decide,true);
 const body={client_id:10,decision:'approve',expected_digest:detail.recommendation.digest,idempotency_key:'http-approval-fixture-976'};
 let response=await fetch(`${origin}/api/v1/clarity/recommendations/${recommendations[2].id}/decision`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
 assert.equal(response.status,403);
 response=await fetch(`${origin}/api/v1/clarity/recommendations/${recommendations[2].id}/decision`,{method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':detail.csrf_token,Origin:origin},body:JSON.stringify(body)});
 assert.equal(response.status,200);const result=await response.json();assert.equal(result.routed,true);assert.equal(result.recommendation.status,'approved');
 assert.equal(result.decision.website_changes_authorized,false);
 await db.query('UPDATE users SET active=FALSE WHERE id=1');
 assert.equal((await fetch(`${origin}/api/v1/clarity/recommendations/${rec.id}?client_id=10`)).status,403,'stale active session cannot read');
 await db.query('UPDATE users SET active=TRUE WHERE id=1');
 session.user={id:2,role:'manager',client_id:10,active:true};
 assert.equal((await fetch(`${origin}/api/v1/clarity/recommendations/${rec.id}?client_id=10`)).status,403,'current database tenant overrides stale session');
 session.user={id:3,role:'viewer',client_id:10,active:true};
 response=await fetch(`${origin}/api/v1/clarity/recommendations/${recommendations[2].id}/decision`,{method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':detail.csrf_token},body:JSON.stringify(body)});
 assert.equal(response.status,403);
});
