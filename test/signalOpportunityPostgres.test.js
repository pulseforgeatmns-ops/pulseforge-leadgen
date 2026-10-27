'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {createTrackedPostgresPool,closePostgresFixture}=require('./helpers/closePostgresPool');
const {OpportunityStore}=require('../services/signalOpportunities/store');
const {DEFINITION,VERSION_HASH,hash}=require('../services/signalOpportunities/definition');
const {CA,W}=require('./helpers/signalDashboardFixtures');
test('opportunity PostgreSQL serializes state changes, dedupes delivery, fences restarts and isolates wallet/tenant',{timeout:90000},async t=>{
 const pg=await startDisposablePostgres('signal-opp-'),pool=createTrackedPostgresPool({connectionString:pg.connectionString,max:10});t.after(()=>closePostgresFixture(pool,pg));
 await require('../standalone/signal-dashboard/migrate').migrate(pool,{SIGNAL_DASHBOARD_SCHEMA_APPROVAL:'SPEC-SIGNAL-DASH-001'});
 const store=new OpportunityStore(pool,{tenantId:'1',wallet:W}),base=Date.now()-10000;
 const decision=(state,ms)=>{const at=new Date(base+ms).toISOString();return {id:hash([state,ms]),tokenAddress:CA,opportunityState:state,definitionVersion:DEFINITION.version,definitionHash:VERSION_HASH,readOnly:true,provenance:{namespace:'signal-opportunities-live'},knowledgeAt:at,knownAt:at,occurredAt:at};};
 await store.append(decision('WATCH',0));const op=decision('OPPORTUNITY',1000);await Promise.all(Array.from({length:8},()=>store.append(op)));
 assert.equal((await store.history(CA)).length,2);assert.equal((await store.deliveryHealth())[0].count,1);
 assert.equal(await new OpportunityStore(pool,{tenantId:'2',wallet:W}).latest(CA),null);assert.equal(await new OpportunityStore(pool,{tenantId:'1',wallet:CA}).latest(CA),null);
 const claimed=await Promise.all(Array.from({length:6},()=>store.claim('email')));assert.equal(claimed.filter(Boolean).length,1);
 await store.finish(claimed.find(Boolean),'UNKNOWN');assert.equal(await new OpportunityStore(pool,{tenantId:'1',wallet:W}).claim('email'),null);
 await store.append(decision('HOT',2000));assert.ok(await store.claim('sms'));assert.ok(await store.claim('email'));
 await pool.query("UPDATE signal_opportunity_notifications SET claimed_at=clock_timestamp()-interval '2 minutes' WHERE status='SENDING'");
 assert.equal(await store.claim('sms'),null);assert.equal((await pool.query("SELECT status FROM signal_opportunity_notifications WHERE channel='sms'")).rows[0].status,'UNKNOWN');
 await store.append(decision('WATCH',3000));await store.append(decision('HOT',4000));assert.equal((await pool.query('SELECT count(*)::int AS n FROM signal_opportunity_notifications')).rows[0].n,3);
 await assert.rejects(()=>store.append({...decision('HOT',5000),definitionHash:'bad'}),/invalid_opportunity_definition/);
 // A held-token AVOID emits email; an unheld-token AVOID stays in dashboard.
 await store.append(decision('AVOID',6000),{held:true});const alert=await store.claim('email');assert.equal(alert.payload.opportunityState,'AVOID');
 await store.finish(alert,'ACCEPTED','TEST-ONLY-RECEIPT');assert.equal((await store.latest(CA)).opportunityState,'AVOID');
});
