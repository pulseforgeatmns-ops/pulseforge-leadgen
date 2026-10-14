'use strict';
// Non-skipping release gate, disposable local PostgreSQL only; no external URL.
const test=require('node:test');const assert=require('node:assert/strict');
const fs=require('node:fs');const path=require('node:path');const {Pool}=require('pg');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {DeliveryStore}=require('../services/signalDistribution/deliveryStore');
test('delivery PostgreSQL: concurrent dedupe, namespace isolation, channel receipts, cooldown and expiry',async t=>{
 const pg=await startDisposablePostgres('signal-delivery-');const pool=new Pool({connectionString:pg.connectionString,max:8});
 t.after(async()=>{await pool.end();await pg.stop();});
 for(const name of ['2026-10-14-signal-distribution.sql','2026-10-14-signal-distribution-delivery.sql']) await pool.query(fs.readFileSync(path.join(__dirname,'../migrations',name),'utf8'));
 let now=new Date('2026-10-14T13:00:00Z');const store=new DeliveryStore(pool,{now:()=>now});
 const monitor=async(id,namespace='operational',tenant='1')=>pool.query(`INSERT INTO signal_distribution_monitors
  (id,tenant_id,namespace,token_address,event_id,created_at,expires_at,next_poll_at) VALUES($1,$2,$3,'TEST_CA',$1,$4,$5,$4)`,[id,tenant,namespace,now,new Date(+now+86400000)]);
 const persist=async(event,at=now)=>{await pool.query('UPDATE signal_distribution_monitors SET state=$2::jsonb WHERE id=$1',[event.monitorId,JSON.stringify({riskState:event.state})]);await pool.query('INSERT INTO signal_distribution_transitions(id,monitor_id,tenant_id,namespace,payload,created_at) VALUES($1,$2,$3,$4,$5::jsonb,$6)',[event.id,event.monitorId,event.tenantId,event.namespace,JSON.stringify(event),at]);return event;};
 const event=(id,overrides={})=>({id,tenantId:'1',namespace:'operational',monitorId:'m',kind:'RISK_TRANSITION',state:'HIGH',reasons:['DRAWDOWN'],observedAt:now.toISOString(),...overrides});
 await monitor('m');const first=await persist(event('e1'));
 const results=await Promise.all(Array.from({length:8},()=>store.enqueue(first,'sms')));assert.equal(new Set(results.map(r=>r.id)).size,1);
 assert.equal((await pool.query('SELECT count(*)::int n FROM signal_distribution_deliveries')).rows[0].n,1);
 await store.enqueue(first,'email');const claims=await Promise.all([store.claim({tenantId:'1',namespace:'operational',channel:'sms'}),store.claim({tenantId:'1',namespace:'operational',channel:'sms'})]);assert.equal(claims.filter(Boolean).length,1);
 await store.accepted(claims.find(Boolean).id,'fake-sms-receipt');
 const email=await store.claim({tenantId:'1',namespace:'operational',channel:'email'});assert.ok(email);await store.unknown(email.id);
 assert.equal(await store.claim({tenantId:'1',namespace:'operational',channel:'email'}),null);
 const milestone=await persist(event('mile',{kind:'MILESTONE_2X',milestoneMultiple:2}));assert.equal((await store.enqueue(milestone,'sms')).status,'PENDING');
 const milestoneClaim=await store.claim({tenantId:'1',namespace:'operational',channel:'sms'});await store.accepted(milestoneClaim.id,'fake-milestone');
 const repeat=await persist(event('e2'));assert.equal((await store.enqueue(repeat,'sms')).status,'SUPPRESSED');
 const critical=await persist(event('e3',{state:'CRITICAL',coverage:{observationFreshness:'FRESH',pair:'PINNED',sourceFreshness:'UNKNOWN'},reasons:['SEVERE_PRICE_DRAWDOWN_WITH_RESERVE_DECLINE']}));assert.equal((await store.enqueue(critical,'sms')).status,'PENDING');
 const same=await persist(event('e4',{state:'CRITICAL',coverage:{observationFreshness:'FRESH',pair:'PINNED',sourceFreshness:'UNKNOWN'},reasons:['SEVERE_PRICE_DRAWDOWN_WITH_RESERVE_DECLINE']}));assert.equal((await store.enqueue(same,'sms')).status,'SUPPRESSED');
 const novel=await persist(event('e5',{state:'CRITICAL',coverage:{observationFreshness:'FRESH',pair:'PINNED',sourceFreshness:'UNKNOWN'},reasons:['SEVERE_PRICE_DRAWDOWN_WITH_RESERVE_DECLINE','SEVERE_PRICE_DRAWDOWN_WITH_SELL_PRESSURE']}));assert.equal((await store.enqueue(novel,'sms')).status,'PENDING');
 await monitor('mt','test');const testEvent=await persist(event('et',{monitorId:'mt',namespace:'test'}));assert.equal((await store.enqueue(testEvent,'sms')).status,'TEST_ONLY');assert.equal(await store.claim({tenantId:'1',namespace:'test',channel:'sms'}),null);
 await assert.rejects(store.enqueue({...first,tenantId:'2'},'sms'),/transition_not_persisted/);
 await assert.rejects(pool.query("UPDATE signal_distribution_deliveries SET status='PENDING' WHERE namespace='test'"));
 // Process restart preserves unknown claims and dedupe. Expired SENDING is not resent.
 const claimed=await store.claim({tenantId:'1',namespace:'operational',channel:'sms'});assert.ok(claimed);now=new Date(+now+31000);
 const restarted=new DeliveryStore(pool,{now:()=>now});await restarted.claim({tenantId:'1',namespace:'operational',channel:'sms'});
 assert.equal((await pool.query('SELECT status FROM signal_distribution_deliveries WHERE id=$1',[claimed.id])).rows[0].status,'UNKNOWN');
 now=new Date(+now+360000);await restarted.claim({tenantId:'1',namespace:'operational',channel:'sms'});
 await monitor('old');const stale=await persist(event('old-e',{monitorId:'old'}),new Date(+now-360000));await restarted.enqueue(stale,'sms');assert.equal(await restarted.claim({tenantId:'1',namespace:'operational',channel:'sms'}),null);
 assert.equal((await pool.query("SELECT status FROM signal_distribution_deliveries WHERE transition_id='old-e'")).rows[0].status,'EXPIRED');
 const rows=await restarted.getDeliveries({tenantId:'1',namespace:'test',monitorId:'mt'});assert.equal(rows.length,1);assert.equal(rows[0].status,'TEST_ONLY');
 assert.equal((await restarted.getDeliveries({tenantId:'2',namespace:'test',monitorId:'mt'})).length,0);
});
