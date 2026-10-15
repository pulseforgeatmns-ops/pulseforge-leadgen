'use strict';
// Non-skipping release gate, disposable local PostgreSQL only; no external URL.
const test=require('node:test');const assert=require('node:assert/strict');
const fs=require('node:fs');const path=require('node:path');const {createTrackedPostgresPool,closePostgresFixture}=require('./helpers/closePostgresPool');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {DeliveryStore}=require('../services/signalDistribution/deliveryStore');
test('delivery PostgreSQL: concurrent dedupe, namespace isolation, channel receipts, cooldown and expiry',async t=>{
 const pg=await startDisposablePostgres('signal-delivery-');const pool=createTrackedPostgresPool({connectionString:pg.connectionString,max:8});
 t.after(async()=>{await closePostgresFixture(pool,pg);});
 for(const name of ['2026-10-14-signal-distribution.sql','2026-10-14-signal-distribution-delivery.sql']) await pool.query(fs.readFileSync(path.join(__dirname,'../migrations',name),'utf8'));
 let now=(await pool.query('SELECT clock_timestamp() AS now')).rows[0].now;
 await pool.query("INSERT INTO signal_distribution_activations(id,tenant_id,namespace,starts_at,ends_at,sms_limit) VALUES('a','1','operational',clock_timestamp()-interval '1 minute',clock_timestamp()+interval '2 hours',5)");const store=new DeliveryStore(pool,{now:()=>now});
 const monitor=async(id,namespace='operational',tenant='1')=>pool.query(`INSERT INTO signal_distribution_monitors
  (id,tenant_id,namespace,token_address,event_id,created_at,expires_at,next_poll_at) VALUES($1,$2,$3,'TEST_CA',$1,$4,$5,$4)`,[id,tenant,namespace,now,new Date(+now+86400000)]);
 const persist=async(event,at=now)=>{await pool.query('UPDATE signal_distribution_monitors SET state=$2::jsonb WHERE id=$1',[event.monitorId,JSON.stringify({riskState:event.state})]);await pool.query('INSERT INTO signal_distribution_transitions(id,monitor_id,tenant_id,namespace,payload,created_at) VALUES($1,$2,$3,$4,$5::jsonb,$6)',[event.id,event.monitorId,event.tenantId,event.namespace,JSON.stringify(event),at]);return event;};
 const event=(id,overrides={})=>({id,tenantId:'1',namespace:'operational',monitorId:'m',kind:'RISK_TRANSITION',state:'HIGH',reasons:['DRAWDOWN'],observedAt:now.toISOString(),...overrides});
 await monitor('m');await pool.query("UPDATE signal_distribution_monitors SET activation_id='a' WHERE id='m'");const first=await persist(event('e1'));
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
 await monitor('old');await pool.query("UPDATE signal_distribution_monitors SET activation_id='a' WHERE id='old'");const stale=await persist(event('old-e',{monitorId:'old'}),new Date(+now-360000));await restarted.enqueue(stale,'sms');assert.equal(await restarted.claim({tenantId:'1',namespace:'operational',channel:'sms'}),null);
 assert.equal((await pool.query("SELECT status FROM signal_distribution_deliveries WHERE transition_id='old-e'")).rows[0].status,'EXPIRED');
 const rows=await restarted.getDeliveries({tenantId:'1',namespace:'test',monitorId:'mt'});assert.equal(rows.length,1);assert.equal(rows[0].status,'TEST_ONLY');
 assert.equal((await restarted.getDeliveries({tenantId:'2',namespace:'test',monitorId:'mt'})).length,0);
});

test('activation delivery fence uses DB time, durable atomic SMS cap and no refund',async t=>{
 const pg=await startDisposablePostgres('signal-delivery-cap-');const pool=createTrackedPostgresPool({connectionString:pg.connectionString,max:12});
 t.after(async()=>{await closePostgresFixture(pool,pg);});
 for(const name of ['2026-10-14-signal-distribution.sql','2026-10-14-signal-distribution-delivery.sql'])await pool.query(fs.readFileSync(path.join(__dirname,'../migrations',name),'utf8'));
 await pool.query("INSERT INTO signal_distribution_activations(id,tenant_id,namespace,starts_at,ends_at,sms_limit) VALUES('cap','1','operational',clock_timestamp()-interval '1 minute',clock_timestamp()+interval '1 hour',5)");
 await pool.query("INSERT INTO signal_distribution_monitors(id,tenant_id,namespace,token_address,event_id,state,created_at,expires_at,next_poll_at,activation_id) VALUES('m','1','operational','TEST_CA','e','{}',clock_timestamp(),clock_timestamp()+interval '1 hour',clock_timestamp(),'cap')");
 const store=new DeliveryStore(pool);
 async function add(id,channel='sms') {
  const event={id,tenantId:'1',namespace:'operational',monitorId:'m',kind:'MILESTONE_2X',state:'WATCH',reasons:[],milestoneMultiple:2};
  await pool.query("INSERT INTO signal_distribution_transitions(id,monitor_id,tenant_id,namespace,payload,created_at) VALUES($1,'m','1','operational',$2::jsonb,clock_timestamp())",[id,JSON.stringify(event)]);
  await store.enqueue(event,channel);
 }
 for(let i=0;i<8;i++)await add('cap-'+i);
 const rows=(await Promise.all(Array.from({length:8},()=>store.claim({tenantId:'1',namespace:'operational',channel:'sms'})))).filter(Boolean);assert.equal(rows.length,8);
 const auth=await Promise.all(rows.map(row=>store.authorizeSend(row.id)));assert.equal(auth.filter(r=>r.authorized).length,5);
 assert.equal((await pool.query("SELECT sms_reserved FROM signal_distribution_activations WHERE id='cap'")).rows[0].sms_reserved,5);
 const authorized=rows[auth.findIndex(r=>r.authorized)];assert.equal((await store.authorizeSend(authorized.id)).authorized,false);await store.unknown(authorized.id);
 const restarted=new DeliveryStore(pool);assert.equal((await restarted.authorizeSend(authorized.id)).authorized,false);
 await add('after-restart');const over=await restarted.claim({tenantId:'1',namespace:'operational',channel:'sms'});assert.equal((await restarted.authorizeSend(over.id)).authorized,false);
 assert.equal((await pool.query("SELECT sms_reserved FROM signal_distribution_activations WHERE id='cap'")).rows[0].sms_reserved,5);
 await add('email','email');const email=await restarted.claim({tenantId:'1',namespace:'operational',channel:'email'});assert.equal((await restarted.authorizeSend(email.id)).authorized,true);
 assert.equal((await pool.query("SELECT sms_reserved FROM signal_distribution_activations WHERE id='cap'")).rows[0].sms_reserved,5);
 // A claim before end cannot authorize after end, even with a fabricated app clock.
 await add('late','email');const late=await restarted.claim({tenantId:'1',namespace:'operational',channel:'email'});
 await pool.query("UPDATE signal_distribution_activations SET ends_at=clock_timestamp()-interval '1 second' WHERE id='cap'");
 const skewed=new DeliveryStore(pool,{now:()=>new Date('2000-01-01T00:00:00Z')});assert.equal((await skewed.authorizeSend(late.id)).authorized,false);
 await add('closed','email');assert.equal(await store.claim({tenantId:'1',namespace:'operational',channel:'email'}),null);
 await pool.query("UPDATE signal_distribution_activations SET starts_at=clock_timestamp()+interval '1 minute',ends_at=clock_timestamp()+interval '1 hour' WHERE id='cap'");
 assert.equal(await store.claim({tenantId:'1',namespace:'operational',channel:'email'}),null);
 await assert.rejects(pool.query("UPDATE signal_distribution_activations SET sms_limit=21 WHERE id='cap'"));
 // Legacy unbound monitor is never admitted to send.
 await pool.query("UPDATE signal_distribution_monitors SET activation_id=NULL WHERE id='m'");
 assert.equal(await store.claim({tenantId:'1',namespace:'operational',channel:'email'}),null);
});
