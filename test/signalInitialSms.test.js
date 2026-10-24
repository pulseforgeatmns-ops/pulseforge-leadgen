'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {performance}=require('node:perf_hooks');
const {InitialSmsStore,configuration,createInitialSmsTransport,createInitialSmsWorker,startInitialSms}=require('../services/signalOperator/initialSms');
const {createOperatorWorker}=require('../services/signalOperator/worker');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {createTrackedPostgresPool,closePostgresFixture}=require('./helpers/closePostgresPool');
const CA='So11111111111111111111111111111111111111112';
const event=(id=42)=>({sourceId:'telegram-front-runners',externalMessageId:`telegram:123:${id}`,extractedCa:CA,occurredAt:new Date().toISOString(),ingestedAt:new Date().toISOString(),provenance:{dataClass:'EMPIRICAL',telegramChannelId:'123'}});
const row=()=>({id:'d',tenant_id:'1',namespace:'operational',channel:'sms',lease_token:'lease',payload:{tokenAddress:CA,knowledgeAt:new Date().toISOString()}});
const config=()=>({tenantId:'1',channelId:'123',startsAt:new Date(Date.now()-60000).toISOString(),endsAt:new Date(Date.now()+3600000).toISOString(),limit:5});
test('initial SMS default-off values never inspect scope, obtain a database client or arm transport',async()=>{
 for(const flag of [undefined,'','0','false','true','yes',1,true]) {
  const env=new Proxy({SIGNAL_OPERATOR_SMS_ENABLED:flag},{get:(target,key)=>{
   assert.equal(key,'SIGNAL_OPERATOR_SMS_ENABLED','disabled lane must not inspect other configuration');return target[key];
  }});
  const forbidden=()=>assert.fail('disabled initial SMS lane must have no side effects');
  assert.equal(await startInitialSms({env,pool:{query:forbidden,connect:forbidden},channelId:'123',gate:forbidden,setInterval:forbidden}),null);
 }
});
test('initial SMS draft remains manual and never enters startup or automatic migration loaders',()=>{
 const draft=fs.readFileSync(path.join(__dirname,'../docs/sql-drafts/2026-10-22-signal-initial-sms.sql'),'utf8');
 const tables=[...draft.matchAll(/CREATE TABLE IF NOT EXISTS (\w+)/g)].map(m=>m[1]);
 assert.deepEqual(tables,['signal_initial_sms_windows','signal_initial_sms_deliveries']);
 assert.doesNotMatch(draft,/\b(?:ALTER|DROP|DELETE|TRUNCATE)\b/);
 assert.equal(fs.readdirSync(path.join(__dirname,'../migrations')).some(name=>/initial-sms/.test(name)),false);
 for(const file of ['server.js','services/signalOperator/index.js','services/signalOperator/initialSms.js','packages/signal-v1/storage/ensureSignalSchema.js']) {
  assert.doesNotMatch(fs.readFileSync(path.join(__dirname,'..',file),'utf8'),/sql-drafts|2026-10-22-signal-initial-sms\.sql/);
 }
});
test('initial event hook coexists with the newer injected email dispatcher',async()=>{
 const e=event(),stored={...row().payload,id:'a',evidenceId:'kept',sourceId:e.sourceId,channelId:'123',externalMessageId:e.externalMessageId,occurredAt:e.occurredAt,ingestedAt:e.ingestedAt,marketAttempted:true,transportSentAt:e.ingestedAt,researchState:'FIRST_CALLER',latency:{callPersistedAt:e.ingestedAt}};
 let initial=0,dispatched=0;const accepted=[];
 const worker=createOperatorWorker({store:{enqueue:async()=>{},claim:async()=>({id:'a',payload:stored,attempts:1}),setPayload:async(_id,p)=>assert.deepEqual(p,stored),accepted:async(...args)=>accepted.push(args),failed:async()=>assert.fail('dispatch must retain newer behavior')},feedUrl:'http://caller.railway.internal/operator-events',token:'LOCAL-TEST-ONLY-AUTH-FIXTURE-000000',channelId:'123',gate:()=>({ok:true,startAt:new Date(Date.now()-60000).toISOString()}),fetchImpl:async()=>({ok:true,json:async()=>({events:[e]})}),onInitialEvent:async p=>{assert.equal(p,e);initial++;},relay:{send:()=>assert.fail('must dispatch through the existing abstraction')},sendAlert:async p=>{assert.equal(p,stored);dispatched++;return [{receipt:'local-test-email-receipt'}];}});
 assert.equal((await worker.ingestOnce()).enqueued,1);assert.equal((await worker.sendOnce()).accepted,true);
 assert.equal(initial,1);assert.equal(dispatched,1);assert.deepEqual(accepted,[['a','local-test-email-receipt',1]]);
});
test('SMS proceeds while independent email enrichment is unresolved',async()=>{
 let release,started;const waiting=new Promise(r=>started=r),enrichment=new Promise(r=>release=r);let queued=false,sms=false,email=false;const e=event();
 const alert={...row().payload,id:'a',externalMessageId:e.externalMessageId,occurredAt:e.occurredAt,ingestedAt:e.ingestedAt,evidenceId:'e',sourceId:e.sourceId,channelId:'123'};
 const operator=createOperatorWorker({store:{enqueue:async()=>{},claim:async()=>({id:'a',payload:alert,attempts:1}),setPayload:async()=>{},accepted:async()=>{},failed:async()=>{}},feedUrl:'http://caller.railway.internal/operator-events',token:'x'.repeat(32),channelId:'123',gate:()=>({ok:true,startAt:new Date(Date.now()-60000).toISOString()}),fetchImpl:async()=>({ok:true,json:async()=>({events:[e]})}),marketProvider:{getLiveTokenSnapshot:async()=>{started();return enrichment;}},relay:{send:async()=>{email=true;return {receipt:'email'};}},onInitialEvent:async()=>{queued=true;},marketTimeoutMs:10000});
 const emailTask=operator.sendOnce();await waiting;await operator.ingestOnce();assert.equal(queued,true);assert.equal(email,false);
 const worker=createInitialSmsWorker({store:{tenantId:'1',claim:async()=>queued?row():null,authorize:async()=>({remainingMs:1000}),finish:async()=>{}},send:async()=>{sms=true;return 'receipt';},enabled:()=>true,gate:()=>({ok:true})});
 assert.equal((await worker.sendOnce()).accepted,true);assert.equal(sms,true);assert.equal(email,false);release(null);await emailTask;
});
test('fixed recipient, deadline and configuration fail closed',async()=>{
 let sent;const send=createInitialSmsTransport({client:{messages:{create:async p=>{sent=p;return {sid:'receipt'};}}},from:'+15550000001',destination:'+15550000002'});
 await send({...row().payload,to:'+15550000003'},performance.now()+1000);assert.equal(sent.to,'+15550000002');assert.match(sent.body,/Entry risk UNKNOWN/);assert.ok(sent.body.length<300);
 await assert.rejects(()=>send(row().payload,performance.now()-1),/window_closed/);assert.equal(configuration({}),null);assert.throws(()=>configuration({SIGNAL_OPERATOR_SMS_ENABLED:'1'}),/configuration_invalid/);
 let initializes=0;const env={SIGNAL_OPERATOR_SMS_ENABLED:'1',SIGNAL_OPERATOR_SMS_CONSENT:'0',SIGNAL_OPERATOR_TENANT_ID:'1',SIGNAL_OPERATOR_SMS_WINDOW_START:new Date().toISOString(),SIGNAL_OPERATOR_SMS_WINDOW_END:new Date(Date.now()+3600000).toISOString(),TWILIO_PHONE_NUMBER:'+15550000001',OPS_ALERT_PHONE:'+15550000002'};
 assert.equal(await startInitialSms({pool:{query:async()=>{initializes++;}},channelId:'123',gate:()=>({ok:true}),env}),null);assert.equal(initializes,0);
});
test('disabled, closed, wrong tenant and uncertain sends do not report success',async()=>{
 let calls=0;const states=[];const store={tenantId:'1',claim:async()=>row(),authorize:async()=>({remainingMs:1000}),finish:async(_r,s)=>states.push(s)};
 for(const options of [{enabled:()=>false,gate:()=>({ok:true})},{enabled:()=>true,gate:()=>({ok:false})}]){await createInitialSmsWorker({store,send:async()=>{calls++;},...options}).sendOnce();assert.equal(calls,0);}
 const result=await createInitialSmsWorker({store,enabled:()=>true,gate:()=>({ok:true}),send:async()=>{calls++;throw new Error('timeout');}}).sendOnce();assert.equal(result.unknown,true);assert.deepEqual(states,['UNKNOWN']);
 await createInitialSmsWorker({store:{...store,claim:async()=>({...row(),tenant_id:'2'})},enabled:()=>true,gate:()=>({ok:true}),send:async()=>{calls++;}}).sendOnce();assert.equal(calls,1);
});
test('real PG duplicate/restart/concurrency/tenant/cap/expiry/no research writes',{timeout:90000},async t=>{
 const instance=await startDisposablePostgres('initial-sms-'),pool=createTrackedPostgresPool({connectionString:instance.connectionString});t.after(()=>closePostgresFixture(pool,instance));
 await pool.query(fs.readFileSync(path.join(__dirname,'../docs/sql-drafts/2026-10-22-signal-initial-sms.sql'),'utf8'));
 const seed=async s=>{await pool.query('INSERT INTO signal_initial_sms_windows(id,tenant_id,namespace,starts_at,ends_at,sms_limit) VALUES($1,$2,$3,$4,$5,$6)',[s.windowId,s.tenantId,s.namespace,s.startsAt,s.endsAt,s.limit]);await s.initialize();};
 const cfg=config();let store=new InitialSmsStore(pool,cfg);await seed(store);const e=event();await Promise.all(Array.from({length:12},()=>store.enqueue(e)));assert.equal((await pool.query('SELECT * FROM signal_initial_sms_deliveries')).rowCount,1);
 const other=new InitialSmsStore(pool,{...cfg,tenantId:'2'});await seed(other);assert.equal(await other.claim(),null);await other.enqueue(e);
 const claims=await Promise.all(Array.from({length:12},()=>store.claim()));assert.equal(claims.filter(Boolean).length,1);const claimed=claims.find(Boolean);
 assert.equal(await other.authorize(claimed),null);assert.ok(await store.authorize(claimed));assert.equal(await store.authorize(claimed),null);
 await pool.query("UPDATE signal_initial_sms_deliveries SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",[claimed.id]);store=new InitialSmsStore(pool,cfg);await store.initialize();assert.equal(await store.claim(),null);assert.equal((await pool.query('SELECT status FROM signal_initial_sms_deliveries WHERE id=$1',[claimed.id])).rows[0].status,'UNKNOWN');await store.enqueue(e);assert.equal(await store.claim(),null);assert.equal((await pool.query('SELECT sms_reserved FROM signal_initial_sms_windows WHERE id=$1',[store.windowId])).rows[0].sms_reserved,1);
 await assert.rejects(()=>new InitialSmsStore(pool,{...cfg,limit:6}).initialize(),/immutable/);
 const testing=new InitialSmsStore(pool,{...cfg,namespace:'test'});await seed(testing);await testing.enqueue(e);assert.equal(await testing.claim(),null);
 await assert.rejects(()=>store.enqueue({...e,provenance:{...e.provenance,synthetic:true}}),/empirical/);await assert.rejects(()=>store.enqueue({...e,externalMessageId:'telegram:999:42'}),/invalid_event/);await assert.rejects(()=>store.enqueue({...e,extractedCa:'not-a-token'}),/invalid_event/);
 await store.enqueue({...event(43),ingestedAt:new Date(Date.now()+60000).toISOString()});assert.equal(await store.claim(),null);
 const limited=new InitialSmsStore(pool,{...cfg,tenantId:'3',limit:1});await seed(limited);await limited.enqueue(event(44));await limited.enqueue(event(45));const a=await limited.claim(),b=await limited.claim();const auth=await Promise.all([limited.authorize(a),limited.authorize(b)]);assert.equal(auth.filter(Boolean).length,1);
 const expiry=new InitialSmsStore(pool,{...cfg,tenantId:'4'});await seed(expiry);await expiry.enqueue(event(46));const exp=await expiry.claim();await pool.query("UPDATE signal_initial_sms_windows SET ends_at=clock_timestamp()-interval '1 second' WHERE id=$1",[expiry.windowId]);assert.equal(await expiry.authorize(exp),null);
 const future=new InitialSmsStore(pool,{...cfg,tenantId:'6',startsAt:new Date(Date.now()+60000).toISOString(),endsAt:new Date(Date.now()+3600000).toISOString()});await future.initialize();await future.initialize();
 await assert.rejects(()=>new InitialSmsStore(pool,{...cfg,tenantId:'6',startsAt:future.startsAt,endsAt:new Date(Date.now()+3500000).toISOString()}).initialize(),/overlap/);
 await assert.rejects(()=>new InitialSmsStore(pool,{...cfg,tenantId:'7'}).initialize(),/future/);
 const closed=new InitialSmsStore(pool,{...cfg,tenantId:'5',startsAt:new Date(Date.now()-120000).toISOString(),endsAt:new Date(Date.now()-60000).toISOString()});await seed(closed);assert.equal((await closed.enqueue(event(47))).created,false);
 assert.deepEqual((await pool.query("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")).rows.map(r=>r.tablename),['signal_initial_sms_deliveries','signal_initial_sms_windows']);
});
test('revoked consent after claim and expired send fence cause UNKNOWN without transport',async()=>{
 let allowed=true,sends=0;const states=[];
 const store={tenantId:'1',claim:async()=>row(),authorize:async()=>{allowed=false;return {remainingMs:1000};},finish:async(_r,s)=>states.push(s)};
 await createInitialSmsWorker({store,enabled:()=>allowed,gate:()=>({ok:true}),send:async()=>{sends++;}}).sendOnce();assert.equal(sends,0);assert.deepEqual(states,['UNKNOWN']);
 const send=createInitialSmsTransport({client:{messages:{create:async()=>{sends++;return {sid:'r'};}}},from:'+15550000001',destination:'+15550000002'});
 await createInitialSmsWorker({store:{...store,authorize:async()=>({remainingMs:-1})},enabled:()=>true,gate:()=>({ok:true}),send}).sendOnce();assert.equal(sends,0);assert.deepEqual(states,['UNKNOWN','UNKNOWN']);
});
test('SMS enqueue failure cannot block distribution registration or existing email event',async()=>{
 let emailQueued=0,distribution=0;
 const worker=createOperatorWorker({store:{enqueue:async()=>{emailQueued++;}},feedUrl:'http://caller.railway.internal/operator-events',token:'x'.repeat(32),channelId:'123',gate:()=>({ok:true,startAt:new Date(Date.now()-60000).toISOString()}),fetchImpl:async()=>({ok:true,json:async()=>({events:[event()]})}),onInitialEvent:async()=>{throw new Error('local fixture');},onEvent:async()=>{distribution++;}});
 await worker.ingestOnce();assert.equal(emailQueued,1);assert.equal(distribution,1);
});
test('missing draft schema blocks SMS start without applying SQL or constructing transport',async()=>{
 const queries=[];const cfg=config();const env={SIGNAL_OPERATOR_SMS_ENABLED:'1',SIGNAL_OPERATOR_SMS_CONSENT:'1',SIGNAL_OPERATOR_TENANT_ID:'1',SIGNAL_OPERATOR_SMS_WINDOW_START:cfg.startsAt,SIGNAL_OPERATOR_SMS_WINDOW_END:cfg.endsAt,TWILIO_PHONE_NUMBER:'+15550000001',OPS_ALERT_PHONE:'+15550000002'};
 await assert.rejects(()=>startInitialSms({pool:{connect:async()=>({query:async q=>{queries.push(q);if(q.includes('FROM signal_initial_sms_windows'))throw new Error('missing table');return {rows:[]};},release(){}})},channelId:'123',gate:()=>({ok:true}),env}),/missing table/);
 assert.ok(queries.some(q=>q.includes('signal_initial_sms_windows')));assert.doesNotMatch(queries.join(' '),/CREATE|ALTER|DROP/);
});
test('stop during blocked authorization fences transport and records uncertainty',async()=>{
 const cfg=config();const env={SIGNAL_OPERATOR_SMS_ENABLED:'1',SIGNAL_OPERATOR_SMS_CONSENT:'1',SIGNAL_OPERATOR_TENANT_ID:'1',SIGNAL_OPERATOR_SMS_WINDOW_START:cfg.startsAt,SIGNAL_OPERATOR_SMS_WINDOW_END:cfg.endsAt,TWILIO_PHONE_NUMBER:'+15550000001',OPS_ALERT_PHONE:'+15550000002'};
 const old={tenant_id:'1',namespace:'operational',starts_at:new Date(cfg.startsAt),ends_at:new Date(cfg.endsAt),sms_limit:5};
 const pool={connect:async()=>({query:async q=>({rows:q.startsWith('SELECT *')?[old]:[]}),release(){}})};
 let sends=0,cleared=0,armed=0,release,started;const barrier=new Promise(r=>started=r),pending=new Promise(r=>release=r),states=[];
 const runtime=await startInitialSms({pool,channelId:'123',gate:()=>({ok:true}),env,client:{messages:{create:async()=>{sends++;return {sid:'r'};}}},deferStart:true,setInterval:()=>{armed++;return {};},clearInterval:()=>{cleared++;}});
 assert.equal(armed,0);runtime.start();runtime.start();assert.equal(armed,1);
 runtime.store.claim=async()=>row();runtime.store.authorize=async()=>{started();return pending;};runtime.store.finish=async(_r,s)=>states.push(s);
 const task=runtime.worker.sendOnce();await barrier;runtime.stop();release({remainingMs:1000});await task;
 assert.equal(sends,0);assert.equal(cleared,1);runtime.start();assert.equal(armed,1);assert.deepEqual(states,['UNKNOWN']);assert.deepEqual(await runtime.enqueue(event()),{skipped:true});
});
test('later operator construction failure stops already-started lanes',async()=>{
 const vm=require('node:vm');let smsStops=0,distributionStops=0,smsArmed=false;const module={exports:{}};
 const mocks={'node:fs':{readFileSync:()=>''},'node:path':path,'./operationGate':{operationFromEnv:()=>()=>({ok:true})},'./store':{OperatorStore:class{}},'./worker':{createOperatorWorker:()=>{throw new Error('invalid feed');},startOperatorTimers:()=>{throw new Error('must not start');}},'./relay':{createBrevoRelay:()=>({})},'../../packages/signal-v1/providers/GeckoTerminalMarketDataProvider':{GeckoTerminalMarketDataProvider:class{}},'../../db':{query:async()=>({})},'../../packages/signal-v1/storage/ensureSignalSchema':{ensureSignalSchema:async()=>{}},'./initialSms':{startInitialSms:async options=>{assert.equal(options.deferStart,true);return {enqueue:async()=>{},start:()=>{smsArmed=true;},stop:()=>{smsStops++;}};}},'../signalDistribution':{startDistribution:async()=>{await new Promise(resolve=>setTimeout(resolve,30));assert.equal(smsArmed,false);return {register:async()=>{},stop:()=>{distributionStops++;}};}}};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../services/signalOperator/index.js'),'utf8'),{module,__dirname:path.join(__dirname,'../services/signalOperator'),console:{error(){}},process:{env:{SIGNAL_OPERATOR_ENABLED:'1',SIGNAL_OPERATOR_FEED_TOKEN:'x'.repeat(32),SIGNAL_REQUIRED_CALLER_CHANNEL_ID:'123'}},require:id=>{assert.ok(id in mocks,id);return mocks[id];}});
 assert.equal(await module.exports.startSignalOperator(),null);assert.equal(smsStops,1);assert.equal(distributionStops,1);assert.equal(smsArmed,false);
});
