'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {Pool}=require('pg');
const {startDisposablePostgres}=require('../../../test/helpers/disposablePostgres');
const {ensureSignalSchema}=require('../storage/ensureSignalSchema');
const {checkPilot}=require('../../../services/signalOperator/pilotGate');
const {minimalEvents,authorized}=require('../../../services/telegramCallerFeed/operatorEvents');
const {OperatorStore}=require('../../../services/signalOperator/store');
const {createOperatorWorker,startOperatorTimers}=require('../../../services/signalOperator/worker');
const {createBrevoRelay,mailPayload}=require('../../../services/signalOperator/relay');
const token='2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
const fixtureAuth='OPERATIONAL-TEST-ONLY-AUTH-FIXTURE-000';
const t=n=>new Date(Date.UTC(2026,9,7,0,0,n));
const event=()=>({sourceId:'telegram-front-runners',externalMessageId:'telegram:123:45',extractedCa:token,
  occurredAt:t(1).toISOString(),ingestedAt:t(2).toISOString(),provenance:{dataClass:'EMPIRICAL',telegramChannelId:'123'}});
const readiness=()=>({projectId:'5f6c50eb-6a61-4649-885e-dc3f3b80a2e5',environmentId:'7046b237-d3a6-4884-8a1e-bd75d809fa3b',pilotStartAt:t(0).toISOString(),observedAt:t(5).toISOString(),resourceLimitsVerified:true,feedCpu:.25,feedMemoryBytes:268435456,
  feedVolumeGB:1,feedReplicas:1,privateOnly:true,stopMechanismVerified:true,incrementalSpendUsd:0});

test('pilot stops on expiry, stale/unknown usage, spend ceiling and unverified resources; restart cannot reset it',()=>{
  const config={startAt:t(0),expiresAt:new Date(+t(0)+48*3600000),readiness:readiness()};
  assert.equal(checkPilot(config,t(10)).ok,true);
  assert.equal(checkPilot(config,new Date(+t(0)+48*3600000)).ok,false);
  assert.equal(checkPilot(config,t(1000)).ok,false);
  for(const patch of [{incrementalSpendUsd:2},{incrementalSpendUsd:null},{feedCpu:1},{feedMemoryBytes:undefined},
    {resourceLimitsVerified:false},{privateOnly:false},{stopMechanismVerified:false}])
    assert.equal(checkPilot({...config,readiness:{...readiness(),...patch}},t(10)).ok,false);
  assert.equal(checkPilot({...config,expiresAt:new Date(+t(0)+49*3600000)},t(10)).ok,false);
});

test('minimal authenticated feed strips raw message and aggregate fields, rejects other pins and test evidence',()=>{
  const call={...event(),text:`private body ${token}`,rawText:'secret message',passRate:1};
  const rows=minimalEvents([call],'123',{startAt:t(0),now:t(5)});
  assert.equal(rows.length,1);
  assert.equal(rows[0].extractedCa,token);
  for(const forbidden of ['private body','rawText','passRate'])assert.equal(JSON.stringify(rows).includes(forbidden),false);
  assert.equal(minimalEvents([call],'456',{startAt:t(0),now:t(5)}).length,0);
  assert.equal(minimalEvents([{...call,provenance:{...call.provenance,testOnly:true}}],'123',{startAt:t(0),now:t(5)}).length,0);
  assert.equal(minimalEvents([call],'123',{startAt:t(3),now:t(5)}).length,0);
  assert.equal(authorized(`Bearer ${fixtureAuth}`,fixtureAuth),true);
  assert.equal(authorized('Bearer wrong',fixtureAuth),false);
  assert.equal(authorized(undefined,undefined),false);
});

test('relay defaults off and requires separate consent; fake transport records acceptance only',async()=>{
  let sends=0;
  const fakeFetch=async(_url,options)=>{sends++;const data=JSON.parse(options.body);
    assert.equal(data.sender.email,'jacob@gopulseforge.com');assert.equal(data.to[0].email,'pulseforgeatmns@gmail.com');
    assert.equal(data.textContent.includes('RAW PRIVATE'),false);
    return {ok:true,json:async()=>({messageId:'operational-test-receipt'})};};
  const alert={id:'a'.repeat(64),tokenAddress:token,channelId:'123',externalMessageId:'telegram:123:45',knowledgeAt:t(2),
    researchState:'PENDING_RESEARCH',market:{freshness:'UNAVAILABLE'},rawText:'RAW PRIVATE',aggregatePrecision:1};
  await assert.rejects(createBrevoRelay({fetchImpl:fakeFetch}).send(alert),/disabled/);
  await assert.rejects(createBrevoRelay({enabled:true,apiKey:'TEST-ONLY',fetchImpl:fakeFetch,gate:()=>({ok:true})}).send(alert),/disabled/);
  assert.equal(sends,0);
  const relay=createBrevoRelay({enabled:true,consented:true,apiKey:'TEST-ONLY',fetchImpl:fakeFetch,gate:()=>({ok:true})});
  assert.deepEqual(await relay.send(alert),{receipt:'operational-test-receipt',accepted:true,received:false,displayed:false});
  assert.equal(mailPayload(alert).headers.idempotencyKey,mailPayload({...alert,market:{}}).headers.idempotencyKey);
});

test('operator timers run independently every second',async()=>{
  const timers=[];let calls=0;
  const stop=startOperatorTimers({ingestOnce:async()=>{calls++;},sendOnce:async()=>{calls++;}},
    {setInterval:(fn,ms)=>{timers.push({fn,ms});return {};},clearInterval:()=>{}});
  assert.deepEqual(timers.map(t=>t.ms),[1000,1000]);
  await Promise.all(timers.map(t=>t.fn()));assert.equal(calls,2);stop();
});

test('dedicated service entrypoint fails closed without feed auth but module import does not',()=>{
  const {spawnSync}=require('node:child_process');
  const path=require('node:path');
  const entry=path.join(__dirname,'../../../services/telegramCallerFeed/server.js');
  assert.doesNotThrow(()=>require('../../../services/telegramCallerFeed/server'));
  const result=spawnSync(process.execPath,[entry],{
    env:{...process.env,TELEGRAM_API_ID:'1',TELEGRAM_API_HASH:'hash',TELEGRAM_SESSION_STRING:'session',
      SIGNAL_OPERATOR_FEED_TOKEN:'',TELEGRAM_CALLER_SOURCES_JSON:'[]'},
    timeout:8000,encoding:'utf8'});
  assert.notEqual(result.status,0);
  assert.match(result.stderr,/feed_auth_not_configured/);
});

test('protected /feed rejects missing and incorrect token and accepts bearer auth',async()=>{
  process.env.TELEGRAM_API_ID='1';
  process.env.TELEGRAM_API_HASH='hash';
  process.env.TELEGRAM_SESSION_STRING='session';
  const {createApp}=require('../../../services/telegramCallerFeed/server');
  const engine={getHealth:()=>({connected:true,sources:[]}),getRecentCalls:()=>[],pollOnce:async()=>({connected:true,errors:[]})};
  const app=createApp(engine,{env:{SIGNAL_OPERATOR_FEED_TOKEN:fixtureAuth,...process.env}});
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});
  try {
    const port=server.address().port;
    assert.equal((await fetch(`http://127.0.0.1:${port}/feed`)).status,401);
    assert.equal((await fetch(`http://127.0.0.1:${port}/feed`,{headers:{authorization:'Bearer wrong'}})).status,401);
    const ok=await fetch(`http://127.0.0.1:${port}/feed`,{headers:{authorization:`Bearer ${fixtureAuth}`}});
    assert.equal(ok.status,200);
    const body=await ok.json();assert.ok(Array.isArray(body.calls));
  }finally{await new Promise(resolve=>server.close(resolve));}
});

test('health never exposes feed token or session material',async()=>{
  const {createApp}=require('../../../services/telegramCallerFeed/server');
  const secret='SUPER-SECRET-SESSION-AND-TOKEN-VALUE-000000000001';
  const engine={
    credentialsStatus:()=>({ok:true}),
    pollOnce:async()=>({connected:true,errors:[]}),
    getHealth:(meta)=>({connected:Boolean(meta.connected),sources:[]}),
  };
  const app=createApp(engine,{env:{SIGNAL_OPERATOR_FEED_TOKEN:secret}});
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});
  try {
    const response=await fetch(`http://127.0.0.1:${server.address().port}/health`);
    const text=await response.text();
    assert.equal(text.includes(secret),false);
    assert.equal(text.includes('feedAuthConfigured'),true);
  }finally{await new Promise(resolve=>server.close(resolve));}
});

test('private route requires auth and genuine health and returns no raw text',async()=>{
  const {createApp}=require('../../../services/telegramCallerFeed/server');
  const now=new Date();const call={...event(),occurredAt:now.toISOString(),ingestedAt:now.toISOString(),text:`private ${token}`};
  const engine={getHealth:()=>({lastSuccessfulPoll:now.toISOString(),sources:[{sourceId:'telegram-front-runners',channelId:'123',active:true,available:true}]}),getRecentCalls:()=>[call]};
  const app=createApp(engine,{env:{SIGNAL_OPERATOR_FEED_ENABLED:'1',SIGNAL_OPERATOR_FEED_TOKEN:fixtureAuth,SIGNAL_REQUIRED_CALLER_CHANNEL_ID:'123'},
    gate:()=>({ok:true,startAt:new Date(+now-1000).toISOString()})});
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});
  try {
    const url=`http://127.0.0.1:${server.address().port}/operator-events`;
    assert.equal((await fetch(url)).status,401);
    const response=await fetch(url,{headers:{authorization:`Bearer ${fixtureAuth}`}});
    assert.equal(response.status,200);const body=await response.json();assert.equal(body.events.length,1);
    assert.equal(JSON.stringify(body).includes('private'),false);
  }finally{await new Promise(resolve=>server.close(resolve));}
});

test('durable operational enqueue/retry/restart has no research writes and market timeout cannot hold CA enqueue',async()=>{
  const instance=await startDisposablePostgres('sig-operator-',{socketPrefix:'sigo-'});
  const pool=new Pool({connectionString:instance.connectionString});
  try {
    await ensureSignalSchema(pool);
    for(const name of ['2026-10-07-signal-v1-operator-outbox.sql','2026-10-07-signal-v1-operator-relay.sql'])
      await pool.query(fs.readFileSync(path.join(__dirname,'../../../migrations',name),'utf8'));
    let now=t(5),open=true,sends=0;
    const gate=()=>({ok:open,startAt:t(0).toISOString()});
    const store=new OperatorStore(pool,{channelId:'123',now:()=>now});
    const options={store,feedUrl:'http://feed.railway.internal:3099/operator-events',token:fixtureAuth,channelId:'123',gate,now:()=>now,
      marketProvider:{getLiveTokenSnapshot:()=>new Promise(()=>{})},marketTimeoutMs:10,
      fetchImpl:async()=>({ok:true,json:async()=>({events:[event()]})}),
      relay:{send:async alert=>{sends++;assert.equal(alert.market.freshness,'UNAVAILABLE');if(sends===1)throw new Error('relay_transport_unknown');return {receipt:'local-operational-test-receipt'};}}};
    assert.throws(()=>createOperatorWorker({...options,feedUrl:'https://public.example/operator-events'}));
    const worker=createOperatorWorker(options);
    await worker.ingestOnce();await worker.ingestOnce();
    assert.equal((await pool.query('SELECT count(*) FROM signal_operator_events')).rows[0].count,'1');
    assert.equal((await pool.query('SELECT count(*) FROM signal_operator_alert_outbox')).rows[0].count,'1');
    for(const table of ['signal_raw_caller_evidence','signal_events','signal_research_observations','signal_research_cohorts','signal_prospective_research_jobs'])
      assert.equal((await pool.query(`SELECT count(*) FROM ${table}`)).rows[0].count,'0',table);
    await worker.sendOnce();
    const pending=(await pool.query('SELECT * FROM signal_operator_alert_outbox')).rows[0];
    assert.equal(pending.delivery_state,'PENDING');assert.equal(pending.payload.marketAttempted,true);
    now=t(10);
    const restarted=new OperatorStore(pool,{channelId:'123',now:()=>now});
    const next=createOperatorWorker({...options,store:restarted,marketProvider:{getLiveTokenSnapshot:()=>{throw new Error('must preserve original retry body');}}});
    await next.sendOnce();
    const accepted=(await pool.query('SELECT * FROM signal_operator_alert_outbox')).rows[0];
    assert.equal(accepted.delivery_state,'ACCEPTED');assert.equal(accepted.received_at,null);assert.equal(accepted.displayed_at,null);assert.equal(accepted.delivered_at,null);
    await restarted.recordReceipt(accepted.id,'RECEIVED','local-inbox-observation');
    assert.equal((await pool.query('SELECT displayed_at FROM signal_operator_alert_outbox')).rows[0].displayed_at,null);
    await assert.rejects(restarted.recordReceipt(accepted.id,'SEEN','invented'));
    open=false;await next.sendOnce();await next.ingestOnce();assert.equal(sends,2);
    open=true;await store.enqueue({...event(),externalMessageId:'telegram:123:46'});
    assert.equal(await store.claim({since:t(3)}),null,'a new pilot cannot send an old pilot alert');
    const claimed=await store.claim();assert.ok(claimed);
    assert.equal(await restarted.claim(),null,'active lease prevents parallel sends');
    now=t(41);assert.equal((await restarted.claim()).id,claimed.id,'expired lease recovers after crash');
    await assert.rejects(store.accepted(claimed.id,'stale-worker-receipt',claimed.attempts),/lease_lost/);
    now=t(700);assert.equal(await restarted.claim(),null);
    assert.equal((await pool.query('SELECT delivery_state FROM signal_operator_alert_outbox WHERE id=$1',[claimed.id])).rows[0].delivery_state,'UNKNOWN');
    await assert.rejects(store.enqueue({...event(),provenance:{...event().provenance,testOnly:true}}));
  }finally{await pool.end();await instance.stop();}
});

test('front runners CA alert does not require independent convergence',()=>{
  const {buildOperatorAlert}=require('../operator/alertOutbox');
  const evidence={id:'e',sourceId:'telegram-front-runners',externalMessageId:'telegram:123:45',
    extractedCa:token,occurredAt:t(0).toISOString(),ingestedAt:t(2).toISOString(),
    provenance:{dataClass:'EMPIRICAL',telegramChannelId:'123'}};
  const alert=buildOperatorAlert({evidence,approvedChannelId:'123',researchState:'FIRST_CALLER',independentConvergence:'none'});
  assert.equal(alert.independentConvergence,'none');
  assert.match(alert.experimentalLabel,/EXPERIMENTAL/);
});

test('operational transport tests are explicitly labeled, contain no CA and need separate approval',async()=>{
  const {operationalTransportTest}=require('../../../services/signalOperator/relay');
  const fixture=operationalTransportTest('local-only-transport-test');
  const payload=mailPayload(fixture);
  assert.match(payload.subject,/Signal Operational Test/);
  assert.match(payload.textContent,/Exclude from all research/);
  assert.equal(payload.textContent.includes(token),false);
  let sent=0;
  const relay=createBrevoRelay({enabled:true,consented:true,apiKey:'TEST-ONLY',gate:()=>({ok:true}),
    fetchImpl:async()=>{sent++;throw new Error('must not run');}});
  await assert.rejects(relay.send(fixture),/operational_test_not_approved/);
  assert.equal(sent,0);
});
