'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { GeckoTerminalMarketDataProvider } = require('../providers/GeckoTerminalMarketDataProvider');
const { captureMarketSnapshot } = require('../prospective/marketCapture');
const { buildPricePathFromObservations, processDelayCaptureJob } = require('../prospective/outcomeWatch');
const { startShadowScheduler } = require('../prospective/shadowScheduler');
const { buildOperatorAlert } = require('../operator/alertOutbox');
const t = n => new Date(Date.UTC(2026,9,7,0,0,n));

test('live quote uses actual response time; unknown source clock stays unknown', async () => {
  let now = t(0);
  const p = new GeckoTerminalMarketDataProvider({ now: () => now });
  p._fetchJson = async () => { now = t(32); return { data: { attributes: {
    address: 'test-token', price_usd: '1.25', market_cap_usd: null,
    fdv_usd: '900000', total_reserve_in_usd: '850',
  } } }; };
  const result = await captureMarketSnapshot(p,'test-token',t(15), { now: () => t(33) });
  assert.equal(result.ok,true);
  const s = result.snapshot;
  assert.equal(s.occurredAt.toISOString(),t(32).toISOString());
  assert.equal(s.observedTimestamp.toISOString(),t(32).toISOString());
  assert.equal(s.ingestedAt.toISOString(),t(33).toISOString());
  assert.equal(s.providerTimestamp,null);
  assert.equal(s.marketCapUsd,null,'FDV must not silently substitute for market cap');
  assert.equal(s.provenance.freshness,'UNKNOWN');
  assert.equal(s.provenance.targetAt,t(15).toISOString());
});

test('historical snapshot preserves actual candle clock and excludes unclosed candles', async () => {
  const p = new GeckoTerminalMarketDataProvider({ now: () => t(90) });
  p._resolvePrimaryPool = async () => 'test-pool';
  p._fetchJson = async () => ({ data: { attributes: { ohlcv_list: [
    [t(60).getTime()/1000,0,0,0,2,100],
    [t(0).getTime()/1000,0,0,0,1,100],
  ] } } });
  const s = await p.getTokenSnapshot('test-token',t(90));
  assert.equal(s.priceUsd,1);
  assert.equal(s.occurredAt.toISOString(),t(60).toISOString());
  assert.equal(s.providerTimestamp.toISOString(),t(0).toISOString());
  assert.equal(s.requestedAt.toISOString(),t(90).toISOString());
});

test('requested time alone, future time and procedural snapshots fail closed', async () => {
  for (const snap of [
    { asOf:t(15), priceUsd:1 },
    { occurredAt:t(100), observedTimestamp:t(100), priceUsd:1 },
    { occurredAt:t(10), observedTimestamp:t(10), priceUsd:1, provenance:{dataClass:'PROCEDURAL'} },
  ]) {
    const result = await captureMarketSnapshot({getTokenSnapshot:async()=>snap},'test-token',t(15),{now:()=>t(30)});
    assert.equal(result.ok,false);
    assert.equal(result.snapshot,null);
  }
});

test('future, null, zero and procedural prices cannot qualify; late and missing stay explicit', () => {
  const rows = [
    {occurredAt:t(10),priceUsd:1}, {occurredAt:t(16),priceUsd:null},
    {occurredAt:t(17),priceUsd:0}, {occurredAt:t(18),priceUsd:99,provenance:{dataClass:'PROCEDURAL'}},
    {occurredAt:t(31),priceUsd:2}, {occurredAt:t(40),priceUsd:3},
  ];
  const path = buildPricePathFromObservations(rows,t(32));
  assert.deepEqual(path.map(p=>p.price),[1,2]);
  const job = {targetDelaySeconds:15,payload:{knowledgeAt:t(0)}};
  const late = processDelayCaptureJob(job,{occurredAt:t(0)},path);
  assert.equal(late.entryPrice,2);
  assert.equal(late.metadata.captureWindow,'LATE');
  assert.equal(late.metadata.latencyMs,16000);
  assert.equal(processDelayCaptureJob(job,{occurredAt:t(0)},path.slice(0,1)).metadata.captureWindow,'MISSING');
});

test('capture timer continues while coarse ingestion is blocked, without overlapping itself', async () => {
  const timers = []; let release; let captureCalls = 0;
  const pending = new Promise(r=>{release=r;});
  const stop = startShadowScheduler({
    pollCollectorsOnce:()=>pending,
    runDueJobs:async(_limit,{jobType})=>{if(jobType==='DELAY_CAPTURE')captureCalls++;},
  },{setInterval:(fn,ms)=>{const timer={fn,ms};timers.push(timer);return timer;},clearInterval:x=>{x.stopped=true;}});
  assert.deepEqual(timers.map(x=>x.ms),[60000,1000,60000]);
  const polling = timers[0].fn();
  await timers[1].fn(); await timers[1].fn();
  assert.equal(captureCalls,2);
  release(); await polling; stop();
  assert.ok(timers.every(x=>x.stopped));
});

test('outbox dedup and allowlist exclude raw text, outcomes and unverified trade URLs', () => {
  const evidence = {id:'e',sourceId:'telegram-front-runners',externalMessageId:'telegram:123:45',
    extractedCa:'test-token',occurredAt:t(0),ingestedAt:t(5),rawText:'PRIVATE TEXT',
    provenance:{dataClass:'EMPIRICAL',telegramChannelId:'123'}};
  const input = {evidence,approvedChannelId:'123',researchState:'FIRST_CALLER',passRate:1,tradeDestination:'https://unverified.test'};
  const first = buildOperatorAlert(input);
  assert.equal(first.id,buildOperatorAlert({...input,evidence:{...evidence,rawText:'changed'}}).id);
  assert.equal(first.knowledgeAt,t(5).toISOString());
  assert.equal(first.tradeDestination,null);
  assert.equal(first.market.freshness,'UNAVAILABLE');
  assert.equal(JSON.stringify(first).includes('PRIVATE TEXT'),false);
  assert.equal(first.passRate,undefined);
  assert.throws(()=>buildOperatorAlert({...input,researchState:'PASS'}));
  assert.throws(()=>buildOperatorAlert({...input,approvedChannelId:'other'}));
  assert.throws(()=>buildOperatorAlert({...input,evidence:{...evidence,provenance:{dataClass:'PROCEDURAL'}}}));
});

test('cohort gate requires the configured source channel, not connected alone', async () => {
  const {ShadowModeService}=require('../prospective/ShadowModeService');
  const {InMemorySignalStore}=require('../storage/InMemorySignalStore');
  const store=new InMemorySignalStore();
  let source={sourceId:'telegram-front-runners',channelId:'wrong',active:true,available:true};
  const service=new ShadowModeService(store,{now:()=>t(0),requiredCallerSource:{sourceId:source.sourceId,channelId:'123'},
    collectors:[{id:'operator-json-feed',health:async()=>({available:true,connected:true,feedHealth:{sources:[source]}})}]});
  assert.equal(await service.ensureProspectiveCohortWhenCallerFeedReady(),null);
  source={...source,channelId:'123',available:false};
  assert.equal(await service.ensureProspectiveCohortWhenCallerFeedReady(),null);
  source={...source,available:true};
  assert.ok(await service.ensureProspectiveCohortWhenCallerFeedReady());
});

test('activation bundle refuses missing identity and generates only the approved source', () => {
  const {buildActivationBundle}=require('../../../scripts/buildSignalDeploymentBundle');
  assert.throws(()=>buildActivationBundle({source:'frontrunz',accessible:true}));
  const bundle=buildActivationBundle({source:'frontrunz',username:'frontrunz',accessible:true,messageReadSucceeded:true,channelId:'123'});
  assert.equal(bundle.delivery.enabled,false);
  assert.equal(bundle.phase2.variables.SIGNAL_SHADOW_POLL_MS,'60000');
  const sources=JSON.parse(bundle.phase1.variables.TELEGRAM_CALLER_SOURCES_JSON);
  assert.equal(sources.length,1);
  assert.equal(sources[0].expectedChannelId,'123');
  assert.equal(sources[0].clusterRelationshipStatus,'UNKNOWN');
});

test('blinding allowlist removes nested performance and outcome counts', () => {
  const {redactEvaluationIfBlinded}=require('../prospective/blinding');
  const evaluation={cohortId:'c',cohortN:3,dataClass:'EMPIRICAL',executionDelaySeconds:60,
    primaryHypothesisPrecision:0.5,report:{returns:99},sensitivity:{PASS:2},
    layers:[{observationType:'FIRST_CALLER',N:3,PASS:2,FAIL:1,precision:0.5,report:{pnl:99}}]};
  const result=redactEvaluationIfBlinded(evaluation,{metadata:{blinded:true}});
  assert.equal(result.cohortN,3);
  assert.equal(result.layers[0].N,3);
  assert.equal(result.primaryHypothesisPrecision,null);
  for (const secret of ['PASS','FAIL','returns','sensitivity','pnl','report']) {
    assert.equal(JSON.stringify(result).includes(secret),false,secret);
  }
});

test('unclassified cluster never implies independence', () => {
  const {resolveClusterRelationship}=require('../prospective/sourceIndependence');
  const store={clusters:new Map([['c',{clusterType:'organic'}]])};
  assert.equal(resolveClusterRelationship(store,{clusterId:'c'}),'UNKNOWN');
  assert.equal(resolveClusterRelationship(store,{clusterId:'c',clusterRelationshipStatus:'INDEPENDENT'}),'INDEPENDENT');
  assert.equal(resolveClusterRelationship(store,{clusterId:'c'},{forwardedFromSourceId:'s'}),'CORRELATED');
});

test('evaluation and unblinding cannot implicitly create the prospective cohort', async () => {
  const {ShadowModeService}=require('../prospective/ShadowModeService');
  const {InMemorySignalStore}=require('../storage/InMemorySignalStore');
  const store=new InMemorySignalStore();
  const service=new ShadowModeService(store,{now:()=>t(0)});
  await assert.rejects(service.evaluateProspectiveCohort(),/prospective_cohort_not_started/);
  await assert.rejects(service.explicitUnblind({unblindedBy:'test'}),/prospective_cohort_not_started/);
  assert.equal(store.researchCohorts.size,0);
});

test('pinned ingestion rejects unexpected sources, channels and nonempirical evidence before storage', async () => {
  const {ShadowModeService}=require('../prospective/ShadowModeService');
  const {InMemorySignalStore}=require('../storage/InMemorySignalStore');
  const store=new InMemorySignalStore();
  const service=new ShadowModeService(store,{requiredCallerSource:{sourceId:'telegram-front-runners',channelId:'123'}});
  const base={sourceId:'telegram-front-runners',provenance:{telegramChannelId:'123',dataClass:'EMPIRICAL'}};
  for(const row of [{...base,sourceId:'other'},{...base,provenance:{...base.provenance,telegramChannelId:'456'}},
    {...base,provenance:{telegramChannelId:'123'}}, {...base,provenance:{...base.provenance,synthetic:true}},
    {...base,provenance:{...base.provenance,testOnly:true}}]) {
    assert.equal((await service.ingestRawCallerObservation(row,{collectorId:'operator-json-feed'})).accepted,false);
  }
  assert.equal(store.rawCallerEvidence.length,0);
});

test('market identity, explicit empirical provenance and ordered source clocks are mandatory', async () => {
  const base={tokenAddress:'test-token',priceUsd:1,occurredAt:t(10),observedTimestamp:t(11),provenance:{dataClass:'EMPIRICAL'}};
  assert.equal((await captureMarketSnapshot({getTokenSnapshot:async()=>base},'test-token',t(0),{now:()=>t(30)})).ok,true);
  for(const snap of [{...base,tokenAddress:'wrong'}, {...base,provenance:{}},
    {...base,provenance:{...base.provenance,synthetic:true}}, {...base,providerTimestamp:t(12)},
    {...base,observedTimestamp:t(9)}]) {
    assert.equal((await captureMarketSnapshot({getTokenSnapshot:async()=>snap},'test-token',t(0),{now:()=>t(30)})).ok,false);
  }
});

test('approved source cannot persist future or invalid evidence clocks', async () => {
  const {ShadowModeService}=require('../prospective/ShadowModeService');
  const {InMemorySignalStore}=require('../storage/InMemorySignalStore');
  const store=new InMemorySignalStore();
  const service=new ShadowModeService(store,{now:()=>t(30),requiredCallerSource:{sourceId:'telegram-front-runners',channelId:'123'}});
  for (const clocks of [{messageTimestamp:t(31),ingestedAt:t(30)},
    {messageTimestamp:t(1),ingestedAt:t(31)},{messageTimestamp:'invalid',ingestedAt:t(2)}]) {
    const result=await service.ingestRawCallerObservation({...clocks,sourceId:'telegram-front-runners',
      provenance:{dataClass:'EMPIRICAL',telegramChannelId:'123'}},{collectorId:'operator-json-feed'});
    assert.equal(result.reason,'invalid_or_future_evidence_clock');
  }
  assert.equal(store.rawCallerEvidence.length,0);
});
