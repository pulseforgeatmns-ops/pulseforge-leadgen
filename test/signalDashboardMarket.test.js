'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {DexScreenerMarketProvider,GeckoCandleProvider,observedCandles,INTERVALS}=require('../services/signalDashboard/market');
const {metric}=require('../services/signalDashboard/quality');
const {peaks,distribution}=require('../services/signalDashboard/analytics');
const {feedEvents,promotionIntensity}=require('../services/signalDashboard/events');
const {CA,POOL,at,later,observation}=require('./helpers/signalDashboardFixtures');
test('unknown, delayed and provider timestamps are distinct; unavailable never equals zero',()=>{
 assert.equal(metric(null,observation(),Date.parse(at)).state,'UNKNOWN');assert.equal(metric(0,observation(),Date.parse(at)).value,0);
 assert.equal(metric(1,observation(),Date.parse(at)).state,'PARTIAL');assert.equal(metric(1,observation(0,{providerTimestamp:at}),Date.parse(at)+4000).state,'LIVE');
 assert.equal(metric(1,observation(0,{providerTimestamp:at}),Date.parse(at)+60000).state,'DELAYED');
});
test('DEX Screener pins the pool and does not substitute FDV for market cap or pretend its receipt timestamp is source time',async()=>{
 let url;const provider=new DexScreenerMarketProvider({now:()=>new Date(at),budget:{take:async()=>{}},fetchFn:async u=>{url=u;return {ok:true,json:async()=>[{chainId:'solana',pairAddress:POOL,baseToken:{address:CA,symbol:'TEST'},priceUsd:'0.001',fdv:200,liquidity:{usd:0},txns:{m5:{buys:0,sells:1}}}]};}});
 const o=await provider.getSnapshot({tokenAddress:CA});assert.equal(o.marketCapUsd,null);assert.equal(o.liquidityUsd,0);assert.equal(o.providerTimestamp,null);assert.equal(o.sellVolume5mUsd,null);assert.equal(o.buys5m,0);
 await assert.rejects(()=>provider.getSnapshot({tokenAddress:CA,poolAddress:'3'.repeat(32)}),/pair_or_price/);assert.match(url,/\/pairs\/solana/);
});
test('all intervals use actual provider OHLCV and disable padded candles; public Gecko fails closed without shared budget',async()=>{
 let requests=0;const urls=[];const p=new GeckoCandleProvider({budget:{take:async()=>{}},proKey:'test-only',now:()=>new Date(later(3600000)),fetchFn:async u=>{requests++;urls.push(u);return {ok:true,json:async()=>({data:{attributes:{ohlcv_list:[[Date.parse(at)/1000,1,2,.5,1.5,10],[Date.parse(later(7200000))/1000,1,2,.5,1.5,10]]}}})};}});
 for(const interval of Object.keys(INTERVALS)){const c=await p.getCandles({tokenAddress:CA,poolAddress:POOL,interval});assert.equal(c.length,1);assert.equal(c[0].occurredAt,at);assert.match(urls.at(-1),/include_empty_intervals=false/);}
 p.proKey=null;await assert.rejects(()=>p.getCandles({tokenAddress:CA,poolAddress:POOL,interval:'1m'}),/shared_gecko_budget/);assert.equal(requests,5);
});
test('sampled candles retain gaps, real timestamps and partial coverage; future/late observations excluded',()=>{
 const os=[observation(0),observation(30000,{priceUsd:2}),observation(180000,{priceUsd:3}),observation(120000,{knownAt:later(999999),priceUsd:100})];
 const cs=observedCandles(os,'1m',later(180000));assert.equal(cs.length,2);assert.equal(cs[0].open,1);assert.equal(cs[0].close,2);assert.equal(cs[0].volume,null);assert.equal(cs[0].quality,'PARTIAL');assert.equal(cs[1].time-cs[0].time,180);
});
test('entry/call observed peaks and drawdown preserve chronology without future ATH or late history',()=>{
 const os=[observation(-1000,{priceUsd:99}),observation(0),observation(1000,{priceUsd:2}),observation(2000,{priceUsd:1.5}),observation(3000,{priceUsd:100})];
 const p=peaks(os,at,later(1000),later(2000));assert.equal(p.postEntry.priceUsd,2);assert.equal(p.postEntry.drawdown,-.25);assert.equal(p.postEntry.chronology.length,2);assert.equal(p.postCall.chronology.length,1);
});
test('promotion same-cluster counts are descriptive and never independent confirmations',()=>{
 const calls=Array.from({length:8},(_,i)=>({sourceId:i%2?'source-b':'source-a',externalMessageId:String(i),text:`Add to positions 5x ${CA}`,occurredAt:later(i*1000),ingestedAt:later(i*1000),provenance:{dataClass:'EMPIRICAL'}}));
 const registry={'source-a':{clusterId:'same',relationship:'CORRELATED'},'source-b':{clusterId:'same',relationship:'CORRELATED'}};
 const events=calls.flatMap(c=>feedEvents(c,{now:new Date(later(10000)),registry}));const p=promotionIntensity([...events,...events],later(10000));assert.equal(p.state,'HIGH');assert.equal(p.uniquePromotionalSources,2);assert.equal(p.sameClusterActivity.same,8);assert.equal(p.independentConfirmations,null);
 assert.equal(events.some(e=>e.category==='INDEPENDENT_CONVERGENCE'),false);assert.deepEqual(feedEvents({...calls[0],provenance:{dataClass:'PROCEDURAL'}},{now:new Date(later(10000))}),[]);
});
test('distribution explains evidence FOR/AGAINST/UNKNOWN and divergence does not issue an exit',()=>{
 const events=Array.from({length:8},(_,i)=>({category:'PROMOTION_EVENT',id:String(i),sourceId:'same',externalMessageId:String(i),clusterId:'same',occurredAt:later(300000+i),knownAt:later(300000+i)}));
 const r=distribution([observation(0),observation(300000,{priceUsd:.5,sells5m:90,buys5m:10,liquidityUsd:5000})],events,later(301000));
 assert.equal(r.state,'CRITICAL');assert.ok(r.for.includes('PROMOTION_MARKET_DIVERGENCE'));assert.ok(r.unknown.some(x=>/Developer/.test(x)));assert.equal(r.divergence.automaticExit,false);
 assert.equal(distribution([],[],at).state,'UNKNOWN');
});
