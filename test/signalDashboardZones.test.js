'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {VERSION,RULES,closedBars,discover,evolve}=require('../services/signalDashboard/zones');
const {CA,POOL}=require('./helpers/signalDashboardFixtures');
const start=Date.parse('2026-10-25T00:00:00Z'),iso=t=>new Date(t).toISOString(),created=iso(start+24*300000+10000);
function candle(i,extra={}){return {time:start/1000+i*300,interval:'5m',open:1.05,high:1.12,low:1.03,close:1.04,volume:10,complete:true,coverage:'PROVIDER_OHLCV',pairId:POOL,provider:'test-only',provenance:{dataClass:'EMPIRICAL'},knownAt:iso(start+(i+1)*300000),...extra};}
function observation(at,extra={}){return {pairId:POOL,priceUsd:1.03,liquidityUsd:1000,observedAt:at,knownAt:at,provider:'test-only',provenance:{dataClass:'EMPIRICAL'},...extra};}
function input(extra={}){const candles=Array.from({length:24},(_,i)=>candle(i,{low:i===5?1:i===12?1.005:1.03}));return {tokenAddress:CA,pairId:POOL,candles,observations:[observation(created)],risk:'WATCH',transactions:[],asOf:created,...extra};}
function found(){const result=discover(input());assert.ok(result.zone);return result.zone;}
function confirmation(zone,extra={}){const asOf=iso(start+27*300000+10000),candles=[...input().candles,candle(24),candle(25,{low:1.01,close:1.025}),candle(26,{close:1.03})];return {pairId:POOL,candles,observations:[observation(asOf)],risk:'WATCH',asOf,...extra};}
test('research discovery freezes rules, inputs and true discovery time; re-entry needs observed earlier sale evidence',()=>{
 const z=found();assert.equal(z.version,VERSION);assert.equal(z.kind,'ENTRY');assert.equal(z.state,'WATCHING');assert.equal(z.createdAt,created);assert.equal(z.inputSnapshot.candles.length,24);assert.equal(z.paper.validated,false);assert.equal(z.paper.automaticExecution,false);assert.equal(z.paper.netReturn,null);assert.equal(z.paper.slippage,'UNKNOWN');
 const sell={type:'SELL',blockTime:iso(start),knownAt:created};assert.equal(discover(input({transactions:[sell]})).zone.kind,'RE_ENTRY');assert.equal(discover(input({transactions:[{...sell,knownAt:iso(Date.parse(created)+1)}]})).zone.kind,'ENTRY');assert.deepEqual(z.rules,RULES);
});
test('future candles, open bars, sampled prices, late knowledge and missing observations cannot create setups',()=>{
 for(const mutate of [x=>x.candles=x.candles.map(c=>({...c,provenance:{dataClass:'PROCEDURAL'}})),x=>x.candles=x.candles.map(c=>({...c,provenance:{dataClass:'EMPIRICAL',testOnly:true}})),x=>x.candles=x.candles.map(c=>({...c,complete:false})),x=>x.candles=x.candles.map(c=>({...c,coverage:'SAMPLED_OBSERVATIONS_ONLY'})),x=>x.candles=x.candles.map(c=>({...c,knownAt:iso(Date.parse(created)+1)})),x=>x.observations=[],x=>x.observations=[observation(created,{liquidityUsd:null})],x=>x.risk='UNKNOWN',x=>x.risk='HIGH']){const x=input();mutate(x);assert.equal(discover(x).zone,null);}
 assert.equal(discover(input({candles:input().candles.slice(1)})).zone,null);assert.throws(()=>discover(input({asOf:'bad'})),/invalid_zone_time/);
 const gap=input();gap.candles[0]={...gap.candles[0],time:gap.candles[0].time-300};assert.equal(discover(gap).zone,null);
});
test('five-minute rollup requires all five actually observed closed minute candles and retains unknown volume',()=>{
 const rows=Array.from({length:5},(_,i)=>({...candle(0),time:start/1000+i*60,interval:'1m',volume:i?10:null,knownAt:iso(start+300000)}));assert.equal(closedBars(rows,iso(start+300000)).length,1);assert.equal(closedBars(rows,iso(start+300000))[0].volume,null);assert.equal(closedBars(rows.slice(1),iso(start+300000)).length,0);assert.equal(closedBars(rows.map(c=>({...c,complete:false})),iso(start+300000)).length,0);
});
test('conditions need two entirely new closed candles and current liquidity/risk; no retroactive trigger',()=>{
 const z=found(),before=JSON.stringify(z);assert.equal(evolve(z,input()),null);const next=evolve(z,confirmation(z));assert.equal(next.state,'CONDITIONS_OBSERVED');assert.equal(next.paper.reference.recordedAt,confirmation(z).asOf);assert.equal(next.paper.reference.priceUsd,1.03);assert.equal(JSON.stringify(z),before);
 for(const change of [{risk:'UNKNOWN'},{observations:[]},{observations:[observation(confirmation(z).asOf,{liquidityUsd:850})]},{observations:[observation(created)]},{candles:[...input().candles,candle(24,{low:1.01,close:1.025}),candle(25,{close:1.03})]}])assert.equal(evolve(z,confirmation(z,change)),null);
});
test('invalidation and expiry are explicit; old high-risk or invalidated setups never resurrect',()=>{
 const z=found(),now=confirmation(z).asOf;
 for(const change of [{risk:'HIGH'},{pairId:'other-pool'},{observations:[observation(now,{liquidityUsd:790})]},{candles:[...input().candles,candle(25,{low:.95,close:.97})]}]){const n=evolve(z,confirmation(z,change));assert.equal(n.state,'INVALIDATED');assert.equal(evolve(n,confirmation(n,{asOf:iso(Date.parse(now)+1000)})),null);}
 assert.equal(evolve(z,confirmation(z,{asOf:z.expiresAt,observations:[]})).state,'EXPIRED');
});
test('paper checkpoints compare observed prices only, disclose costs, and do not use later prices for missed windows',()=>{
 const z=evolve(found(),confirmation(found())),ref=Date.parse(z.paper.reference.recordedAt),asOf=iso(ref+15*60000+5000),o=observation(asOf,{priceUsd:1.1});
 const n=evolve(z,{...confirmation(z),asOf,observations:[o]});assert.equal(n.paper.checkpoints.length,1);assert.equal(n.paper.checkpoints[0].state,'OBSERVED');assert.equal(n.paper.checkpoints[0].netReturn,null);assert.equal(n.paper.checkpoints[0].slippage,'UNKNOWN');assert.ok(n.paper.checkpoints[0].grossPriceChange>0);
 const missed=iso(ref+63*60000),m=evolve(n,{...confirmation(n),asOf:missed,observations:[observation(missed,{priceUsd:2})]});assert.equal(m.paper.checkpoints.find(x=>x.minutes===60).state,'UNKNOWN');assert.equal(m.paper.checkpoints.find(x=>x.minutes===60).grossPriceChange,null);assert.equal(evolve(m,{...confirmation(m),asOf:iso(ref+64*60000),observations:[o]}),null);
});
module.exports={input,found,confirmation,observation,candle,created,start,iso};
test('invalidated paper setups keep their later comparisons instead of dropping losing observations',()=>{
 const marked=evolve(found(),confirmation(found())),ref=Date.parse(marked.paper.reference.recordedAt),bad=iso(ref+60000);
 const invalid=evolve(marked,{...confirmation(marked),risk:'HIGH',asOf:bad,observations:[observation(bad,{priceUsd:1})]});assert.equal(invalid.state,'INVALIDATED');
 const at=iso(ref+15*60000+1000),result=evolve(invalid,{...confirmation(invalid),asOf:at,observations:[observation(at,{priceUsd:.8})]});assert.equal(result.state,'INVALIDATED');assert.ok(result.paper.checkpoints[0].grossPriceChange<0);assert.equal(result.paper.checkpoints[0].netReturn,null);
});
test('zone collector persists discoveries once, survives restart and writes only its isolated research store',async()=>{
 const {MemoryStore}=require('./helpers/signalDashboardFixtures'),{researchFixture}=require('./helpers/signalDashboardZoneFixtures'),{createZoneWorker}=require('../services/signalDashboard/zoneWorker');const f=researchFixture(),s=new MemoryStore(),events=[];
 await s.put('watch',CA,'watch',{tokenAddress:CA,poolAddress:POOL,occurredAt:f.args.asOf,knownAt:f.args.asOf});for(const c of f.candles)await s.put('candle',CA,String(c.time),{...c,occurredAt:iso(c.time*1000)});await s.put('observation',CA,'o',f.observation);await s.put('cursor',CA,'risk',{key:'risk:'+CA,state:{riskState:'WATCH'},occurredAt:f.args.asOf,knownAt:f.args.asOf});
 const zoneStore={latest:async()=>events.slice(-1),append:async z=>events.push(structuredClone(z))},options={store:s,zoneStore,now:()=>new Date(f.args.asOf)};
 await createZoneWorker(options).tick();await createZoneWorker(options).tick();assert.equal(events.length,1);assert.equal(events[0].createdAt,f.args.asOf);assert.equal((await s.list('transaction',CA)).length,0);assert.equal((await s.list('event',CA)).length,0);assert.equal((await s.list('status','*'))[0].mode,'PAPER_OBSERVATION');
});
