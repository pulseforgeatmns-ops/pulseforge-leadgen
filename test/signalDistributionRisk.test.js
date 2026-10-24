'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {evaluate}=require('../services/signalDistribution/risk');
const {newCriticalEvidence}=require('../services/signalDistribution/deliveryStore');
const start=Date.parse('2026-10-14T13:00:00Z');
function sample(minute, fields={}) {return {tokenAddress:'token',pairId:'pair',observedAt:new Date(start+minute*60000).toISOString(),providerTimestamp:null,priceUsd:100,marketCapUsd:null,liquidityUsd:10000,volume5mUsd:1000,buys5m:50,sells5m:50,buyVolume5mUsd:500,sellVolume5mUsd:500,provenance:{dataClass:'EMPIRICAL',synthetic:false,testOnly:false},...fields};}
function run(previous,minute,fields={},options={}) {return evaluate(previous,sample(minute,fields),{now:new Date(start+minute*60000),...options});}
test('unknown source clock is explicit WATCH, no wallet/dev evidence or invented multiple',()=>{
 const r=run({},0).state;assert.equal(r.riskState,'WATCH');assert.equal(r.coverage.sourceFreshness,'UNKNOWN');assert.equal(r.coverage.walletDistribution,'UNKNOWN');assert.equal(r.coverage.developerIdentity,'UNKNOWN');assert.equal(r.metrics.referenceMultiple,null);assert.equal(r.metrics.observedMfeMultiple,null);
});
test('independent observed peak and corroborating flow raise HIGH then CRITICAL',()=>{
 let s=run({},0).state;s=run(s,1,{priceUsd:200}).state;
 s=run(s,2,{priceUsd:150,sellVolume5mUsd:800,buyVolume5mUsd:200}).state;
 assert.equal(s.riskState,'HIGH');assert.equal(s.metrics.drawdownFromObservedPeak,.25);
 s=run(s,3,{priceUsd:120}).state;assert.equal(s.riskState,'WATCH');
 s=run(s,4,{priceUsd:120,sellVolume5mUsd:900,buyVolume5mUsd:100}).state;assert.equal(s.riskState,'CRITICAL');
});
test('stale, missing and changed-pair samples never downgrade prior CRITICAL or move peak',()=>{
 let s=run({},0).state;s=run(s,1,{priceUsd:60,sellVolume5mUsd:900,buyVolume5mUsd:100}).state;assert.equal(s.riskState,'CRITICAL');
 for(const fields of [{priceUsd:null},{pairId:'other',priceUsd:999},{providerTimestamp:new Date(start-600000).toISOString(),priceUsd:999}]) {
  const next=run(s,2,fields).state;assert.equal(next.riskState,'CRITICAL');assert.equal(next.observedPeakPriceUsd,100);
 }
 const old=sample(2,{priceUsd:999});const stale=evaluate(s,old,{now:new Date(start+10*60000)}).state;
 assert.equal(stale.riskState,'CRITICAL');assert.equal(stale.observedPeakPriceUsd,100);
});
test('duplicate and older observations are ignored, future samples ignored, fixtures rejected',()=>{
 const s=run({},1).state;assert.equal(run(s,1).ignored,true);assert.equal(run(s,0).ignored,true);
 assert.equal(evaluate(s,sample(3),{now:new Date(start+2*60000)}).ignored,true);
 assert.throws(()=>run(s,2,{provenance:{dataClass:'EMPIRICAL',testOnly:true}}),/empirical/);
});
test('manual reference computes only forward sample multiple/MFE; milestone once after restart',()=>{
 let s=run({},0,{priceUsd:1000}).state;s={...s,reference:{priceUsd:100,recordedAt:new Date(start+2*60000).toISOString(),provenance:'EXPLICIT_MANUAL_OPERATOR'},referencePeakPriceUsd:null};
 s=run(s,1,{priceUsd:1000}).state;assert.equal(s.metrics.referenceMultiple,null);assert.equal(s.metrics.observedMfeMultiple,null);
 let r=run(s,2,{priceUsd:200});assert.equal(r.milestoneNew,true);assert.equal(r.state.metrics.observedMfeMultiple,2);
 const restored=JSON.parse(JSON.stringify(r.state));r=run(restored,3,{priceUsd:150});assert.equal(r.milestoneNew,false);assert.equal(r.state.metrics.referenceMultiple,1.5);assert.equal(r.state.metrics.observedMfeMultiple,2);
});
test('null flow/liquidity/volume are unknown; no fabricated numeric values',()=>{
 const s=run({},0,{liquidityUsd:null,volume5mUsd:null,buys5m:null,sells5m:null,buyVolume5mUsd:null,sellVolume5mUsd:null}).state;
 assert.equal(s.metrics.sellShare,null);assert.equal(s.metrics.sellCountShare,null);assert.equal(s.metrics.liquidityValueDeclineFromBaseline,null);assert.equal(s.coverage.buySellActivity,'UNKNOWN');
});
test('invalid zero quote is unavailable evidence, never a complete catastrophic market sample',()=>{
 const s=run({},0).state;const r=run(s,1,{priceUsd:0,sellVolume5mUsd:900,buyVolume5mUsd:100});
 assert.equal(r.state.coverage.price,'UNKNOWN');assert.notEqual(r.state.riskState,'CRITICAL');
});
test('missingness on retained CRITICAL is not new critical evidence for cooldown bypass',()=>{
 const prior={kind:'RISK_TRANSITION',state:'CRITICAL',reasons:['SEVERE_PRICE_DRAWDOWN_WITH_DISTRIBUTION_EVIDENCE']};
 const changed={...prior,reasons:[...prior.reasons,'PRICE_UNKNOWN','PRIOR_RISK_RETAINED_INCOMPLETE_EVIDENCE']};
 assert.equal(newCriticalEvidence(changed,prior),false);
});

test('actual engine severe causes bypass cooldown, missingness does not',()=>{
  const {evaluate}=require('../services/signalDistribution/risk');
  const {newCriticalEvidence}=require('../services/signalDistribution/deliveryStore');
  const now=new Date('2026-10-14T12:00:00Z');
  const snap={tokenAddress:'CA',pairId:'POOL',observedAt:now.toISOString(),priceUsd:1,liquidityUsd:100,volume5mUsd:100,buys5m:10,sells5m:10,provenance:{dataClass:'EMPIRICAL'}};
  const first=evaluate({},snap,{now}).state;
  const later=new Date(+now+60000);
  const critical=evaluate(first,{...snap,observedAt:later.toISOString(),priceUsd:0.5,liquidityUsd:50,sells5m:90},{now:later}).state;
  assert.equal(critical.riskState,'CRITICAL');
  assert.equal(newCriticalEvidence({kind:'RISK_TRANSITION',state:critical.riskState,reasons:critical.reasons,coverage:critical.coverage},{state:'WATCH',reasons:[]}),true);
});
test('backdated reference never claims earlier sample MFE or retroactive milestone',()=>{
  const {evaluate}=require('../services/signalDistribution/risk');
  const now=new Date('2026-10-14T12:10:00Z');
  const previous={reference:{priceUsd:1,recordedAt:'2026-10-14T12:00:00Z',savedAt:'2026-10-14T12:09:00Z'}};
  const result=evaluate(previous,{pairId:'p',observedAt:'2026-10-14T12:08:00Z',priceUsd:5,liquidityUsd:1,volume5mUsd:1,buys5m:1,sells5m:1,provenance:{dataClass:'EMPIRICAL'}},{now});
  assert.equal(result.state.metrics.referenceMultiple,null);
  assert.equal(result.state.metrics.observedMfeMultiple,null);
  assert.equal(result.milestoneNew,false);
});
