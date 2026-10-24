'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {evaluate}=require('../services/signalDistribution/risk');
const source=fs.readFileSync(path.join(__dirname,'../public/signal-distribution.html'),'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
// Isolated DOM fixtures only: no provider requests, database writes or evidence insertion.
class Element {
 constructor(tag){this.tag=tag;this.children=[];this.listeners={};this.classList={add(){},remove(){}};this._text='';}
 set textContent(s){this._text=String(s);this.children=[];}get textContent(){return this._text+this.children.map(c=>c.textContent).join('');}
 append(...children){this.children.push(...children);}replaceChildren(...children){this._text='';this.children=children;}
 setAttribute(){}addEventListener(name,fn){this.listeners[name]=fn;}
 all(){return [this,...this.children.flatMap(c=>c.all())];}
}
function setup(){
 const ids=new Map(),requests=[];
 const get=id=>{if(!ids.has(id))ids.set(id,new Element('div'));return ids.get(id);};
 const context=vm.createContext({document:{getElementById:get,createElement:t=>new Element(t)},fetch:(url,options)=>{requests.push({url,options});return new Promise(()=>{});},window:{confirm(){throw Error('Risk card must not request a mutation.');}},Date,Number,String,Error,encodeURIComponent,JSON});
 vm.runInContext(source,context);
 return {context,get,requests};
}
function fields(target){
 const found={};
 for(const e of target.all())for(let i=0;i<e.children.length-1;i++)if(e.children[i].tag==='dt'&&e.children[i+1].tag==='dd')found[e.children[i].textContent]=e.children[i+1].textContent;
 return found;
}
function render(h,monitor){const target=new Element('div');h.context.renderRiskCard(target,monitor);return {target,values:fields(target)};}
const unsupported=['Bundle risk','Top-holder concentration','Developer holdings','Token age','Buyer/seller trend','Large-holder behavior','FIRST_CALLER'];
const start=Date.parse('2026-10-14T13:00:00Z');
function snapshot(minute,overrides={}){return {tokenAddress:'TEST_TOKEN',pairId:'TEST_POOL',provider:'geckoterminal',observedAt:new Date(start+minute*60000).toISOString(),providerTimestamp:null,priceUsd:100,marketCapUsd:null,liquidityUsd:10000,volume5mUsd:1000,buys5m:50,sells5m:50,buyers5m:40,sellers5m:45,buyVolume5mUsd:500,sellVolume5mUsd:500,provenance:{dataClass:'EMPIRICAL',synthetic:false,testOnly:false,timestampBasis:'response_received'},...overrides};}
function evaluated(previous,minute,overrides={}){return evaluate(previous,snapshot(minute,overrides),{now:new Date(start+minute*60000)}).state;}

test('risk card is complete with explicit UNKNOWN for every unavailable dimension',()=>{
 const h=setup(),{target,values}=render(h,{});
 for(const label of [...unsupported,'Distribution risk','Liquidity (pool reserves USD)'])assert.equal(values[label],'UNKNOWN',label);
 assert.equal(values['Buy/sell balance'],'Sell-volume share: UNKNOWN; sell-count share: UNKNOWN (fractions, rolling 5m)');
 assert.equal(values['Buyers / sellers, rolling 5m (latest saved sample)'],'UNKNOWN / UNKNOWN');
 assert.equal(values['Liquidity trend'],'Reserve-value decline from baseline (fraction): UNKNOWN');
 for(const label of ['Risk version','Risk evaluated at','Sample observed at','Sample provider','Sample pair','Provider timestamp','Timestamp basis','Source freshness','Sample data class','Reserve baseline observed at','Reserve baseline pair','Reserve baseline USD','Caller admission source','Caller admission message','Caller occurred at','Caller ingested at'])assert.equal(values[label],'UNKNOWN',label);
 for(const warning of ['Blind spots','UNKNOWN means missing evidence, not low risk or safety','not a validated trading signal','not proof of LP withdrawal','Receipt time is not a provider timestamp','Monitor age is not token age'])assert.ok(target.textContent.includes(warning),warning);
 assert.equal(h.requests.length,1,'rendering adds no request to the initial read');
 assert.equal(target.all().some(e=>['form','button','input','script','img'].includes(e.tag)),false);
});

test('risk card renders persisted engine state, shares, baseline and sample provenance without recalculation',()=>{
 let state=evaluated({},0);state=evaluated(state,1,{priceUsd:60,liquidityUsd:6000,buyVolume5mUsd:100,sellVolume5mUsd:900,buyers5m:12,sellers5m:84});
 state.event={sourceId:'telegram-front-runners',externalMessageId:'telegram:123:456',occurredAt:'2026-10-14T12:59:00Z',ingestedAt:'2026-10-14T12:59:01Z'};
 const h=setup(),before=JSON.stringify(state),{values}=render(h,{state});
 assert.equal(values['Distribution risk'],'CRITICAL');
 assert.equal(values['Liquidity (pool reserves USD)'],'6000');
 assert.equal(values['Buy/sell balance'],'Sell-volume share: 0.9; sell-count share: 0.5 (fractions, rolling 5m)');
 assert.equal(values['Buyers / sellers, rolling 5m (latest saved sample)'],'12 / 84');
 assert.equal(values['Liquidity trend'],'Reserve-value decline from baseline (fraction): 0.4');
 assert.equal(values['Risk version'],'distribution-v1.0');
 assert.equal(values['Sample provider'],'geckoterminal');assert.equal(values['Sample pair'],'TEST_POOL');
 assert.equal(values['Sample observed at'],'2026-10-14T13:01:00.000Z');
 assert.equal(values['Provider timestamp'],'UNKNOWN');assert.equal(values['Source freshness'],'UNKNOWN');
 assert.equal(values['Timestamp basis'],'response_received');assert.equal(values['Sample data class'],'EMPIRICAL');
 assert.equal(values['Reserve baseline observed at'],'2026-10-14T13:00:00.000Z');assert.equal(values['Reserve baseline USD'],'10000');
 assert.equal(values['Caller admission source'],'telegram-front-runners');assert.equal(values['Caller admission message'],'telegram:123:456');
 assert.equal(values['Caller occurred at'],'2026-10-14T12:59:00.000Z');assert.equal(values['Caller ingested at'],'2026-10-14T12:59:01.000Z');
 for(const label of unsupported)assert.equal(values[label],'UNKNOWN',label);
 assert.equal(JSON.stringify(state),before,'rendering must not mutate saved evidence');
});

test('risk card preserves zero and signed reserve decline, but never derives a missing measurement',()=>{
 const h=setup(),state=evaluated({},0,{buys5m:0,sells5m:0,buyers5m:0,sellers5m:0,buyVolume5mUsd:0,sellVolume5mUsd:0});
 state.metrics={...state.metrics,liquidityUsd:0,sellShare:0,sellCountShare:null,liquidityValueDeclineFromBaseline:-0.25};
 let {values}=render(h,{state});
 assert.equal(values['Liquidity (pool reserves USD)'],'0');assert.equal(values['Buyers / sellers, rolling 5m (latest saved sample)'],'0 / 0');
 assert.equal(values['Buy/sell balance'],'Sell-volume share: 0; sell-count share: UNKNOWN (fractions, rolling 5m)');
 assert.equal(values['Liquidity trend'],'Reserve-value decline from baseline (fraction): -0.25');
 state.metrics={liquidityUsd:null,sellShare:NaN,sellCountShare:Infinity};
 values=render(h,{state}).values;
 assert.equal(values['Liquidity (pool reserves USD)'],'UNKNOWN','must not backfill from latestSnapshot');
 assert.match(values['Buy/sell balance'],/Sell-volume share: UNKNOWN; sell-count share: UNKNOWN/);
 assert.equal(values['Liquidity trend'],'Reserve-value decline from baseline (fraction): UNKNOWN','must not recompute from the saved baseline');
});

test('research-like extras, monitor age and caller event never substitute for missing operational evidence',()=>{
 const h=setup(),state=evaluated({},0);state.event={sourceId:'telegram-front-runners',occurredAt:'2026-10-14T12:00:00Z'};
 // These fields are not in the operational persistence contract. Do not join or guess them.
 state.features={bundledSupplyPct:1,top10HolderPct:2,devHoldingPct:3,tokenAgeSeconds:400};state.researchState='FIRST_CALLER';
 state.latestSnapshot={...state.latestSnapshot,buyerSellerTrend:'RISING',largeHolderBehavior:'SELLING'};
 const {values}=render(h,{created_at:'2026-10-01T00:00:00Z',state});
 for(const label of unsupported)assert.equal(values[label],'UNKNOWN',label);
});

test('risk-card evidence uses text nodes, including hostile saved metadata',()=>{
 const h=setup(),payload='<img src=x onerror=alert(1)>',state={riskState:payload,latestSnapshot:{provider:payload,provenance:{timestampBasis:payload}},event:{sourceId:payload,externalMessageId:payload}};
 const {target,values}=render(h,{state});
 assert.equal(values['Sample provider'],payload);assert.equal(values['Caller admission message'],payload);
 assert.equal(target.all().some(e=>['img','script','a'].includes(e.tag)),false);
});

test('detail renders the complete card through the existing scoped read without another request or mutation',async()=>{
 const h=setup(),calls=[];vm.runInContext("namespace='test';",h.context);
 h.context.api=async(url,options)=>{calls.push({url,options});return {monitor:{id:'monitor-1',token_address:'TEST_TOKEN',monitoring_status:'EXPIRED',expires_at:'2026-10-14T14:00:00Z',state:{riskState:'CRITICAL',lastObservedAt:'2026-10-14T13:00:00Z',lastPollError:'MARKET_UNAVAILABLE',coverage:{observationFreshness:'STALE'}}},observations:[],transitions:[],deliveries:[]};};
 await h.context.showDetail('monitor-1');const target=h.get('detail-content');
 assert.deepEqual(calls,[{url:'/api/v1/signal/distribution/monitor-1?namespace=test',options:undefined}]);
 assert.match(target.textContent,/Risk card · saved evidence only/);assert.match(target.textContent,/STALE \/ EXPIRED \/ UNAVAILABLE/);assert.match(target.textContent,/Polling error: MARKET_UNAVAILABLE/);
 assert.equal(fields(target)['Distribution risk'],'CRITICAL','incomplete evidence must not erase retained risk');
 assert.equal(target.all().some(e=>e.tag==='form'),false,'TEST and EXPIRED do not gain reference controls');
 assert.match(target.textContent,/Receipt and display on your device are unverified/);
});
