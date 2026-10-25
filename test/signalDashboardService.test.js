'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {DashboardService}=require('../services/signalDashboard/service');
const {createDashboardWorker}=require('../services/signalDashboard/worker');
const {MemoryStore,seed,tx,observation,CA,at,later}=require('./helpers/signalDashboardFixtures');
async function complete(store){await store.put('cursor','*','history',{key:'history',exhausted:true,occurredAt:at,knownAt:at});await store.put('cursor','*','head',{key:'head',caughtUp:true,occurredAt:at,knownAt:at});}
test('mobile payload has canonical wallet balance, FIFO values and exact marker timestamp with contemporaneous UNKNOWN state',async()=>{
 const s=new MemoryStore();await seed(s);await complete(s);await s.put('transaction',CA,'t',tx(1));await s.put('observation',CA,'o',observation(2000));
 await s.put('balance',CA,'b',{tokens:'10',slot:5,observedAt:later(2000),knownAt:later(2000)});
 const p=await new DashboardService(s,{now:()=>new Date(later(3000))}).detail(CA);assert.equal(p.accounting.positionValue.value,'10');assert.equal(p.accounting.totalPnl.value,'0');assert.equal(p.readOnly,true);
 const b=p.events.find(x=>x.category==='BUY');assert.equal(b.occurredAt,later(1000));assert.equal(b.time,Date.parse(later(1000))/1000);assert.equal(b.evidence.signalState,'UNKNOWN');assert.equal(b.evidence.distributionRisk,'UNKNOWN');
 assert.equal(p.accounting.realizedCash.state,'PARTIAL');assert.ok(JSON.stringify(p).length<50000);assert.doesNotMatch(JSON.stringify(p),/not-a-secret-do-not-expose-raw/);
});
test('missing wallet evidence is unknown, not a zero-dollar position',async()=>{
 const s=new MemoryStore();await seed(s);await s.put('observation',CA,'o',observation());const p=await new DashboardService(s,{now:()=>new Date(later(1000))}).detail(CA);
 assert.equal(p.accounting.positionValue.value,null);assert.equal(p.accounting.tokensRemaining.value,null);assert.equal(p.accounting.grossCapitalDeployed.value,null);assert.equal(p.accounting.totalPnl.value,null);
});
test('canonical live wallet balance is displayed despite reconciliation mismatch and P&L is withheld',async()=>{
 const s=new MemoryStore();await seed(s);await complete(s);await s.put('transaction',CA,'t',tx(1));await s.put('observation',CA,'o',observation(2000));await s.put('balance',CA,'b',{tokens:'7',slot:5,observedAt:later(2000),knownAt:later(2000)});
 const p=await new DashboardService(s,{now:()=>new Date(later(3000))}).detail(CA);assert.equal(p.accounting.positionValue.value,'7');assert.equal(p.accounting.tokensRemaining.value,'7');assert.equal(p.coverage.reconciliation.state,'MISMATCH');assert.equal(p.accounting.totalPnl.value,null);
});
test('later known research/risk cannot leak into an earlier buy marker',async()=>{
 const s=new MemoryStore();await seed(s);await s.put('transaction',CA,'t',tx(1));await s.put('event',CA,'e',{id:'e',category:'FIRST_CALLER',researchState:'FIRST_CALLER',occurredAt:at,knownAt:later(2000),provenance:{dataClass:'EMPIRICAL'}});await s.put('observation',CA,'o',observation(2000));
 const p=await new DashboardService(s,{now:()=>new Date(later(3000))}).detail(CA);assert.equal(p.signalState.value,'FIRST_CALLER');assert.equal(p.events.find(e=>e.category==='BUY').evidence.signalState,'UNKNOWN');
});
test('annotations append separately, never change empirical evidence; subresources exclude raw transactions',async()=>{
 const s=new MemoryStore();await seed(s);await s.put('transaction',CA,'t',tx(1));const service=new DashboardService(s,{now:()=>new Date(later(3000))});await service.annotate(CA,'Recovered principal','1');
 const notes=await s.list('annotation',CA);assert.equal(notes[0].provenance.type,'OPERATOR_ANNOTATION');assert.equal((await s.list('transaction',CA)).length,1);assert.doesNotMatch(JSON.stringify(await service.transactions(CA)),/not-a-secret-do-not-expose-raw/);
 await assert.rejects(()=>service.annotate(CA,'','1'),/invalid_annotation/);
});
test('read cache coalesces concurrent mobile requests without provider calls',async()=>{
 const s=new MemoryStore();await seed(s);let reads=0;const original=s.list.bind(s);s.list=(...args)=>{reads++;return original(...args);};const service=new DashboardService(s,{now:()=>new Date(later(3000))});await Promise.all(Array.from({length:10},()=>service.detail(CA)));assert.equal(reads,9);
});
test('history pagination, overlapping polls and retries dedupe signatures without losing head pages',async()=>{
 const s=new MemoryStore();await seed(s);let requests=0;
 const provider={getHistoryPage:async({before})=>{requests++;if(!before)return{transactions:[tx(1)],signatures:['head','a'],nextBefore:'a',exhausted:false};return{transactions:[tx(1)],signatures:[],nextBefore:null,exhausted:true};}};
 const w=createDashboardWorker({store:s,walletProvider:provider,now:()=>new Date(later(5000))});await w.historyTick();await w.historyTick();assert.equal((await s.list('transaction',CA)).length,1);assert.equal((await s.list('cursor',null)).find(c=>c.key==='history').exhausted,true);
 await w.headTick();assert.equal((await s.list('cursor',null)).find(c=>c.key==='head').caughtUp,true);assert.equal(requests,3);
});
test('collector rate-limit failure leaves persisted observations available and next cycle recovers',async()=>{
 const s=new MemoryStore();await seed(s);let failed=true,now=()=>new Date(later(5000));const market={getSnapshot:async()=>{if(failed)throw Error('rate limited');return observation(5000);}};
 const w=createDashboardWorker({store:s,marketProvider:market,now});await w.pollOnce();assert.equal((await s.list('observation',CA)).length,0);failed=false;await w.pollOnce();assert.equal((await s.list('observation',CA)).length,1);
});
test('backfill respects page cap and does not persist/enrich unwatched token activity',async()=>{
 const s=new MemoryStore();await seed(s);let calls=0;const w=createDashboardWorker({store:s,maxHistoryPages:1,now:()=>new Date(later(5000)),walletProvider:{getHistoryPage:async()=>{calls++;return {transactions:[tx(1),tx(2,'BUY',{tokenAddress:'3'.repeat(32)})],signatures:['a'],nextBefore:'a',exhausted:false};}}});
 await w.historyTick();await w.historyTick();assert.equal(calls,1);assert.equal((await s.list('transaction',null)).length,1);assert.equal((await s.list('status','*')).find(x=>x.component==='wallet-history').reason,'BACKFILL_PAGE_LIMIT_REACHED');
});
