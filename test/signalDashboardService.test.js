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
test('unchanged provider OHLCV is not duplicated on every live poll; changed real revisions are retained',async()=>{
 const s=new MemoryStore();await seed(s);let n=0,close=1;
 const w=createDashboardWorker({store:s,marketProvider:{getSnapshot:async()=>observation(n*10000)},now:()=>new Date(later(n*10000)),candleProvider:{getCandles:async({interval})=>[{time:Date.parse(at)/1000,open:1,high:2,low:1,close,volume:10,complete:false,interval,provider:'test',pairId:'2'.repeat(32),occurredAt:at,knownAt:later(n*10000)}]}});
 for(n=0;n<8;n++)await w.pollOnce();assert.equal((await s.list('candle',CA)).length,5);
 close=2;n++;await w.pollOnce();assert.equal((await s.list('candle',CA)).length,6);
});
test('an empty wallet gets a head cursor so later deposits can be discovered',async()=>{
 const s=new MemoryStore();await seed(s);const w=createDashboardWorker({store:s,walletProvider:{getHistoryPage:async()=>({transactions:[],signatures:[],nextBefore:null,exhausted:true})},now:()=>new Date(later(1000))});
 await w.historyTick();const head=(await s.list('cursor',null)).find(x=>x.key==='head');assert.equal(head.lastSeen,null);await w.headTick();assert.equal((await s.list('cursor',null)).find(x=>x.key==='head').caughtUp,true);
});
test('a metadata-only RPC page creates the head cursor when its evidence page is processed',async()=>{
 const s=new MemoryStore();await seed(s);let n=0;const w=createDashboardWorker({store:s,walletProvider:{getHistoryPage:async()=>++n===1?{transactions:[],signatures:[],nextBefore:{pending:['head']},exhausted:false}:{transactions:[tx(1)],signatures:['head'],nextBefore:{pending:[]},exhausted:true}},now:()=>new Date(later(1000))});
 await w.historyTick();assert.equal((await s.list('cursor',null)).some(x=>x.key==='head'),false);await w.historyTick();assert.equal((await s.list('cursor',null)).find(x=>x.key==='head').lastSeen,'head');
});
test('operational diagnostics prove collection health without exposing financial values or wallet identities',async()=>{
 const s=new MemoryStore();await seed(s);await s.put('transaction',CA,'t',tx(1));await s.put('observation',CA,'o',observation(2000));const p=await new DashboardService(s,{now:()=>new Date(later(3000))}).detail(CA);
 const health=require('../services/signalDashboard/diagnostics').operationalSummary(p,[observation(0),observation(10000),observation(20000)]);assert.deepEqual(health.marketIntervalsMs,[10000,10000]);assert.equal(health.priceAvailable,true);assert.equal(health.tradeMarkerCount,1);assert.equal(health.reconciliation,'UNKNOWN');assert.doesNotMatch(JSON.stringify(health),new RegExp(CA+'|tokenAmount|signature|realizedCash|rawTransaction|wallet'));
});
test('ten-second market polls tolerate receipt-time jitter without skipping every other update',async()=>{
 const s=new MemoryStore();await seed(s);let n=0,requests=0;
 const w=createDashboardWorker({store:s,includeCurrentMark:false,now:()=>new Date(later(n*10000)),marketProvider:{getSnapshot:async()=>{requests++;return observation(n*10000+500);}}});
 for(n=0;n<4;n++)await w.pollOnce();assert.equal(requests,4);assert.equal((await s.list('observation',CA,{asOf:later(50000)})).length,4);
});
test('native USDC analytics create principal-recovery markers without inventing historical dollar values',async()=>{
 const {USDC}=require('../services/signalDashboard/wallet');const s=new MemoryStore();await seed(s);await complete(s);
 const native={quoteAsset:USDC,quoteCashBasis:'NET_WALLET_DELTA',walletLamportDelta:'0',feesLamports:'0',valueUsd:null,feesUsd:'0'};
 await s.put('transaction',CA,'buy',tx(1,'BUY',{...native,tokenAmount:'10',quoteAmount:'10'}));await s.put('transaction',CA,'sell',tx(2,'SELL',{...native,tokenAmount:'5',quoteAmount:'12',executionPriceQuote:'2.4'}));
 await s.put('balance',CA,'b',{tokens:'5',slot:5,observedAt:later(2000),knownAt:later(2000)});
 const service=new DashboardService(s,{now:()=>new Date(later(3000))}),p=await service.detail(CA);assert.equal(p.accounting.realizedPnl.value,null);assert.equal(p.quoteAccounting.realizedPnl.value,'7');
 const milestone=p.events.find(e=>e.category==='DE_RISK_MILESTONE');assert.equal(milestone.occurredAt,later(2000));assert.equal(milestone.provenance.unit,'USDC');assert.equal(p.events.find(e=>e.category==='SELL').evidence.executionPriceQuote,'2.4');
 const home=await service.positions();assert.ok(home.positions[0].market);assert.equal(home.positions[0].reconciliation,'MATCH');assert.equal(home.positions[0].quoteAccounting.realizedCash.value,'12');
});
test('minute quote marks tolerate receipt jitter so native position valuations remain available',async()=>{
 const {USDC}=require('../services/signalDashboard/wallet');const s=new MemoryStore();await seed(s);let n=0,requests=0;const w=createDashboardWorker({store:s,now:()=>new Date(later(n*60000)),marketProvider:{getSnapshot:async()=>{requests++;return observation(n*60000+500,{tokenAddress:USDC,priceUsd:.999});}}});
 for(n=0;n<3;n++)await w.currentMarkTick();assert.equal(requests,3);assert.equal((await s.list('quote',null)).length,3);
});
test('slow historical candles run independently of current market observations',async()=>{
 const {createCollectors}=require('../services/signalDashboard/collector');const {POOL}=require('./helpers/signalDashboardFixtures');const s=new MemoryStore();await seed(s);await s.put('watch',CA,'watch',{tokenAddress:CA,poolAddress:POOL,occurredAt:at,knownAt:at},{mutable:true});
 let release,entered;const started=new Promise(r=>entered=r),gate=new Promise(r=>release=r);const collectors=createCollectors({store:s,budget:{take:async()=>{}},publicCandleBudget:{consume:async()=>true},now:()=>new Date(later(60000)),fetchFn:async url=>{
  if(url.includes('geckoterminal')){entered();await gate;return {ok:true,json:async()=>({data:{attributes:{ohlcv_list:[[Date.parse(at)/1000,1,2,.5,1.5,10]]}}})};}
  return {ok:true,json:async()=>[{chainId:'solana',pairAddress:POOL,baseToken:{address:CA},priceUsd:'1',marketCap:1000,liquidity:{usd:100}}]};
 }});
 const history=collectors.candleTick();await started;await collectors.marketTick();assert.equal((await s.list('observation',CA)).length,1);assert.equal((await s.list('candle',CA)).length,0);release();await history;assert.equal((await s.list('candle',CA))[0].interval,'15m');collectors.stop();
});
