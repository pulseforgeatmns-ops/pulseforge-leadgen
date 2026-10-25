'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {evaluateOpportunity}=require('../services/signalOpportunities/evaluator');
const {clusterEvidence,leadLag}=require('../services/signalOpportunities/graph');
const {sourcePerformance}=require('../services/signalOpportunities/measurement');
const {alertText,createTransports,deliver}=require('../services/signalOpportunities/notifications');
const {MemoryStore,CA,at,later,observation}=require('./helpers/signalDashboardFixtures');
const {createOpportunityWorker}=require('../services/signalOpportunities/worker');
const {DashboardService}=require('../services/signalDashboard/service');
const {DEFINITION,VERSION_HASH}=require('../services/signalOpportunities/definition');
const sources=['a','b','c','telegram-front-runners'].map((sourceId,i)=>({sourceId,identityStatus:'VERIFIED',sourceRole:i===3?'AMPLIFIER':'DISCOVERY',clusterId:sourceId,universeVersion:'TEST ONLY',active:true,available:true}));
const call=(sourceId='a',ms=1000,extra={})=>({id:sourceId+ms,tokenAddress:CA,sourceId,sourceRole:sources.find(s=>s.sourceId===sourceId)?.sourceRole||'DISCOVERY',clusterId:sourceId,externalMessageId:'telegram:1:'+ms,occurredAt:later(ms),knowledgeAt:later(ms),knownAt:later(ms),category:'CALLER',provenance:{dataClass:'EMPIRICAL'},...extra});
const rel=(a,b,status='INDEPENDENT',extra={})=>({a,b,status,evidence:'TEST ONLY',knowledgeAt:at,...extra});
const allRelations=[rel('a','b'),rel('a','c'),rel('b','c')];
const market=[observation(0,{liquidityUsd:30000,priceUsd:1,volume5mUsd:100,buyers5m:100}),observation(300000,{liquidityUsd:32000,priceUsd:1.1,volume5mUsd:150,buyers5m:150,symbol:'TEST ONLY'})];
const run=extra=>evaluateOpportunity({tokenAddress:CA,sources,calls:[call()],observations:[],asOf:later(300000),...extra});
test('unknown values never earn positive points; unverified/synthetic/future observations cannot enter',()=>{
 const p=run();assert.equal(p.opportunityState,'OBSERVED');assert.equal(p.opportunityScore,5);assert.equal(p.independentClusters.length,0);assert.ok(p.unknowns.length>=6);
 assert.equal(run({sources:sources.map(s=>({...s,identityStatus:'CANDIDATE'}))}),null);
 assert.equal(run({calls:[call('a',1000,{provenance:{dataClass:'EMPIRICAL',synthetic:true}})]}),null);
 assert.equal(run({calls:[call('a',1000,{knowledgeAt:later(400000)})]}),null);
 assert.equal(run({calls:[call('a',400000)]}),null);
 assert.equal(run({observations:market.map(o=>({...o,knownAt:later(400000)}))}).opportunityScore,5);
});
test('roles do not earn points; UNKNOWN and correlated clusters are not independence',()=>{
 const p=run({calls:[call('a'),call('b',2000),call('c',3000)],observations:market});
 assert.equal(p.independentClusters.length,0);assert.notEqual(p.opportunityState,'HOT');
 const echo=run({calls:[call('a'),call('b',2000)],relationships:[rel('a','b','CORRELATED')]});assert.ok(echo.evidence.decreases.some(e=>e.code==='CORRELATED_ECHO'));
 const g=clusterEvidence([call('a'),call('b'),call('c')],sources,[rel('a','b'),rel('a','c')],later(300000));assert.equal(g.independentClusters.length,2);
 const transitive=clusterEvidence([call('a'),call('b'),call('c')],sources,[rel('a','b','CORRELATED'),rel('b','c','CORRELATED'),rel('a','c')],later(300000));assert.equal(transitive.independentClusters.length,0);
 const forward=run({calls:[call('a'),call('b',2000,{forwarding:{fromId:'a'}})],relationships:[rel('a','b')]});assert.equal(forward.independentClusters.length,0);
 const future=run({calls:[call('a'),call('b')],relationships:[rel('a','b','INDEPENDENT',{knowledgeAt:later(400000)})]});assert.equal(future.independentClusters.length,0);
 const machine=run({calls:[call('a'),call('b',2000,{sourceRole:'MACHINE_INTELLIGENCE'})],relationships:[rel('a','b')]});assert.equal(machine.independentClusters.length,0);
});
test('HOT requires three empirically independent clusters and five favorable dimensions',()=>{
 const p=run({calls:[call('a'),call('b',2000),call('c',3000)],observations:market,relationships:allRelations});assert.equal(p.opportunityState,'HOT');assert.equal(p.opportunityScore,75);assert.equal(p.definitionHash,VERSION_HASH);
 const withoutBuyers=run({calls:[call('a'),call('b',2000),call('c',3000)],observations:market.map(o=>({...o,buyers5m:null})),relationships:allRelations});assert.equal(withoutBuyers.opportunityState,'OPPORTUNITY');
 const stale=run({calls:[call('a'),call('b',2000),call('c',3000)],observations:market,relationships:allRelations,asOf:later(500000)});assert.notEqual(stale.opportunityState,'HOT');assert.equal(stale.quality,'DELAYED');
 const pair=run({calls:[call('a')],observations:[market[0],{...market[1],pairId:'different'}]});assert.equal(pair.priceMomentum,null);
});
test('vertical extension, liquidity loss, distribution and stale prior warning reduce or block entry',()=>{
 const p=run({calls:[call('a')],observations:[market[0],{...market[1],priceUsd:2,liquidityUsd:10000}]});assert.ok(p.evidence.decreases.some(e=>e.code==='VERTICAL_EXTENSION'));assert.ok(p.evidence.decreases.some(e=>e.code==='LIQUIDITY_DETERIORATION'));
 const risk={state:'CRITICAL',occurredAt:later(300000),knownAt:later(300000),provenance:{dataClass:'EMPIRICAL'}};
 assert.equal(run({observations:market,risk}).opportunityState,'AVOID');
 assert.equal(run({previous:{opportunityState:'AVOID'}}).opportunityState,'AVOID');
 assert.notEqual(run({risk:{...risk,provenance:{dataClass:'EMPIRICAL',synthetic:true}}}).opportunityState,'AVOID');
});
test('caller sequences never attach a later market cap to an earlier call; FR leads use known observation order',()=>{
 const p=run({calls:[call('a',1000),call('telegram-front-runners',2000)],observations:market});assert.equal(p.callerSequence[0].marketCap,100000);assert.equal(p.frontRunnersLeadMs,1000);assert.equal(p.preFrontRunners,true);
 const q=run({calls:[call('a',1000)],observations:[market[1]]});assert.equal(q.callerSequence[0].marketCap,null);
 const f=run({calls:[call('telegram-front-runners',1000),call('a',2000)]});assert.equal(f.preFrontRunners,false);
 const unknown=run({sources:sources.map(s=>s.sourceRole==='AMPLIFIER'?{...s,active:false,available:false}:s)});assert.equal(unknown.preFrontRunners,false);assert.equal(unknown.amplifiers[0].status,'COVERAGE_UNKNOWN');
});
test('lead/lag aggregates distinct tokens and requires repeated observations; reports no causality',()=>{
 const seqs=Array.from({length:12},(_,i)=>[call('a',1000,{tokenAddress:'test-'+i}),call('b',2000,{tokenAddress:'test-'+i})]);
 assert.equal(leadLag(seqs.slice(0,1))[0].confidence,'INSUFFICIENT');const p=leadLag([...seqs,...seqs]);assert.equal(p[0].sampleSize,12);assert.equal(p[0].confidence,'REPEATED_ASSOCIATION');assert.equal(p[0].causality,'NOT_ESTABLISHED');
});
test('performance excludes future, blinded, incomplete, promotional and duplicate outcomes',()=>{
 const row={sourceId:'a',observationId:'x',status:'COMPLETED',dataClass:'EMPIRICAL',blinded:false,prospective:true,startedAt:at,completedAt:later(1000),knowledgeAt:later(2000),twoXBefore30Percent:true,threeX:false,mfe:1.5,mae:-.1,timeTo2xSeconds:60};
 const p=sourcePerformance('a',[row,row,{...row,observationId:'future',knowledgeAt:later(999999)},{...row,observationId:'blind',blinded:true},{...row,observationId:'claim',dataClass:'PROMOTIONAL'},{...row,observationId:'incomplete',status:'OPEN'}],later(300000));assert.equal(p.sampleSize,1);assert.equal(p.twoXBefore30PercentRate.value,1);assert.equal(p.threeXRate.value,0);assert.equal(p.medianMfe,1.5);
});
test('alert text discloses score drivers, risks and unknowns without inventing buyer acceleration',()=>{
 const p=run();const text=alertText(p);for(const term of ['WHY SCORE INCREASED','WHY SCORE DECREASED','WHAT IS UNKNOWN','Manual decision','0 verified independent clusters'])assert.ok(text.includes(term));assert.ok(!text.includes('Buyers accelerating'));
 const transports=createTransports({});assert.ok(transports.every(t=>!t.ready));
});
test('ambiguous provider result is recorded UNKNOWN and a disabled transport never claims',async()=>{
 const finished=[];let claims=0;const store={claim:async()=>{claims++;return {id:'x',payload:run()};},finish:async(r,s)=>finished.push(s)};
 await deliver(store,[{channel:'sms',ready:false},{channel:'email',ready:true,send:async()=>{throw Error('timeout');}}]);assert.equal(claims,1);assert.deepEqual(finished,['UNKNOWN']);
});
test('discovery worker persists real-address evidence without adding wallet positions; duplicate feed is idempotent',async()=>{
 const s=new MemoryStore();let atTime=300000;const decisions=[];
 const opp={list:async()=>[],append:async p=>{decisions.push(p);return {created:true,transition:true};},claim:async()=>null};
 const payload={calls:[{sourceId:'telegram-front-runners',communityId:'2807649463',externalMessageId:'telegram:2807649463:999',occurredAt:later(1000),ingestedAt:later(2000),text:CA,provenance:{dataClass:'EMPIRICAL',telegramMessageId:'999'}}],health:{sources:[{sourceId:'telegram-front-runners',channelId:'2807649463',identityStatus:'VERIFIED',active:true,available:true,verifiedAt:at}]}};
 const worker=createOpportunityWorker({store:s,opportunities:opp,feed:async()=>payload,now:()=>new Date(later(atTime)),marketProvider:{getSnapshot:async()=>observation(atTime,{liquidityUsd:30000})}});
 await worker.tick();atTime+=10000;await worker.tick();assert.equal((await s.list('watch',null)).length,0);assert.equal((await s.list('event',CA)).filter(e=>e.category==='CALLER').length,1);assert.equal(decisions.length,2);assert.equal(decisions[0].sources[0],'telegram-front-runners');assert.ok(decisions[0].definitionVersion);
});
test('unwatched opportunity has a token intelligence page with unknown wallet values',async()=>{
 const s=new MemoryStore(),p=run({observations:market});for(const o of market)await s.put('observation',CA,o.knownAt,o);
 const service=new DashboardService(s,{now:()=>new Date(later(300000)),opportunities:{latest:async()=>p,list:async()=>[p],deliveryHealth:async()=>[]}});
 const detail=await service.detail(CA);assert.equal(detail.positionTracked,false);assert.equal(detail.accounting.tokensRemaining.value,null);assert.equal(detail.opportunity.opportunityState,p.opportunityState);
 assert.equal((await service.positions()).positions.length,0);
});
