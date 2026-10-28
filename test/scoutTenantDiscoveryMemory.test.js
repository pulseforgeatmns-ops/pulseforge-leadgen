'use strict';
require('./helpers/denyExternalProviders.cjs');
const test=require('node:test'),assert=require('node:assert/strict');
const dbPath=require.resolve('../db');
require.cache[dbPath]={id:dbPath,filename:dbPath,loaded:true,exports:{query:async()=>({rows:[]})}};
const spendPath=require.resolve('../utils/apiSpendControl');
require.cache[spendPath]={id:spendPath,filename:spendPath,loaded:true,exports:{reserveApiCall:async()=> 'synthetic',settleApiCall:async()=>{}}};
const {businessObservation,createTenantDiscoveryMemory,isTenantDiscoveryMemoryEnabled}=require('../services/scoutTenantDiscoveryMemory');
const {createCheckpointStore,createCheckpointSession,queryIdentity}=require('../services/scoutQueryCheckpoints');
const {createPlacesProvider}=require('../packages/capabilities/discovery/providers/PlacesProvider');
const {queryProgressionDiagnostic,observedQueryDependency}=require('../services/scoutCandidateTicketDispatch');

test('the Anchor activation switch cannot activate another tenant or grant research authority',()=>{
  const env={SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED:'true'};
  assert.equal(isTenantDiscoveryMemoryEnabled('10',env),true);
  for(const tenant of ['13','11','',null])assert.equal(isTenantDiscoveryMemoryEnabled(tenant,env),false);
  assert.equal(isTenantDiscoveryMemoryEnabled('10',{}),false);
  assert.equal(isTenantDiscoveryMemoryEnabled('10',{SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED:'false'}),false);
});

test('reused business facts exclude old query labels, ticket claims and arbitrary provider data',()=>{
  const raw={source:'google_places',placeId:'p1',companyName:'Fixture office',website:'office.example',industry:'Old query',
    discoveryQuery:'Old query',_scoutTicketRefs:[{secret:'not authority'}],next_page_token:'private',address:'Bedford NH'};
  const safe=businessObservation(raw);
  assert.equal(safe.companyName,'Fixture office');assert.equal(safe.placeId,'p1');
  for(const key of ['industry','discoveryQuery','_scoutTicketRefs','next_page_token'])assert.equal(Object.hasOwn(safe,key),false);
  assert.equal(businessObservation({...raw,source:'unknown'}),null);
  assert.throws(()=>createTenantDiscoveryMemory({pool:{},tenantId:'13',fingerprint:'a'}),/binding_required/);
});

test('tenant history skips a previous mission before page allocation and advances to unsearched approved coverage',async()=>{
  const pageKeys=[],claims=[],rows=new Map();const first='Real Estate Agency Bedford NH';
  const memory={tenantId:'10',fingerprint:'mission-b',checkQuery:async key=>key===queryIdentity(first,false)?'tenant_query_previously_searched':null,
    reserveQuery:async key=>{claims.push(key);return null;},snapshot:()=>({queriesSkipped:1,queriesUnconfirmed:0,detailsReused:0,detailsReserved:0})};
  const store={tenantId:'10',fingerprint:'mission-b',load:async k=>rows.get(k),stage:async(k,c)=>{rows.set(k,{state:'fetched',candidates:c});return rows.get(k);},acknowledge:async()=>{}};
  const pages={tenantId:'10',plan:async k=>{pageKeys.push(k);return {key:k,index:0};},reserve:async()=> 'reserved',
    stage:async(plan,c)=>store.stage(plan.key,c)};
  const session=createCheckpointSession({store,pageStore:pages,tenantMemory:memory});
  assert.equal((await session.lookup(first,false)).continuationStatus,'tenant_query_previously_searched');
  assert.equal((await session.lookup('Real Estate Agency Hooksett NH',false)).singlePage,true);
  assert.equal(pageKeys.length,1);assert.equal(claims.length,1);
  assert.equal(session.snapshot().continuation.newPagesReserved,1);
  assert.equal(queryProgressionDiagnostic(session.snapshot()).tenantMemory.queriesSkipped,1);
});

test('an uncertain tenant query is reported to Max without producing a refresh permission',async()=>{
  const memory={tenantId:'10',fingerprint:'b',checkQuery:async()=> 'tenant_query_outcome_unconfirmed',snapshot:()=>({queriesUnconfirmed:1})};
  const session=createCheckpointSession({store:{tenantId:'10',fingerprint:'b'},pageStore:{tenantId:'10'},tenantMemory:memory});
  await session.lookup('Fixture unknown Bedford NH',false);
  const result=session.snapshot();assert.equal(result.fetchedQueries,0);assert.equal(result.unresolvedQueries,1);
  assert.equal(result.coverageApprovalManifest,undefined);
  assert.equal(observedQueryDependency(result).statuses.tenant_query_outcome_unconfirmed,1);
});

test('a reused Place ID makes no detail call and preserves truthful detail observation age',async()=>{
  const state=new Map(),calls=[];const db={query:async(sql,p)=>{
    if(sql.startsWith('SELECT'))return {rows:state.has(p[2])?[state.get(p[2])]:[]};
    if(sql.startsWith('INSERT'))state.set(p[2],{state:'fetched',candidates:JSON.parse(p[4])});
    return {rows:[]};}};
  const stored={source:'google_places',placeId:'known',companyName:'Known office',website:'known.example',address:'Bedford NH',placeTypes:['real_estate_agency']};
  let completed=0;const memory={tenantId:'10',claimPlace:async id=>id==='known'?{kind:'reuse',candidate:stored,observedAt:null}:{kind:'fetch',placeId:id},
    finishPlace:async()=>{completed++;}};
  const provider=createPlacesProvider({apiKey:'synthetic',useCache:false,tenantDiscoveryMemory:memory,
    queryCheckpointSession:createCheckpointSession({store:createCheckpointStore({db,tenantId:'10',fingerprint:'fixture'})}),
    fetchImpl:async input=>{const u=new URL(input);calls.push(u.pathname);
      return {ok:true,status:200,json:async()=>u.pathname.includes('details')?{status:'OK',result:{place_id:'new',name:'New office',website:'https://new.example',types:['real_estate_agency']}}
        :{status:'OK',results:[{place_id:'known'},{place_id:'new'}]}};}});
  const result=await provider.collectEvidence({tenantId:'10',scope:'market',segment:'commercial_cleaning_buyers',evidenceType:'identity',
    discoveryConcept:'Real Estate Agency',geography:{cities:['Bedford'],state:'NH'}});
  assert.equal(result.length,2);assert.equal(calls.length,2);assert.equal(calls.filter(x=>x.includes('details')).length,1);
  assert.equal(result[0].detailsReused,true);assert.equal(result[0].detailObservedAt,null);
  assert.equal(result[0].discoveryQuery,'Real Estate Agency Bedford NH');assert.equal(completed,1);
});

test('memory scope mismatch fails before page allocation or provider transport',async()=>{
  const store={tenantId:'10',fingerprint:'a'},pages={tenantId:'10'};
  assert.throws(()=>createCheckpointSession({store,pageStore:pages,tenantMemory:{tenantId:'13',fingerprint:'a'}}),/exact checkpoint scope/);
  assert.throws(()=>createCheckpointSession({store,pageStore:pages,tenantMemory:{tenantId:'10',fingerprint:'b'}}),/exact checkpoint scope/);
});

test('awaiting a tenant-wide claim cannot race past the one-page cycle allowance',async()=>{
  let release,entered;const gate=new Promise(r=>release=r),started=new Promise(r=>entered=r);let reservations=0;
  const memory={tenantId:'10',fingerprint:'a',checkQuery:async()=>null,reserveQuery:async()=>{entered();await gate;return null;},snapshot:()=>({})};
  const session=createCheckpointSession({store:{tenantId:'10',fingerprint:'a',load:async()=>null},tenantMemory:memory,
    pageStore:{tenantId:'10',plan:async k=>({key:k,index:0}),reserve:async()=>{reservations++;return 'fixture';}}});
  const first=session.lookup('Concurrent A Bedford NH',false);await started;
  const second=await session.lookup('Concurrent B Bedford NH',false);
  assert.equal(second.continuationStatus,'cycle_page_budget_reached');release();await first;
  assert.equal(reservations,1);assert.equal(session.snapshot().continuation.newPagesReserved,1);
});

test('a measured duplicate-heavy page yields to fresh roots without claiming exhaustion',async()=>{
  let reserved=0;const memory={tenantId:'10',fingerprint:'a',checkQuery:async()=>null,
    continuationStatus:async()=> 'tenant_query_low_novelty',snapshot:()=>({lowNoveltyQueriesDeferred:1})};
  const s=createCheckpointSession({store:{tenantId:'10',fingerprint:'a'},tenantMemory:memory,
    pageStore:{tenantId:'10',plan:async()=>({key:'page-two',rootKey:'root',index:1}),reserve:async()=>{reserved++;}}});
  assert.equal((await s.lookup('Known Root Bedford NH',false)).continuationStatus,'tenant_query_low_novelty');
  assert.equal(reserved,0);assert.equal(s.snapshot().handledQueries,0);assert.equal(s.snapshot().unresolvedQueries,1);
  assert.equal(queryProgressionDiagnostic(s.snapshot()).tenantMemory.lowNoveltyQueriesDeferred,1);
});
