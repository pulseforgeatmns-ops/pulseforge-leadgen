'use strict';
require('./helpers/denyExternalProviders.cjs');
// Real runtime/store/facade integration, synthetic provider HTTP only. No live DB.
const test=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');
const {Pool}=require('pg');const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const reservations=[],providerCalls=[],queries=new Map();let websiteCalls=0;
const dbPath=require.resolve('../db');require.cache[dbPath]={id:dbPath,filename:dbPath,loaded:true,exports:{query:async sql=>{
  assert.match(sql,/places_api_requests|CREATE TABLE|CREATE INDEX|ALTER TABLE/);return{rows:[]};}}};
const spendPath=require.resolve('../utils/apiSpendControl');require.cache[spendPath]={id:spendPath,filename:spendPath,loaded:true,exports:{
  reserveApiCall:async input=>{reservations.push(input);return'fixture';},settleApiCall:async()=>{}}};
const websitePath=require.resolve('../services/scoutWebsiteBusinessEvidence');const website=require(websitePath);
require.cache[websitePath].exports={...website,acquireBusinessEvidence:async()=>{websiteCalls++;return {outcome:'no_evidence',result:null,reason:'no_supported_business_evidence'};}};
const providerPath=require.resolve('../packages/capabilities/discovery/providers/PlacesProvider');const providerModule=require(providerPath);
require.cache[providerPath].exports={...providerModule,createPlacesProvider:options=>providerModule.createPlacesProvider({...options,apiKey:'synthetic',fetchImpl:async input=>{
  const url=new URL(input);providerCalls.push(url.pathname);
  if(url.pathname.includes('details')) {const id=url.searchParams.get('place_id');return{ok:true,status:200,json:async()=>({status:'OK',result:{
    place_id:id,name:id==='p0'?'Unresolved Business':`Fixture Painting Contractor ${id}`,website:`https://${id}.example`,
    formatted_address:'Manchester, NH',types:id==='p0'?[]:['painter']}})};}
  if(!url.searchParams.has('pagetoken')) assert.match(url.searchParams.get('query'), /Manchester$/);
  const token=url.searchParams.get('pagetoken');
  const query=url.searchParams.get('query');if(query&&!queries.has(query))queries.set(query,queries.size);
  const root=token?Number(token.split('-').at(-2)):queries.get(query);
  const page=token?Number(token.split('-').at(-1)):0;
  return{ok:true,status:200,json:async()=>({status:'OK',results:Array.from({length:20},(_,i)=>({place_id:`p${root*60+20*page+i}`,name:`Fixture ${20*page+i}`})),
    next_page_token:page<2?`private-token-${root}-${page+1}`:null})};
}})};
const {defaultScoutRamp}=require('../services/maxOutboundControlLoop');
const {constructCandidateUniverse}=require('../packages/max/scoutAcquisition/CandidateUniverse');
const {createPlacesDiscoveryAdapter}=require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const {createPageCheckpointStore}=require('../services/scoutQueryContinuation');
const cache=require('../utils/placesQueryCache');

test('actual provider→checkpoint page→durable ticket→governed queue survives repeated cycles and finite exhaustion', {timeout:60000},async t=>{
  const old=process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;process.env.SCOUT_DURABLE_CANDIDATES_ENABLED='true';
  const pg=await startDisposablePostgres('scout-full-pipeline-');const db=new Pool({connectionString:pg.connectionString,max:3});
  t.after(async()=>{if(old==null)delete process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;else process.env.SCOUT_DURABLE_CANDIDATES_ENABLED=old;await db.end();await pg.stop();});
  for(const file of ['2026-10-17-scout-query-checkpoints.sql','2026-10-19-scout-query-pages.sql','2026-10-19-scout-candidate-tickets.sql'])
    await db.query(fs.readFileSync(path.join(__dirname,'../migrations',file),'utf8'));
  await db.query(`CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,domain text,vertical text,
    location text,source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);
    CREATE TABLE acquisition_outbound_events(id text PRIMARY KEY,tenant_id text,event_type text,payload jsonb,created_at timestamptz DEFAULT now());`);
  const pool={connect:(...args)=>db.connect(...args),query:(sql,params)=>/FROM prospects p/.test(sql)?Promise.resolve({rows:[]}):db.query(sql,params)};
  const events=[];const store={tenantId:'13',clientId:13,candidateOwnership:async()=>null,event:async(...args)=>events.push(args)};
  let now=Date.now();const definition={tenantId:'13',segments:['small_business_owners'],businessNeed:'business_transformation',
    geography:{label:'Manchester',cities:['Manchester'],state:null},evidenceRequest:{tenantId:'13',scope:'market',segment:'small_business_owners',
      evidenceType:'identity',discoveryConcept:'Painting Contractor',geography:{cities:['Manchester'],state:null}}};
  const run=()=>defaultScoutRamp({pool,store,program:{id:'p',tenant_id:'13',policy:{}},
    source:{payload:{structuredMission:{market:{segment:'small_business_owners'},geography:{region:'Manchester',scope:'local',cities:['Manchester']}}}},
    plan:{deficit:1},searchFingerprint:'unchanged-approved-mission',skipVerificationRetry:true,
    enrichment:{run:async()=>({considered:0})},recoverExistingInventory:async()=>({}),loadDiscoveryBackoff:async()=>null,
    createPageCheckpointStore:input=>createPageCheckpointStore({...input,now:()=>now}),
    runDiscovery:async(_input,options)=>{
      const universe=await constructCandidateUniverse({searchDefinition:definition,forceDiscover:true,existing:{companies:[],people:[]},
        adapters:[createPlacesDiscoveryAdapter({placesProvider:options.placesProvider})],
        persistCompanies:options.persistCompanies,acknowledgeDiscovery:options.acknowledgeDiscovery});
      assert.deepEqual(universe.discoveryErrors,[],JSON.stringify(universe.discoveryErrors));return{kind:'investigated'};
    },logger:{log(){},warn(){}}});
  for(let cycle=0;cycle<63;cycle++){
    cache.clearPlacesQueryCache();const before=providerCalls.length;const result=await run();
    assert.equal(result.discoveredQueued,cycle===0?19:20,JSON.stringify({result,providerCalls}));
    assert.equal(providerCalls.length-before,21);assert.equal(result.candidateWork.total,(cycle+1)*20);
    assert.equal(result.candidateWork.byState.handled,(cycle+1)*20-1);
    assert.equal(result.candidateWork.byReason.unresolved_business_evidence,1);
    assert.equal(result.queryProgression.pendingAcknowledgements,0);now+=6000;
  }
  const firstTotal=providerCalls.length;
  for(let cycle=0;cycle<3;cycle++){
    cache.clearPlacesQueryCache();const result=await run();assert.equal(result.discoveredQueued,0);
    assert.equal(result.queryProgression.continuation.statuses.finite_query_exhausted,21);
    assert.equal(providerCalls.length,firstTotal);now+=6000;
  }
  assert.equal(websiteCalls,1,'the unresolved business never replays alongside later pages');
  assert.equal((await db.query('SELECT count(*)::int AS n FROM scout_unenriched')).rows[0].n,1259);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM scout_candidate_memberships')).rows[0].n,1260);
  assert.ok((await db.query('SELECT candidates,state FROM scout_query_checkpoints')).rows.every(row=>row.candidates.length===20&&row.state==='handled'));
  assert.equal(reservations.length,1323);assert.equal(reservations.filter(row=>row.operation==='text_search').length,63);
  assert.equal(reservations.filter(row=>row.operation==='place_details').length,1260);
  assert.ok(Math.abs(reservations.reduce((sum,row)=>sum+row.estimatedUsd,0)-34.02)<1e-9);
  assert.ok(!JSON.stringify(events).includes('private-token'));assert.equal(db.waitingCount,0);assert.equal(db.idleCount,db.totalCount);
});
