'use strict';
require('./helpers/denyExternalProviders.cjs');
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {Pool}=require('pg');const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const dbPath=require.resolve('../db');
require.cache[dbPath]={id:dbPath,filename:dbPath,loaded:true,exports:{query:async()=>({rows:[]})}};
const spendPath=require.resolve('../utils/apiSpendControl');
require.cache[spendPath]={id:spendPath,filename:spendPath,loaded:true,exports:{reserveApiCall:async()=> 'synthetic',settleApiCall:async()=>{}}};
const {createTenantDiscoveryMemory}=require('../services/scoutTenantDiscoveryMemory');
const {createCheckpointStore,createCheckpointSession,queryIdentity}=require('../services/scoutQueryCheckpoints');
const {createPageCheckpointStore}=require('../services/scoutQueryContinuation');
const {createPlacesProvider}=require('../packages/capabilities/discovery/providers/PlacesProvider');

test('tenant-wide query and detail memory uses durable native PostgreSQL across missions, crashes and concurrency',{timeout:90000},async t=>{
  const pg=await startDisposablePostgres('scout-tenant-memory-'),db=new Pool({connectionString:pg.connectionString,max:8});
  t.after(async()=>{await db.end();await pg.stop();});
  for(const file of ['2026-10-17-scout-query-checkpoints.sql','2026-10-19-scout-query-pages.sql','2026-10-19-scout-candidate-tickets.sql'])
    await db.query(fs.readFileSync(path.join(__dirname,'../migrations',file),'utf8'));
  const memory=fp=>createTenantDiscoveryMemory({pool:db,tenantId:'10',fingerprint:fp});
  await assert.rejects(memory('missing').assertReady(),/schema_required/);
  assert.equal((await db.query("SELECT to_regclass('scout_tenant_query_owners') AS name")).rows[0].name,null,'readiness installs no schema');
  await db.query(fs.readFileSync(path.join(__dirname,'../migrations/2026-10-27-scout-tenant-search-memory.sql'),'utf8'));
  await memory('ready').assertReady();
  const calls=[];
  const request=concept=>({tenantId:'10',scope:'market',segment:'commercial_cleaning_buyers',evidenceType:'identity',
    discoveryConcept:concept,geography:{cities:['Bedford'],state:'NH'}});
  function fixture(fp,concept,ids,options={}) {
    const tenantMemory=createTenantDiscoveryMemory({pool:options.pool||db,tenantId:'10',fingerprint:fp});
    const session=createCheckpointSession({store:createCheckpointStore({db,tenantId:'10',fingerprint:fp}),
      pageStore:createPageCheckpointStore({db,tenantId:'10',fingerprint:fp}),tenantMemory});
    const provider=createPlacesProvider({apiKey:'synthetic',useCache:false,queryCheckpointSession:session,tenantDiscoveryMemory:tenantMemory,
      fetchImpl:async input=>{const u=new URL(input);const detail=u.pathname.includes('details'),id=u.searchParams.get('place_id');
        calls.push({kind:detail?'detail':'search',id,query:u.searchParams.get('query')});
        if(detail&&options.details)await options.details(id);
        return {ok:true,status:200,json:async()=>detail?{status:'OK',result:{place_id:id,name:`Fixture real estate ${id}`,
          website:`https://${id}.example`,formatted_address:'Bedford NH',types:['real_estate_agency']}}
          :{status:'OK',results:ids.map(place_id=>({place_id,name:`Fixture ${place_id}`}))}};}});
    return {session,tenantMemory,run:()=>provider.collectEvidence(request(concept))};
  }
  await t.test('same completed query is skipped after process restart and after mission replacement',async()=>{
    const first=fixture('source-a','Real Estate Agency',['alpha','beta']);
    assert.equal((await first.run()).length,2);await first.session.acknowledge();assert.equal(calls.length,3);
    await fixture('source-a','Real Estate Agency',['alpha','beta']).run();
    const alternate=fixture('source-a','Real Estate Agency',['alpha','beta']);
    // The HTTP query is unchanged even when the evidence/output contract changes.
    const alternateKey=queryIdentity('Real Estate Agency Bedford NH',true);
    assert.equal(await alternate.tenantMemory.checkQuery(alternateKey,'Real Estate Agency Bedford NH'),'tenant_query_previously_searched');
    const changed=fixture('source-b','Real Estate Agency',['alpha','beta']);assert.equal((await changed.run()).length,0);
    assert.equal(calls.length,3);assert.equal(changed.tenantMemory.snapshot().queriesSkipped,1);
    assert.equal((await db.query("SELECT search_fingerprint FROM scout_tenant_query_owners WHERE query_key=$1",[queryIdentity('Real Estate Agency Bedford NH',false)])).rows[0].search_fingerprint,'source-a');
  });
  await t.test('different authorized search reuses known details and only fetches genuinely unknown IDs',async()=>{
    const before=calls.length,next=fixture('source-b','Property Management',['alpha','gamma']);
    const result=await next.run();await next.session.acknowledge();
    assert.equal(calls.length-before,2);assert.equal(result.length,2);assert.equal(result[0].detailsReused,true);
    assert.ok(result[0].detailObservedAt);assert.equal(next.tenantMemory.snapshot().detailsReused,1);
    assert.equal((await db.query("SELECT count(*)::int AS n FROM scout_tenant_place_observations WHERE place_id='alpha'")).rows[0].n,1);
  });
  await t.test('pre-migration checkpoints remain authoritative and missing detail age is never fabricated',async()=>{
    const raw={placeId:'legacy-place',source:'google_places',companyName:'Legacy office',website:'legacy.example',address:'Bedford NH'};
    await db.query("INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates) VALUES('10','legacy-source',$1,'places-market-query-v1','handled',$2)",
      [queryIdentity('Legacy Realty Bedford NH',false),JSON.stringify([raw])]);
    const before=calls.length;
    await fixture('new-source','Legacy Realty',['legacy-place']).run();assert.equal(calls.length,before);
    const fresh=fixture('new-source','Legacy Alternate',['legacy-place']);const result=await fresh.run();await fresh.session.acknowledge();
    assert.equal(calls.length,before+1);assert.equal(result[0].detailObservedAt,null);assert.equal(result[0].detailsReused,true);
  });
  await t.test('two concurrent missions reserve only one paid copy of the same query',async()=>{
    const before=calls.length,a=fixture('race-a','Concurrent Realty',['race-place']),b=fixture('race-b','Concurrent Realty',['race-place']);
    const results=await Promise.all([a.run(),b.run()]);
    assert.equal(calls.length-before,2);assert.equal(results.flat().length,1);
    assert.equal((await db.query('SELECT count(*)::int AS n FROM scout_tenant_query_owners WHERE query_key=$1',[queryIdentity('Concurrent Realty Bedford NH',false)])).rows[0].n,1);
  });
  await t.test('concurrent different queries cannot buy the same unfinished detail request',async()=>{
    let release,entered;const gate=new Promise(r=>release=r),started=new Promise(r=>entered=r);
    const before=calls.length,a=fixture('place-race-a','Race Offices A',['shared-place'],{details:async()=>{entered();await gate;}});
    const running=a.run();await started;
    try { await assert.rejects(fixture('place-race-b','Race Offices B',['shared-place']).run(),/detail_unconfirmed/); }
    finally {release();}
    await running;
    assert.equal(calls.slice(before).filter(x=>x.kind==='detail').length,1);
  });
  await t.test('failed transport is retained and another mission cannot automatically retry it',async()=>{
    const broken=fixture('failed-a','Interrupted Offices',['uncertain-place'],{details:async()=>{throw Error('synthetic interrupted HTTP');}});
    await broken.run();
    const before=calls.length;
    await assert.rejects(fixture('failed-b','Different Interrupted Offices',['uncertain-place']).run(),/detail_unconfirmed/);
    assert.equal(calls.slice(before).filter(x=>x.kind==='detail').length,0);
    assert.equal((await db.query("SELECT state FROM scout_tenant_place_observations WHERE place_id='uncertain-place'")).rows[0].state,'uncertain');
  });
  await t.test('lost query-claim commit acknowledgement is a durable stop, never a refund or retry',async()=>{
    const uncertain={query:db.query.bind(db),connect:async()=>{const c=await db.connect();return {release:c.release.bind(c),query:async(sql,p)=>{
      const r=await c.query(sql,p);if(sql==='COMMIT')throw Error('synthetic lost commit acknowledgement');return r;}};}};
    const before=calls.length;
    await assert.rejects(fixture('commit-a','Commit Lost Realty',['commit-place'],{pool:uncertain}).run(),/commit_uncertain/);
    const later=fixture('commit-b','Commit Lost Realty',['commit-place']);await later.run();
    assert.equal(calls.length,before);assert.equal(later.session.snapshot().continuation.statuses.tenant_query_outcome_unconfirmed,1);
  });
  await t.test('cross-tenant bindings and resetting consumed memory fail closed',async()=>{
    assert.throws(()=>createTenantDiscoveryMemory({pool:db,tenantId:'13',fingerprint:'source-a'}),/binding_required/);
    await assert.rejects(db.query("UPDATE scout_tenant_query_owners SET search_fingerprint='reset'"),/permanent/);
    await assert.rejects(db.query("DELETE FROM scout_tenant_query_owners"),/permanent/);
    await assert.rejects(db.query("UPDATE scout_tenant_place_observations SET state='reserved' WHERE place_id='alpha'"),/permanent/);
    await assert.rejects(db.query("DELETE FROM scout_tenant_place_observations"),/permanent/);
    assert.equal((await db.query("SELECT count(*)::int AS n FROM scout_query_checkpoints WHERE search_fingerprint='legacy-source'")).rows[0].n,1);
  });
  await t.test('low-novelty continuation is measured from retained facts and remains an explicit review dependency',async()=>{
    const key=queryIdentity('Low Novelty Realty Bedford NH',false),fp='novelty-source';
    const pageStore=createPageCheckpointStore({db,tenantId:'10',fingerprint:fp}),plan=await pageStore.plan(key);
    plan.reservationId=await pageStore.reserve(plan);
    const candidates=Array.from({length:5},(_,i)=>({placeId:`novelty-${i}`,source:'google_places',companyName:`Novelty ${i}`,detailsReused:i<4}));
    await pageStore.stage(plan,candidates,{nextPageToken:'private-continuation',rawResults:[]});
    await db.query("UPDATE scout_query_checkpoints SET state='handled' WHERE tenant_id='10' AND search_fingerprint=$1 AND query_key=$2",[fp,key]);
    const m=memory(fp);assert.equal(await m.continuationStatus(key),'tenant_query_low_novelty');
    assert.equal(m.snapshot().lowNoveltyQueriesDeferred,1);
    assert.equal(await memory('different-source').continuationStatus(key),null,'another source cannot adopt continuation authority');
  });
  await t.test('actual Max ramp admits a new raw business then preserves history through restart and mission replacement',async()=>{
    await db.query(`CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,domain text,vertical text,
      location text,source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);
      CREATE TABLE acquisition_outbound_events(id text PRIMARY KEY,tenant_id text,event_type text,payload jsonb,created_at timestamptz DEFAULT now());`);
    const oldFlag=process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;process.env.SCOUT_DURABLE_CANDIDATES_ENABLED='true';
    const providerPath=require.resolve('../packages/capabilities/discovery/providers/PlacesProvider'),previous=require.cache[providerPath].exports;
    const transport=[];require.cache[providerPath].exports={...previous,createPlacesProvider:options=>previous.createPlacesProvider({...options,
      apiKey:'synthetic',useCache:false,fetchImpl:async input=>{const u=new URL(input);transport.push(u.pathname);
        return {ok:true,status:200,json:async()=>u.pathname.includes('details')?{status:'OK',result:{place_id:'pipeline-new',name:'Fixture Realty Group',
          website:'https://pipeline-new.example',formatted_address:'Bedford, NH',types:['real_estate_agency']}}
          :{status:'OK',results:[{place_id:'pipeline-new',name:'Fixture Realty Group'}]}};}})};
    try {
      const {defaultScoutRamp}=require('../services/maxOutboundControlLoop');
      const {constructCandidateUniverse}=require('../packages/max/scoutAcquisition/CandidateUniverse');
      const {createPlacesDiscoveryAdapter}=require('../packages/max/scoutAcquisition/DiscoveryAdapters');
      const pool={connect:db.connect.bind(db),query:(sql,p)=>/FROM prospects p/.test(sql)?Promise.resolve({rows:[]}):db.query(sql,p)};
      const events=[],store={tenantId:'10',clientId:10,candidateOwnership:async()=>null,event:async(...args)=>events.push(args)};
      const definition={tenantId:'10',segments:['commercial_cleaning_buyers'],businessNeed:'commercial_cleaning',
        geography:{label:'Bedford NH',cities:['Bedford'],state:'NH'},evidenceRequest:request('Pipeline Realty')};
      const run=mission=>defaultScoutRamp({pool,store,program:{id:`program-${mission}`,tenant_id:'10',source_mission_id:mission,policy:{}},
        source:{id:mission,payload:{structuredMission:{market:{segment:'commercial_cleaning_buyers',eligibleSubsegments:['realtor']},
          geography:{region:'Greater Manchester',cities:['Bedford'],scope:'local'}}}},plan:{deficit:1},skipVerificationRetry:true,
        enrichment:{run:async()=>({considered:0})},recoverExistingInventory:async()=>({}),loadDiscoveryBackoff:async()=>null,
        runDiscovery:async(_input,options)=>{const result=await constructCandidateUniverse({searchDefinition:definition,forceDiscover:true,
          existing:{companies:[],people:[]},adapters:[createPlacesDiscoveryAdapter({placesProvider:options.placesProvider})],
          persistCompanies:options.persistCompanies,acknowledgeDiscovery:options.acknowledgeDiscovery});
          assert.deepEqual(result.discoveryErrors,[]);return {kind:'investigated'};},logger:{log(){},warn(){}}});
      const first=await run('pipeline-mission-a');assert.equal(first.discoveredQueued,1,JSON.stringify(first));assert.equal(transport.length,2);
      const restart=await run('pipeline-mission-a');assert.equal(restart.discoveredQueued,0);assert.equal(transport.length,2);
      const changed=await run('pipeline-mission-b');assert.equal(changed.discoveredQueued,0);assert.equal(transport.length,2);
      assert.equal(changed.queryProgression.tenantMemory.queriesSkipped,1);
      assert.equal((await db.query('SELECT count(*)::int AS n FROM scout_unenriched')).rows[0].n,1);
      assert.ok(events.some(e=>e[0]==='scout_query_progression'&&e[2].newQueueAdmissionsThisCycle===1));
    } finally {
      require.cache[providerPath].exports=previous;
      if(oldFlag==null)delete process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;else process.env.SCOUT_DURABLE_CANDIDATES_ENABLED=oldFlag;
    }
  });
});
