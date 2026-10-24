'use strict';
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { runBoundedAcquisition, validateManifest, PROGRAM_AUTHORITY_COLUMNS } = require('../services/scoutBoundedAcquisition');
const { hash } = require('../services/scoutBoundedAcquisitionBudget');
const { scoutSearchFingerprint } = require('../services/maxOutboundControlLoop');
const { queryIdentity } = require('../services/scoutQueryCheckpoints');
const { createCandidateTicketStore } = require('../services/scoutCandidateTickets');
const { ensurePlacesAttributionSchema } = require('../utils/placesCostAttribution');

const migrationNames = ['2026-10-17-scout-query-checkpoints.sql','2026-10-19-scout-query-pages.sql',
  '2026-10-19-scout-candidate-tickets.sql','2026-10-19-scout-query-coverage-epochs.sql','2026-10-20-scout-coverage-grants.sql',
  '2026-10-14-api-usage-control.sql','2026-10-20-scout-bounded-acquisition.sql'];
const request = { tenantId:'10',scope:'market',segment:'property_manager',evidenceType:'identity',
  discoveryConcept:'Property Management',geography:{ cities:['Concord'],state:'NH' } };
const query = 'Property Management Concord NH';
const response = data => ({ ok:true,status:200,json:async () => data });

test('bounded actual Scout acquisition preserves replay, scope and every transport allowance', { timeout:120000 }, async t => {
  const old = process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED;
  process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED='false';
  const pg = await startDisposablePostgres('scout-bounded-');
  const db = createTrackedPostgresPool({ connectionString:pg.connectionString,max:6,connectionTimeoutMillis:1000,statement_timeout:30000,query_timeout:30000,lock_timeout:2000 });
  t.after(async () => { old == null ? delete process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED : process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED=old;
    await closePostgresFixture(db,pg); });
  for (const file of migrationNames) await db.query(fs.readFileSync(path.join(__dirname,'../migrations',file),'utf8'));
  await ensurePlacesAttributionSchema(db);
  await db.query(`CREATE TABLE acquisition_outbound_programs(id text PRIMARY KEY,tenant_id text,source_mission_id text,mode text,policy jsonb,authorized_at timestamptz,policy_hash text,scope_hash text,authorized_by text,last_tick_at timestamptz,last_error text);
    CREATE TABLE acquisition_missions(id text PRIMARY KEY,tenant_id text,objective text,target_segment text,payload jsonb);
    CREATE TABLE companies(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id int,name text,domain text,website text);
    CREATE TABLE prospects(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),company_id uuid,client_id int,email text,email_verified boolean,
      email_status text,do_not_contact boolean,enrichment_provenance jsonb,acquisition_metadata jsonb,assigned_ao_id int,closer_id int,
      last_contacted_at timestamptz,last_reply_at timestamptz,service_area_match text,vertical text,updated_at timestamptz);
    CREATE TABLE ao_prospect_tasks(client_id int,prospect_id uuid);
    CREATE TABLE touchpoints(client_id int,prospect_id uuid,action_type text);
    CREATE TABLE ao_leads(id int,client_id int,business_name text,crm_prospect_id uuid);
    CREATE TABLE ao_contacts(lead_id int,email text);
    CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,domain text,vertical text,location text,
      source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);`);
  let n=0;
  async function fixture() {
    const id=String(++n), missionId=`synthetic-mission-${id}`, programId=`synthetic-program-${id}`;
    const payload={ structuredMission:{ market:{ segment:'commercial_cleaning_buyers',industry:'property_and_office_operations',
      eligibleSubsegments:['property_manager'] },geography:{ region:'Concord NH',scope:'local',cities:['Concord'] } } };
    await db.query("INSERT INTO acquisition_outbound_programs(id,tenant_id,source_mission_id,mode,policy,authorized_at,policy_hash,scope_hash,authorized_by) VALUES($1,'10',$2,'paused',$3,now(),'synthetic-policy','synthetic-scope','synthetic-operator')",[programId,missionId,{tenantId:'10'}]);
    await db.query("INSERT INTO acquisition_missions VALUES($1,'10','Synthetic property buyers','commercial_cleaning_buyers',$2)",[missionId,payload]);
    const program=(await db.query(`SELECT ${PROGRAM_AUTHORITY_COLUMNS} FROM acquisition_outbound_programs WHERE id=$1`,[programId])).rows[0];
    const source=(await db.query('SELECT id,objective,target_segment,payload FROM acquisition_missions WHERE id=$1',[missionId])).rows[0];
    const fingerprint=scoutSearchFingerprint(program,source), rootQueryKey=queryIdentity(query,false);
    const raw=Array.from({length:20},(_,i)=>({placeId:`old-${id}-${i}`,companyName:`Old Property Management ${i}`,website:`old-${id}-${i}.example`,address:'Concord, NH',placeTypes:['real_estate_agency'],source:'google_places',discoveryQuery:query}));
    await db.query("INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates) VALUES('10',$1,$2,'places-market-query-v1','fetched',$3)",[fingerprint,rootQueryKey,JSON.stringify(raw)]);
    await createCandidateTicketStore({pool:db,tenantId:'10',fingerprint}).transferQuery(rootQueryKey);
    const manifest={version:1,approvalRef:`synthetic-approval-${id}`,tenantId:'10',programId,missionId,programHash:hash(program),sourceHash:hash(source),fingerprint,query,rootQueryKey,evidenceRequest:request,
      grant:{version:2,epoch:1,approvalRef:`synthetic-approval-${id}`,tenantId:'10',fingerprint,rootQueryKeys:[rootQueryKey],maxNewPages:1,
        notBefore:new Date(Date.now()-60000).toISOString(),expiresAt:new Date(Date.now()+3600000).toISOString()},maxModeledUsdMicros:620000};
    const calls=[];
    const fetchImpl=async (input,options)=>{ const url=new URL(input); calls.push(url.pathname); assert.equal(options.redirect,'error');
      if (url.pathname.includes('textsearch')) return response({status:'OK',results:Array.from({length:20},(_,i)=>({place_id:i===0?raw[0].placeId:`new-${id}-${i}`,name:`Property Management ${i}`})),next_page_token:'must-never-follow'});
      const place=url.searchParams.get('place_id');
      return response({status:'OK',result:{place_id:place,name:`Property Management ${place}`,website:`https://${place}.example`,formatted_address:'Concord, NH',types:['real_estate_agency']}}); };
    return { manifest,calls,fetchImpl,raw,run:opts=>runBoundedAcquisition({pool:db,manifest,apiKey:'synthetic',fetchImpl,...opts}) };
  }
  await t.test('one actual page queues new-only work; old 20 attempts and raw payload remain untouched; restart cannot fetch page two',async()=>{
    const f=await fixture(); const result=await f.run();
    assert.equal(result.status,'completed'); assert.equal(result.claimedNewPageOnlyCandidates,19); assert.equal(result.queuedForEnrichment,19);
    assert.equal(result.cleanVerifiedContacts,0); assert.equal(f.calls.length,21);
    assert.equal(result.distinctPageCandidates,20);assert.equal(result.sharedWithOlderQueries,1);assert.equal(result.unclaimedPageCandidates,1);
    assert.equal(result.modeledReservedUsdMicros,540000);assert.equal(result.admissionCounters.admittedToEnrichment,19);
    const old=(await db.query("SELECT attempts FROM scout_candidate_work w JOIN scout_candidate_memberships m USING(tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key) WHERE w.search_fingerprint=$1 AND m.query_key=$2",[f.manifest.fingerprint,f.manifest.rootQueryKey])).rows;
    assert.equal(old.length,20); assert.ok(old.every(row=>Number(row.attempts)===0));
    assert.deepEqual((await db.query('SELECT candidates FROM scout_query_checkpoints WHERE search_fingerprint=$1 AND query_key=$2',[f.manifest.fingerprint,f.manifest.rootQueryKey])).rows[0].candidates,f.raw);
    await assert.rejects(f.run(),/bounded_fresh_first_page_required/); assert.equal(f.calls.length,21);
    const attempts=(await db.query('SELECT * FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1',[f.manifest.approvalRef])).rows;
    assert.equal(attempts.length,21); assert.ok(attempts.every(row=>row.state==='settled' && row.api_usage_id));
    assert.equal(attempts.reduce((sum,row)=>sum+row.reserved_usd_micros,0),540000);
    assert.equal((await db.query('SELECT count(*)::int n FROM api_usage_events WHERE mission_id=$1',[f.manifest.missionId])).rows[0].n,21);
  });
  await t.test('two retry searches count toward same durable cap, then exactly 20 details reaches 620000',async()=>{
    const f=await fixture();let searches=0;
    await f.run({fetchImpl:async (url,opts)=>{ if (url.includes('textsearch') && ++searches<3) { f.calls.push('retry');return response({status:'UNKNOWN_ERROR'}); } return f.fetchImpl(url,opts); }});
    assert.equal(f.calls.length,23);
    const total=(await db.query('SELECT sum(reserved_usd_micros)::int n FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1',[f.manifest.approvalRef])).rows[0].n;
    assert.equal(total,620000);
  });
  await t.test('lower approved budget stops before next HTTP and remains permanently consumed after restart',async()=>{
    const f=await fixture();f.manifest.maxModeledUsdMicros=40000;
    await assert.rejects(f.run(),/bounded_acquisition_not_confirmed/); assert.equal(f.calls.length,1);
    await assert.rejects(f.run()); assert.equal(f.calls.length,1);
    assert.equal((await db.query('SELECT count(*)::int n FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1',[f.manifest.approvalRef])).rows[0].n,1);
  });
  await t.test('uncertain HTTP retains allowance and blocks replay',async()=>{
    const f=await fixture();await assert.rejects(f.run({fetchImpl:async()=>{f.calls.push('uncertain');throw Error('synthetic lost response');}}));
    assert.equal(f.calls.length,1);await assert.rejects(f.run());assert.equal(f.calls.length,1);
    assert.equal((await db.query('SELECT state FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1',[f.manifest.approvalRef])).rows[0].state,'uncertain');
  });
  await t.test('shared request policy still denies each actual call, not one envelope event',async()=>{
    const f=await fixture();const used=(await db.query("SELECT count(*)::int n FROM api_usage_events WHERE provider='google_places'")).rows[0].n;
    await db.query("INSERT INTO api_spend_policies(provider,daily_request_limit,updated_by) VALUES('google_places',$1,'fixture')",[used+1]);
    try {await assert.rejects(f.run());assert.equal(f.calls.length,1);} finally {await db.query('DELETE FROM api_spend_policies');}
  });

  const guardedPool = onQuery => ({ options:db.options, connect:()=>db.connect(), query:async(sql,args)=>{
    if (/\b(CREATE|ALTER|DROP)\s/i.test(sql)) throw Error('unexpected runtime DDL');
    const result=await db.query(sql,args); await onQuery?.(sql,args,result); return result;
  } });
  await t.test('runtime never creates schema and persists every consumed body before next call',async()=>{
    const f=await fixture();await f.run({pool:guardedPool()});
    const rows=(await db.query('SELECT response_evidence,response_hash,state FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1',[f.manifest.approvalRef])).rows;
    assert.equal(rows.length,21);assert.ok(rows.every(row=>row.response_evidence && row.response_hash===hash(row.response_evidence)));
  });
  await t.test('source timestamp-only change fails exact binding with zero HTTP',async()=>{
    const f=await fixture();await db.query("UPDATE acquisition_outbound_programs SET authorized_at=authorized_at+interval '1 second' WHERE id=$1",[f.manifest.programId]);
    await assert.rejects(f.run(),/bounded_source_changed/);assert.equal(f.calls.length,0);
  });
  await t.test('source revocation after search prevents all detail transport',async()=>{
    const f=await fixture();let changed=false;
    const pool=guardedPool(async(sql,args)=>{if(!changed && /SET state=\$2,http_status/.test(sql)){changed=true;await db.query("UPDATE acquisition_outbound_programs SET mode='revoked' WHERE id=$1",[f.manifest.programId]);}});
    await assert.rejects(f.run({pool}));assert.equal(f.calls.length,1);
  });
  await t.test('revocation and expiry after search deny the next attempt',async()=>{
    for(const clause of ["revoked_at=clock_timestamp()","expires_at=clock_timestamp()-interval '1 second'"]){
      const f=await fixture();let changed=false;
      const pool=guardedPool(async sql=>{if(!changed && /SET state=\$2,http_status/.test(sql)){changed=true;await db.query(`UPDATE scout_query_coverage_grants SET ${clause} WHERE approval_ref=$1`,[f.manifest.approvalRef]);}});
      await assert.rejects(f.run({pool}));assert.equal(f.calls.length,1);
    }
  });
  await t.test('pause, unknown recovery and missing attribution schema fail closed without HTTP',async()=>{
    const f=await fixture();process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED='true';
    try{await assert.rejects(f.run(),/anchor_research_provider_paused/);}finally{process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED='false';}
    await db.query('CREATE TABLE anchor_email_recovery_control(singleton boolean,maintenance_armed boolean,manifest jsonb)');
    try{await assert.rejects(f.run(),/bounded_recovery_not_legacy/);}finally{await db.query('DROP TABLE anchor_email_recovery_control');}
    await db.query('ALTER TABLE places_api_requests RENAME TO hidden_places_api_requests');
    try{await assert.rejects(f.run());}finally{await db.query('ALTER TABLE hidden_places_api_requests RENAME TO places_api_requests');}
    assert.equal(f.calls.length,0);
  });
  await t.test('maintenance interlock wins before the next call and a held exclusive lock times out at two seconds',async()=>{
    const f=await fixture();let changed=false;
    const pool=guardedPool(async sql=>{if(!changed && /SET state=\$2,http_status/.test(sql)){changed=true;
      await db.query('CREATE TABLE anchor_email_recovery_control(singleton boolean,maintenance_armed boolean,manifest jsonb)');}});
    try{await assert.rejects(f.run({pool}));assert.equal(f.calls.length,1);}finally{await db.query('DROP TABLE anchor_email_recovery_control');}
    const held=await db.connect();await held.query('BEGIN');await held.query("SELECT pg_advisory_xact_lock(hashtextextended('anchor-email-recovery:10',0))");
    const next=await fixture(), start=Date.now();
    try{await assert.rejects(next.run(),/lock timeout/);assert.equal(next.calls.length,0);assert.ok(Date.now()-start<4000);}finally{await held.query('ROLLBACK');held.release();}
  });
  await t.test('concurrent invocations share one durable identity and cannot multiply allowances',async()=>{
    const f=await fixture();const results=await Promise.allSettled([f.run(),f.run()]);
    assert.equal(results.filter(row=>row.status==='fulfilled').length,1);assert.equal(f.calls.length,21);
  });
  await t.test('redirect response is not followed and unknown outcomes keep their reservation',async()=>{
    const f=await fixture();await assert.rejects(f.run({fetchImpl:async()=>{f.calls.push('redirect');return {status:302,ok:false,json:async()=>({})};}}));
    assert.equal(f.calls.length,1);await assert.rejects(f.run());assert.equal(f.calls.length,1);
  });

  await t.test('response evidence survives lost transport COMMIT acknowledgement and stops further calls',async()=>{
    const f=await fixture();let bodySaved=false,lost=false;
    const pool={options:db.options,query:async(sql,args)=>{const result=await db.query(sql,args);
      if(/SET response_evidence=/.test(sql))bodySaved=true;return result;},connect:async()=>{
      const client=await db.connect();return {query:async(sql,args)=>{const result=await client.query(sql,args);
        if(sql==='COMMIT'&&bodySaved&&!lost){lost=true;throw Error('synthetic commit acknowledgement lost');}return result;},release:broken=>client.release(broken),on:()=>{},removeListener:()=>{}};
    }};
    await assert.rejects(f.run({pool}));assert.equal(f.calls.length,1);assert.equal(lost,true);
    const row=(await db.query('SELECT response_evidence,response_hash,state FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1',[f.manifest.approvalRef])).rows[0];
    assert.equal(row.response_evidence.results.length,20);assert.equal(row.response_hash,hash(row.response_evidence));assert.equal(row.state,'uncertain');
    await assert.rejects(f.run());assert.equal(f.calls.length,1);
  });
  await t.test('unconfirmed body persistence stops all later calls while keeping the full debit',async()=>{
    const f=await fixture();const pool={options:db.options,connect:()=>db.connect(),query:async(sql,args)=>{
      if(/SET response_evidence=/.test(sql))throw Error('synthetic persistence failure');return db.query(sql,args);
    }};
    await assert.rejects(f.run({pool}));assert.equal(f.calls.length,1);
    const row=(await db.query('SELECT reserved_usd_micros,state FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1',[f.manifest.approvalRef])).rows[0];
    assert.equal(row.reserved_usd_micros,40000);assert.equal(row.state,'uncertain');await assert.rejects(f.run());
  });
  await t.test('manually altered grant window cannot silently widen approval',async()=>{
    const f=await fixture();let changed=false;const pool=guardedPool(async sql=>{
      if(!changed && /SET state=\$2,http_status/.test(sql)){changed=true;await db.query("UPDATE scout_query_coverage_grants SET expires_at=expires_at+interval '1 day' WHERE approval_ref=$1",[f.manifest.approvalRef]);}
    });
    await assert.rejects(f.run({pool}));assert.equal(f.calls.length,1);
  });

  await t.test('unconfirmed shared settlement or attribution stops before another HTTP',async()=>{
    for(const failure of ['settlement','attribution']){
      const f=await fixture();const pool={options:db.options,connect:()=>db.connect(),query:async(sql,args)=>{
        if(failure==='settlement' && /UPDATE api_usage_events SET status/.test(sql))return {rows:[],rowCount:0};
        if(failure==='attribution' && /INSERT INTO places_api_requests/.test(sql))throw Error('synthetic attribution unavailable');
        return db.query(sql,args);
      }};
      await assert.rejects(f.run({pool}));assert.equal(f.calls.length,failure==='settlement'?1:2);
      // Legacy search yield attribution is deferred until details are processed;
      // the first detail's failed attribution stops every subsequent call.
      const attempt=(await db.query('SELECT response_evidence,reserved_usd_micros FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1',[f.manifest.approvalRef])).rows[0];
      assert.ok(attempt.response_evidence);assert.equal(attempt.reserved_usd_micros,40000);
    }
  });

  await t.test('routine scheduler telemetry does not invalidate approved authority',async()=>{
    const f=await fixture();await db.query("UPDATE acquisition_outbound_programs SET last_tick_at=now(),last_error='synthetic tick telemetry' WHERE id=$1",[f.manifest.programId]);
    assert.equal((await f.run()).status,'completed');assert.equal(f.calls.length,21);
  });
  await t.test('expiry during a row-lock wait is checked after the wait before any detail HTTP',async()=>{
    const f=await fixture();let held=null,released=null,changed=false;
    // A lock held without modifying the grant reproduces predicate-before-wait.
    const pool=guardedPool(async sql=>{if(!changed && /SET state=\$2,http_status/.test(sql)){
      changed=true;held=await db.connect();await held.query('BEGIN');
      await held.query('SELECT * FROM scout_query_coverage_grants WHERE approval_ref=$1 FOR UPDATE',[f.manifest.approvalRef]);
      released=new Promise(resolve=>setTimeout(async()=>{await held.query('ROLLBACK');held.release();resolve();},700));
    }});
    f.manifest.grant.expiresAt=new Date(Date.now()+500).toISOString();
    await assert.rejects(f.run({pool}));if(released)await released;assert.equal(f.calls.length,1);
  });
  await t.test('uncapped bounded accounting skips unused history but a new shared cap still fails closed',async()=>{
    const { reserveApiCall, updateApiPolicy }=require('../utils/apiSpendControl');
    let scans=0;
    const pool={options:db.options,query:db.query.bind(db),async connect(){
      const client=await db.connect();return {release:client.release.bind(client),async query(sql,args){
        if(/SELECT COUNT\(\*\) FILTER/.test(sql)){scans++;throw Object.assign(Error('synthetic history timeout'),{code:'57014'});}
        return client.query(sql,args);
      }};
    }};
    const metadata={provider:'google_places',agentName:'scout',tenantId:10,operation:'text_search',estimatedUsd:0.04};
    await db.query("INSERT INTO api_spend_policies(provider,updated_by) VALUES('google_places','fixture') ON CONFLICT(provider) DO UPDATE SET paused=false,daily_request_limit=NULL,monthly_request_limit=NULL,daily_usd_limit=NULL,monthly_usd_limit=NULL");
    try {
      const id=await reserveApiCall(metadata,{db:pool,existingSchemaOnly:true});assert.ok(id);assert.equal(scans,0);
      await updateApiPolicy('google_places',{paused:true},{db});
      await assert.rejects(reserveApiCall(metadata,{db:pool,existingSchemaOnly:true}),/api_provider_paused/);assert.equal(scans,0);
      await updateApiPolicy('google_places',{paused:false,daily_request_limit:999999},{db});
      await assert.rejects(reserveApiCall(metadata,{db:pool,existingSchemaOnly:true}),/synthetic history timeout/);assert.equal(scans,1);
      await updateApiPolicy('google_places',{daily_request_limit:null,monthly_usd_limit:0},{db});
      await assert.rejects(reserveApiCall(metadata,{db:pool,existingSchemaOnly:true}),/synthetic history timeout/);assert.equal(scans,2);
      await updateApiPolicy('google_places',{monthly_usd_limit:null},{db});
      await assert.rejects(reserveApiCall(metadata,{db:pool}),/synthetic history timeout/);assert.equal(scans,3);
      const holder=await db.connect();
      try {
        await holder.query('BEGIN');
        await holder.query('SELECT pg_advisory_xact_lock(7141026, hashtext($1))',['google_places']);
        await holder.query("UPDATE api_spend_policies SET paused=true WHERE provider='google_places'");
        const before=Number((await db.query('SELECT count(*) n FROM api_usage_events')).rows[0].n);
        const waiting=reserveApiCall(metadata,{db:pool,existingSchemaOnly:true}).then(()=>null,e=>e);
        await new Promise(resolve=>setTimeout(resolve,30));await holder.query('COMMIT');
        assert.equal((await waiting).code,'api_provider_paused');assert.equal(scans,3);
        assert.equal(Number((await db.query('SELECT count(*) n FROM api_usage_events')).rows[0].n),before);
      } finally {await holder.query('ROLLBACK');holder.release();}

    } finally {await db.query('DELETE FROM api_spend_policies');}
  });

});
