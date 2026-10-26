'use strict';
// Disposable synthetic fixtures only. No application DB bootstrap or hydration.
const hydrationGuard = require('./denyAcquisitionHydration.cjs');
const fs = require('node:fs'), path = require('node:path'), assert = require('node:assert/strict');
const { startDisposablePostgres } = require('./disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./closePostgresPool');
const { seedScopedResume } = require('./anchorScopedResumePostgresFixture');
const { applyCrmPreparationSchema } = require('./anchorCrmPreparationSchema');
const { insert } = require('./anchorRecoveryPostgresFixture');
const { hash } = require('../../services/scoutBoundedAcquisitionBudget');
const { scoutSearchFingerprint } = require('../../services/maxOutboundControlLoop');
const { queryIdentity } = require('../../services/scoutQueryCheckpoints');
const { createCandidateTicketStore } = require('../../services/scoutCandidateTickets');
const { ensurePlacesAttributionSchema } = require('../../utils/placesCostAttribution');
const REVISION = 'a'.repeat(40);
const ENV = Object.freeze({ RAILWAY_GIT_COMMIT_SHA:REVISION, ANCHOR_RESEARCH_PROVIDERS_PAUSED:'true',
  ANCHOR_EMAIL_RECOVERY_MAINTENANCE:'true', ANCHOR_GOVERNED_PREPARATION_ENABLED:'true',
  ANCHOR_GOVERNED_OUTBOUND_ENABLED:'false', ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED:'true',
  ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED:'true', GOVERNED_OUTBOUND_DURABLE_SCHEDULING_ENABLED:'true',
  GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED:'true' });
const request = { tenantId:'10',scope:'market',segment:'property_manager',evidenceType:'identity',
  discoveryConcept:'Property Management',geography:{ cities:['Concord'],state:'NH' } };
const query = 'Property Management Concord NH';
const migrations = ['2026-10-17-scout-query-checkpoints.sql','2026-10-19-scout-query-pages.sql',
  '2026-10-19-scout-candidate-tickets.sql','2026-10-19-scout-query-coverage-epochs.sql',
  '2026-10-20-scout-coverage-grants.sql','2026-10-14-api-usage-control.sql','2026-10-20-scout-bounded-acquisition.sql',
  '2026-10-26-scout-bounded-research-authority.sql'];
async function startResearchHarness() {
 const postgres = await startDisposablePostgres('scout-research-');
 const pool = createTrackedPostgresPool({connectionString:postgres.connectionString,max:8,connectionTimeoutMillis:1000,statement_timeout:30000,query_timeout:30000,lock_timeout:2000});
 const keys=[...Object.keys(ENV),'ANCHOR_EMAIL_SCOPED_RESUME_ID','ANCHOR_EMAIL_RECOVERY_MANIFEST_ID'];
 const previous=Object.fromEntries(keys.map(key=>[key,process.env[key]]));Object.assign(process.env,ENV);
 delete process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;delete process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID;
 async function close(){try{await closePostgresFixture(pool,postgres);}finally{for(const key of keys)previous[key]===undefined?delete process.env[key]:process.env[key]=previous[key];}
  for(const p of [postgres.rootDirectory,postgres.socketDirectory,postgres.portLockPath])assert.equal(fs.existsSync(p),false,'native fixture cleanup');}
 try {
  const legacy=await seedScopedResume(pool);await applyCrmPreparationSchema(pool);
  await pool.query(`ALTER TABLE acquisition_outbound_programs ADD COLUMN last_tick_at timestamptz,ADD COLUMN last_error text;
   ALTER TABLE acquisition_missions ADD COLUMN objective text,ADD COLUMN target_segment text,ADD COLUMN stage text,ADD COLUMN status text,ADD COLUMN client_id int;
   ALTER TABLE prospects ADD COLUMN service_area_match text,ADD COLUMN vertical text,ADD COLUMN updated_at timestamptz;
   CREATE TABLE scout_unenriched(id serial PRIMARY KEY,client_id int,company text,website_url text,domain text,vertical text,location text,source text,enrichment_attempts int,last_attempt_at timestamptz,notes text);
   UPDATE acquisition_outbound_items SET status='sent',provider_message_id='synthetic-historical-sent' WHERE status='pending';`);
  await pool.query("INSERT INTO acquisition_missions(id,tenant_id,client_id,stage,status,payload) VALUES('history-invalid-ready','10',10,'execute','active',$1)",
   [{id:'history-invalid-ready',tenantId:'10',stage:'ready',pendingOperatorDecision:{stage:'ready',kind:'execution_approval'},legacyPreparedArtifact:'intentionally-unusable-history'}]);
  await pool.query("INSERT INTO acquisition_mission_contributions(id,tenant_id,mission_id,specialist,kind,payload) VALUES('history-stale-paige','10','history-invalid-ready','paige','variants',$1)",
   [{missionId:'history-invalid-ready',variants:[{subject:'Synthetic historical draft',body:'Not an execution approval.'}],superseded:true}]);
  // Production-sized historical shape:45 existing missions and298 persisted
  // contributions before adding the current synthetic discovery source. One SQL
  // execute/payload-ready mismatch is deliberately invalid for strict hydration.
  const extraMissions=Array.from({length:43},(_,i)=>({id:`history-mission-${i}`,tenant_id:i%5===0?'11':'10',client_id:i%5===0?11:10,stage:'discover',status:'active',objective:'Synthetic historical mission',target_segment:'property_management',payload:{...structuredClone(legacy.sourceMission),id:`history-mission-${i}`,tenantId:i%5===0?'11':'10',clientId:i%5===0?11:10,stage:'discover'}}));
  await pool.query('INSERT INTO acquisition_missions(id,tenant_id,client_id,stage,status,objective,target_segment,payload) SELECT * FROM jsonb_to_recordset($1) AS x(id text,tenant_id text,client_id int,stage text,status text,objective text,target_segment text,payload jsonb)',[JSON.stringify(extraMissions)]);
  const extraContributions=Array.from({length:296},(_,i)=>({id:`history-contribution-${i}`,tenant_id:(i%43)%5===0?'11':'10',mission_id:`history-mission-${i%43}`,specialist:['scout','max','paige','emmett'][i%4],kind:['discovery','prioritization','variants','capacity'][i%4],payload:{missionId:`history-mission-${i%43}`,tenantId:(i%43)%5===0?'11':'10',syntheticHistorical:true,revision:`historical-${i}`,superseded:i%3===0}}));
  await pool.query('INSERT INTO acquisition_mission_contributions(id,tenant_id,mission_id,specialist,kind,payload) SELECT * FROM jsonb_to_recordset($1) AS x(id text,tenant_id text,mission_id text,specialist text,kind text,payload jsonb)',[JSON.stringify(extraContributions)]);
  const historicalMissionIds=(await pool.query('SELECT id FROM acquisition_missions ORDER BY id')).rows.map(r=>r.id);
  assert.equal(historicalMissionIds.length,45);assert.equal((await pool.query('SELECT count(*)::int n FROM acquisition_mission_contributions')).rows[0].n,298);
  for(let i=1;i<=2;i++){
   const manifest={...structuredClone(legacy.manifest),id:`history-prep-${i}`};
   await insert(pool,'anchor_scoped_resume_grants',{manifest_id:manifest.id,tenant_id:'10',program_id:manifest.programId,manifest_hash:hash(manifest),manifest,approval_ref:`synthetic-history-${i}`,operator_id:'3',operator_role:'admin',review_initializations:[],installed_at:'2026-10-25T12:00:00Z',revoked_at:'2026-10-25T13:00:00Z',revoke_approval_ref:`synthetic-history-close-${i}`,revoked_by:'3',revoked_role:'admin'});
   await insert(pool,'anchor_scoped_resume_consumption',{operation_id:`history-operation-${i}`,tenant_id:'10',program_id:manifest.programId,manifest_id:manifest.id,capability:'preparation',operation:'prepare_existing_crm',local_day:'2026-10-25',item_count:1,identities:manifest.admissions,approval_ref:`synthetic-history-${i}`,consumed_at:'2026-10-25T12:01:00Z'});
  }
  for(const name of migrations)await pool.query(fs.readFileSync(path.join(__dirname,'../../migrations',name),'utf8'));
  await ensurePlacesAttributionSchema(pool);
  let n=0;
  async function newManifest(){
   const id=String(++n),missionId=`synthetic-research-mission-${id}`,programId=`synthetic-research-program-${id}`;
   const payload={id:missionId,tenantId:'10',stage:'discover',structuredMission:{immutable:true,market:{segment:'commercial_cleaning_buyers',industry:'property_and_office_operations',eligibleSubsegments:['property_manager']},geography:{region:'Concord NH',scope:'local',cities:['Concord']}}};
   await pool.query("INSERT INTO acquisition_outbound_programs(id,tenant_id,source_mission_id,mode,policy,authorized_at,policy_hash,scope_hash,authorized_by) VALUES($1,'10',$2,'paused',$3,now(),'synthetic-policy','synthetic-scope','synthetic-operator')",[programId,missionId,{tenantId:'10'}]);
   await pool.query("INSERT INTO acquisition_missions(id,tenant_id,objective,target_segment,payload,stage,client_id) VALUES($1,'10','Synthetic property buyers','commercial_cleaning_buyers',$2,'discover',10)",[missionId,payload]);
   const {PROGRAM_AUTHORITY_COLUMNS,runBoundedAcquisition}=require('../../services/scoutBoundedAcquisition');
   const program=(await pool.query(`SELECT ${PROGRAM_AUTHORITY_COLUMNS} FROM acquisition_outbound_programs WHERE id=$1`,[programId])).rows[0];
   const source=(await pool.query('SELECT id,objective,target_segment,payload FROM acquisition_missions WHERE id=$1',[missionId])).rows[0];
   const fingerprint=scoutSearchFingerprint(program,source),rootQueryKey=queryIdentity(query,false);
   const raw=Array.from({length:20},(_,i)=>({placeId:`old-${id}-${i}`,companyName:`Old Property Management ${i}`,website:`old-${id}-${i}.example`,address:'Concord, NH',placeTypes:['real_estate_agency'],source:'google_places',discoveryQuery:query}));
   await pool.query("INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates) VALUES('10',$1,$2,'places-market-query-v1','fetched',$3)",[fingerprint,rootQueryKey,JSON.stringify(raw)]);
   await createCandidateTicketStore({pool,tenantId:'10',fingerprint}).transferQuery(rootQueryKey);
   const manifest={version:2,approvalRef:`synthetic-research-${id}`,tenantId:'10',programId,missionId,programHash:hash(program),sourceHash:hash(source),fingerprint,query,rootQueryKey,evidenceRequest:structuredClone(request),
    grant:{version:2,epoch:1,approvalRef:`synthetic-research-${id}`,tenantId:'10',fingerprint,rootQueryKeys:[rootQueryKey],maxNewPages:1,notBefore:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+3500000).toISOString()},maxModeledUsdMicros:620000,
    researchAuthority:{kind:'bounded_places_research',expectedRevision:REVISION,quarantineDecisionHash:legacy.quarantine.decision_hash,recoveryControlHash:hash({singleton:true,maintenance_armed:true,manifest:null}),provider:'google_places',maxSearchAttempts:3,maxDetailsAttempts:20,enrichment:false,verification:false,preparation:false,email:false}};
   const calls=[];const fetchImpl=async(input,options)=>{const url=new URL(input);calls.push(url.pathname);assert.equal(options.redirect,'error');
    const data=url.pathname.includes('textsearch')?{status:'OK',results:Array.from({length:20},(_,i)=>({place_id:i===0?raw[0].placeId:`new-${id}-${i}`,name:`Property Management ${i}`})),next_page_token:'must-never-follow'}
    :{status:'OK',result:{place_id:url.searchParams.get('place_id'),name:`Property Management ${url.searchParams.get('place_id')}`,website:`https://${url.searchParams.get('place_id')}.example`,formatted_address:'Concord, NH',types:['real_estate_agency']}};
    return {ok:true,status:200,json:async()=>data};};
   return {manifest,raw,calls,fetchImpl,run:opts=>runBoundedAcquisition({pool,manifest,apiKey:'synthetic-local-key',fetchImpl,...opts})};
  }
  async function historySnapshot(){const result={};for(const table of ['anchor_email_recovery_control','anchor_email_quarantines','anchor_email_recovery_reservations','anchor_scoped_resume_grants','anchor_scoped_resume_consumption','acquisition_outbound_items','acquisition_mission_outbound_executions'])
   result[table]=(await pool.query(`SELECT to_jsonb(x) AS row FROM ${table} x ORDER BY to_jsonb(x)::text`)).rows;
   result.legacyMissions=(await pool.query("SELECT to_jsonb(x) AS row FROM acquisition_missions x WHERE id=ANY($1::text[]) ORDER BY id",[historicalMissionIds])).rows;
   result.legacyContributions=(await pool.query('SELECT to_jsonb(x) AS row FROM acquisition_mission_contributions x ORDER BY id')).rows;return result;}
  return {pool,postgres,env:{...ENV,DATABASE_URL:postgres.connectionString,DATABASE_SSL:'false'},newManifest,historySnapshot,close,legacy,hydrationGuard,historicalMissionIds};
 }catch(e){await close();throw e;}
}
module.exports={startResearchHarness,REVISION,ENV,request,query};
