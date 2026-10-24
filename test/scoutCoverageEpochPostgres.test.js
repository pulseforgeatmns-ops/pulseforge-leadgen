'use strict';
// Local disposable PostgreSQL only. No production connection strings or HTTP.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHash, randomUUID } = require('node:crypto');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { createCheckpointStore, createCheckpointSession, queryIdentity, PLAN_VERSION } = require('../services/scoutQueryCheckpoints');
const { createPageCheckpointStore, pageIdentity } = require('../services/scoutQueryContinuation');
const { createCandidateTicketStore } = require('../services/scoutCandidateTickets');
const { epochRootIdentity } = require('../services/scoutCoverageEpoch');
const hash = value => createHash('sha256').update(value).digest('hex');

test('explicit epoch1 retains legacy responsibility and enforces durable scoped finite reacquisition on PostgreSQL', { timeout: 60000 }, async t => {
  const instance = await startDisposablePostgres('scout-epoch-');
  const db = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 4 });
  t.after(() => closePostgresFixture(db, instance));
  const migrate = file => db.query(fs.readFileSync(path.join(__dirname,'../migrations',file),'utf8'));
  for (const file of ['2026-10-17-scout-query-checkpoints.sql','2026-10-19-scout-query-pages.sql','2026-10-19-scout-candidate-tickets.sql']) await migrate(file);
  let clock = Date.now();
  async function fixture(count = 1) {
    const tenantId = '13', fingerprint = hash(randomUUID());
    const queries = Array.from({length:count},(_,i)=>`Painting Contractor Fixture ${i}`);
    const roots = queries.map(query=>queryIdentity(query,false));
    const originals = roots.map((_,i)=>[{placeId:`old-${i}`,companyName:`Unknown ${i}`,website:`old${i}.example`,
      source:'google_places',industry:queries[i],enrichment_attempts:7}]);
    for (let i=0;i<roots.length;i++) await db.query(`INSERT INTO scout_query_checkpoints
      (tenant_id,search_fingerprint,query_key,plan_version,state,candidates) VALUES($1,$2,$3,$4,'fetched',$5::jsonb)`,
    [tenantId,fingerprint,roots[i],PLAN_VERSION,JSON.stringify(originals[i])]);
    const checkpoints = createCheckpointStore({db,tenantId,fingerprint});
    const tickets = createCandidateTicketStore({pool:db,tenantId,fingerprint});
    const config = {version:1,epoch:1,approvalRef:'synthetic-review-only',tenantId,fingerprint,rootQueryKeys:[roots[0]],
      notBefore:new Date(clock-1000).toISOString(),expiresAt:new Date(clock+3600000).toISOString()};
    const pages = coverageEpoch=>createPageCheckpointStore({db,tenantId,fingerprint,coverageEpoch,now:()=>clock});
    const transferAll = async()=>{for(const root of roots)await tickets.transferQuery(root);};
    return {tenantId,fingerprint,queries,roots,originals,checkpoints,tickets,config,pages,transferAll};
  }
  await t.test('old durable rollout needs no epoch table; active manifest fails before any acquisition when migration absent', async()=>{
    const f=await fixture();await f.transferAll();
    assert.equal((await f.pages(null).plan(f.roots[0])).status,'legacy_continuation_unknown');
    await assert.rejects(f.pages(f.config).assertReady(),/migration required/);
  });
  await migrate('2026-10-19-scout-query-coverage-epochs.sql');
  await migrate('2026-10-19-scout-query-coverage-epochs.sql');
  await t.test('21 fetched legacy roots transfer first, preserving raw attempts and independent blocked/deferred tickets',async()=>{
    const f=await fixture(21);const pages=f.pages(f.config);
    assert.equal((await pages.plan(f.roots[0])).legacy,true);
    await assert.rejects(pages.reserve({rootKey:epochRootIdentity(f.roots[0]),key:epochRootIdentity(f.roots[0]),index:0,
      originalRootKey:f.roots[0],coverageEpoch:1}),/mapping missing/);
    assert.equal((await db.query('SELECT count(*)::int n FROM scout_query_coverage_epochs WHERE search_fingerprint=$1',[f.fingerprint])).rows[0].n,0);
    const session=createCheckpointSession({store:f.checkpoints,ticketStore:f.tickets,pageStore:pages});
    for(const query of f.queries)assert.equal((await session.lookup(query,false)).candidates.length,1);
    assert.equal(session.snapshot().coverageApprovalManifest.rootQueryKeys.length,21);
    assert.equal((await db.query('SELECT count(*)::int n FROM scout_query_pages WHERE search_fingerprint=$1',[f.fingerprint])).rows[0].n,0);
    const claims=await f.tickets.claimDue({dependencyKey:'old-dependency',limit:100});
    assert.equal(claims.length,21);
    for(let i=0;i<claims.length;i++)await f.tickets.finish(claims[i].refs,i===0
      ?{state:'deferred',reason:'evidence_budget_deferred',retryAfterMs:0,dependencyKey:'old-dependency'}
      :{state:'blocked',reason:'unresolved_business_evidence',dependencyKey:'old-dependency'});
    const oldWork=(await db.query(`SELECT candidate_key,state,reason,attempts FROM scout_candidate_work
      WHERE search_fingerprint=$1 ORDER BY candidate_key`,[f.fingerprint])).rows;
    const next=await f.pages(f.config).plan(f.roots[0]);
    assert.equal(next.index,0);assert.equal(next.rootKey,epochRootIdentity(f.roots[0]));
    assert.equal((await f.pages(null).plan(f.roots[1])).status,'legacy_continuation_unknown');
    const id=await f.pages(f.config).reserve(next);assert.ok(id);
    const fresh=[{placeId:'fresh',companyName:'Observed Painting Contractor',website:'fresh.example'}];
    await f.pages(f.config).stage({...next,reservationId:id},fresh,{rawResults:[{place_id:'fresh'}],nextPageToken:null});
    await f.tickets.transferQuery(next.key);
    assert.equal((await f.pages(f.config).plan(f.roots[0])).status,'finite_query_exhausted');
    assert.deepEqual((await db.query(`SELECT candidate_key,state,reason,attempts FROM scout_candidate_work
      WHERE search_fingerprint=$1 AND candidate_key=ANY($2::text[]) ORDER BY candidate_key`,[f.fingerprint,oldWork.map(row=>row.candidate_key)])).rows,oldWork);
    for(let i=0;i<21;i++)assert.deepEqual((await f.checkpoints.load(f.roots[i])).candidates,f.originals[i]);
    const due=await f.tickets.claimDue({dependencyKey:'old-dependency',limit:100});
    assert.equal(due.length,2,'old deferred candidate and new candidate remain independently due');
  });
  await t.test('concurrent initial claims converge and manifest changes or crafted identities cannot reset them',async()=>{
    const f=await fixture();await f.transferAll();
    const a=f.pages(f.config),b=f.pages(f.config);
    const [pa,pb]=await Promise.all([a.plan(f.roots[0]),b.plan(f.roots[0])]);
    assert.equal(pa.key,pb.key);
    const winners=await Promise.all([a.reserve(pa),b.reserve(pb)]);assert.equal(winners.filter(Boolean).length,1);
    assert.equal((await f.pages({...f.config,approvalRef:'different-review'}).plan(f.roots[0])).status,'page_fetch_in_progress');
    await assert.rejects(a.reserve({...pa,originalRootKey:hash('wrong')}),/identity mismatch/);
    const {originalRootKey,manifestHash,coverageEpoch,...stripped}=pa;
    await assert.rejects(a.reserve(stripped),/identity mismatch/);
    assert.equal((await db.query('SELECT count(*)::int n FROM scout_query_coverage_epochs WHERE search_fingerprint=$1',[f.fingerprint])).rows[0].n,1);
    assert.equal((await db.query('SELECT count(*)::int n FROM scout_query_pages WHERE search_fingerprint=$1',[f.fingerprint])).rows[0].n,1);
  });
  await t.test('one immutable scope receipt prevents disjoint allowlists or windows from expanding epoch allocation',async()=>{
    const f=await fixture(3);await f.transferAll();
    const other={...f.config,approvalRef:'second-manifest',rootQueryKeys:[f.roots[1]]};
    const plans=await Promise.all([f.pages(f.config).plan(f.roots[0]),f.pages(other).plan(f.roots[1])]);
    assert.equal(plans.filter(plan=>plan.key).length,1,'one complete authorization wins concurrent disjoint manifests');
    assert.equal(plans.filter(plan=>plan.status==='coverage_epoch_manifest_changed').length,1);
    const receipt=(await db.query('SELECT * FROM scout_query_coverage_epoch_receipts WHERE search_fingerprint=$1',[f.fingerprint])).rows;
    assert.equal(receipt.length,1);assert.equal(receipt[0].root_query_keys.length,1);
    const expanded={...f.config,rootQueryKeys:f.roots,expiresAt:new Date(clock+7200000).toISOString()};
    assert.equal((await f.pages(expanded).plan(f.roots[2])).status,'coverage_epoch_manifest_changed');
    assert.deepEqual((await db.query('SELECT * FROM scout_query_coverage_epoch_receipts WHERE search_fingerprint=$1',[f.fingerprint])).rows,receipt);
    assert.equal((await db.query('SELECT count(*)::int n FROM scout_query_coverage_epochs WHERE search_fingerprint=$1',[f.fingerprint])).rows[0].n,1);
  });
  await t.test('same-manifest concurrent roots map safely while the acquisition session admits only one new page',async()=>{
    const f=await fixture(2);await f.transferAll();const config={...f.config,rootQueryKeys:f.roots};
    const plans=await Promise.all(f.roots.map(root=>f.pages(config).plan(root)));
    assert.ok(plans.every(plan=>plan.key));
    const session=createCheckpointSession({store:f.checkpoints,ticketStore:f.tickets,pageStore:f.pages(config)});
    assert.equal((await session.lookup(f.queries[0],false)).candidates,null);
    assert.equal((await session.lookup(f.queries[1],false)).handled,true);
    assert.equal(session.snapshot().continuation.newPagesReserved,1);
    assert.equal(session.snapshot().continuation.statuses.cycle_page_budget_reached,1);
    const expired={...config,expiresAt:new Date(Date.parse(config.expiresAt)+3600000).toISOString()};
    assert.equal((await f.pages(expired).plan(f.roots[1])).status,'coverage_epoch_manifest_changed','unfetched mapped roots cannot renew');
  });
  await t.test('expiry and removal preserve paid staged payload while refusing later reservations',async()=>{
    const f=await fixture();await f.transferAll();
    const pages=f.pages(f.config),plan=await pages.plan(f.roots[0]);const id=await pages.reserve(plan);
    clock=Date.parse(f.config.expiresAt)+1;
    const raw=[{placeId:'late-result',companyName:'Late Painting Contractor'}];
    await pages.stage({...plan,reservationId:id},raw,{rawResults:[{place_id:'late-result'}],nextPageToken:'private-token'});
    for(const permission of [null,f.config]){
      const replay=await f.pages(permission).plan(f.roots[0]);assert.equal(replay.key,plan.key);assert.deepEqual(replay.row.candidates,raw);
    }
    const restart=createCheckpointSession({store:f.checkpoints,ticketStore:f.tickets,pageStore:f.pages(null)});
    assert.equal((await restart.lookup(f.queries[0],false)).candidates.length,1);
    clock+=6000;
    assert.equal((await f.pages(null).plan(f.roots[0])).status,'coverage_epoch_permission_required');
    assert.equal((await f.pages(f.config).plan(f.roots[0])).status,'coverage_epoch_expired');
    assert.equal(await pages.reserve({...plan,key:pageIdentity(plan.rootKey,1),index:1,pageToken:'private-token'}),null);
    assert.deepEqual((await f.checkpoints.load(plan.key)).candidates,raw);
  });
  await t.test('expiry is checked again at reserve; immutable approval cannot be extended or changed',async()=>{
    const f=await fixture();await f.transferAll();const pages=f.pages(f.config);const plan=await pages.plan(f.roots[0]);
    clock=Date.parse(f.config.expiresAt);assert.equal(await pages.reserve(plan),null);
    const changed={...f.config,expiresAt:new Date(clock+3600000).toISOString()};
    assert.equal((await f.pages(changed).plan(f.roots[0])).status,'coverage_epoch_manifest_changed');
    assert.equal((await db.query('SELECT count(*)::int n FROM scout_query_pages WHERE search_fingerprint=$1',[f.fingerprint])).rows[0].n,0);
  });
  await t.test('only proven zero-network budget denial releases an unused page, retaining the epoch and legacy audit',async()=>{
    const f=await fixture();await f.transferAll();const pages=f.pages(f.config),plan=await pages.plan(f.roots[0]);
    const before=(await db.query('SELECT * FROM scout_query_coverage_epochs WHERE search_fingerprint=$1',[f.fingerprint])).rows;
    const id=await pages.reserve(plan);await pages.fail({...plan,reservationId:id},{noNetwork:true});
    assert.deepEqual((await db.query('SELECT * FROM scout_query_coverage_epochs WHERE search_fingerprint=$1',[f.fingerprint])).rows,before);
    assert.deepEqual((await f.checkpoints.load(f.roots[0])).candidates,f.originals[0]);
    const retry=await f.pages(f.config).plan(f.roots[0]);assert.equal(retry.key,plan.key);const second=await pages.reserve(retry);assert.ok(second);
    await pages.fail({...retry,reservationId:second},{noNetwork:false,googleStatus:'UNKNOWN_ERROR'});
    assert.equal((await f.pages(f.config).plan(f.roots[0])).status,'page_provider_failure');
  });
  await t.test('legacy state/version and original page metadata are checked at reservation, not inferred from config',async()=>{
    const f=await fixture();await f.transferAll();const pages=f.pages(f.config),plan=await pages.plan(f.roots[0]);
    await db.query("UPDATE scout_query_checkpoints SET plan_version='wrong' WHERE search_fingerprint=$1",[f.fingerprint]);
    assert.equal(await pages.reserve(plan),null);
    await db.query("UPDATE scout_query_checkpoints SET plan_version=$2,state='fetched',handled_at=NULL WHERE search_fingerprint=$1",[f.fingerprint,PLAN_VERSION]);
    assert.equal(await pages.reserve(plan),null,'a reverted old handoff cannot reserve');
    await db.query("UPDATE scout_query_checkpoints SET state='handled',handled_at=now() WHERE search_fingerprint=$1",[f.fingerprint]);
    await db.query(`INSERT INTO scout_query_pages(tenant_id,search_fingerprint,query_key,root_query_key,page_index,state,reservation_id)
      VALUES($1,$2,$3,$3,0,'reserved',$4)`,[f.tenantId,f.fingerprint,f.roots[0],randomUUID()]);
    assert.equal(await pages.reserve(plan),null,'new original page metadata cannot be ignored');
    const other=await fixture();await other.transferAll();
    await db.query("UPDATE scout_query_checkpoints SET plan_version='wrong' WHERE search_fingerprint=$1",[other.fingerprint]);
    assert.equal((await other.pages(other.config).plan(other.roots[0])).status,'coverage_epoch_legacy_required');
    assert.equal((await db.query('SELECT count(*)::int n FROM scout_query_coverage_epochs WHERE search_fingerprint=$1',[other.fingerprint])).rows[0].n,0);
    assert.equal((await db.query('SELECT count(*)::int n FROM scout_query_coverage_epoch_receipts WHERE search_fingerprint=$1',[other.fingerprint])).rows[0].n,0);
    const different=createPageCheckpointStore({db,tenantId:'10',fingerprint:f.fingerprint});
    assert.equal((await different.plan(f.roots[0])).index,0,'another tenant does not inherit the epoch');
  });
  await t.test('optional-schema probing propagates permission errors and rejects either partial migration',async()=>{
    const f=await fixture();await f.transferAll();
    for (const code of ['42501','08006']) {
      const denied={query:async(sql,args)=>{
        if(sql.includes('to_regclass'))throw Object.assign(Error('schema probe failed'),{code});
        return db.query(sql,args);
      },connect:db.connect.bind(db)};
      await assert.rejects(createPageCheckpointStore({db:denied,tenantId:f.tenantId,fingerprint:f.fingerprint}).assertReady(),{code});
    }
    for (const table of ['scout_query_coverage_epochs','scout_query_coverage_epoch_receipts']) {
      await db.query(`ALTER TABLE ${table} RENAME TO ${table}_partial_fixture`);
      try { await assert.rejects(f.pages(null).assertReady(),/Incomplete Scout coverage epoch schema/); }
      finally { await db.query(`ALTER TABLE ${table}_partial_fixture RENAME TO ${table}`); }
    }
  });
});
