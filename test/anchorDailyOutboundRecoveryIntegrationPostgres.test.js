'use strict';
// Local disposable PostgreSQL only; no provider SDK or production connection.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {Pool}=require('pg');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {seedRecovery,insert}=require('./helpers/anchorRecoveryPostgresFixture');
const recovery=require('../services/anchorEmailRecovery');
const integration=require('../services/anchorEmailRecoveryIntegration');
const {GovernedOutboundStore}=require('../services/governedOutboundStore');
function guardAt(f) {
  const module={exports:{}};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../utils/callEligibility.js'),'utf8'),{module,exports:module.exports,require(id){
    if(id==='../services/anchorEmailRecovery') return {
      loadRecoveryState:(db)=>recovery.loadRecoveryState(db,{env:f.env,now:f.now}),
      assertRecoveryLeaf:(db,input)=>recovery.assertRecoveryLeaf(db,input,{env:f.env,now:f.now}),
    };
    if(id==='../services/anchorEmailRecoveryIntegration') return { ...integration, consumeRecoveryDispatch:(pool,auth)=>integration.consumeRecoveryDispatch(pool,auth,{now:()=>f.now,env:f.env}) }; 
    if(id==='../services/anchorScopedResume')return require('../services/anchorScopedResume');
    throw new Error('Unexpected dependency '+id);
  }});
  return module.exports;
}
function authorize(pool,f,dispatch,channel='email') {
  const g=guardAt(f);
  const options={context:f.input.context,command:f.input.command};
  return channel==='email'?g.withEmailAuthorization(pool,f.input.identities,dispatch,options):g.withSmsAuthorization(pool,f.input.identities,dispatch);
}
test('real PostgreSQL gated leaf, claim, replay and cap integration',{timeout:120000},async t=>{
  const pg=await startDisposablePostgres('anchor-recovery-integration-');
  const pool=new Pool({connectionString:pg.connectionString,max:12});
  t.after(async()=>{await pool.end();await pg.stop();});
  const prior=process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID;
  process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID='local-one-send-manifest';
  t.after(()=>{if(prior===undefined)delete process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID;else process.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID=prior;});
  await t.test('only exact approved email reaches stub; same attempt replay is consumed',async()=>{
    const f=await seedRecovery(pool);let calls=0;
    await authorize(pool,f,async()=>{calls++;return 'stub-accepted';});
    assert.equal(calls,1);
    assert.ok((await pool.query('SELECT dispatch_started_at FROM anchor_email_recovery_reservations')).rows[0].dispatch_started_at);
    await assert.rejects(authorize(pool,f,async()=>{calls++;}),/reservation_invalid|already_consumed/);
    assert.equal(calls,1);
  });
  await t.test('replacement manifest cannot replay the same globally consumed item',async()=>{
    const f=await seedRecovery(pool);let calls=0;
    await authorize(pool,f,async()=>{calls++;});
    const replacement={...f.manifest,id:'replacement-one-send-manifest',expiresAt:'2026-10-19T16:59:00.000Z'};
    await pool.query('UPDATE anchor_email_recovery_control SET manifest=$1',[replacement]);
    f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID=replacement.id;
    await assert.rejects(authorize(pool,f,async()=>{calls++;}),/reservation_invalid|already_consumed/);
    assert.equal(calls,1);
    await assert.rejects(pool.query(`INSERT INTO anchor_email_recovery_reservations
      (manifest_id,item_id,attempt_token,prospect_id,company_id,recipient_email)
      SELECT $1,item_id,attempt_token,prospect_id,company_id,recipient_email FROM anchor_email_recovery_reservations`,[replacement.id]),{code:'23505'});
  });
  await t.test('a fresh canary becoming uncertain blocks even scheduler own-attempt exclusion',async()=>{
    const f=await seedRecovery(pool);
    await pool.query("UPDATE acquisition_outbound_items SET status='uncertain',reason='lost_provider_response' WHERE id=$1",[f.item.id]);
    const counts=await integration.uncertaintyState(pool,'10',{env:f.env,now:f.now,programId:f.program.id,
      ownAttempt:{tenantId:'10',programId:f.program.id,envelopeId:f.envelope.id,itemId:f.item.id,attemptToken:f.attemptToken}});
    assert.equal(counts.uncertain,2);assert.equal(counts.quarantined_uncertain,1);
    assert.equal(counts.own_attempt_uncertain,0);assert.equal(counts.blocking_uncertain,1);
  });
  await t.test('lost authorization COMMIT cannot refund durable dispatch marker',async()=>{
    const f=await seedRecovery(pool);let calls=0,connections=0;
    const wrapped={async connect(){const c=await pool.connect();const authorizationConnection=connections++===0;return{
      release:c.release.bind(c),query(q,v){if(authorizationConnection&&(typeof q==='string'?q:q.text)==='COMMIT')throw Object.assign(new Error('lost COMMIT'),{code:'08006'});return c.query(q,v);}
    };}};
    await assert.rejects(authorize(wrapped,f,async()=>{calls++;}),{code:'08006'});
    await assert.rejects(authorize(pool,f,async()=>{calls++;}),/reservation_invalid|already_consumed/);
    assert.equal(calls,1);
  });
  await t.test('concurrent same-attempt leaf calls permit one provider stub total',async()=>{
    const f=await seedRecovery(pool);let calls=0;
    const outcomes=await Promise.allSettled([1,2].map(()=>authorize(pool,f,async()=>{calls++;await new Promise(r=>setTimeout(r,20));})));
    assert.equal(outcomes.filter(x=>x.status==='fulfilled').length,1);assert.equal(calls,1);
  });
  await t.test('SMS, contextless, wrong-recipient and expired manifest remain closed',async()=>{
    for(const kind of ['sms','manual','recipient','expired']){
      const f=await seedRecovery(pool);let calls=0;
      if(kind==='manual')f.input.context=undefined;
      if(kind==='recipient')f.input.command.toEmail=f.quarantine.recipient_email;
      if(kind==='expired')f.now=new Date('2026-10-19T17:00:00Z');
      await assert.rejects(authorize(pool,f,async()=>{calls++;},kind==='sms'?'sms':'email'));
      assert.equal(calls,0,kind);
    }
  });
  await t.test('NULL review and second malformed unknown prevent handoff',async()=>{
    for(const kind of ['review','unknown']){
      const f=await seedRecovery(pool);let calls=0;
      if(kind==='review')await pool.query('UPDATE prospects SET ao_outreach_review_required=NULL WHERE id=$1',[f.prospect.id]);
      else await insert(pool,'acquisition_outbound_items',{id:'malformed-unknown',tenant_id:'10',envelope_id:f.envelope.id,status:'uncertain',attempted_at:null});
      await assert.rejects(authorize(pool,f,async()=>{calls++;}));assert.equal(calls,0,kind);
    }
  });
  await t.test('durable cancellation committed first closes leaf without consuming or dispatching',async()=>{
    const f=await seedRecovery(pool);let calls=0;
    await pool.query('UPDATE anchor_email_recovery_control SET manifest=NULL');
    await assert.rejects(authorize(pool,f,async()=>{calls++;}),{code:'anchor_recovery_maintenance'});
    assert.equal(calls,0);
    assert.equal((await pool.query('SELECT dispatch_started_at FROM anchor_email_recovery_reservations')).rows[0].dispatch_started_at,null);
  });
  await t.test('handoff owns control lock before cancellation; later closure cannot recall it or retry',async()=>{
    const f=await seedRecovery(pool);let calls=0,entered,release;
    const started=new Promise(r=>{entered=r;});const gate=new Promise(r=>{release=r;});
    const dispatch=authorize(pool,f,async()=>{calls++;entered();await gate;});
    await started;
    const cancel=await pool.connect();
    try {await cancel.query('BEGIN');await cancel.query("SET LOCAL lock_timeout='75ms'");
      await assert.rejects(cancel.query('UPDATE anchor_email_recovery_control SET manifest=NULL'),{code:'55P03'});
      await cancel.query('ROLLBACK');
    } finally {cancel.release();release();}
    await dispatch;assert.equal(calls,1);
    await pool.query('UPDATE anchor_email_recovery_control SET manifest=NULL');
    await assert.rejects(authorize(pool,f,async()=>{calls++;}),{code:'anchor_recovery_maintenance'});
    assert.equal(calls,1);
    assert.ok((await pool.query('SELECT dispatch_started_at FROM anchor_email_recovery_reservations')).rows[0].dispatch_started_at);
  });
  await t.test('deadline or runtime deactivation during durable CAS consumes but does not dispatch',async()=>{
    for(const kind of ['expiry','deactivation']) {
      const f=await seedRecovery(pool);let calls=0;
      const wrapped={async connect(){const c=await pool.connect();return {release:c.release.bind(c),async query(q,v){
        const result=await c.query(q,v);
        if(/UPDATE anchor_email_recovery_reservations/.test(typeof q==='string'?q:q.text)) {
          if(kind==='expiry')f.now=new Date('2026-10-19T17:00:00Z');else delete f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID;
        }
        return result;
      }};}};
      await assert.rejects(authorize(wrapped,f,async()=>{calls++;}));assert.equal(calls,0,kind);
      assert.ok((await pool.query('SELECT dispatch_started_at FROM anchor_email_recovery_reservations')).rows[0].dispatch_started_at,kind);
    }
  });
  await t.test('active send manifest never grants normal paid preparation or another program',async()=>{
    const f=await seedRecovery(pool,{pending:true});let providers=0;
    const adapters=new Proxy({}, {get(){return async()=>{providers++;throw new Error('paid preparation forbidden');};}});
    const svc=require('../services/governedOutbound').service({pool,tenantId:'10',now:()=>f.now,adapters});
    for(const name of ['initializePreparation','resumeReservedPreparation','replenish']) {
      await assert.rejects(svc[name]({},{}),{code:'anchor_recovery_separate_preparation_approval_required'});
    }
    const result=await svc.runPreparationRefill();
    assert.equal(result.prepareSkippedReason,'anchor_recovery_separate_preparation_approval_required');
    assert.equal(providers,0);
    const counts=await svc.store.counts({...f.program,id:'unapproved-other-program'},'2026-10-19',{now:f.now});
    assert.ok(counts.blocking_uncertain>0);assert.equal(counts.quarantined_uncertain,0);
    const adapter=require('../services/governedOutboundAdapters').adapters(pool,{governedContext:{tenantId:'10',clientId:10,transport:{usesBrevoTransport:true}}});
    await assert.rejects(adapter.prepare(f.program,{mission:{stage:'ready'}},'2026-10-19',svc.store),{code:'anchor_recovery_separate_preparation_approval_required'});
  });
  await t.test('Max control and direct exported ramp/refill stop before discovery, enrichment or assignments',async()=>{
    const f=await seedRecovery(pool);let sideEffects=0;
    const blocked=async()=>{sideEffects++;throw new Error('forbidden side effect');};
    const max=require('../services/maxOutboundControlLoop');
    const result=await max.runMaxOutboundControlLoop({pool,program:f.program,tenantId:'10',now:f.now,
      source:{id:f.program.source_mission_id,payload:{structuredMission:{market:{segment:'property_manager'},geography:{region:'Manchester NH',cities:['Manchester']}}}},
      infrastructure:{cap:1,snapshot:{sentToday:0},assessed:{governor:{outcome:'proceed'}},operating:{planningDailyCapacity:1,dispatchCapacityNow:1}},
      inventory:{clean:[],excluded:[],exclusionCounts:{},scope:{}},timestamps:{},funnel:{},
      scoutRamp:blocked,refillAoBooks:blocked,runPreparationRefill:blocked});
    assert.equal(result.halted,'anchor_recovery_separate_preparation_approval_required');
    await assert.rejects(max.defaultScoutRamp({pool,program:f.program,runDiscovery:blocked,recoverExistingInventory:blocked,enrichment:blocked}),{code:'anchor_recovery_separate_preparation_approval_required'});
    await assert.rejects(max.applyPreparationRefill({pool,tenantId:'10',preparation:{prepareRequested:true},runPreparationRefill:blocked}),{code:'anchor_recovery_separate_preparation_approval_required'});
    assert.equal(sideEffects,0);
  });
  await t.test('expiry sweep preserves quarantined attempted row and exact original reason/token',async()=>{
    const f=await seedRecovery(pool,{pending:true,heldStatus:'attempted'});const store=new GovernedOutboundStore(pool,'10');
    const before=(await pool.query('SELECT * FROM acquisition_outbound_items WHERE id=$1',[f.quarantine.item_id])).rows[0];
    await store.expire('2026-10-19');
    const after=(await pool.query('SELECT * FROM acquisition_outbound_items WHERE id=$1',[f.quarantine.item_id])).rows[0];
    assert.deepEqual(after,before);
    const counts=await store.counts(f.program,'2026-10-19',{now:f.now});
    assert.equal(counts.uncertain,1);assert.equal(counts.quarantined_uncertain,1);assert.equal(counts.blocking_uncertain,0);
  });
  await t.test('claim atomically consumes cap; raw unknown stays visible and no retry reservation',async()=>{
    const f=await seedRecovery(pool,{pending:true});const store=new GovernedOutboundStore(pool,'10');
    const counts=await store.counts(f.program,'2026-10-19',{now:f.now});
    assert.equal(counts.uncertain,1);assert.equal(counts.quarantined_uncertain,1);assert.equal(counts.blocking_uncertain,0);
    const outcomes=await Promise.allSettled([1,2].map(()=>store.claim(f.item,f.program,'2026-10-19',f.now)));
    assert.equal(outcomes.filter(x=>x.status==='fulfilled').length,1);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM anchor_email_recovery_reservations')).rows[0].n,1);
    const held=(await pool.query('SELECT status,reason FROM acquisition_outbound_items WHERE id=$1',[f.quarantine.item_id])).rows[0];
    assert.deepEqual(held,{status:f.quarantine.expected_status,reason:f.quarantine.expected_reason});
    await assert.rejects(store.releaseUnsent({...f.item,id:f.quarantine.item_id,prospect_id:f.quarantine.prospect_id,company_id:f.quarantine.company_id,email:f.quarantine.recipient_email},'retry'),/quarantined/);
  });
});
