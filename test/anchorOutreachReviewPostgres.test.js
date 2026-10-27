'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const hold = require('../utils/anchorOutreachReview');

async function database() {
  // Optional PostgreSQL/WASM fallback for restricted local runtimes. CI keeps
  // the existing native disposable PostgreSQL harness as its default.
  if (process.env.ANCHOR_REVIEW_PGLITE_MODULE) {
    const { PGlite } = require(process.env.ANCHOR_REVIEW_PGLITE_MODULE);
    const engine = new PGlite();
    const query = (text, params) => engine.query(text, params);
    return { kind: 'PostgreSQL/WASM', query, exec: text => engine.exec(text),
      connect: async () => ({ query, release() {} }), close: () => engine.close() };
  }
  const postgres = await startDisposablePostgres('anchor-review-');
  const pool = new Pool({ connectionString: postgres.connectionString });
  return { kind: 'native PostgreSQL', query: pool.query.bind(pool), exec: pool.query.bind(pool),
    connect: pool.connect.bind(pool), close: async () => { await pool.end(); await postgres.stop(); } };
}

test('review holds exclude queue admission in actual PostgreSQL queries without changing ownership', { timeout: 180000 }, async t => {
  const db = await database();
  t.diagnostic(db.kind);
  const ids = ['00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002',
    '00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000004'];
  const company = '00000000-0000-0000-0000-000000000010';
  try {
    await db.exec(`
      CREATE TABLE clients(id integer PRIMARY KEY,name text);
      CREATE TABLE users(id integer PRIMARY KEY,client_id integer NOT NULL REFERENCES clients(id),
        name text,email text,role text,active boolean,territory text,ao_operational_status text DEFAULT 'active');
      CREATE TABLE companies(id uuid PRIMARY KEY,client_id integer NOT NULL REFERENCES clients(id),
        name text,location text,website text,industry text);
      CREATE TABLE prospects(id uuid PRIMARY KEY,client_id integer NOT NULL REFERENCES clients(id),company_id uuid REFERENCES companies(id),
        email text,phone text,vertical text,icp_score integer,status text,service_area_match text,
        do_not_contact boolean DEFAULT false,ao_outreach_review_required boolean DEFAULT false,
        ao_call_suppressed boolean DEFAULT false,disposition_status text DEFAULT 'active',is_hot boolean DEFAULT false,
        first_name text,last_name text,next_action_due_at timestamptz,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now(),
        setter_visible boolean DEFAULT false,setter_visibility_reason text,setter_updated_at timestamptz);
      CREATE TABLE touchpoints(id bigserial PRIMARY KEY,client_id integer,prospect_id uuid,
        action_type text,channel text,outcome text,created_at timestamptz DEFAULT now());
      INSERT INTO clients VALUES(10,'Offline Anchor'),(13,'Offline Other');
      INSERT INTO users(id,client_id,name,email,role,active) VALUES(7,10,'Offline AO','ao@example.test','ao',true);
    `);
    await db.exec(fs.readFileSync(path.join(__dirname,'../migrations/2026-09-22-ao-prospect-routing.sql'),'utf8'));
    await db.query('INSERT INTO companies(id,client_id,name,location) VALUES($1,10,$2,$3)', [company,'Offline Company','Manchester, NH']);
    const heldCompany = '00000000-0000-0000-0000-000000000011';
    const unknownCompany = '00000000-0000-0000-0000-000000000012';
    await db.query('INSERT INTO companies(id,client_id,name) VALUES($1,10,$2),($3,10,$4)',[heldCompany,'Offline Held Company',unknownCompany,'Offline Unknown Company']);
    for (const [index, value] of [false,true,null,null].entries()) {
      await db.query(`INSERT INTO prospects(id,client_id,company_id,status,icp_score,ao_outreach_review_required)
        VALUES($1,$2,$3,'cold',90,$4)`,[ids[index],index===3?13:10,[company,heldCompany,unknownCompany,null][index],value]);
    }
    const selected = await db.query(`SELECT id::text FROM prospects p WHERE ${hold.anchorOutreachReviewSql('p')} ORDER BY id`);
    assert.deepEqual(selected.rows.map(r=>r.id),[ids[0],ids[3]]);
    const projected = await db.query(`SELECT id::text, ${hold.anchorOutreachReviewValueSql('p')} AS review FROM prospects p ORDER BY id`);
    assert.deepEqual(projected.rows.map(r=>r.review),[false,true,null,false], 'Anchor NULL is preserved; other-tenant projection keeps its legacy false default');

    const service = require('../services/aoProspectTaskService');
    const routing = {recommended_ao_id:7,recommended_motion:'AO_LED',assignment_category:'HIGH_VALUE_ICP',
      recommended_first_action:'Offline fixture action',required_log_fields:[],
      reasoning:{account:'Offline Company',segment:'property_manager',location:'Manchester, NH',priority:'high'}};
    const created = await service.createTaskFromRouting({clientId:10,prospectId:ids[0],routing,db});
    assert.ok(created.task.id);
    assert.equal((await service.listOpenTasks({clientId:10,db})).length,1);

    await db.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1',[ids[0]]);
    assert.equal((await service.listOpenTasks({clientId:10,db})).length,0,'a previously queued account disappears from active tasks once held');
    const future = '00000000-0000-0000-0000-000000000005';
    await db.query(`INSERT INTO prospects(id,client_id,company_id,status,icp_score,service_area_match,ao_outreach_review_required,setter_visible,setter_visibility_reason)
      VALUES($1,10,$2,'cold',90,'Manchester, NH',false,true,'manual')`,[future,company]);
    const futureState = await db.query(`SELECT ${hold.anchorOutreachReviewValueSql('p')} AS review FROM prospects p WHERE id=$1`,[future]);
    assert.equal(futureState.rows[0].review,true,'a newly enriched contact inherits the held company boundary');
    await assert.rejects(hold.withAnchorOutreachReview(db,future,10,()=>assert.fail('held sibling reached external action')),{code:'outreach_review_required'});
    await assert.rejects(service.createTaskFromRouting({clientId:10,prospectId:future,routing,db}),{code:'outreach_review_required'});
    await require('../utils/setterVisibility').recomputeSetterVisibility(db,{clientId:10,preserveOverrides:true});
    const setter = await db.query('SELECT setter_visible,setter_visibility_reason FROM prospects WHERE id=$1',[future]);
    assert.equal(setter.rows[0].setter_visible,false,'manual visibility cannot override a held sibling');
    assert.equal(setter.rows[0].setter_visibility_reason,'hard_exclusion');
    // Releasing one contact cannot release the other contact's explicit hold.
    await db.query('UPDATE prospects SET ao_outreach_review_required=false WHERE id=$1',[future]);
    assert.equal((await db.query(`SELECT id FROM prospects p WHERE id=$1 AND ${hold.anchorOutreachReviewSql('p')}`,[future])).rows.length,0);
    for (const id of ids.slice(0,3)) {
      await assert.rejects(service.createTaskFromRouting({clientId:10,prospectId:id,routing,db}),{code:'outreach_review_required'});
      assert.equal(await service.persistRouting(id,10,routing,db),false);
    }
    assert.equal((await db.query('SELECT count(*)::int AS n FROM ao_prospect_tasks')).rows[0].n,1);
    const owners = await db.query('SELECT assigned_ao_id,do_not_contact,prospect_motion FROM prospects WHERE client_id=10');
    assert.ok(owners.rows.every(r=>r.assigned_ao_id===null && r.do_not_contact===false && r.prospect_motion===null));

    // Exercise the future inspection safeguard only against this disposable DB.
    await db.query('BEGIN TRANSACTION READ ONLY');
    assert.equal((await db.query("SHOW transaction_read_only")).rows[0].transaction_read_only,'on');
    await assert.rejects(db.query("UPDATE prospects SET status='warm'"),{code:'25006'});
    await db.query('ROLLBACK');
    assert.ok((await db.query('SELECT status FROM prospects')).rows.every(r=>r.status==='cold'));

    await db.query(`INSERT INTO touchpoints(client_id,prospect_id,action_type,channel,outcome)
      VALUES(10,$1,'email_sent','email','sent')`,[ids[0]]);
    const proposal={records:Array.from({length:19},(_,i)=>({review_key:`offline-${i}`,
      identity:{name:i===0?'Offline Company':`Offline Unmatched ${i}`},eligibility:{cold_cohort_status:'POTENTIAL_ONLY_NOT_CLEARED'}}))};
    const report=await require('../scripts/anchorIcpReadOnlyReview').inspect(db,proposal);
    assert.equal(report.readOnlyVerified,true);
    assert.equal(report.productionWrites,0);
    assert.equal(report.candidates.length,19);
    assert.equal(report.candidates[0].companies.length,1);
    assert.ok(report.candidates[0].evidence.find(e=>e.source==='touchpoints').records.some(e=>e.action_type==='email_sent'));
    assert.ok(report.candidates.every(c=>c.eligibleForImport===false && c.executableWrites.length===0));
    assert.ok(report.limitations.includes('UNVERIFIED_SOURCE_COVERAGE:ao_leads'));
  } finally { await db.close(); }
});

test('native PostgreSQL keeps a review hold and new sibling insert behind the in-flight handoff', { timeout: 180000 }, async t => {
  if (process.env.ANCHOR_REVIEW_PGLITE_MODULE) {
    t.skip('Native multi-connection locking runs in the required CI job; PGlite cannot exercise it');
    return;
  }
  const db = await database();
  const company = '00000000-0000-0000-0000-000000000020';
  const target = '00000000-0000-0000-0000-000000000021';
  const future = '00000000-0000-0000-0000-000000000022';
  let writer, pending, releaseAction;
  try {
    await db.exec(`CREATE TABLE companies(id uuid PRIMARY KEY,client_id integer NOT NULL);
      CREATE TABLE prospects(id uuid PRIMARY KEY,client_id integer NOT NULL,company_id uuid REFERENCES companies(id),
        ao_outreach_review_required boolean,assigned_ao_id integer,do_not_contact boolean DEFAULT false);`);
    await db.query('INSERT INTO companies VALUES($1,10)',[company]);
    await db.query('INSERT INTO prospects(id,client_id,company_id,ao_outreach_review_required,assigned_ao_id) VALUES($1,10,$2,false,24)',[target,company]);
    let entered;
    const enteredAction = new Promise(resolve => { entered=resolve; });
    const actionGate = new Promise(resolve => { releaseAction=resolve; });
    let handoffs=0;
    pending=hold.withAnchorOutreachReview(db,target,10,async()=>{
      entered();await actionGate;handoffs++;
    });
    await Promise.race([enteredAction,pending.then(()=>assert.fail('Handoff finished before the action gate'))]);
    writer=await db.connect();
    // Real lock timeouts, rather than sleeps, prove the conflicting writes
    // cannot commit while the external handoff owns its review snapshot.
    for(const [sql,values] of [
      ['UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1',[target]],
      ['INSERT INTO prospects(id,client_id,company_id,ao_outreach_review_required) VALUES($1,10,$2,false)',[future,company]],
    ]){
      await writer.query('BEGIN');
      await writer.query("SET LOCAL lock_timeout='250ms'");
      await assert.rejects(writer.query(sql,values),{code:'55P03'});
      await writer.query('ROLLBACK');
    }
    releaseAction();await pending;pending=null;
    assert.equal(handoffs,1);
    await writer.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1',[target]);
    await writer.query('INSERT INTO prospects(id,client_id,company_id,ao_outreach_review_required) VALUES($1,10,$2,false)',[future,company]);
    for(const id of [target,future])await assert.rejects(hold.withAnchorOutreachReview(db,id,10,()=>assert.fail('Held company reached another handoff')),{code:'outreach_review_required'});
    const preserved=await db.query('SELECT assigned_ao_id,do_not_contact FROM prospects WHERE id=$1',[target]);
    assert.equal(preserved.rows[0].assigned_ao_id,24);
    assert.equal(preserved.rows[0].do_not_contact,false);
  } finally {
    releaseAction?.();
    if(pending)await Promise.allSettled([pending]);
    if(writer){try{await writer.query('ROLLBACK');}finally{writer.release();}}
    await db.close();
  }
});
