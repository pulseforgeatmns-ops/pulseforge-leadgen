'use strict';
// Real SQL against a disposable loopback instance. No runtime/provider imports.
const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { seedPrepareExisting } = require('./helpers/anchorRecoveryPrepareExistingPostgresFixture');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const prepare = require('../services/anchorEmailRecoveryPrepareExisting');
const recovery = require('../services/anchorEmailRecovery');
const options = f => ({ now: f.now, env: f.env });
async function freshCounts(pool) {
  return (await pool.query(`SELECT
    (SELECT count(*)::int FROM acquisition_outbound_envelopes WHERE local_day='2026-10-19') AS envelopes,
    (SELECT count(*)::int FROM acquisition_outbound_items WHERE tenant_id='10' AND status='pending') AS items,
    (SELECT count(*)::int FROM acquisition_outbound_events WHERE event_type='anchor_recovery_existing_prepared') AS events,
    (SELECT count(*)::int FROM anchor_email_recovery_reservations) AS reservations`)).rows[0];
}
function intercept(pool, callback) {
  return { async connect() {
    const client = await pool.connect();
    return { release: client.release.bind(client), query(q,v) { return callback(client,q,v); } };
  } };
}
test('native approved existing-artifact freeze remains closed and atomic', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('anchor-recovery-prepare-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 12 });
  t.after(async () => { await pool.end(); await pg.stop(); });
  await t.test('read-only review then exact three-row freeze never creates send authority', async () => {
    const f = await seedPrepareExisting(pool);
    const review = await prepare.reviewExistingPreparation(pool,f.config,options(f));
    assert.equal(review.ready,true,JSON.stringify(review));
    assert.deepEqual(await freshCounts(pool),{envelopes:0,items:0,events:0,reservations:0});
    const result = await prepare.prepareExistingApproved(pool,f.config,options(f));
    assert.equal(result.frozen,true); assert.equal(result.providerAuthorized,false);
    assert.deepEqual(await freshCounts(pool),{envelopes:1,items:1,events:1,reservations:0});
    const envelope = (await pool.query('SELECT * FROM acquisition_outbound_envelopes WHERE id=$1',[result.envelopeId])).rows[0];
    const item = (await pool.query('SELECT * FROM acquisition_outbound_items WHERE id=$1',[result.itemId])).rows[0];
    assert.equal(envelope.status,'frozen'); assert.equal(envelope.approval_id,null);
    assert.equal(item.status,'pending'); assert.equal(item.attempted_at,null); assert.equal(item.provider_message_id,null);
    assert.equal(item.candidate_id,f.config.candidate.companyId); assert.notEqual(item.candidate_id,item.prospect_id);
    assert.deepEqual(envelope.manifest,[item.snapshot]); assert.equal(envelope.manifest_hash,hash(envelope.manifest));
    assert.equal(item.snapshot.message.subject,f.config.candidate.subject); assert.equal(item.snapshot.message.body,f.config.candidate.body);
    const audit = (await pool.query("SELECT payload FROM acquisition_outbound_events WHERE event_type='anchor_recovery_existing_prepared'")).rows[0].payload;
    assert.equal(audit.configHash,hash(f.config)); assert.equal(audit.operatorId,f.operator.id); assert.equal(audit.operatorRole,'admin');
    assert.equal(audit.testMode,false); assert.equal(audit.executionApprovalCreated,false); assert.equal(audit.providerAuthorized,false);
    assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest,null);
    assert.equal((await recovery.loadRecoveryState(pool,options(f))).mode,'maintenance');
    assert.throws(() => recovery.validateManifest(result.proposedManifest,options(f)));
    await assert.rejects(prepare.prepareExistingApproved(pool,f.config,options(f)),{code:'anchor_prepare_existing_envelope_exists'});
    assert.deepEqual(await freshCounts(pool),{envelopes:1,items:1,events:1,reservations:0});
  });
  await t.test('concurrent identical and different grants freeze one envelope total', async () => {
    for (const distinct of [false,true]) {
      const f = await seedPrepareExisting(pool);
      const second = structuredClone(f.config); if (distinct) second.id += '-second';
      const outcomes = await Promise.allSettled([
        prepare.prepareExistingApproved(pool,f.config,options(f)),
        prepare.prepareExistingApproved(pool,second,{now:f.now,env:{...f.env,ANCHOR_EMAIL_RECOVERY_PREPARE_EXISTING_ID:second.id}}),
      ]);
      assert.equal(outcomes.filter(row=>row.status==='fulfilled').length,1);
      assert.match(outcomes.find(row=>row.status==='rejected').reason.code,/overlap|envelope_exists/);
      assert.deepEqual(await freshCounts(pool),{envelopes:1,items:1,events:1,reservations:0});
    }
  });
  await t.test('item failure and audit CAS zero both roll back earlier envelope/item inserts', async () => {
    for (const target of ['item','audit']) {
      const f = await seedPrepareExisting(pool);
      if (target==='item') await pool.query(`ALTER TABLE acquisition_outbound_items ADD CONSTRAINT synthetic_insert_failure CHECK(candidate_id <> '${f.config.candidate.candidateId}')`);
      const wrapped = intercept(pool,async(client,q,v)=>{
        const sql = typeof q==='string'?q:q.text;
        // A separate committed writer wins the event ID after our initial read.
        // The real INSERT ... ON CONFLICT then returns zero in PostgreSQL.
        if (target==='audit' && /INSERT INTO acquisition_outbound_events/.test(sql)) {
          await pool.query("INSERT INTO acquisition_outbound_events(id,tenant_id,event_type,payload) VALUES($1,'10','synthetic_conflict','{}')",[q.values[0]]);
        }
        return client.query(q,v);
      });
      await assert.rejects(prepare.prepareExistingApproved(wrapped,f.config,options(f)));
      assert.deepEqual(await freshCounts(pool),{envelopes:0,items:0,events:0,reservations:0});
      assert.equal((await pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest,null);
    }
  });
  await t.test('durable review hold and changed source artifact invalidate prior preview', async () => {
    for (const target of ['hold','artifact','operator','old-day']) {
      const f = await seedPrepareExisting(pool);
      assert.equal((await prepare.reviewExistingPreparation(pool,f.config,options(f))).ready,true);
      if (target==='hold') await pool.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1',[f.prospect.id]);
      if (target==='artifact') await pool.query("UPDATE acquisition_mission_contributions SET payload=jsonb_set(payload,'{payload,changed}', 'true') WHERE id=$1",[f.artifactRows[0].id]);
      if (target==='operator') await pool.query('UPDATE users SET active=false WHERE id=$1',[f.operator.id]);
      if (target==='old-day') await pool.query("UPDATE acquisition_missions SET updated_at='2026-10-16T15:00:00Z' WHERE id=$1",[f.missionRow.id]);
      await assert.rejects(prepare.prepareExistingApproved(pool,f.config,options(f)));
      assert.deepEqual(await freshCounts(pool),{envelopes:0,items:0,events:0,reservations:0});
    }
  });
  await t.test('locked live artifacts cannot mutate between final inspection and freeze commit', async () => {
    const f = await seedPrepareExisting(pool);
    let reached, release; const ready = new Promise(resolve=>{reached=resolve;}); const gate = new Promise(resolve=>{release=resolve;});
    const wrapped = intercept(pool,async(client,q,v)=>{
      if (/INSERT INTO acquisition_outbound_envelopes/.test(typeof q==='string'?q:q.text)) { reached(); await gate; }
      return client.query(q,v);
    });
    const freeze = prepare.prepareExistingApproved(wrapped,f.config,options(f));
    await ready;
    try {
      for (const [sql,values] of [
        ['UPDATE prospects SET ao_outreach_review_required=true WHERE id=$1',[f.prospect.id]],
        ["UPDATE acquisition_mission_contributions SET at=now() WHERE id=$1",[f.artifactRows[0].id]],
        ["UPDATE acquisition_missions SET updated_at=now() WHERE id=$1",[f.missionRow.id]],
        ["UPDATE acquisition_outbound_programs SET mode='paused' WHERE id=$1",[f.program.id]],
      ]) {
        const other = await pool.connect();
        try { await other.query('BEGIN'); await other.query("SET LOCAL lock_timeout='50ms'");
          await assert.rejects(other.query(sql,values),{code:'55P03'}); await other.query('ROLLBACK');
        } finally { other.release(); }
      }
    } finally { release(); }
    await freeze;
    assert.deepEqual(await freshCounts(pool),{envelopes:1,items:1,events:1,reservations:0});
  });
});
