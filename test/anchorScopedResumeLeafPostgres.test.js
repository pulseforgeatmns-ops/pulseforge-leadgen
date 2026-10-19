'use strict';
// SPEC-262: committed changes after claim must be observed again at the leaf.
// Local PostgreSQL only. No transport/provider imports or credentials.
const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { seedScopedResume } = require('./helpers/anchorScopedResumePostgresFixture');
const control = require('../services/anchorScopedResumeControl');
const recovery = require('../services/anchorEmailRecovery');
const { GovernedOutboundStore } = require('../services/governedOutboundStore');

test('native scoped leaf rechecks current governed suppression after claim', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('anchor-scoped-late-suppression-');
  const pool = new Pool({ connectionString: pg.connectionString, max: 8 });
  const previous = process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;
  process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = 'local-scoped-resume';
  t.after(async () => {
    if (previous === undefined) delete process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID;
    else process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = previous;
    await pool.end(); await pg.stop();
  });
  async function setup() {
    const f = await seedScopedResume(pool);
    await control.installScopedResume(pool, { manifest: f.manifest, approvalReference: f.manifest.approvalReference }, f.actor, { now: f.now, env: f.env });
    await new GovernedOutboundStore(pool, '10').claim(f.item, f.program, '2026-10-19', f.now);
    return f;
  }
  async function finalLeaf(f, dispatch) {
    const db = await pool.connect();
    try {
      await db.query('BEGIN');
      await recovery.assertRecoveryLeaf(db, f.input, { now: f.now, env: f.env });
      await dispatch();
      await db.query('COMMIT');
    } catch (error) { await db.query('ROLLBACK'); throw error; }
    finally { db.release(); }
  }
  await t.test('unchanged eligible item passes existing predicate', async () => {
    const f = await setup(); let fakeHandoffs = 0;
    await finalLeaf(f, async () => { fakeHandoffs++; });
    assert.equal(fakeHandoffs, 1);
  });
  const mutations = [
    ['AO task', f => pool.query('INSERT INTO ao_prospect_tasks(id,client_id,prospect_id) VALUES($1,10,$2)', ['late-task', f.prospect.id])],
    ['AO lead', f => pool.query('INSERT INTO ao_leads(id,client_id,crm_prospect_id,business_name) VALUES($1,10,$2,$3)', ['late-lead', f.prospect.id, f.companies[0].name])],
    ['inbound reply', f => pool.query('INSERT INTO touchpoints(id,client_id,prospect_id,action_type) VALUES($1,10,$2,$3)', ['late-reply', f.prospect.id, 'inbound_reply'])],
    ['prior outbound touch', f => pool.query('INSERT INTO touchpoints(id,client_id,prospect_id,action_type) VALUES($1,10,$2,$3)', ['late-touch', f.prospect.id, 'email_sent'])],
    ['lifecycle hold', f => pool.query('INSERT INTO acquisition_outbound_lifecycle(tenant_id,email,company_id,suppressed,state) VALUES($1,$2,$3,true,$4)', ['10', f.prospect.email, f.prospect.company_id, 'human_owned'])],
    ['AO assignment', f => pool.query('UPDATE prospects SET assigned_ao_id=1 WHERE id=$1', [f.prospect.id])],
    ['closer assignment', f => pool.query('UPDATE prospects SET closer_id=1 WHERE id=$1', [f.prospect.id])],
    ['contact timestamp', f => pool.query('UPDATE prospects SET last_contacted_at=now() WHERE id=$1', [f.prospect.id])],
    ['reply timestamp', f => pool.query('UPDATE prospects SET last_reply_at=now() WHERE id=$1', [f.prospect.id])],
    ['operational status', f => pool.query("UPDATE prospects SET operational_status='replied' WHERE id=$1", [f.prospect.id])],
    ['setter status', f => pool.query("UPDATE prospects SET setter_status='booked' WHERE id=$1", [f.prospect.id])],
    ['closer status', f => pool.query("UPDATE prospects SET closer_status='proposal_sent' WHERE id=$1", [f.prospect.id])],
  ];
  for (const [label, mutate] of mutations) await t.test(`${label} committed after claim blocks fake handoff`, async () => {
    const f = await setup(); let fakeHandoffs = 0;
    await mutate(f);
    await assert.rejects(finalLeaf(f, async () => { fakeHandoffs++; }), error =>
      ['anchor_scoped_resume_identity_changed', 'anchor_scoped_resume_live_suppression'].includes(error.code));
    assert.equal(fakeHandoffs, 0);
    const reservation = (await pool.query('SELECT dispatch_started_at FROM anchor_email_recovery_reservations WHERE item_id=$1', [f.item.id])).rows[0];
    assert.equal(reservation.dispatch_started_at, null);
  });
});
