'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { service } = require('../services/governedOutbound');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { captureRaw, classifyPending } = require('../services/governedOutboundReplies');

test('governed daily outbound on disposable PostgreSQL', { skip: process.env.ANCHOR_OUTBOUND_TEST_POSTGRES !== 'true', timeout: 90000 }, async t => {
  const pg = await startDisposablePostgres('anchor-batch-');
  const pool = new Pool({ connectionString: pg.connectionString });
  t.after(async () => { await pool.end(); await pg.stop(); });
  await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
    CREATE TABLE acquisition_missions(id TEXT PRIMARY KEY);
    CREATE TABLE clients(id INT PRIMARY KEY);
    CREATE TABLE users(id INT PRIMARY KEY,client_id INT,active BOOLEAN);
    CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),client_id INT,name TEXT);
    CREATE TABLE prospects(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),company_id UUID,client_id INT,email TEXT,do_not_contact BOOLEAN DEFAULT false,setter_status TEXT,closer_status TEXT);
    CREATE TABLE touchpoints(id SERIAL PRIMARY KEY,prospect_id UUID,client_id INT,action_type TEXT);
    CREATE TABLE agent_actions(id SERIAL PRIMARY KEY,created_by TEXT,action_type TEXT,title TEXT,description TEXT,payload JSONB,status TEXT,client_id INT);
    CREATE TABLE agent_log(id SERIAL PRIMARY KEY,client_id INT,agent_name TEXT,action TEXT,ran_at TIMESTAMPTZ);
    CREATE TABLE ao_leads(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),client_id INT,business_name TEXT,ao_owner_id INT,crm_prospect_id UUID,attribution_source TEXT,original_visit_note TEXT);
    CREATE TABLE ao_follow_up_tasks(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),lead_id UUID,ao_owner_id INT,due_date DATE,next_action TEXT,last_interaction_summary TEXT,status TEXT DEFAULT 'open');
    CREATE TABLE acquisition_mission_outbound_executions(id TEXT PRIMARY KEY,tenant_id TEXT,mission_id TEXT,prospect_id TEXT,status TEXT,payload JSONB,attempted_at TIMESTAMPTZ,prepared_artifact_revision TEXT,provider_message_id TEXT,sent_at TIMESTAMPTZ,updated_at TIMESTAMPTZ);
    CREATE TABLE acquisition_mission_provider_events(id TEXT PRIMARY KEY,tenant_id TEXT,prospect_id TEXT,event_type TEXT,execution_record_id TEXT);
    CREATE TABLE tenant_outreach_messages(id TEXT PRIMARY KEY,tenant_id TEXT,prospect_id TEXT,direction TEXT,sender JSONB,status TEXT,sent_at TIMESTAMPTZ);
    CREATE TABLE tenant_outreach_scheduled_sends(id TEXT PRIMARY KEY,tenant_id TEXT,prospect_id TEXT,recipient_email TEXT,status TEXT,skip_reason TEXT,cancelled_at TIMESTAMPTZ);
    INSERT INTO users VALUES(7,10,true);
    INSERT INTO acquisition_missions VALUES('source'),('daily');`);
  const migration = fs.readFileSync(path.join(__dirname, '../migrations/2026-09-18-anchor-daily-outbound.sql'), 'utf8');
  await pool.query(migration);
  await pool.query(migration); // deploy/replay safe
  const source = { id: 'source', tenantId: '10', objective: 'Anchor approved scope', structuredMission: { immutable: true }, stage: 'execute' };
  let clock = new Date('2026-09-18T14:00:00Z');
  let prepared;
  let calls;
  let fault;
  let liveHook;
  let enabled;
  const actor = { id: 'jake', role: 'admin' };
  let contacts;
  let svc;
  let program;
  async function reset() {
    await pool.query(`TRUNCATE acquisition_outbound_events,acquisition_outbound_replies,acquisition_outbound_lifecycle,
      acquisition_outbound_items,acquisition_outbound_envelopes,acquisition_outbound_preparation,acquisition_outbound_programs,
      acquisition_outbound_inbox_health,
      ao_follow_up_tasks,ao_leads,agent_actions,agent_log,touchpoints,tenant_outreach_messages,tenant_outreach_scheduled_sends,
      acquisition_mission_provider_events,acquisition_mission_outbound_executions,prospects,companies CASCADE`);
    clock = new Date('2026-09-18T14:00:00Z'); calls = 0; fault = null; liveHook = async () => {}; enabled = true;
    contacts = new Map();
    const candidates = [];
    for (let i=0; i<7; i++) {
      const company = (await pool.query('INSERT INTO companies(client_id,name) VALUES(10,$1) RETURNING id', [`Company ${i}`])).rows[0].id;
      const crm = (await pool.query('INSERT INTO prospects(company_id,client_id,email) VALUES($1,10,$2) RETURNING *', [company, `ops${i}@customer.example`])).rows[0];
      contacts.set(`c${i}`, { ...crm, prospect_id: crm.id, email_verified: true, email_status: 'valid', email_provenance_source: 'website_email' });
      candidates.push({ candidateId: `c${i}`, item: { email: crm.email, sendable: true, paige: { candidateId: `c${i}` } },
        message: { subject: `Cleaning ${i}`, body: 'Would a written quote help?', candidateId: `c${i}` } });
    }
    prepared = { candidates, revision: 'revision1', capacity: 5, sender: { senderEmail: 'sender@anchor.example' } };
    const adapters = {
      loadMission: async id => ({ mission: { ...source, id, stage: id==='source' ? 'execute' : 'ready' } }),
      validateTenant: async () => {}, contact: async id => contacts.get(id),
      prepare: async () => ({ mission: { ...source, id: 'daily', stage: 'ready' } }), prepared: async () => structuredClone(prepared),
      approve: async (_snapshot, _program, binding) => ({ id: 'approval1', payload: { dailyEnvelope: binding } }),
      liveGate: async (...args) => liveHook(...args),
      send: async () => { calls++; if (fault) throw new Error(fault); return { success: true, providerMessageId: `provider-${calls}` }; },
      execute: async (_envelope, item, _program, send) => {
        const command = { toEmail: item.email, subject: item.snapshot.message.subject,
          body: item.snapshot.message.body, sender: { email: item.snapshot.sender.senderEmail } };
        await send.beforeAttempt(command);
        return send(command);
      },
    };
    svc = service({ pool, adapters, now: () => clock, enabled: () => enabled });
    const input = { sourceMissionId: 'source', senderEmail: 'sender@anchor.example', inboxIntegrationId: 'mailbox', aoOwnerIds: [7],
      startsAt: '2026-09-18T00:00:00Z', expiresAt: '2026-10-01T00:00:00Z' };
    const review = await svc.authorize(input, actor);
    assert.equal(review.reviewRequired, true);
    assert.equal(await svc.store.program(), null);
    program = await svc.authorize({ ...review.policy, reviewHash: review.reviewHash }, actor);
    assert.equal(program.scope_hash, hash(missionScope(source)));
  }
  async function activate() { await svc.setMode(program.id, 'active', program.policy_hash, actor); }
  async function ageAttempts() {
    await pool.query("UPDATE acquisition_outbound_items SET attempted_at=now()-interval '61 minutes' WHERE attempted_at IS NOT NULL");
  }
  await t.test('shadow freezes exactly five, creates no execution approval and never invokes transport', async () => {
    await reset(); const result = await svc.tick();
    assert.equal(result.planned, 5); assert.equal(result.sent, 0); assert.equal(calls, 0);
    assert.equal((await svc.store.envelope('2026-09-18')).approval_id, null);
    assert.equal((await svc.tick()).envelopeId, result.envelopeId);
  });
  await t.test('one bounded grant runs five spaced sends; concurrent ticks, restart, cap and replay cannot send six', async () => {
    await reset(); await activate();
    const outcomes = await Promise.all([svc.tick(), svc.tick(), svc.tick()]);
    assert.equal(calls, 1); assert.ok(outcomes.some(x => x.halted === 'overlap'));
    assert.equal((await svc.tick()).halted, 'spacing');
    for (let i=0; i<4; i++) { await ageAttempts(); assert.equal((await svc.tick()).sent, 1); }
    assert.equal(calls, 5); assert.equal((await svc.tick()).halted, 'cap_reached');
    assert.equal((await svc.store.items((await svc.store.envelope('2026-09-18')).id)).filter(x => x.status==='sent').length, 5);
    clock = new Date('2026-09-19T14:00:00Z'); assert.equal((await svc.tick()).halted, 'weekend'); assert.equal(calls, 5);
  });
  await t.test('ambiguous provider failure is durable and blocks retries; explicit reconciliation never resends', async () => {
    await reset(); await activate(); fault = 'timeout after acceptance';
    await svc.tick(); assert.equal(calls, 1);
    const item = (await svc.store.items((await svc.store.envelope('2026-09-18')).id)).find(x => x.status==='uncertain');
    assert.ok(item); fault = null;
    assert.equal((await svc.tick()).halted, 'uncertain_send_requires_reconciliation'); assert.equal(calls, 1);
    await svc.reconcile(item.id, 'accepted', 'recovered-id', 'Provider activity export confirms acceptance', actor);
    assert.equal((await svc.store.items(item.envelope_id)).find(x => x.id===item.id).provider_message_id, 'recovered-id');
    assert.equal(calls, 1);
  });
  await t.test('reply capture suppresses immediately even before Riley fails; replay creates one reply', async () => {
    await reset(); await svc.tick(); await activate();
    const integration = { tenantId: '10', id: 'mailbox' };
    const raw = { messageId: '<reply1>', from: 'ops0@customer.example', body: 'Please quote this', subject: 'Re: Cleaning' };
    await captureRaw(pool, integration, raw); await captureRaw(pool, integration, raw);
    const envelope = await svc.store.envelope('2026-09-18');
    assert.equal((await svc.store.items(envelope.id))[0].status, 'suppressed');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM acquisition_outbound_replies')).rows[0].n, 1);
    await classifyPending(pool, { classify: async () => { throw new Error('LLM unavailable'); } });
    assert.equal((await svc.store.items(envelope.id))[0].status, 'suppressed');
    await classifyPending(pool, { classify: async () => ({ classification: 'quote_request', reason: 'asks for a quote' }) });
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM ao_follow_up_tasks')).rows[0].n, 1);
    await classifyPending(pool, { classify: async () => { throw new Error('must not classify replay'); } });
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM agent_actions')).rows[0].n, 1);
    await captureRaw(pool, integration, raw);
    assert.equal((await pool.query('SELECT classification FROM acquisition_outbound_lifecycle WHERE email=$1', [raw.from])).rows[0].classification, 'quote_request');
  });
  await t.test('AO database activity and provider stop signals cancel scheduled and pending items, tenant scoped', async () => {
    await reset(); await svc.tick();
    const crm = contacts.get('c0');
    await pool.query("INSERT INTO tenant_outreach_scheduled_sends VALUES('a','10',$1,$2,'SCHEDULED',null,null),('other','11',$1,$2,'SCHEDULED',null,null)", [crm.id, crm.email]);
    await pool.query("INSERT INTO ao_leads(client_id,business_name,ao_owner_id,crm_prospect_id) VALUES(10,'Company 0',7,$1)", [crm.id]);
    const schedules = (await pool.query('SELECT * FROM tenant_outreach_scheduled_sends ORDER BY id')).rows;
    assert.equal(schedules[0].status, 'CANCELLED'); assert.equal(schedules[1].status, 'SCHEDULED');
    const env = await svc.store.envelope('2026-09-18'); assert.equal((await svc.store.items(env.id))[0].status, 'suppressed');
    await pool.query("INSERT INTO acquisition_mission_outbound_executions(id,tenant_id,prospect_id,payload) VALUES('e','10','c1',$1)", [{ email: contacts.get('c1').email }]);
    await pool.query("INSERT INTO acquisition_mission_provider_events VALUES('p','10','c1','hard_bounce','e')");
    assert.equal((await svc.store.items(env.id))[1].status, 'suppressed');
    await pool.query("INSERT INTO ao_leads(client_id,business_name,ao_owner_id) VALUES(10,'Company 2',7)");
    assert.equal((await svc.store.items(env.id))[2].status, 'suppressed');
    assert.ok((await pool.query("SELECT * FROM acquisition_outbound_learning_facts WHERE event_type='ao_activity'")).rows.length >= 2);
    await pool.query("UPDATE prospects SET closer_status='proposal_sent' WHERE id=$1", [contacts.get('c3').id]);
    assert.equal((await pool.query('SELECT state FROM acquisition_outbound_lifecycle WHERE email=$1', [contacts.get('c3').email])).rows[0].state, 'proposal_pending');
    await pool.query("UPDATE prospects SET closer_status='closed_won' WHERE id=$1", [contacts.get('c3').id]);
    assert.equal((await pool.query('SELECT state FROM acquisition_outbound_lifecycle WHERE email=$1', [contacts.get('c3').email])).rows[0].state, 'converted');
  });
  await t.test('changed revision, DNC, unsafe copy, stale inbox, disabled environment and kill during readiness send nothing', async () => {
    await reset(); await svc.tick(); await activate(); prepared.revision = 'new-revision';
    assert.equal((await svc.tick()).halted, 'artifacts_changed'); assert.equal(calls, 0);
    await reset(); await svc.tick(); await activate(); contacts.get('c0').do_not_contact = true;
    assert.equal((await svc.tick()).halted, 'do_not_contact'); assert.equal(calls, 0);
    await reset(); await svc.tick(); await activate(); prepared.candidates[0].message.body = 'Mission focus: leakage';
    assert.equal((await svc.tick()).halted, 'copy_changed'); assert.equal(calls, 0);
    await reset(); await activate(); liveHook = async () => { throw Object.assign(new Error('stale'), { code: 'reply_poll_stale' }); };
    assert.equal((await svc.tick()).halted, 'reply_poll_stale'); assert.equal(calls, 0);
    await reset(); await activate(); enabled = false;
    assert.equal((await svc.tick()).halted, 'environment_kill_switch'); assert.equal(calls, 0);
    await reset(); await activate(); liveHook = () => svc.setMode(program.id, 'paused', null, actor);
    await svc.tick(); assert.equal(calls, 0); assert.equal((await svc.store.program()).mode, 'paused');
  });
  await t.test('item tampering and CRM reassignment cannot change a frozen recipient', async () => {
    await reset(); await svc.tick(); await activate();
    const env = await svc.store.envelope('2026-09-18');
    const item = (await svc.store.items(env.id))[0];
    await pool.query("UPDATE acquisition_outbound_items SET snapshot=jsonb_set(snapshot,'{message,body}','\"tampered\"') WHERE id=$1", [item.id]);
    assert.equal((await svc.tick()).halted, 'item_manifest_mismatch'); assert.equal(calls, 0);
    await reset(); await svc.tick(); await activate(); contacts.get('c0').prospect_id = contacts.get('c1').prospect_id;
    assert.equal((await svc.tick()).halted, 'crm_binding_changed'); assert.equal(calls, 0);
  });
  await t.test('Riley failures create one attention card and revoked grants still capture replies', async () => {
    await reset(); await svc.tick(); await svc.setMode(program.id, 'revoked', null, actor);
    await captureRaw(pool, { tenantId: '10', id: 'mailbox' }, { messageId: 'late-reply', from: contacts.get('c0').email, body: 'Stop' });
    for (let n=0;n<4;n++) await classifyPending(pool, { classify: async () => { throw new Error('offline'); } });
    assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_replies')).rows[0].attempts, 3);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM agent_actions')).rows[0].n, 1);
    assert.equal((await pool.query('SELECT suppressed FROM acquisition_outbound_lifecycle')).rows[0].suppressed, true);
  });
  await t.test('legacy mailbox scheduler cannot bypass an existing program', async () => {
    await reset();
    const { sendTenantEmail, PostgresTenantMailboxStore } = require('../services/tenantMailbox');
    await assert.rejects(sendTenantEmail({ tenantId: '10', metadata: { scheduleId: 'old-schedule' } },
      { store: new PostgresTenantMailboxStore(pool) }), { code: 'governed_executor_required' });
    assert.equal(calls, 0);
  });
  await t.test('capacity and spacing include canonical, legacy and mailbox sends before activation', async () => {
    await reset();
    await pool.query("INSERT INTO agent_log(client_id,agent_name,action,ran_at) VALUES(10,'emmett','email_sent',now()),(11,'emmett','email_sent',now())");
    await pool.query("INSERT INTO tenant_outreach_messages(id,tenant_id,direction,status,sent_at) VALUES('m1','10','outbound','sent',now())");
    await pool.query("INSERT INTO acquisition_mission_outbound_executions(id,tenant_id,prospect_id,status,prepared_artifact_revision,attempted_at) VALUES('e1','10','c5','sent','old',now())");
    const history = await require('../services/governedOutboundAdapters').readOutboundHistory(pool);
    assert.equal(history.today, 3);
    assert.ok(+new Date(history.last_attempt)>Date.now()-10000);
  });
  await t.test('missed days expire without catch-up; permanent recipient/company ledger survives new days', async () => {
    await reset(); await activate(); await svc.tick(); assert.equal(calls, 1);
    await ageAttempts(); clock = new Date('2026-09-21T14:00:00Z');
    await svc.tick(); assert.equal(calls, 2);
    const oldItems = await svc.store.items((await svc.store.envelope('2026-09-18')).id);
    assert.equal(oldItems.filter(x => x.status==='expired').length, 4);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM acquisition_outbound_learning_facts WHERE event_type='send_expired'")).rows[0].n, 4);
    const today = await svc.store.items((await svc.store.envelope('2026-09-21')).id);
    assert.ok(today.every(x => x.email !== 'ops0@customer.example'));
  });
});
