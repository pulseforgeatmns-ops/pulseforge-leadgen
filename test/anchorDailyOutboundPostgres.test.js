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
    CREATE TABLE acquisition_missions(id TEXT PRIMARY KEY,tenant_id TEXT,stage TEXT,status TEXT,objective TEXT,target_segment TEXT,payload JSONB);
    CREATE TABLE acquisition_mission_contributions(id TEXT PRIMARY KEY, tenant_id TEXT, mission_id TEXT, payload JSONB);
    CREATE TABLE clients(id INT PRIMARY KEY,active BOOLEAN DEFAULT true,autosend_enabled BOOLEAN DEFAULT false);
    CREATE TABLE users(id INT PRIMARY KEY,client_id INT,active BOOLEAN);
    CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),client_id INT,name TEXT,domain TEXT,website TEXT);
    CREATE TABLE prospects(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),company_id UUID,client_id INT,email TEXT,do_not_contact BOOLEAN DEFAULT false,setter_status TEXT,closer_status TEXT,assigned_ao_id INT,last_contacted_at TIMESTAMPTZ,closer_id INT,last_reply_at TIMESTAMPTZ);
    CREATE TABLE ao_prospect_tasks(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),client_id INT,prospect_id UUID,assigned_ao_id INT);
    CREATE TABLE touchpoints(id SERIAL PRIMARY KEY,prospect_id UUID,client_id INT,action_type TEXT);
    CREATE TABLE agent_actions(id SERIAL PRIMARY KEY,created_by TEXT,action_type TEXT,title TEXT,description TEXT,payload JSONB,status TEXT,client_id INT);
    CREATE TABLE agent_log(id SERIAL PRIMARY KEY,client_id INT,agent_name TEXT,action TEXT,ran_at TIMESTAMPTZ);
    CREATE TABLE ao_leads(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),client_id INT,business_name TEXT,ao_owner_id INT,crm_prospect_id UUID,attribution_source TEXT,original_visit_note TEXT);
    CREATE TABLE ao_contacts(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),lead_id UUID,email TEXT);
    CREATE TABLE ao_follow_up_tasks(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),lead_id UUID,ao_owner_id INT,due_date DATE,next_action TEXT,last_interaction_summary TEXT,status TEXT DEFAULT 'open');
    CREATE TABLE acquisition_mission_outbound_executions(id TEXT PRIMARY KEY,tenant_id TEXT,mission_id TEXT,prospect_id TEXT,status TEXT,payload JSONB,attempted_at TIMESTAMPTZ,prepared_artifact_revision TEXT,provider_message_id TEXT,sent_at TIMESTAMPTZ,updated_at TIMESTAMPTZ);
    CREATE TABLE acquisition_mission_provider_events(id TEXT PRIMARY KEY,tenant_id TEXT,prospect_id TEXT,event_type TEXT,execution_record_id TEXT);
    CREATE TABLE tenant_outreach_messages(id TEXT PRIMARY KEY,tenant_id TEXT,prospect_id TEXT,direction TEXT,sender JSONB,status TEXT,sent_at TIMESTAMPTZ);
    CREATE TABLE tenant_outreach_scheduled_sends(id TEXT PRIMARY KEY,tenant_id TEXT,prospect_id TEXT,recipient_email TEXT,status TEXT,skip_reason TEXT,cancelled_at TIMESTAMPTZ);
    INSERT INTO users VALUES(7,10,true);
    INSERT INTO clients(id) VALUES(10);
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
  let adapterSet;
  async function reset(overrides = {}) {
    await pool.query(`TRUNCATE acquisition_mission_contributions,acquisition_outbound_events,acquisition_outbound_replies,acquisition_outbound_lifecycle,
      acquisition_outbound_items,acquisition_outbound_envelopes,acquisition_outbound_preparation,acquisition_outbound_programs,
      acquisition_outbound_inbox_health,
      ao_prospect_tasks,ao_contacts,ao_follow_up_tasks,ao_leads,agent_actions,agent_log,touchpoints,tenant_outreach_messages,tenant_outreach_scheduled_sends,
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
    adapterSet = adapters;
    svc = service({ pool, adapters, now: () => clock, enabled: () => enabled });
    const input = { sourceMissionId: 'source', senderEmail: 'sender@anchor.example', inboxIntegrationId: 'mailbox', aoOwnerIds: [7],
      startsAt: '2026-09-18T00:00:00Z', expiresAt: '2026-10-01T00:00:00Z', ...overrides };
    const review = await svc.authorize(input, actor);
    assert.equal(review.reviewRequired, true);
    assert.equal(await svc.store.program(), null);
    program = await svc.authorize({ ...review.policy, reviewHash: review.reviewHash }, actor);
    assert.equal(program.scope_hash, hash(missionScope(source)));
  }
  async function activate() { await svc.setMode(program.id, 'active', program.policy_hash, actor); }
  async function ageAttempts() {
    await pool.query("UPDATE acquisition_outbound_items SET attempted_at=attempted_at-interval '61 minutes' WHERE attempted_at IS NOT NULL");
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

  async function recoverySetup() {
    source.targetSegment = 'Short-Term Rental Operators';
    source.structuredMission = { immutable: true, market: { segment: 'short_term_rental' }, geography: { cities: ['Manchester'] } };
    await reset({ dailyCap: 1, totalCap: 1 }); enabled = false;
    for (const id of ['source', 'daily']) {
      await pool.query("UPDATE acquisition_missions SET tenant_id='10',stage=$2,status=$2,objective=$3,target_segment=$4,payload=$5 WHERE id=$1",
        [id, id === 'source' ? 'execute' : 'ready', source.objective, source.targetSegment, { ...source, id }]);
    }
    await pool.query("UPDATE acquisition_outbound_programs SET last_error='verified_inventory_shortfall' WHERE id=$1", [program.id]);
    await pool.query("INSERT INTO acquisition_outbound_preparation(program_id,local_day,mission_id,attempts,last_attempt_at) VALUES($1,'2026-09-18','daily',1,'2026-09-18T12:00:00.123456Z')", [program.id]);
    const input = { programId: program.id, policyHash: program.policy_hash, scopeHash: program.scope_hash,
      fromMissionId: 'daily', localDay: '2026-09-18', research: [{ name: 'Research Host Co', website: 'https://host.example/',
        operatingCity: 'Manchester', headquarters: 'Worcester, MA', evidence: ['property','services'].map(kind => ({
          kind, url: `https://host.example/${kind}`, summary: `Observed ${kind} evidence; qualification pending.`, observedAt: '2026-09-18T12:00:00Z' })) }] };
    adapterSet.prepare = async (_program, _source, _day, _store, plan) => {
      const progress = (await pool.query('SELECT * FROM acquisition_outbound_preparation WHERE program_id=$1', [program.id])).rows[0];
      assert.equal(progress.attempts, 2);
      assert.equal(progress.mission_id, plan.nextMissionId);
      await pool.query("INSERT INTO acquisition_missions(id,tenant_id,stage,payload) VALUES($1,'10','ready',$2) ON CONFLICT(id) DO NOTHING", [plan.nextMissionId, source]);
      return { mission: { ...source, id: plan.nextMissionId, stage: 'ready' } };
    };
    adapterSet.approve = async () => { throw Error('Recovery must not approve execution'); };
    adapterSet.execute = async () => { throw Error('Recovery must not execute outbound'); };
    return input;
  }
  await t.test('replenishment review makes no writes; commit consumes one attempt and freezes one in shadow; replay is blocked', async () => {
    const input = await recoverySetup();
    const oldMission = (await pool.query("SELECT * FROM acquisition_missions WHERE id='daily'")).rows[0];
    const beforeEvents = (await pool.query('SELECT count(*)::int AS n FROM acquisition_outbound_events')).rows[0].n;
    const review = await svc.replenish(input, actor);
    assert.equal(review.review.nextAttempt, 2);
    assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts, 1);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM acquisition_outbound_events')).rows[0].n, beforeEvents);
    input.reviewHash = review.reviewHash;
    const result = await svc.replenish(input, actor, true);
    assert.equal(result.sent, 0); assert.equal(result.mode, 'shadow'); assert.equal(result.planned, 1);
    assert.equal(result.preparationAttempt, 2); assert.equal(calls, 0);
    assert.deepEqual((await pool.query("SELECT * FROM acquisition_missions WHERE id='daily'")).rows[0], oldMission);
    assert.equal((await svc.store.envelope(input.localDay)).approval_id, null);
    assert.equal((await svc.store.program()).policy_hash, input.policyHash);
    await assert.rejects(svc.replenish(input, actor, true), { code: 'replenishment_preparation_changed' });
    assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts, 2);
  });
  await t.test('explicit immediate operator recovery is hashed, bounded, shadow-only and never sends', async () => {
    const input = await recoverySetup();
    await pool.query("UPDATE acquisition_outbound_preparation SET last_attempt_at='2026-09-18T13:59:00Z'");
    await assert.rejects(svc.replenish(input, actor), { code: 'preparation_backoff' });
    input.immediatePreparation = true;
    await assert.rejects(svc.replenish(input, actor), { code: 'immediate_preparation_reason_required' });
    input.operatorReason = 'Operator requested immediate evidenced inventory recovery.';
    const review = await svc.replenish(input, actor);
    assert.equal(review.review.immediatePreparation, true);
    await assert.rejects(svc.replenish({...input,reviewHash:review.reviewHash,operatorReason:'Changed'}, actor,true), {code:'replenishment_review_changed'});
    const result = await svc.replenish({...input,reviewHash:review.reviewHash},actor,true);
    assert.equal(result.preparationAttempt,2); assert.equal(result.sent,0); assert.equal(calls,0);
    assert.equal((await svc.store.program()).policy.spacingMinutes,60);
  });
  await t.test('new AO assignments, related company contacts and prior contact are excluded before freeze', async () => {
    await reset();
    const c=contacts.get('c0');
    await pool.query('UPDATE prospects SET assigned_ao_id=19 WHERE id=$1',[c.id]);
    assert.equal(await svc.store.candidateOwnership({companyId:c.company_id}),'prior_contact_or_human_owned');
    await pool.query('UPDATE prospects SET assigned_ao_id=NULL,last_contacted_at=now() WHERE id=$1',[c.id]);
    assert.equal(await svc.store.candidateOwnership({companyId:c.company_id}),'prior_contact_or_human_owned');
    await pool.query('UPDATE prospects SET last_contacted_at=NULL WHERE id=$1',[c.id]);
    await pool.query('INSERT INTO ao_prospect_tasks(client_id,prospect_id,assigned_ao_id) VALUES(10,$1,19)',[c.id]);
    assert.equal(await svc.store.candidateOwnership({companyId:c.company_id}),'prior_contact_or_human_owned');
    assert.equal(calls,0);
  });
  await t.test('replenishment fails closed on stale review, backoff, exhausted budget, active grant and owned alias', async () => {
    let input = await recoverySetup();
    const reviewed = await svc.replenish(input, actor);
    input.reviewHash = reviewed.reviewHash;
    input.research[0].evidence[0].summary = 'Changed research';
    await assert.rejects(svc.replenish(input, actor, true), { code: 'replenishment_review_changed' });
    assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts, 1);
    input = await recoverySetup();
    await pool.query("UPDATE acquisition_outbound_preparation SET last_attempt_at='2026-09-18T13:30:00Z'");
    await assert.rejects(svc.replenish(input, actor), { code: 'preparation_backoff' });
    await pool.query('UPDATE acquisition_outbound_preparation SET attempts=3');
    await assert.rejects(svc.replenish(input, actor), { code: 'preparation_retry_budget' });
    input = await recoverySetup(); await activate();
    await assert.rejects(svc.replenish(input, actor), { code: 'replenishment_shadow_grant_required' });
    input = await recoverySetup();
    await pool.query("INSERT INTO ao_leads(client_id,business_name,ao_owner_id) VALUES(10,'Research Host LLC',7)");
    await assert.rejects(svc.replenish(input, actor), { code: 'research_candidate_ao_owned' });
    assert.equal(calls, 0);
  });
  async function discoveryFailureSetup() {
    const input = await recoverySetup();
    const missionId = `mission_daily_${hash([program.id, input.localDay]).slice(0, 24)}`;
    await pool.query('DELETE FROM acquisition_missions WHERE id=$1', [missionId]);
    await pool.query("INSERT INTO acquisition_missions(id,tenant_id,stage,status,objective,target_segment,payload) VALUES($1,'10','discover','Discovering',$2,$3,$4)",
      [missionId, source.objective, source.targetSegment, { ...source, id: missionId, stage: 'discover', version: 0,
        lastTransactionId: null, structuredMissionApproved: true, orchestrationMissionId: 'source',
        pendingOperatorDecision: { kind: 'discovery_approval' } }]);
    await pool.query("UPDATE acquisition_outbound_preparation SET mission_id=$1,last_error='verified_inventory_shortfall'", [missionId]);
    const payload = { programId: program.id, missionId, attempts: 2, eligible: 0,
      candidates: { c0: { eligible: false, reason: 'invalid_outreach_email' }, c1: { eligible: false, reason: 'prior_contact_or_human_owned' } } };
    await svc.store.event('inventory_replenished', [missionId, hash(payload.candidates)], payload);
    await pool.query("UPDATE acquisition_outbound_events SET created_at='2026-09-18T12:00:30Z' WHERE event_type='inventory_replenished'");
    return { ...input, fromMissionId: missionId };
  }
  await t.test('documented initial Scout shortfall is reviewable without changing Discovery, attempts or policy; one canonical reservation only', async () => {
    const input = await discoveryFailureSetup();
    const before = (await pool.query('SELECT * FROM acquisition_missions WHERE id=$1', [input.fromMissionId])).rows[0];
    const beforeEvents = (await pool.query('SELECT * FROM acquisition_outbound_events ORDER BY id')).rows;
    const grant = await svc.store.program();
    const review = await svc.replenish(input, actor);
    assert.ok(review.review.discoveryFailure.payloadHash);
    assert.equal(review.review.nextAttempt, 2);
    assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts, 1);
    assert.deepEqual((await pool.query('SELECT * FROM acquisition_outbound_events ORDER BY id')).rows, beforeEvents);
    assert.deepEqual(await svc.store.program(), grant);
    input.reviewHash = review.reviewHash;
    const result = await svc.replenish(input, actor, true);
    assert.equal(result.preparationAttempt, 2); assert.equal(result.sent, 0); assert.equal(calls, 0);
    assert.equal(result.mode, 'shadow');
    assert.deepEqual((await pool.query('SELECT * FROM acquisition_missions WHERE id=$1', [input.fromMissionId])).rows[0], before);
    assert.equal((await svc.store.envelope(input.localDay)).approval_id, null);
    assert.equal((await svc.store.program()).policy_hash, input.policyHash);
    await assert.rejects(svc.replenish(input, actor, true), { code: 'replenishment_preparation_changed' });
    assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts, 2);
  });
  await t.test('a documented failed recovery permits only the remaining bounded attempt', async () => {
    const input = await discoveryFailureSetup();
    const review = await svc.replenish(input, actor);
    const old = (await pool.query('SELECT * FROM acquisition_missions WHERE id=$1',[input.fromMissionId])).rows[0];
    await pool.query('DELETE FROM acquisition_missions WHERE id=$1',[review.nextMissionId]);
    await pool.query("INSERT INTO acquisition_missions(id,tenant_id,stage,status,objective,target_segment,payload) VALUES($1,'10','discover','Discovering',$2,$3,$4)",
      [review.nextMissionId,old.objective,old.target_segment,{...old.payload,id:review.nextMissionId}]);
    await svc.store.event('preparation_recovery_reserved',[program.id,review.reviewHash],{programId:program.id,missionId:review.nextMissionId,reviewHash:review.reviewHash,review:review.review});
    await pool.query("UPDATE acquisition_outbound_preparation SET mission_id=$1,attempts=2,last_attempt_at='2026-09-18T13:00:00Z'",[review.nextMissionId]);
    const payload={programId:program.id,missionId:review.nextMissionId,attempts:1,eligible:0,candidates:{c0:{eligible:false,reason:'missing_row'}}};
    await svc.store.event('inventory_replenished',[review.nextMissionId,hash(payload)],payload);
    await pool.query("UPDATE acquisition_outbound_events SET created_at='2026-09-18T13:00:01Z' WHERE payload->>'missionId'=$1",[review.nextMissionId]);
    const next={...input,fromMissionId:review.nextMissionId,immediatePreparation:true,operatorReason:'Repair proven identity handoff defect.'};
    assert.equal((await svc.replenish(next,actor)).review.nextAttempt,3);
    await pool.query("UPDATE acquisition_outbound_events SET payload=jsonb_set(payload,'{reviewHash}','\"changed\"') WHERE event_type='preparation_recovery_reserved'");
    await assert.rejects(svc.replenish(next,actor),{code:'replenishment_discovery_failure_unproven'});
    assert.equal(calls,0);
  });
  await t.test('Discovery exception refuses undocumented, stale, successful, partially committed or noninitial attempts', async () => {
    const changes = [
      "DELETE FROM acquisition_outbound_events WHERE event_type='inventory_replenished'",
      "UPDATE acquisition_outbound_events SET created_at='2026-09-18T11:59:59Z' WHERE event_type='inventory_replenished'",
      "UPDATE acquisition_outbound_events SET created_at='2026-09-18T15:00:00Z' WHERE event_type='inventory_replenished'",
      "UPDATE acquisition_outbound_events SET payload=jsonb_set(payload,'{eligible}','1') WHERE event_type='inventory_replenished'",
      "UPDATE acquisition_outbound_events SET payload=jsonb_set(payload,'{candidates,c0,eligible}','true') WHERE event_type='inventory_replenished'",
      "UPDATE acquisition_outbound_events SET payload=jsonb_set(payload,'{candidates}','{}') WHERE event_type='inventory_replenished'",
      "UPDATE acquisition_outbound_events SET payload=jsonb_set(payload,'{attempts}','16') WHERE event_type='inventory_replenished'",
      "UPDATE acquisition_outbound_preparation SET last_error=NULL",
      "UPDATE acquisition_outbound_preparation SET attempts=2",
      "UPDATE acquisition_missions SET payload=jsonb_set(payload,'{version}','1') WHERE stage='discover'",
      "UPDATE acquisition_missions SET payload=payload-'orchestrationMissionId' WHERE stage='discover'",
      "UPDATE acquisition_missions SET payload=payload-'pendingOperatorDecision' WHERE stage='discover'",
      "UPDATE acquisition_missions SET stage='understand' WHERE stage='discover'",
      "INSERT INTO acquisition_mission_contributions SELECT 'partial','10',mission_id,'{}' FROM acquisition_outbound_preparation",
    ];
    for (const sql of changes) {
      const input = await discoveryFailureSetup();
      await pool.query(sql);
      const attempts = (await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts;
      await assert.rejects(svc.replenish(input, actor), { code: 'replenishment_discovery_failure_unproven' }, sql);
      assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts, attempts);
      assert.equal(await svc.store.envelope(input.localDay), null);
    }
    assert.equal(calls, 0);
  });
  await t.test('Discovery failure does not bypass backoff, original actor, hashes, disabled sending, execution or envelope checks', async () => {
    let input = await discoveryFailureSetup();
    clock = new Date('2026-09-18T12:59:59Z');
    await assert.rejects(svc.replenish(input, actor), { code: 'preparation_backoff' });
    clock = new Date('2026-09-18T13:00:00.124Z');
    assert.ok((await svc.replenish(input, actor)).reviewHash);
    await assert.rejects(svc.replenish(input, { ...actor, id: 'other' }), { code: 'replenishment_authorizing_operator_required' });
    await assert.rejects(svc.replenish({ ...input, policyHash: 'changed' }, actor), { code: 'policy_changed' });
    enabled = true;
    await assert.rejects(svc.replenish(input, actor), { code: 'replenishment_requires_disabled_sending' });
    enabled = false;
    await pool.query("INSERT INTO acquisition_mission_outbound_executions(id,tenant_id,mission_id,status) VALUES('unexpected','10',$1,'attempted')", [input.fromMissionId]);
    await assert.rejects(svc.replenish(input, actor), { code: 'replenishment_execution_exists' });
    input = await discoveryFailureSetup();
    await svc.store.freeze(program, input.localDay, input.fromMissionId, 'existing', []);
    await assert.rejects(svc.replenish(input, actor), { code: 'replenishment_envelope_exists' });
    assert.equal(calls, 0);
  });
  await t.test('Discovery failure receipt is review-bound and rechecked in the reservation transaction', async () => {
    const recovery = require('../services/governedOutboundReplenishment');
    let input = await discoveryFailureSetup();
    input.reviewHash = (await svc.replenish(input, actor)).reviewHash;
    await pool.query("UPDATE acquisition_outbound_events SET payload=jsonb_set(payload,'{candidates,c0,reason}','\"email_not_verified\"') WHERE event_type='inventory_replenished'");
    await assert.rejects(svc.replenish(input, actor, true), { code: 'replenishment_review_changed' });
    input = await discoveryFailureSetup();
    let plan = await recovery.reviewReplenishment(svc.store, input, actor, clock, false);
    await pool.query("UPDATE acquisition_missions SET payload=jsonb_set(payload,'{version}','1') WHERE id=$1", [input.fromMissionId]);
    await assert.rejects(recovery.reserveReplenishment(svc.store, plan, clock), { code: 'replenishment_preparation_changed' });
    input = await discoveryFailureSetup();
    plan = await recovery.reviewReplenishment(svc.store, input, actor, clock, false);
    await pool.query("DELETE FROM acquisition_outbound_events WHERE event_type='inventory_replenished'");
    await assert.rejects(recovery.reserveReplenishment(svc.store, plan, clock), { code: 'replenishment_discovery_failure_unproven' });
    assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts, 1);
    assert.equal(calls, 0);
  });
  await t.test('grant activation during preparation cannot freeze a recovery envelope or call a sender', async () => {
    const input = await recoverySetup();
    input.reviewHash = (await svc.replenish(input, actor)).reviewHash;
    const originalPrepare = adapterSet.prepare;
    adapterSet.prepare = async (...args) => { const result = await originalPrepare(...args); await activate(); return result; };
    const result = await svc.replenish(input, actor, true);
    assert.equal(result.halted, 'replenishment_grant_changed');
    assert.equal(await svc.store.envelope(input.localDay), null);
    assert.equal(calls, 0);
    assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts, 2);
  });
  await t.test('replenishment binds the operator, day, policy, source scope and zero-execution state', async () => {
    let input = await recoverySetup();
    await assert.rejects(svc.replenish(input, { ...actor, id: 'other' }), { code: 'replenishment_authorizing_operator_required' });
    await assert.rejects(svc.replenish({ ...input, localDay: '2026-09-17' }, actor), { code: 'replenishment_day_changed' });
    await assert.rejects(svc.replenish({ ...input, policyHash: 'changed' }, actor), { code: 'policy_changed' });
    await pool.query("UPDATE acquisition_missions SET objective='Changed scope' WHERE id='source'");
    await assert.rejects(svc.replenish(input, actor), { code: 'source_scope_changed' });
    input = await recoverySetup();
    await pool.query("INSERT INTO acquisition_mission_outbound_executions(id,tenant_id,mission_id,status) VALUES('prior','10','daily','attempted')");
    await assert.rejects(svc.replenish(input, actor), { code: 'replenishment_execution_exists' });
    assert.equal((await pool.query('SELECT attempts FROM acquisition_outbound_preparation')).rows[0].attempts, 1);
    assert.equal(calls, 0);
  });
  await t.test('shadow requirement is checked again inside the freeze transaction', async () => {
    await recoverySetup();
    await activate();
    await assert.rejects(svc.store.freeze(program, '2026-09-18', 'daily', 'revision1', [], { requireShadow: true }),
      { code: 'replenishment_grant_changed' });
    assert.equal(await svc.store.envelope('2026-09-18'), null);
    assert.equal(calls, 0);
  });
  await t.test('Lot 202 naming variants and AO contact domains remain suppressed without CRM links', async () => {
    await reset();
    await pool.query("INSERT INTO ao_leads(client_id,business_name,ao_owner_id) VALUES(10,'Lot 202 LLC',7)");
    assert.equal(await svc.store.candidateOwnership({ company: 'Lot 202 - Property Management Company' }), 'ao_owned_alias');
    assert.equal(await svc.store.candidateOwnership({ company: 'Completely Different' }), null);
    const lead = (await pool.query('SELECT id FROM ao_leads')).rows[0];
    await pool.query('INSERT INTO ao_contacts(lead_id,email) VALUES($1,$2)', [lead.id, 'owner@brand.example']);
    assert.equal(await svc.store.candidateOwnership({ company: 'Different Legal Name', domain: 'brand.example' }), 'ao_owned_alias');
  });
  async function initializationSetup() {
    const input = await recoverySetup();
    clock = new Date('2026-09-22T15:54:26Z');
    await pool.query("UPDATE acquisition_outbound_preparation SET local_day='2026-09-21',last_attempt_at='2026-09-21T13:57:13.228Z'");
    for (const name of ['prepare', 'prepared', 'contact', 'approve', 'execute', 'send', 'complete']) {
      adapterSet[name] = async () => { throw Error(`Initialization must not call ${name}`); };
    }
    adapterSet.validateTenant = async () => {
      const client = (await pool.query('SELECT * FROM clients WHERE id=10')).rows[0];
      if (!client.active || client.autosend_enabled !== false) throw Object.assign(Error(), { code: 'tenant_inactive_or_legacy_autosend_enabled' });
    };
    return { ...input, sourceMissionId: 'source', localDay: '2026-09-22' };
  }
  const savedEnabledEnv = process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
  process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'false';
  t.after(() => {
    if (savedEnabledEnv === undefined) delete process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
    else process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = savedEnabledEnv;
  });
  async function preparationRows() {
    return (await pool.query('SELECT * FROM acquisition_outbound_preparation ORDER BY local_day')).rows;
  }
  await t.test('September 22 initialization creates only a 0-attempt row and one audit event; yesterday and grant stay unchanged', async () => {
    const input = await initializationSetup();
    const yesterday = (await preparationRows())[0];
    const grant = await svc.store.program();
    const missions = (await pool.query('SELECT * FROM acquisition_missions ORDER BY id')).rows;
    const result = await svc.initializePreparation(input, actor);
    assert.deepEqual(result, { mode: 'shadow', initialized: true, programId: program.id, localDay: input.localDay,
      missionId: `mission_daily_${hash([program.id, input.localDay]).slice(0, 24)}`, attempts: 0,
      preparationAttemptsPerDay: 3, lastAttemptAt: null, lastError: null, preparationAttemptsReserved: 0, sent: 0 });
    const rows = await preparationRows();
    assert.equal(rows.length, 2); assert.deepEqual(rows[0], yesterday);
    assert.equal(rows[1].attempts, 0); assert.equal(rows[1].last_attempt_at, null);
    assert.deepEqual(await svc.store.program(), grant);
    assert.deepEqual((await pool.query('SELECT * FROM acquisition_missions ORDER BY id')).rows, missions);
    for (const table of ['acquisition_outbound_envelopes', 'acquisition_outbound_items', 'acquisition_mission_outbound_executions']) {
      assert.equal((await pool.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n, 0);
    }
    assert.equal(calls, 0);
    const again = await svc.initializePreparation(input, actor);
    assert.deepEqual(again, { ...result, initialized: false });
    assert.deepEqual(await preparationRows(), rows);
    const events = (await pool.query("SELECT * FROM acquisition_outbound_events WHERE event_type='preparation_initialized'")).rows;
    assert.equal(events.length, 1); assert.equal(events[0].payload.actor, actor.id);
    // Neither an empty row nor substituting today's mission ID fabricates a
    // same-day failed attempt, so the old recovery gate deliberately still fails.
    await assert.rejects(svc.replenish(input, actor), { code: 'replenishment_preparation_changed' });
    await assert.rejects(svc.replenish({ ...input, fromMissionId: result.missionId }, actor), { code: 'replenishment_preparation_changed' });
  });
  await t.test('initialization preserves a consumed recovery reservation and its error without resets', async () => {
    const input = await initializationSetup();
    await pool.query("INSERT INTO acquisition_outbound_preparation VALUES($1,'2026-09-22','reviewed-recovery',2,'2026-09-22T14:00:00.123456Z','preparation_failed')", [program.id]);
    const before = await preparationRows();
    const result = await svc.initializePreparation(input, actor);
    assert.equal(result.initialized, false); assert.equal(result.attempts, 2); assert.equal(result.missionId, 'reviewed-recovery');
    assert.equal(result.lastError, 'preparation_failed'); assert.deepEqual(await preparationRows(), before);
  });
  await t.test('initialization binds the operator, grant, day, hashes and source and requires explicit disabled sending', async () => {
    const input = await initializationSetup();
    const before = await preparationRows();
    for (const [change, error] of [
      [{ programId: 'other' }, 'preparation_shadow_grant_required'],
      [{ localDay: '2026-09-21' }, 'preparation_day_changed'],
      [{ policyHash: 'changed' }, 'policy_changed'],
      [{ scopeHash: 'changed' }, 'source_scope_changed'],
      [{ sourceMissionId: 'daily' }, 'source_scope_changed'],
    ]) await assert.rejects(svc.initializePreparation({ ...input, ...change }, actor), { code: error });
    await assert.rejects(svc.initializePreparation(input, { ...actor, id: 'other' }), { code: 'preparation_authorizing_operator_required' });
    await assert.rejects(svc.initializePreparation(input, { ...actor, role: 'viewer' }), { code: 'operator_required' });
    for (const value of ['true', '', undefined]) {
      if (value === undefined) delete process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
      else process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = value;
      await assert.rejects(svc.initializePreparation(input, actor), { code: 'preparation_requires_disabled_sending' });
    }
    process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'false';
    enabled = true;
    await assert.rejects(svc.initializePreparation(input, actor), { code: 'preparation_requires_disabled_sending' });
    enabled = false;
    await pool.query('UPDATE clients SET autosend_enabled=true WHERE id=10');
    await assert.rejects(svc.initializePreparation(input, actor), { code: 'tenant_inactive_or_legacy_autosend_enabled' });
    await pool.query('UPDATE clients SET autosend_enabled=false WHERE id=10');
    adapterSet.loadMission = async () => ({ mission: { ...source, objective: 'changed' } });
    await assert.rejects(svc.initializePreparation(input, actor), { code: 'source_scope_changed' });
    assert.deepEqual(await preparationRows(), before); assert.equal(calls, 0);
  });
  await t.test('initialization rejects active, paused, revoked, expired and out-of-hours grants without creating a row', async () => {
    for (const mode of ['active', 'paused', 'revoked']) {
      const input = await initializationSetup();
      await svc.setMode(program.id, mode, program.policy_hash, actor);
      await assert.rejects(svc.initializePreparation(input, actor), { code: 'preparation_shadow_grant_required' });
      assert.equal((await preparationRows()).length, 1);
    }
    const input = await initializationSetup();
    for (const [time, error] of [
      ['2026-09-22T21:00:00Z', 'outside_business_hours'],
      ['2026-10-01T14:00:00Z', 'authorization_expired'],
    ]) {
      clock = new Date(time);
      await assert.rejects(svc.initializePreparation(input, actor), { code: error });
    }
    assert.equal((await preparationRows()).length, 1);
  });
  await t.test('initialization detects day rollover and disabled-state changes during validation', async () => {
    let input = await initializationSetup();
    adapterSet.validateTenant = async () => { clock = new Date('2026-09-23T04:00:00Z'); };
    await assert.rejects(svc.initializePreparation(input, actor), { code: 'preparation_day_changed' });
    input = await initializationSetup();
    adapterSet.validateTenant = async () => { enabled = true; };
    await assert.rejects(svc.initializePreparation(input, actor), { code: 'preparation_requires_disabled_sending' });
    assert.equal((await preparationRows()).length, 1);
  });
  await t.test('initialization refuses envelopes and provider attempts, and rolls back the row if auditing fails', async () => {
    let input = await initializationSetup();
    await svc.store.freeze(program, input.localDay, 'daily', 'revision1', []);
    await assert.rejects(svc.initializePreparation(input, actor), { code: 'preparation_envelope_exists' });
    input = await initializationSetup();
    svc.store.counts = async () => ({ today: 0, total: 1, uncertain: 0 });
    await assert.rejects(svc.initializePreparation(input, actor), { code: 'preparation_attempt_exists' });
    input = await initializationSetup();
    svc.store.event = async () => { throw Error('Audit unavailable'); };
    await assert.rejects(svc.initializePreparation(input, actor), /Audit unavailable/);
    assert.equal((await preparationRows()).length, 1);
  });
  await t.test('overlapping initialization calls converge on one empty row without spending an attempt', async () => {
    const input = await initializationSetup();
    const results = await Promise.all([svc.initializePreparation(input, actor), svc.initializePreparation(input, actor)]);
    assert.equal(results.filter(x => x.initialized).length, 1);
    assert.ok(results.every(x => x.initialized || x.initialized === false || x.halted === 'overlap'));
    assert.equal((await preparationRows()).length, 2);
    assert.equal((await preparationRows())[1].attempts, 0); assert.equal(calls, 0);
  });

});
