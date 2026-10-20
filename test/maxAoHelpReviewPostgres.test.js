'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const express = require('express');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { applyCanonicalAoFixture } = require('./helpers/canonicalAoPostgres');
const { SCOPE, IDS, BUSINESS_KEY, previewAoHelpReview, applyAoHelpReview, reconcileAoHelpReview } = require('../services/maxAoHelpReviewService');
const { PostgresAttentionStore } = require('../packages/max/attention');
const { wakeAttentionForSubject, wakeAttentionForIngestion } = require('../packages/max/attention/wake');
const { PostgresStateStore } = require('../packages/max/stateIngestion');
const { PostgresDecisionStore } = require('../packages/max/decisionExecution');

function serviceWith(db) {
  const filename = path.join(__dirname, '../services/maxAttentionService.js');
  const module = { exports: {} };
  const dependencies = { '../db': db, '../packages/max/attention': require('../packages/max/attention'),
    '../packages/max/stateIngestion': require('../packages/max/stateIngestion'), '../packages/max/decisionExecution': require('../packages/max/decisionExecution') };
  vm.compileFunction(fs.readFileSync(filename, 'utf8'), ['module', 'require'], { filename })(module, name => {
    assert.ok(Object.hasOwn(dependencies, name), name); return dependencies[name];
  });
  return module.exports;
}
function wrappingPool(pool, intercept, leases = []) {
  return { connect: async () => {
    const db = await pool.connect(); const log = []; leases.push(log);
    return { release: () => db.release(), query: async (sql, args) => {
      log.push(sql); return intercept ? intercept(sql, args, db) : db.query(sql, args);
    } };
  } };
}
async function counts(db) {
  const tables = ['max_operational_ingestions', 'max_operational_decisions', 'max_attention_items',
    'max_operational_action_intents', 'max_ao_follow_up_tasks', 'max_open_expectations', 'max_ingestion_mutations', 'max_ingestion_claims', 'max_ingestion_evidence_links', 'max_evidence_artifacts'];
  return Object.fromEntries(await Promise.all(tables.map(async table => [table, Number((await db.query(`SELECT count(*) AS n FROM ${table}`)).rows[0].n)])));
}
async function assertOnlyThree(db) {
  const actual = await counts(db);
  for (const [table, n] of Object.entries(actual)) assert.equal(n, ['max_operational_ingestions', 'max_operational_decisions', 'max_attention_items'].includes(table) ? 1 : 0, table);
}
async function assertEmpty(db) { for (const [table, n] of Object.entries(await counts(db))) assert.equal(n, 0, table); }
async function originalRows(db) {
  const tables = ['prospects', 'ao_prospect_activity', 'ao_prospect_tasks', 'users', 'clients', 'companies', 'touchpoints', 'email_events',
    'ao_followup_drafts', 'ao_account_flags', 'acquisition_mission_outbound_executions', 'acquisition_mission_provider_events', 'acquisition_outbound_items', 'acquisition_outbound_replies'];
  return Object.fromEntries(await Promise.all(tables.map(async table => [table, (await db.query(`SELECT to_jsonb(t) AS row FROM ${table} t ORDER BY to_jsonb(t)::text`)).rows])));
}
function applyOptions(pool, preview) { return { pool, ...SCOPE, expectedSourceVersion: preview.source_version, previewObservedAt: preview.observed_at }; }

test('existing AO help review has exact bounded durable effects in disposable PostgreSQL', { timeout: 120000 }, async t => {
  const instance = await startDisposablePostgres('max-help-review-');
  const admin = createTrackedPostgresPool({ connectionString: instance.connectionString });
  t.after(() => closePostgresFixture(admin, instance));
  async function fixture(t) {
    const schema = `help_${randomUUID().replaceAll('-', '')}`;
    await admin.query(`CREATE SCHEMA ${schema}`);
    const db = createTrackedPostgresPool({ connectionString: instance.connectionString, options: `-c search_path=${schema},public`, max: 6 });
    t.after(() => db.end());
    await applyCanonicalAoFixture(db);
    await db.query(`ALTER TABLE clients ADD active boolean DEFAULT true;
      ALTER TABLE users ADD client_id integer, ADD name text, ADD role text, ADD active boolean DEFAULT true;
      ALTER TABLE companies ADD client_id integer;
      INSERT INTO clients(id) VALUES (10);
      INSERT INTO users(id,client_id,name,role) VALUES (24,10,'Fixture AO','ao'),(25,10,'Fixture Manager','manager');
      INSERT INTO companies VALUES ('11111111-1111-4111-8111-111111111111','Fixture Account',10);
      CREATE TABLE touchpoints(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id integer,prospect_id uuid,channel text,action_type text,outcome text,created_at timestamptz);
      CREATE TABLE ao_prospect_tasks(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id integer,prospect_id uuid,assigned_ao_id integer,status text,created_at timestamptz,completed_at timestamptz);
      CREATE TABLE email_events(event_id text PRIMARY KEY,client_id integer,prospect_id uuid,event_type text,event_at timestamptz);
      CREATE TABLE ao_followup_drafts(id bigint PRIMARY KEY,tenant_id integer,account_id uuid,assigned_ao_id integer,status text,created_at timestamptz,updated_at timestamptz);
      CREATE TABLE ao_account_flags(id uuid PRIMARY KEY,client_id integer,account_id uuid,ao_id integer,status text,created_at timestamptz,resolved_at timestamptz);
      CREATE TABLE acquisition_outbound_items(id text PRIMARY KEY,tenant_id text,prospect_id text,status text,attempted_at timestamptz);
      CREATE TABLE acquisition_outbound_replies(id text PRIMARY KEY,tenant_id text,prospect_id text,classification text,created_at timestamptz,classified_at timestamptz);`);
    await db.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-08-27-canonical-execute-outbound.sql'), 'utf8'));
    await db.query(`INSERT INTO prospects(id,client_id,company_id,assigned_ao_id,status,help_requested,help_requested_at,ao_current_status,ao_next_action)
      VALUES ($1,10,'11111111-1111-4111-8111-111111111111',24,'cold',true,'2026-10-07T18:49:20Z','gatekeeper_reached','call')`, [SCOPE.prospectId]);
    await db.query(`INSERT INTO ao_prospect_activity(id,tenant_id,prospect_id,ao_id,activity_type,outcome,notes,metadata,created_at)
      VALUES ($1,10,$3,24,'help_requested','needs_jake','Synthetic fixture request','{}','2026-10-07 18:49:20.378505+00'),
        ($2,10,$3,24,'help_requested','spoke_gatekeeper','Synthetic fixture outcome','{"source":"ao_crm_outcome_form"}','2026-10-07 18:49:20.366645+00')`,
    [SCOPE.activityId, SCOPE.companionActivityId, SCOPE.prospectId]);
    await db.query(`INSERT INTO ao_prospect_tasks(client_id,prospect_id,assigned_ao_id,status,created_at,completed_at)
      VALUES (10,$1,24,'completed','2026-10-01','2026-10-07 18:49:20.37+00')`, [SCOPE.prospectId]);
    return db;
  }

  await t.test('preview is read-only, no-init, exact-scoped and preserves full precision; completed assignment is not contact proof', async t => {
    const db = await fixture(t); const before = await originalRows(db); const leases = [];
    const originals = [PostgresStateStore, PostgresDecisionStore, PostgresAttentionStore].map(Type => Type.prototype.init);
    for (const Type of [PostgresStateStore, PostgresDecisionStore, PostgresAttentionStore]) Type.prototype.init = () => { throw new Error('Initialization forbidden'); };
    try {
      const preview = await previewAoHelpReview({ pool: wrappingPool(db, null, leases), ...SCOPE });
      assert.equal(preview.mode, 'preview'); assert.equal(preview.proposed_rows, 3);
      assert.match(preview.snapshot.source.created_at, /20\.378505/);
      assert.equal(preview.snapshot.evidence.assignment_tasks[0].status, 'completed');
      assert.match(preview.reason, /outside this system is unknown/);
      assert.equal(preview.outside_contact, 'unknown');
      assert.ok(leases.flat().every(sql => /^(SELECT|BEGIN.*READ ONLY|SET LOCAL|ROLLBACK)/.test(sql.trim())));
      assert.deepEqual(await originalRows(db), before); await assertEmpty(db);
      for (const wrong of [{ clientId: 2 }, { prospectId: randomUUID() }, { aoId: 11 }, { activityId: randomUUID() }, { clientId: [10] }]) {
        await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE, ...wrong }), { code: 'AO_HELP_REVIEW_SCOPE_MISMATCH' });
      }
    } finally { [PostgresStateStore, PostgresDecisionStore, PostgresAttentionStore].forEach((Type, i) => { Type.prototype.init = originals[i]; }); }
  });

  await t.test('apply atomically writes only receipt/blocked decision/manual attention; fresh lease readback, repeated apply unchanged', async t => {
    const db = await fixture(t); const before = await originalRows(db);
    const preview = await previewAoHelpReview({ pool: db, ...SCOPE }); const leases = [];
    const result = await applyAoHelpReview(applyOptions(wrappingPool(db, null, leases), preview));
    assert.equal(result.status, 'applied'); assert.equal(result.readback_verified, true); assert.equal(leases.length, 2);
    assert.ok(result.observed_at); assert.equal(result.freshness_boundary, 'database_transaction_snapshot');
    assert.match(result.receipt_summary, /^As of the database transaction snapshot at /);
    assert.match(leases[1][0], /READ ONLY/);
    await assertOnlyThree(db); assert.deepEqual(await originalRows(db), before);
    const chain = (await db.query('SELECT * FROM max_operational_ingestions')).rows[0];
    assert.ok(chain.receipt_summary); assert.equal(chain.raw_source.source_version, preview.source_version);
    assert.equal(chain.raw_source.assignment_task_completion_is_not_contact_proof, true);
    assert.equal(chain.raw_source.outside_contact, 'unknown'); assert.deepEqual(chain.pipeline_audit.linked_ids, IDS);
    assert.equal(Object.keys(chain.raw_source.checked_sources).length, 10);
    assert.match(chain.raw_source.snapshot.source.created_at, /20\.378505/);
    const decision = (await db.query('SELECT * FROM max_operational_decisions')).rows[0];
    assert.equal(decision.execution_status, 'EXECUTION_BLOCKED'); assert.equal(decision.authority_class, 'C'); assert.ok(decision.receipt_summary);
    const attention = (await db.query('SELECT * FROM max_attention_items')).rows[0];
    assert.equal(attention.review_trigger, 'MANUAL'); assert.equal(attention.next_review_at, null); assert.equal(attention.audience_tier, 'OPERATOR_VISIBLE');
    const all = await db.query('SELECT to_jsonb(a) row FROM max_attention_items a');
    assert.equal((await applyAoHelpReview(applyOptions(db, preview))).status, 'already_recorded');
    assert.deepEqual((await db.query('SELECT to_jsonb(a) row FROM max_attention_items a')).rows, all.rows); await assertOnlyThree(db);
    await db.query("UPDATE max_attention_items SET status='RESOLVED', resolved_at=now() WHERE id=$1", [IDS.attention]);
    assert.equal((await applyAoHelpReview(applyOptions(db, preview))).status, 'already_closed');
    assert.equal((await db.query('SELECT status FROM max_attention_items')).rows[0].status, 'RESOLVED');
  });

  await t.test('freshness catches nested source edits and same-millisecond timestamp changes; stale preview cannot apply', async t => {
    const db = await fixture(t);
    for (const sql of ["UPDATE ao_prospect_activity SET metadata='{\"deep\":{\"value\":1}}' WHERE outcome='needs_jake'",
      "UPDATE ao_prospect_activity SET created_at=created_at+interval '1 microsecond' WHERE outcome='needs_jake'",
      "UPDATE prospects SET help_reason='changed'", "UPDATE ao_prospect_tasks SET completed_at=completed_at+interval '1 microsecond'"]) {
      const preview = await previewAoHelpReview({ pool: db, ...SCOPE }); await db.query(sql);
      await assert.rejects(applyAoHelpReview(applyOptions(db, preview)), { code: 'AO_HELP_REVIEW_SOURCE_CHANGED' }); await assertEmpty(db);
    }
    const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    await assert.rejects(applyAoHelpReview({ ...applyOptions(db, preview), previewObservedAt: '2020-01-01T00:00:00Z' }), { code: 'AO_HELP_REVIEW_PREVIEW_EXPIRED' });
    await assert.rejects(applyAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_PREVIEW_REQUIRED' }); await assertEmpty(db);
  });

  await t.test('changed owner, resolved help, later contact and unreadable/truncated evidence fail closed', async t => {
    const db = await fixture(t);
    await db.query('UPDATE prospects SET assigned_ao_id=11');
    await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_OWNER_MISMATCH' });
    await db.query('UPDATE prospects SET assigned_ao_id=24,help_resolved_at=now()');
    await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_HELP_NOT_OPEN' });
    await db.query('UPDATE prospects SET help_resolved_at=null');
    const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    await db.query("INSERT INTO touchpoints(client_id,prospect_id,channel,action_type,created_at) VALUES (10,$1,'email','sent',now())", [SCOPE.prospectId]);
    await assert.rejects(applyAoHelpReview(applyOptions(db, preview)), { code: 'AO_HELP_REVIEW_SOURCE_CHANGED' });
    await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_EVIDENCE_REQUIRES_REVIEW' });
    await db.query('DELETE FROM touchpoints');
    await db.query('ALTER TABLE email_events RENAME TO unavailable_events');
    await assert.rejects(applyAoHelpReview(applyOptions(db, preview)), { code: '42P01' });
    await db.query('ALTER TABLE unavailable_events RENAME TO email_events');
    await db.query("INSERT INTO touchpoints(client_id,prospect_id,channel,action_type,created_at) VALUES (10,$1,'email','sent',null)", [SCOPE.prospectId]);
    await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_EVIDENCE_REQUIRES_REVIEW' });
    await db.query('DELETE FROM touchpoints');
    await db.query("INSERT INTO email_events(event_id,client_id,prospect_id,event_type) SELECT n::text,10,$1,'sent' FROM generate_series(1,1001)n", [SCOPE.prospectId]);
    await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_EVIDENCE_TRUNCATED' }); await assertEmpty(db);
  });

  await t.test('equal-time and backdated contact/activity inserts invalidate preview versions', async t => {
    const db = await fixture(t);
    for (const timestamp of ['2026-10-07 18:49:20.378505+00', '2026-10-01T00:00:00Z']) {
      const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
      await db.query("INSERT INTO touchpoints(client_id,prospect_id,channel,action_type,created_at) VALUES (10,$1,'email','sent',$2)", [SCOPE.prospectId, timestamp]);
      await assert.rejects(applyAoHelpReview(applyOptions(db, preview)), { code: 'AO_HELP_REVIEW_SOURCE_CHANGED' });
      if (timestamp.includes('378505')) await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_EVIDENCE_REQUIRES_REVIEW' });
      else assert.notEqual((await previewAoHelpReview({ pool: db, ...SCOPE })).source_version, preview.source_version);
      await db.query('DELETE FROM touchpoints');
    }
    const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    await db.query("INSERT INTO ao_prospect_activity(tenant_id,prospect_id,ao_id,activity_type,outcome,created_at) VALUES (10,$1,24,'note','historical','2026-10-01')", [SCOPE.prospectId]);
    await assert.rejects(applyAoHelpReview(applyOptions(db, preview)), { code: 'AO_HELP_REVIEW_SOURCE_CHANGED' }); await assertEmpty(db);
  });

  await t.test('absent uniqueness fails before writes, never recreating schema', async t => {
    const db = await fixture(t);
    await db.query('ALTER TABLE max_attention_items DROP CONSTRAINT max_attention_items_client_id_dedup_fingerprint_key');
    await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_SCHEMA_NOT_READY' }); await assertEmpty(db);
  });

  await t.test('each write failure rolls back all rows; unacknowledged commit only reconciles', async t => {
    const db = await fixture(t); const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    for (const table of ['max_operational_ingestions', 'max_operational_decisions', 'max_attention_items']) {
      const failing = wrappingPool(db, (sql, args, client) => {
        if (sql.includes(`INSERT INTO ${table}`)) throw new Error('Injected write failure');
        return client.query(sql, args);
      });
      await assert.rejects(applyAoHelpReview(applyOptions(failing, preview)), /Injected write failure/); await assertEmpty(db);
    }
    const beforeCommit = wrappingPool(db, (sql, args, client) => {
      if (sql === 'COMMIT') throw new Error('Lost before commit'); return client.query(sql, args);
    });
    await assert.rejects(applyAoHelpReview(applyOptions(beforeCommit, preview)), { code: 'AO_HELP_REVIEW_COMMIT_UNCERTAIN' });
    assert.equal((await reconcileAoHelpReview({ pool: db, ...SCOPE })).status, 'not_recorded'); await assertEmpty(db);
    const afterCommit = wrappingPool(db, async (sql, args, client) => {
      const result = await client.query(sql, args); if (sql === 'COMMIT') throw new Error('Lost commit acknowledgement'); return result;
    });
    await assert.rejects(applyAoHelpReview(applyOptions(afterCommit, preview)), { code: 'AO_HELP_REVIEW_COMMIT_UNCERTAIN' });
    assert.equal((await reconcileAoHelpReview({ pool: db, ...SCOPE })).status, 'recorded'); await assertOnlyThree(db);
  });

  await t.test('incomplete or corrupted receipts never report success or repair themselves', async t => {
    const db = await fixture(t); const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    await db.query("INSERT INTO max_operational_ingestions(id,client_id,source_type) VALUES ($1,10,'AO_REPORTED')", [IDS.ingestion]);
    await assert.rejects(applyAoHelpReview(applyOptions(db, preview)), { code: 'AO_HELP_REVIEW_INCOMPLETE_OR_CHANGED_CHAIN' });
    await assert.rejects(reconcileAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_INCOMPLETE_OR_CHANGED_CHAIN' });
    assert.equal((await counts(db)).max_operational_decisions, 0);
    await db.query('DELETE FROM max_operational_ingestions');
    await applyAoHelpReview(applyOptions(db, preview));
    await db.query("UPDATE max_operational_ingestions SET receipt_summary='incorrect persisted receipt'");
    await assert.rejects(reconcileAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_INCOMPLETE_OR_CHANGED_CHAIN' });
    await assert.rejects(applyAoHelpReview(applyOptions(db, preview)), { code: 'AO_HELP_REVIEW_INCOMPLETE_OR_CHANGED_CHAIN' });
    await assertOnlyThree(db);
  });

  await t.test('readback verifies invariant owner, action, evidence, telemetry and exact stored timestamps', async t => {
    const db = await fixture(t); const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    await applyAoHelpReview(applyOptions(db, preview));
    for (const [table, column, value] of [
      ['max_attention_items', 'owner', 'somebody_else'],
      ['max_attention_items', 'priority', { manual_review_only: false }],
      ['max_operational_decisions', 'owner', 'somebody_else'],
      ['max_operational_decisions', 'selected_action', { action_type: 'ESCALATE_OPERATOR', review_only: false }],
      ['max_operational_decisions', 'telemetry', { actions_executed: 1 }],
      ['max_operational_decisions', 'triggering_evidence', []],
      ['max_operational_decisions', 'supporting_evidence', []],
      ['max_operational_decisions', 'trigger_type', 'SCHEDULED_REVIEW'],
    ]) {
      const old = (await db.query(`SELECT ${column} value FROM ${table}`)).rows[0].value;
      // Explicit stringify also keeps JS arrays as JSON rather than PG arrays.
      const bind = input => typeof input === 'object' ? JSON.stringify(input) : input;
      await db.query(`UPDATE ${table} SET ${column}=$1`, [bind(value)]);
      await assert.rejects(reconcileAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_INCOMPLETE_OR_CHANGED_CHAIN' });
      await db.query(`UPDATE ${table} SET ${column}=$1`, [bind(old)]);
      assert.equal((await reconcileAoHelpReview({ pool: db, ...SCOPE })).status, 'recorded');
    }
    await db.query("UPDATE max_operational_decisions SET created_at=created_at+interval '1 microsecond'");
    await assert.rejects(reconcileAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_INCOMPLETE_OR_CHANGED_CHAIN' });
    await assertOnlyThree(db);
  });

  await t.test('postcommit readback failure requires read-only recovery, not another apply', async t => {
    const db = await fixture(t); const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    let leases = 0;
    const failsReadback = { connect: async () => { if (++leases === 2) throw new Error('Injected readback disconnect'); return db.connect(); } };
    await assert.rejects(applyAoHelpReview(applyOptions(failsReadback, preview)), { code: 'AO_HELP_REVIEW_READBACK_REQUIRED' });
    await assertOnlyThree(db);
    const recovery = await reconcileAoHelpReview({ pool: db, ...SCOPE });
    assert.equal(recovery.status, 'recorded'); assert.deepEqual(recovery.ids, IDS);
  });

  await t.test('tenant revocation blocks preview/apply; unrelated-tenant contact cannot change this source version', async t => {
    const db = await fixture(t); const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    await db.query("INSERT INTO touchpoints(client_id,prospect_id,channel,action_type,created_at) VALUES (2,$1,'email','sent',now())", [SCOPE.prospectId]);
    assert.equal((await previewAoHelpReview({ pool: db, ...SCOPE })).source_version, preview.source_version);
    await db.query('UPDATE clients SET active=false WHERE id=10');
    await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_TENANT_UNAVAILABLE' });
    await assert.rejects(applyAoHelpReview(applyOptions(db, preview)), { code: 'AO_HELP_REVIEW_TENANT_UNAVAILABLE' }); await assertEmpty(db);
  });

  await t.test('simultaneous applies produce one durable chain; loser never retries writes', async t => {
    const db = await fixture(t); const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    const outcomes = await Promise.allSettled([applyAoHelpReview(applyOptions(db, preview)), applyAoHelpReview(applyOptions(db, preview))]);
    assert.ok(outcomes.some(r => r.status === 'fulfilled' && r.value.status === 'applied'));
    for (const r of outcomes.filter(r => r.status === 'rejected')) assert.ok(['40001', '23505'].includes(r.reason.code), r.reason.code);
    await assertOnlyThree(db); assert.equal((await reconcileAoHelpReview({ pool: db, ...SCOPE })).status, 'recorded');
  });

  await t.test('concurrent later contact is truthfully outside the saved transaction snapshot, never a commit-time freshness claim', async t => {
    const db = await fixture(t); const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    let inserted = false;
    const concurrent = wrappingPool(db, async (sql, args, client) => {
      if (!inserted && sql.includes('INSERT INTO max_operational_ingestions')) {
        inserted = true;
        await db.query("INSERT INTO touchpoints(client_id,prospect_id,channel,action_type,created_at) VALUES (10,$1,'email','sent',now())", [SCOPE.prospectId]);
      }
      return client.query(sql, args);
    });
    const result = await applyAoHelpReview(applyOptions(concurrent, preview));
    assert.equal(result.status, 'applied'); assert.equal(inserted, true); await assertOnlyThree(db);
    assert.equal(result.freshness_boundary, 'database_transaction_snapshot');
    assert.match(result.receipt_summary, /^As of the database transaction snapshot at /);
    assert.equal(result.outside_contact, 'unknown');
    const receipt = (await db.query('SELECT raw_source FROM max_operational_ingestions')).rows[0].raw_source;
    assert.equal(receipt.snapshot.evidence.touchpoints.length, 0);
    assert.equal(Number((await db.query('SELECT count(*) n FROM touchpoints')).rows[0].n), 1);
    await assert.rejects(previewAoHelpReview({ pool: db, ...SCOPE }), { code: 'AO_HELP_REVIEW_EVIDENCE_REQUIRES_REVIEW' });
  });

  await t.test('scheduler/wake excludes manual review even corrupted due rows; operator queue is authenticated tenant-scoped SELECT-only', async t => {
    const db = await fixture(t); const preview = await previewAoHelpReview({ pool: db, ...SCOPE });
    await applyAoHelpReview(applyOptions(db, preview));
    const store = new PostgresAttentionStore(db, { clientId: 10 });
    const before = (await db.query('SELECT * FROM max_attention_items')).rows;
    assert.deepEqual(await wakeAttentionForSubject({ attentionStore: store, clientId: 10, subjectType: 'prospect', subjectId: SCOPE.prospectId }), []);
    assert.deepEqual(await wakeAttentionForIngestion({ attentionStore: store, clientId: 10, ingestionResult: { prospect: { id: SCOPE.prospectId } }, expectations: [] }), []);
    assert.deepEqual((await db.query('SELECT * FROM max_attention_items')).rows, before);
    await db.query("UPDATE max_attention_items SET review_trigger='EVIDENCE', next_review_at=now()-interval '1 day', priority='{}'");
    assert.deepEqual(await store.claimDueItems({ clientId: 10, claimToken: 'test', limit: 20 }), []);
    for (const priority of [{ manual_review_only: false }, { manual_review_only: null }, { manual_review_only: 'true' }, ['manual_review_only'], 'manual_review_only']) {
      await db.query('UPDATE max_attention_items SET dedup_fingerprint=$1,priority=$2', ['ordinary:fixture', JSON.stringify(priority)]);
      assert.deepEqual(await store.claimDueItems({ clientId: 10, claimToken: 'test', limit: 20 }), []);
    }
    await db.query('UPDATE max_attention_items SET dedup_fingerprint=$1,priority=$2', [BUSINESS_KEY, JSON.stringify(before[0].priority)]);
    const service = serviceWith(db);
    const cycle = await service.runAttentionScheduler(10, { db }); assert.equal(cycle.claimed, 0); await assertOnlyThree(db);
    // Restore creation invariants for exact adapter readback; this is disposable fixture manipulation only.
    await db.query("UPDATE max_attention_items SET review_trigger='MANUAL',next_review_at=null");
    const originalInit = PostgresAttentionStore.prototype.init;
    PostgresAttentionStore.prototype.init = () => { throw new Error('Forbidden initialization'); };
    const routeModule = { exports: {} }; const authModule = { exports: {} };
    const authDeps = { bcryptjs: {}, '../db': db, '../utils/userRoles': require('../utils/userRoles'),
      '../utils/usersRoleConstraint': { refreshUsersRoleConstraint: () => { throw new Error('init forbidden'); } }, '../utils/requestIdentity': require('../utils/requestIdentity') };
    vm.compileFunction(fs.readFileSync(path.join(__dirname, '../middleware/auth.js'), 'utf8'), ['module', 'require'])(authModule, name => {
      assert.ok(Object.hasOwn(authDeps, name), name); return authDeps[name];
    });
    const dependencies = { express, '../db': db, '../middleware/auth': authModule.exports,
      '../utils/maxAttentionAuthorization': require('../utils/maxAttentionAuthorization'), '../services/maxAttentionService': service };
    vm.compileFunction(fs.readFileSync(path.join(__dirname, '../routes/maxAttention.js'), 'utf8'), ['module', 'require'])(routeModule, name => {
      assert.ok(Object.hasOwn(dependencies, name), name); return dependencies[name];
    });
    let session = { user: { id: 25, role: 'manager', client_id: 10 } };
    const app = express(); app.use((req, res, next) => { req.session = session; next(); }); app.use(routeModule.exports);
    const server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
    try {
      const url = `http://127.0.0.1:${server.address().port}/api/v1/max/attention/operator-queue`;
      const response = await fetch(`${url}?client_id=10`); assert.equal(response.status, 200);
      const body = await response.json(); assert.equal(body.count, 1); assert.equal(body.items[0].id, IDS.attention);
      assert.equal(body.items[0].source_decision_id, IDS.decision);
      assert.equal((await fetch(`${url}?client_id=1`)).status, 403);
      session = {}; assert.equal((await fetch(url)).status, 401);
      await assertOnlyThree(db);
    } finally { PostgresAttentionStore.prototype.init = originalInit; await new Promise(resolve => server.close(resolve)); }
  });
});
