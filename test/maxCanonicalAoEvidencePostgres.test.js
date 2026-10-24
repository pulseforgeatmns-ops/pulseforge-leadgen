'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { randomUUID } = require('node:crypto');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { applyCanonicalAoFixture, seedAoActivity } = require('./helpers/canonicalAoPostgres');
const { PostgresStateStore } = require('../packages/max/stateIngestion');
const { PostgresDecisionStore, evaluateOperationalDecision, reevaluateOnIngestion } = require('../packages/max/decisionExecution');
const { PostgresAttentionStore } = require('../packages/max/attention/store/postgresStore');

// Non-skipping, actual-schema regression gate. Every connection is to this
// disposable loopback instance; no DATABASE_URL, routes, providers or credentials.
test('real PostgreSQL canonical AO evidence, scope and ingestion boundaries', { timeout: 90000 }, async t => {
  const instance = await startDisposablePostgres('max-ao-evidence-');
  const db = new Pool({ connectionString: instance.connectionString });
  t.after(async () => { await db.end(); await instance.stop(); });
  await applyCanonicalAoFixture(db);
  const now = new Date('2026-10-05T08:05:00.000Z');
  const end = '2026-10-05T07:00:00.000Z';

  function stores(pool = db, clientId = 1) {
    return { clientId, now, stateStore: new PostgresStateStore(pool, { clientId }),
      decisionStore: new PostgresDecisionStore(pool, { clientId }),
      attentionStore: new PostgresAttentionStore(pool, { clientId }) };
  }
  async function reset() {
    await db.query(`TRUNCATE max_attention_items, max_attention_scheduler_runs,
      max_attention_heartbeats, max_operational_decisions, max_operational_action_intents,
      max_ao_follow_up_tasks, max_open_expectations, ao_prospect_activity, prospects CASCADE;
      INSERT INTO max_attention_heartbeats (id) VALUES ('global')`);
  }
  async function seed({ clientId = 1, aoId = 10, prospectId = randomUUID(),
    status = 'OVERDUE', windowEnd = end, expectationType = 'inbound_call' } = {}) {
    await db.query(`INSERT INTO prospects (id,client_id,assigned_ao_id,status,acquisition_metadata)
      VALUES ($1,$2,$3,'warm','{"maxStateIngestion":{"relationship_active":true}}')
      ON CONFLICT (id) DO NOTHING`, [prospectId, clientId, aoId]);
    return stores(db, clientId).stateStore.createExpectation({ client_id: clientId,
      prospect_id: prospectId, ao_id: aoId, expectation_type: expectationType, status,
      expected_window: { ends_at: windowEnd } });
  }
  async function evaluate(expectation, context = stores(), type = 'NEW_EVIDENCE', extra = {}) {
    return evaluateOperationalDecision({ ...context, expectation,
      trigger: { type, payload: {} }, ...extra });
  }
  async function persistedExpectation(id) {
    return (await db.query('SELECT * FROM max_open_expectations WHERE id=$1', [id])).rows[0];
  }
  async function count(table) {
    return (await db.query(`SELECT count(*)::integer AS count FROM ${table}`)).rows[0].count;
  }
  function assertAbstained(result) {
    assert.equal(result.decision.selected_action.action_type, 'ABSTAIN_INSUFFICIENT_EVIDENCE');
    assert.equal(result.decision.execution_status, 'INSUFFICIENT_EVIDENCE');
    assert.equal(result.intent, null);
  }
  async function waitingAttention(expectation) {
    const item = { id: `fixture-${randomUUID()}`, client_id: expectation.client_id,
      subject_type: 'expectation', subject_id: expectation.id, reason: 'Pending canonical evidence',
      status: 'WAITING', next_review_at: '2026-10-08T00:00:00.000Z', review_trigger: 'TIME',
      audience_tier: 'WAITING_SILENT', dedup_fingerprint: randomUUID() };
    await stores(db, expectation.client_id).attentionStore.createAttention(item);
    return item;
  }
  function resultsOf(result) { return result.results || [result]; }
  const isActivityRead = sql => /(?:FROM|JOIN) ao_prospect_activity/.test(sql);

  await t.test('actual AO migration enforces tenant/prospect FK; activity reads retain provenance and filter AO/tenant', async () => {
    await reset();
    const own = await seed();
    const otherTenant = await seed({ clientId: 2, aoId: 11 });
    const reader = stores().stateStore;
    const empty = await reader.readProspectActivities(own.prospect_id, { clientId: 1, aoId: 10 });
    assert.equal(empty.status, 'complete');
    assert.deepEqual(empty.activities, []);
    assert.equal(empty.source, 'ao_prospect_activity');
    const positive = await seedAoActivity(db, own, { outcome: 'spoke_decision_maker', activityType: 'call' });
    await seedAoActivity(db, own, { aoId: 11 });
    await seedAoActivity(db, otherTenant);
    await assert.rejects(db.query(`INSERT INTO ao_prospect_activity
      (prospect_id,tenant_id,ao_id,activity_type,outcome) VALUES ($1,2,11,'call','spoke_decision_maker')`,
      [own.prospect_id]), { code: '23503' });
    const read = await reader.readProspectActivities(own.prospect_id, { clientId: 1, aoId: 10 });
    assert.equal(read.status, 'complete');
    assert.equal(read.activities.length, 1);
    const activity = read.activities[0];
    assert.equal(activity.id, positive.id);
    assert.equal(activity.tenant_id, 1);
    assert.equal(activity.prospect_id, own.prospect_id);
    assert.equal(activity.ao_id, 10);
    assert.equal(activity.kind, 'call_completed');
    assert.equal(activity.source, 'ao_prospect_activity');
    assert.equal(new Date(activity.at).toISOString(), positive.created_at.toISOString());
    assert.deepEqual(activity.metadata, { internal_outcome: 'spoke_decision_maker' });
    for (const scope of [{ clientId: 2, aoId: 10 }, { clientId: 1, aoId: 11 },
      { clientId: 1 }, { clientId: 1, aoId: 10, prospectId: otherTenant.prospect_id }]) {
      const result = await reader.readProspectActivities(scope.prospectId || own.prospect_id, scope);
      assert.equal(result.status, 'unavailable');
      assert.deepEqual(result.activities, []);
    }
  });

  for (const { activityType, outcome } of [
    { activityType: 'call', outcome: 'spoke_decision_maker' },
    { activityType: 'outcome_logged', outcome: 'booked_walkthrough' },
    { activityType: 'walkthrough_booked', outcome: 'booked_walkthrough' },
  ]) {
    await t.test(`${activityType}/${outcome} resolves durably and supersedes only its stale follow-up`, async () => {
      await reset();
      const own = await seed();
      const unrelated = await seed();
      const prior = await evaluate(own, stores(), 'EXPECTATION_OVERDUE');
      const otherPrior = await evaluate(unrelated, stores(), 'EXPECTATION_OVERDUE');
      const activity = await seedAoActivity(db, own, { activityType, outcome });
      // Forged caller fields cannot supply evidence or overwrite canonical ownership.
      const result = await evaluate(own, stores(), 'NEW_EVIDENCE', { prospect: {
        id: own.prospect_id, client_id: 2, assigned_ao_id: 11,
        activities: [{ kind: 'meeting_scheduled', at: now.toISOString() }],
      } });
      assert.equal(result.decision.selected_action.action_type, 'RESOLVE_EXPECTATION');
      assert.equal(result.decision.execution_status, 'EXECUTED');
      assert.equal(result.decision.verification_status, 'VERIFIED');
      assert.equal(result.snapshot.account.owner_id, 10);
      assert.ok(result.decision.supporting_evidence.some(e => e.id === activity.id
        && e.source === 'ao_prospect_activity' && e.tenant_id === 1 && e.ao_id === 10));
      const row = await persistedExpectation(own.id);
      assert.equal(row.status, 'RESOLVED');
      assert.ok(row.resolved_at instanceof Date);
      const priorState = (await db.query('SELECT execution_status FROM max_operational_decisions WHERE id=$1', [prior.decision.id])).rows[0];
      assert.equal(priorState.execution_status, 'SUPERSEDED');
      assert.equal((await db.query('SELECT status FROM max_ao_follow_up_tasks WHERE decision_id=$1', [prior.decision.id])).rows[0].status, 'superseded');
      assert.equal((await persistedExpectation(unrelated.id)).status, 'OVERDUE');
      assert.equal((await db.query('SELECT status FROM max_ao_follow_up_tasks WHERE decision_id=$1', [otherPrior.decision.id])).rows[0].status, 'open');
    });
  }

  await t.test('bounded context reads only the requested canonical subject; payload and foreign AO evidence cannot resolve it', async () => {
    await reset();
    const own = await seed();
    const sibling = await seed();
    const foreign = await seed({ clientId: 2, aoId: 11 });
    await seedAoActivity(db, own, { aoId: 11 });
    await seedAoActivity(db, sibling);
    await seedAoActivity(db, foreign);
    const queries = [];
    const tracedDb = { query: async (sql, args) => {
      queries.push({ sql, args });
      return db.query(sql, args);
    } };
    const context = stores();
    context.stateStore = new PostgresStateStore(tracedDb, { clientId: 1 });
    context.stateStore.snapshotContext = async () => assert.fail('bounded decision context must not load a full CRM snapshot');
    const result = await evaluate(own, context, 'NEW_EVIDENCE', {
      prospect: { id: own.prospect_id, client_id: 1, assigned_ao_id: 11,
        activities: [{ kind: 'meeting_scheduled', at: now.toISOString() }] },
      trigger: { type: 'NEW_EVIDENCE', payload: { prospect_id: own.prospect_id,
        evidence: [{ kind: 'meeting_scheduled', at: now.toISOString() }] } },
    });
    assert.equal(result.snapshot.account.owner_id, 10);
    assert.equal(result.snapshot.subsequent_activity_count, 0);
    assert.equal(result.snapshot.has_resolving_activity, false);
    assert.equal(result.decision.selected_action.action_type, 'ASK_AO_STATUS');
    assert.equal((await persistedExpectation(own.id)).status, 'OVERDUE');
    const activityQueries = queries.filter(({ sql }) => isActivityRead(sql));
    assert.equal(activityQueries.length, 1);
    assert.deepEqual(activityQueries[0].args, [1, own.prospect_id, 10]);
    assert.match(activityQueries[0].sql, /p\.client_id = \$1 AND p\.id = \$2 AND p\.assigned_ao_id = \$3/);
    assert.ok(queries.every(({ args }) => !args?.includes(sibling.prospect_id) && !args?.includes(foreign.prospect_id)));
    assert.ok(queries.every(({ sql }) => !/FROM users|FROM companies/.test(sql)));
  });

  await t.test('negative outcomes, notes, unsupported types and contradictory metadata never resolve', async () => {
    await reset();
    for (const row of [
      { outcome: 'called_no_answer', activityType: 'call' },
      { outcome: 'left_voicemail', activityType: 'call' },
      { outcome: 'spoke_gatekeeper', activityType: 'call' },
      { outcome: 'not_interested' }, { outcome: 'interested' }, { outcome: 'needs_jake' },
      { outcome: 'booked_walkthrough', activityType: 'note', notes: 'They called. Walkthrough booked.' },
      { outcome: 'spoke_decision_maker', activityType: 'email' },
      { outcome: 'booked_walkthrough', metadata: { internal_outcome: 'called_no_answer' } },
      { outcome: null, activityType: 'note', metadata: { kind: 'meeting_scheduled' } },
    ]) {
      const exp = await seed();
      await seedAoActivity(db, exp, row);
      const result = await evaluate(exp);
      assert.equal(result.snapshot.activity_read_status, 'complete');
      assert.equal(result.snapshot.has_resolving_activity, false, JSON.stringify(row));
      assert.notEqual(result.decision.selected_action.action_type, 'RESOLVE_EXPECTATION');
      assert.equal((await persistedExpectation(exp.id)).status, 'OVERDUE');
      assert.equal((await persistedExpectation(exp.id)).resolved_at, null);
    }
  });

  await t.test('explicit empty history is known, a failed database history read is unavailable and abstains', async () => {
    await reset();
    const exp = await seed();
    const empty = await evaluate(exp);
    assert.equal(empty.snapshot.activity_read_status, 'complete');
    assert.equal(empty.snapshot.subsequent_activity_count, 0);
    assert.equal(empty.snapshot.epistemic.subsequent_activity, 'KNOWN');
    assert.equal(empty.decision.selected_action.action_type, 'ASK_AO_STATUS');
    const unavailableDb = { query: async (sql, args) => {
      if (isActivityRead(sql)) throw Error('local fixture: activity database unavailable');
      return db.query(sql, args);
    } };
    const reader = new PostgresStateStore(unavailableDb, { clientId: 1 });
    const unavailable = await reader.readProspectActivities(exp.prospect_id, { clientId: 1, aoId: 10 });
    assert.equal(unavailable.status, 'unavailable');
    assert.deepEqual(unavailable.activities, []);
    const context = stores();
    context.stateStore = reader;
    const result = await evaluate(exp, context);
    assertAbstained(result);
    assert.equal(result.snapshot.activity_read_status, 'unavailable');
    assert.equal(result.snapshot.subsequent_activity_count, null);
    assert.equal(result.snapshot.epistemic.subsequent_activity, 'UNKNOWN');
    assert.equal((await persistedExpectation(exp.id)).status, 'OVERDUE');
    assert.equal(await count('max_operational_action_intents'), 1, 'only earlier known-empty AO task has an intent');
  });

  await t.test('history before the window, future timestamps and unrelated expectation types do not resolve', async () => {
    await reset();
    for (const fixture of [
      { createdAt: '2026-10-05T06:59:59.999Z' },
      { createdAt: '2026-10-05T08:05:00.001Z' },
      { expectationType: 'proposal_reply', createdAt: '2026-10-05T08:00:00.000Z' },
    ]) {
      const exp = await seed(fixture);
      await seedAoActivity(db, exp, fixture);
      const result = await evaluate(exp);
      assert.equal(result.snapshot.has_resolving_activity, false);
      assert.equal((await persistedExpectation(exp.id)).status, 'OVERDUE');
    }
    const boundary = await seed();
    await seedAoActivity(db, boundary, { createdAt: end });
    assert.equal((await evaluate(boundary)).decision.verification_status, 'VERIFIED');
  });

  await t.test('caller-supplied stale status/window are refreshed and missing or mismatched rows abstain without intent', async () => {
    await reset();
    const future = await seed({ status: 'WAITING', windowEnd: '2026-10-06T00:00:00.000Z' });
    await seedAoActivity(db, future);
    const result = await evaluate({ ...future, status: 'OVERDUE', expected_window: { ends_at: end } });
    assert.equal(result.snapshot.expectation.status, 'WAITING');
    assert.deepEqual(result.snapshot.expectation.expected_window, future.expected_window);
    assert.equal(result.snapshot.has_resolving_activity, false);
    assert.equal(result.decision.selected_action.action_type, 'NO_ACTION');
    assert.equal((await persistedExpectation(future.id)).status, 'WAITING');
    const overdue = await seed();
    await seedAoActivity(db, overdue);
    const refreshed = await evaluate({ ...overdue, status: 'RESOLVED',
      expected_window: { ends_at: '2026-10-06T00:00:00.000Z' } });
    assert.equal(refreshed.snapshot.expectation.status, 'OVERDUE');
    assert.equal(refreshed.snapshot.expectation.expected_window.ends_at, end);
    assert.equal(refreshed.decision.selected_action.action_type, 'RESOLVE_EXPECTATION');
    assert.equal(refreshed.decision.verification_status, 'VERIFIED');
    for (const failure of ['missing', 'wrong tenant', 'wrong AO', 'wrong prospect', 'read failure']) {
      const exp = await seed();
      await seedAoActivity(db, exp);
      const supplied = { ...exp };
      let context = stores();
      if (failure === 'missing') await db.query('DELETE FROM max_open_expectations WHERE id=$1', [exp.id]);
      if (failure === 'wrong tenant') await db.query('UPDATE max_open_expectations SET client_id=2 WHERE id=$1', [exp.id]);
      if (failure === 'wrong AO') await db.query('UPDATE max_open_expectations SET ao_id=11 WHERE id=$1', [exp.id]);
      if (failure === 'wrong prospect') await db.query('UPDATE max_open_expectations SET prospect_id=$2 WHERE id=$1', [exp.id, randomUUID()]);
      if (failure === 'read failure') {
        context.stateStore = new PostgresStateStore({ query: async (sql, args) => {
          if (/SELECT \* FROM max_open_expectations/.test(sql)) throw Error('local expectation read failure');
          return db.query(sql, args);
        } }, { clientId: 1 });
      }
      assertAbstained(await evaluate(supplied, context));
    }
    assert.equal(await count('max_operational_action_intents'), 1, 'only the refreshed overdue expectation has an intent');
  });

  await t.test('fresh-store replay prefers verified resolution over an older NEW_EVIDENCE no-action receipt', async () => {
    await reset();
    const exp = await seed({ status: 'WAITING', windowEnd: '2026-10-06T00:00:00.000Z' });
    const waiting = await evaluate(exp);
    assert.equal(waiting.decision.selected_action.action_type, 'NO_ACTION');
    assert.equal(waiting.decision.execution_status, 'NO_ACTION_RECORDED');
    assert.equal(waiting.intent, null);
    await db.query(`UPDATE max_open_expectations
      SET status='OVERDUE', expected_window=$2::jsonb WHERE id=$1`,
      [exp.id, JSON.stringify({ ends_at: end })]);
    await seedAoActivity(db, exp, { activityType: 'call', outcome: 'spoke_decision_maker' });
    const resolved = await evaluate(exp);
    assert.equal(resolved.decision.selected_action.action_type, 'RESOLVE_EXPECTATION');
    assert.equal(resolved.decision.execution_status, 'EXECUTED');
    assert.equal(resolved.decision.verification_status, 'VERIFIED');
    assert.notEqual(resolved.decision.id, waiting.decision.id);
    const beforeReplay = await persistedExpectation(exp.id);
    const queries = [];
    const replayDb = { query: async (sql, args) => {
      queries.push(sql);
      assert.doesNotMatch(sql, /UPDATE max_open_expectations/, 'replay must only read the persisted resolution');
      return db.query(sql, args);
    } };
    const replay = await evaluate(exp, stores(replayDb));
    assert.equal(replay.duplicate, true);
    assert.equal(replay.snapshot.expectation.status, 'RESOLVED');
    assert.equal(replay.decision.id, resolved.decision.id);
    assert.notEqual(replay.decision.id, waiting.decision.id);
    assert.equal(replay.decision.selected_action.action_type, 'RESOLVE_EXPECTATION');
    assert.equal(replay.decision.execution_status, 'EXECUTED');
    assert.equal(replay.decision.verification_status, 'VERIFIED');
    assert.equal(replay.intent.id, resolved.intent.id);
    assert.equal(await count('max_operational_decisions'), 2);
    assert.equal(await count('max_operational_action_intents'), 1);
    assert.equal(queries.filter(sql => /SELECT \* FROM max_open_expectations/.test(sql)).length, 2,
      'fresh canonical refresh is followed by scoped durable resolution readback');
    const afterReplay = await persistedExpectation(exp.id);
    assert.equal(afterReplay.resolved_at.toISOString(), beforeReplay.resolved_at.toISOString());
    assert.equal(afterReplay.updated_at.toISOString(), beforeReplay.updated_at.toISOString());
    assert.equal((await db.query('SELECT execution_status FROM max_operational_decisions WHERE id=$1',
      [waiting.decision.id])).rows[0].execution_status, 'SUPERSEDED');
  });

  await t.test('ownership change between prospect and activity reads is unavailable, never a fabricated empty history', async () => {
    await reset();
    const exp = await seed();
    await seedAoActivity(db, exp);
    let changed = false;
    const racingDb = { query: async (sql, args) => {
      if (isActivityRead(sql) && !changed) {
        changed = true;
        await db.query('UPDATE prospects SET assigned_ao_id=11 WHERE id=$1', [exp.prospect_id]);
      }
      return db.query(sql, args);
    } };
    const context = stores();
    context.stateStore = new PostgresStateStore(racingDb, { clientId: 1 });
    const result = await evaluate(exp, context);
    assert.equal(changed, true);
    assertAbstained(result);
    assert.equal(result.snapshot.activity_read_status, 'unavailable');
    assert.equal(result.snapshot.subsequent_activity_count, null);
    assert.equal(await count('max_operational_action_intents'), 0);
    assert.equal(await count('max_ao_follow_up_tasks'), 0);
    assert.equal((await persistedExpectation(exp.id)).status, 'OVERDUE');
  });

  await t.test('ownership changing after evidence selection prevents scoped resolution and preserves prior follow-up', async () => {
    await reset();
    const exp = await seed();
    const prior = await evaluate(exp, stores(), 'EXPECTATION_OVERDUE');
    await seedAoActivity(db, exp);
    let changed = false;
    const racingDb = { query: async (sql, args) => {
      const result = await db.query(sql, args);
      if (isActivityRead(sql) && !changed) {
        changed = true;
        await db.query('UPDATE prospects SET assigned_ao_id=11 WHERE id=$1', [exp.prospect_id]);
      }
      return result;
    } };
    const context = stores();
    context.stateStore = new PostgresStateStore(racingDb, { clientId: 1 });
    const result = await evaluate(exp, context);
    assert.equal(changed, true);
    assert.equal(result.decision.selected_action.action_type, 'RESOLVE_EXPECTATION');
    assert.notEqual(result.decision.execution_status, 'EXECUTED');
    assert.equal(result.decision.verification_status, 'VERIFICATION_FAILED');
    assert.equal((await persistedExpectation(exp.id)).status, 'OVERDUE');
    assert.equal((await persistedExpectation(exp.id)).resolved_at, null);
    assert.equal((await db.query('SELECT execution_status FROM max_operational_decisions WHERE id=$1', [prior.decision.id])).rows[0].execution_status, 'EXECUTED');
    assert.equal((await db.query('SELECT status FROM max_ao_follow_up_tasks WHERE decision_id=$1', [prior.decision.id])).rows[0].status, 'open');
  });

  await t.test('ingestion evaluates every independently scoped expectation across supplied prospects using durable listing', async () => {
    await reset();
    const first = await seed();
    const future = await seed({ prospectId: first.prospect_id, status: 'WAITING', windowEnd: '2026-10-06T00:00:00.000Z' });
    const second = await seed({ aoId: 11 });
    const foreign = await seed({ clientId: 2, aoId: 11 });
    await seedAoActivity(db, first);
    await seedAoActivity(db, second, { outcome: 'spoke_decision_maker' });
    await seedAoActivity(db, foreign);
    const context = stores();
    assert.equal(context.stateStore.expectations, undefined, 'real Postgres has no process-local expectation array');
    const result = await reevaluateOnIngestion({ ...context, ingestionResult: {
      ingestion_id: 'local-multiple-subjects', prospects: [{ id: first.prospect_id }, { id: second.prospect_id }],
    } });
    const evaluations = resultsOf(result);
    assert.equal(evaluations.length, 3);
    assert.deepEqual(new Set(evaluations.map(r => r.decision.subject_id)), new Set([first.id, future.id, second.id]));
    assert.equal((await persistedExpectation(first.id)).status, 'RESOLVED');
    assert.equal((await persistedExpectation(second.id)).status, 'RESOLVED');
    assert.equal((await persistedExpectation(future.id)).status, 'WAITING');
    assert.equal((await persistedExpectation(foreign.id)).status, 'OVERDUE');
    assert.equal(await count('max_operational_action_intents'), 2);
  });

  await t.test('blocked and ambiguous ingestion do not wake or evaluate an arbitrary subject', async () => {
    await reset();
    const first = await seed();
    const second = await seed();
    const items = [await waitingAttention(first), await waitingAttention(second)];
    for (const ingestionResult of [{ ingestion_id: 'local-ambiguous' },
      { ingestion_id: 'local-blocked', prospect: { id: first.prospect_id }, commit_blocked: true },
      { ingestion_id: 'local-clarification', prospect: { id: first.prospect_id }, clarification_required: 'Which account?' }]) {
      const result = await reevaluateOnIngestion({ ...stores(), ingestionResult });
      assert.equal(result.skipped, true);
    }
    assert.equal(await count('max_operational_decisions'), 0);
    for (const item of items) {
      const row = await stores().attentionStore.findById(item.id);
      assert.equal(row.status, 'WAITING');
      assert.equal(row.review_trigger, 'TIME');
      assert.equal(row.next_review_at, item.next_review_at);
    }
  });

  await t.test('single-subject fallback is canonical and attention wake stays scoped to that prospect and tenant', async () => {
    await reset();
    const own = await seed();
    const foreign = await seed({ clientId: 2, aoId: 11 });
    const ownItem = await waitingAttention(own);
    const foreignItem = await waitingAttention(foreign);
    await seedAoActivity(db, own);
    const result = await reevaluateOnIngestion({ ...stores(), ingestionResult: { ingestion_id: 'local-one-subject' } });
    assert.equal(result.decision.subject_id, own.id);
    assert.equal((await persistedExpectation(own.id)).status, 'RESOLVED');
    const ownAfter = await stores().attentionStore.findById(ownItem.id);
    assert.equal(ownAfter.review_trigger, 'EVIDENCE');
    assert.ok(ownAfter.supporting_evidence.some(e => e.ingestion_id === 'local-one-subject'));
    const foreignAfter = await stores(db, 2).attentionStore.findById(foreignItem.id);
    assert.equal(foreignAfter.status, 'WAITING');
    assert.equal(foreignAfter.review_trigger, 'TIME');
  });

  for (const failure of ['prospect read', 'expectation list', 'expectation refresh', 'activity read']) {
    await t.test(`all ingestion target reads finish before wakes or actions: ${failure}`, async () => {
      await reset();
      const first = await seed();
      const second = await seed();
      const items = [await waitingAttention(first), await waitingAttention(second)];
      await seedAoActivity(db, first);
      await seedAoActivity(db, second);
      let injected = false;
      const failingDb = { query: async (sql, args) => {
        const secondTarget = args?.some(arg => arg === second.prospect_id || arg === second.id);
        const relevant = failure === 'prospect read' ? /FROM prospects p/.test(sql)
          : failure === 'expectation list' ? /FROM max_open_expectations/.test(sql) && /status IN/.test(sql)
            : failure === 'expectation refresh' ? /SELECT \* FROM max_open_expectations/.test(sql) && /WHERE id =/.test(sql)
              : isActivityRead(sql);
        if (secondTarget && relevant) { injected = true; throw Error(`local second-target ${failure} failure`); }
        return db.query(sql, args);
      } };
      const result = await reevaluateOnIngestion({ ...stores(failingDb), ingestionResult: {
        ingestion_id: 'local-read-all', prospects: [{ id: first.prospect_id }, { id: second.prospect_id }],
      } });
      assert.equal(injected, true);
      assert.equal(result.skipped, true);
      assert.equal(await count('max_operational_decisions'), 0);
      assert.equal(await count('max_operational_action_intents'), 0);
      assert.equal((await persistedExpectation(first.id)).status, 'OVERDUE');
      for (const item of items) {
        const row = await stores().attentionStore.findById(item.id);
        assert.equal(row.status, 'WAITING');
        assert.equal(row.next_review_at, item.next_review_at);
      }
    });
  }

  await t.test('scope-less same-ingestion retry recovers supersession failure without reopening resolution or unrelated history', async () => {
    await reset();
    const exp = await seed();
    const unrelated = await seed();
    const prior = await evaluate(exp, stores(), 'EXPECTATION_OVERDUE');
    const unrelatedPrior = await evaluate(unrelated, stores(), 'EXPECTATION_OVERDUE');
    // Keep an unrelated historical task as a scoping sentinel, but exactly one
    // expectation remains open when this scope-less ingestion first runs.
    await db.query("UPDATE max_open_expectations SET status='RESOLVED', resolved_at=NOW() WHERE id=$1", [unrelated.id]);
    await seedAoActivity(db, exp, { activityType: 'call', outcome: 'spoke_decision_maker' });
    const ingestionResult = { ingestion_id: 'local-scopeless-supersession-retry' };
    const error = new Error('local supersession write failure');
    let injected = false;
    const failingDb = { query: async (sql, args) => {
      if (!injected && /UPDATE max_operational_decisions/.test(sql)) {
        injected = true;
        throw error;
      }
      return db.query(sql, args);
    } };
    await assert.rejects(reevaluateOnIngestion({ ...stores(failingDb), ingestionResult }), error);
    assert.equal(injected, true);
    const beforeRetry = await persistedExpectation(exp.id);
    assert.equal(beforeRetry.status, 'RESOLVED');
    assert.ok(beforeRetry.resolved_at instanceof Date);
    const resolution = (await db.query(`SELECT * FROM max_operational_decisions
      WHERE client_id=1 AND subject_id=$1 AND selected_action->>'action_type'='RESOLVE_EXPECTATION'`, [exp.id])).rows[0];
    const intent = (await db.query('SELECT * FROM max_operational_action_intents WHERE decision_id=$1', [resolution.id])).rows[0];
    assert.equal(intent.execution_status, 'EXECUTED');
    assert.equal(intent.verification_status, 'VERIFIED');
    assert.equal((await db.query('SELECT status FROM max_ao_follow_up_tasks WHERE decision_id=$1', [prior.decision.id])).rows[0].status, 'open');
    // A new account becomes the only open subject after the first resolution.
    // Its existence must not redirect or block recovery of the original receipt.
    const newcomer = await seed({ aoId: 11 });
    const newcomerItem = await waitingAttention(newcomer);
    const retryDb = { query: async (sql, args) => {
      assert.doesNotMatch(sql, /UPDATE max_open_expectations/, 'verified same-ingestion recovery must never repeat resolution');
      return db.query(sql, args);
    } };
    const retry = await reevaluateOnIngestion({ ...stores(retryDb), ingestionResult });
    assert.equal(retry.skipped, undefined);
    assert.equal(retry.duplicate, true);
    assert.equal(retry.decision.id, resolution.id);
    assert.equal(retry.decision.execution_status, 'EXECUTED');
    assert.equal(retry.decision.verification_status, 'VERIFIED');
    assert.equal(retry.intent.id, intent.id);
    const replay = await reevaluateOnIngestion({ ...stores(retryDb), ingestionResult });
    assert.equal(replay.duplicate, true);
    assert.equal(replay.decision.id, resolution.id);
    assert.equal(replay.intent.id, intent.id);
    assert.equal(await count('max_operational_decisions'), 3);
    assert.equal(await count('max_operational_action_intents'), 3);
    assert.equal((await persistedExpectation(exp.id)).resolved_at.toISOString(), beforeRetry.resolved_at.toISOString());
    assert.equal((await persistedExpectation(exp.id)).updated_at.toISOString(), beforeRetry.updated_at.toISOString());
    assert.equal((await db.query('SELECT status FROM max_ao_follow_up_tasks WHERE decision_id=$1', [prior.decision.id])).rows[0].status, 'superseded');
    assert.equal((await db.query('SELECT status FROM max_ao_follow_up_tasks WHERE decision_id=$1', [unrelatedPrior.decision.id])).rows[0].status, 'open');
    assert.equal((await db.query('SELECT execution_status FROM max_operational_decisions WHERE id=$1', [unrelatedPrior.decision.id])).rows[0].execution_status, 'EXECUTED');
    assert.equal((await persistedExpectation(newcomer.id)).status, 'OVERDUE');
    const newcomerAfter = await stores().attentionStore.findById(newcomerItem.id);
    assert.equal(newcomerAfter.status, 'WAITING');
    assert.equal(newcomerAfter.review_trigger, 'TIME');
    assert.equal(newcomerAfter.next_review_at, newcomerItem.next_review_at);
    assert.ok(!newcomerAfter.supporting_evidence.some(e => e.ingestion_id === ingestionResult.ingestion_id));
    // With a different ingestion ID, two current open subjects remain ambiguous;
    // this original resolution receipt must not supply an unrelated target.
    await seed({ aoId: 11 });
    const unmatched = await reevaluateOnIngestion({ ...stores(), ingestionResult: { ingestion_id: 'local-unrelated-history' } });
    assert.equal(unmatched.skipped, true);
    assert.equal(unmatched.reason, 'ambiguous_ingestion_subject');
    assert.equal(await count('max_operational_action_intents'), 3);
  });

  for (const failure of ['attention wake', 'decision persistence', 'intent persistence']) {
    await t.test(`ingestion ${failure} error is not disguised as skipped; retry is truthful`, async () => {
      await reset();
      const exp = await seed();
      await waitingAttention(exp);
      await seedAoActivity(db, exp);
      const pattern = failure === 'attention wake' ? /UPDATE max_attention_items/
        : failure === 'decision persistence' ? /INSERT INTO max_operational_decisions/
          : /INSERT INTO max_operational_action_intents/;
      let injected = false;
      const error = new Error(`local ${failure} write failure`);
      const failingDb = { query: async (sql, args) => {
        if (!injected && pattern.test(sql)) { injected = true; throw error; }
        return db.query(sql, args);
      } };
      const ingestionResult = { ingestion_id: 'local-truthful-retry', prospect: { id: exp.prospect_id } };
      await assert.rejects(reevaluateOnIngestion({ ...stores(failingDb), ingestionResult }), error);
      assert.equal(injected, true);
      const beforeRetry = await persistedExpectation(exp.id);
      const retryDb = { query: async (sql, args) => {
        if (failure === 'intent persistence') {
          assert.doesNotMatch(sql, /UPDATE max_open_expectations/, 'a missing historical intent cannot authorize repeating resolution');
        }
        return db.query(sql, args);
      } };
      const retry = await reevaluateOnIngestion({ ...stores(retryDb), ingestionResult });
      assert.equal(retry.skipped, undefined);
      if (failure === 'intent persistence') {
        assert.equal(retry.duplicate, true);
        assert.notEqual(retry.decision.execution_status, 'EXECUTED');
        assert.equal(retry.decision.verification_status, 'VERIFICATION_FAILED');
        assert.equal(await count('max_operational_action_intents'), 0);
        assert.equal((await persistedExpectation(exp.id)).resolved_at.toISOString(), beforeRetry.resolved_at.toISOString());
      } else {
        assert.equal(retry.decision.execution_status, 'EXECUTED');
        assert.equal(retry.decision.verification_status, 'VERIFIED');
        assert.equal((await persistedExpectation(exp.id)).status, 'RESOLVED');
        assert.equal(await count('max_operational_action_intents'), 1);
      }
      const replay = await reevaluateOnIngestion({ ...stores(retryDb), ingestionResult });
      assert.equal(replay.duplicate, true);
      assert.equal(replay.decision.id, retry.decision.id);
      assert.equal(replay.decision.verification_status, retry.decision.verification_status);
      assert.equal(await count('max_operational_action_intents'), failure === 'intent persistence' ? 0 : 1);
      assert.equal(await count('max_operational_decisions'), 1);

    });
  }
});
