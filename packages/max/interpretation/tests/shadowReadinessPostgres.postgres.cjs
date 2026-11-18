'use strict';

/**
 * P0-MAX-REASONING-001 PR 2: semantic-shadow migration and persistence
 * readiness against a disposable PostgreSQL instance. Never touches a real
 * database. Run: node --test packages/max/interpretation/tests/shadowReadinessPostgres.postgres.cjs
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('../../../../test/helpers/disposablePostgres');
const { ShadowInterpreter } = require('../ShadowInterpreter');
const { ModelFrameProducer } = require('../producers/ModelFrameProducer');
const { countExpired, purgeExpired } = require('../shadowRetention');
const { review } = require('../../../../scripts/maxInterpretationShadowReview');
const { resetObserverHealth, observerHealthSnapshot } = require('../../observability/ObserverHealth');

const MIGRATION = fs.readFileSync('migrations/2026-11-09-max-interpretation-shadow.sql', 'utf8');
const ROLLBACK = fs.readFileSync('migrations/2026-11-09-max-interpretation-shadow.rollback.sql', 'utf8');
const MODEL = 'claude-sonnet-5-5';
const SECRET_TEXT = 'Confidential operator text about Acme Plumbing';
const SECRET_LABEL = 'Sensitive objective label for Acme Plumbing';
const PROMPT = 'Pending prompt: approve outreach to jane@acme.example';

const frame = (over = {}) => ({
  intent: 'question', subjectKind: 'mission', relationToPrior: 'new_topic', referents: [],
  evidenceNeeds: [], facets: ['status'], requestedOutcome: 'status',
  requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
  scope: 'focused', foreignTenantReference: false, ambiguities: [], confidence: 0.9, ...over,
});
const ok = (f, over = {}) => ({ stop_reason: 'end_turn', model: MODEL, content: [{ type: 'text', text: JSON.stringify(f) }], usage: { input_tokens: 700, output_tokens: 60 }, ...over });
const producer = (respond) => new ModelFrameProducer({ model: MODEL, client: { messages: { create: async () => respond() } } });

test('PostgreSQL: semantic-shadow migration, persistence, tenant isolation, privacy, retention, failure, rollback', { timeout: 120000 }, async (t) => {
  const instance = await startDisposablePostgres('max-shadow-ready-');
  const db = new Pool({ connectionString: instance.connectionString });
  t.after(async () => { await db.end(); await instance.stop(); });
  const env = { MAX_INTERPRETATION_SHADOW: 'true', MAX_INTERPRETATION_MODEL: MODEL, DATABASE_URL: instance.connectionString, DATABASE_SSL: 'false' };

  await t.test('migration is additive and idempotent, with the expected shape', async () => {
    await db.query(MIGRATION);
    await db.query(MIGRATION);
    const cols = (await db.query(`SELECT column_name, data_type, is_nullable FROM information_schema.columns
      WHERE table_name = 'max_interpretation_shadow' ORDER BY ordinal_position`)).rows;
    assert.deepEqual(cols.map((c) => [c.column_name, c.data_type, c.is_nullable]), [
      ['id', 'uuid', 'NO'], ['tenant_id', 'text', 'NO'], ['observed_at', 'timestamp with time zone', 'NO'], ['observation', 'jsonb', 'NO'],
    ]);
    const idx = (await db.query(`SELECT indexdef FROM pg_indexes WHERE tablename = 'max_interpretation_shadow' AND indexname = 'max_interpretation_shadow_tenant_recent'`)).rows;
    assert.match(idx[0].indexdef, /\(tenant_id, observed_at DESC\)/);
    await assert.rejects(
      db.query(`INSERT INTO max_interpretation_shadow VALUES (gen_random_uuid(), 't', now(), '{"shadowOnly":false}')`),
      /check constraint/,
    );
  });

  await t.test('observer persists truthful rows per tenant without message, label or prompt text', async () => {
    resetObserverHealth();
    const cases = [
      ['t1', () => ok(frame()), false],
      ['t1', () => Promise.reject(Object.assign(new Error('down'), { status: 500 })), false],
      ['t1', () => ok(frame(), { stop_reason: 'max_tokens' }), false],
      ['t1', null, true], // authorization bound to a captured pending decision
      ['t2', () => ok(frame()), false],
    ];
    for (const [tenant, respond, bound] of cases) {
      const context = { tenantId: tenant, candidates: [{ id: 'm_x', kind: 'mission', label: SECRET_LABEL }], focusIds: [], pendingDecisionId: null, pendingDecisionKind: null };
      const observer = new ShadowInterpreter({ env, producer: producer(respond || (() => ok(frame({
        intent: 'authorization', subjectKind: 'pending_decision', requestedOutcome: 'action',
        requestedAction: { verb: 'approve', targetCandidateId: context.pendingDecisionId, polarity: 'affirmative' },
      })))) });
      if (bound) observer.captureMission(context, { id: 'm_x', tenantId: tenant, pendingOperatorDecision: { kind: 'plan_approval', prompt: PROMPT } });
      observer.observe({ context, text: SECRET_TEXT, result: { route: 'inspection', workspaceOwnership: { owner: 'mission_inspection' } } });
      await observer.close();
      assert.equal(observer.counts.completed, 1);
    }
    const rows = (await db.query('SELECT tenant_id, observation FROM max_interpretation_shadow ORDER BY observed_at')).rows;
    assert.equal(rows.length, 5);
    const dump = JSON.stringify(rows);
    for (const secret of [SECRET_TEXT, SECRET_LABEL, PROMPT, 'jane@acme.example']) assert.ok(!dump.includes(secret), `persisted: ${secret}`);
    const t1 = rows.filter((r) => r.tenant_id === 't1').map((r) => r.observation);
    assert.deepEqual(t1.map((o) => o.frameSource), ['model', 'degraded_fallback', 'degraded_fallback', 'model']);
    assert.deepEqual(t1.map((o) => o.failure), [null, 'http_500', 'stop_reason_max_tokens', null]);
    assert.equal(t1[2].truncated, true);
    assert.equal(t1[1].disagreement, null);
    assert.equal(t1[3].frame.intent, 'authorization');
    assert.equal(t1[3].pendingDecisionBound, true);
    assert.ok(rows.every((r) => r.observation.frame.authority.grantsAuthority === false && r.observation.shadowOnly === true));
    const health = observerHealthSnapshot().observers.semantic_shadow;
    assert.equal(health.completed, 3);
    assert.equal(health.failed, 2);
  });

  await t.test('review is tenant-scoped and reports degraded, truncated, incomparable and bound counts', async () => {
    const sum = (rows, k) => rows.reduce((a, r) => a + Number(r[k]), 0);
    const r1 = await review(db, 't1');
    const r2 = await review(db, 't2');
    assert.equal(sum(r1, 'turns'), 4);
    assert.equal(sum(r1, 'degraded'), 2);
    assert.equal(sum(r1, 'truncated'), 1);
    assert.equal(sum(r1, 'incomparable'), 2);
    assert.equal(sum(r1, 'pending_decision_bound'), 1);
    assert.equal(sum(r2, 'turns'), 1);
    assert.equal((await review(db, 't3')).length, 0);
    await assert.rejects(review(db, "t1' OR '1'='1"), /tenant identifier/);
  });

  await t.test('retention deletes only rows older than the cutoff, for every tenant, in batches', async () => {
    for (const tenant of ['t1', 't2']) {
      for (let i = 0; i < 3; i += 1) {
        await db.query(`INSERT INTO max_interpretation_shadow VALUES (gen_random_uuid(), $1, now() - interval '100 days' - ($2 || ' minutes')::interval, '{"shadowOnly":"true"}')`, [tenant, String(i)]);
      }
    }
    await db.query(`INSERT INTO max_interpretation_shadow VALUES (gen_random_uuid(), 't1', now() - interval '89 days', '{"shadowOnly":"true"}')`);
    const before = await countExpired(db);
    assert.equal(before.expired, 6);
    const result = await purgeExpired(db, { batchSize: 4 });
    assert.equal(result.deleted, 6);
    assert.equal(result.batches, 2);
    assert.equal((await countExpired(db)).expired, 0);
    assert.equal((await db.query('SELECT count(*)::int AS n FROM max_interpretation_shadow')).rows[0].n, 6, '5 observations + the 89-day row remain');
    await assert.rejects(purgeExpired(db, { retentionDays: 0 }), /retention_days_invalid/);
  });

  await t.test('a persistence failure is counted and isolated; it never throws into the caller', async () => {
    resetObserverHealth();
    await db.query('CREATE DATABASE no_shadow_table');
    const missing = instance.connectionString.replace(/\/[^/?]+(\?|$)/, '/no_shadow_table$1');
    const observer = new ShadowInterpreter({ env: { ...env, DATABASE_URL: missing }, producer: producer(() => ok(frame())) });
    assert.doesNotThrow(() => observer.observe({ context: { tenantId: 't1', candidates: [], focusIds: [], pendingDecisionId: null }, text: 'x', result: {} }));
    await observer.close();
    assert.equal(observer.counts.completed, 0);
    assert.equal(observer.counts.failed, 1);
    assert.equal(observerHealthSnapshot().observers.semantic_shadow.lastFailureReason, 'observer_failed');
  });

  await t.test('rollback removes the table and the migration re-applies cleanly', async () => {
    await db.query(ROLLBACK);
    assert.equal((await db.query(`SELECT to_regclass('public.max_interpretation_shadow') AS t`)).rows[0].t, null);
    await db.query(ROLLBACK);
    await db.query(MIGRATION);
    assert.ok((await db.query(`SELECT to_regclass('public.max_interpretation_shadow') AS t`)).rows[0].t);
  });
});
