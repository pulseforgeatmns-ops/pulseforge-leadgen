'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { randomUUID } = require('node:crypto');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { applyCanonicalAoFixture, seedAoActivity } = require('./helpers/canonicalAoPostgres');
const { PostgresStateStore } = require('../packages/max/stateIngestion');
const { PostgresDecisionStore, evaluateOperationalDecision } = require('../packages/max/decisionExecution');
const { PostgresAttentionStore } = require('../packages/max/attention/store/postgresStore');

// Deliberately not skippable: this gate must prove persistence using its own disposable
// database. Never connect to DATABASE_URL or invoke a provider, scheduler or route.
test('real PostgreSQL: scoped expectation resolution, truthful failure and restart replay', { timeout: 60000 }, async t => {
  const instance = await startDisposablePostgres('max-expectation-');
  const db = new Pool({ connectionString: instance.connectionString });
  t.after(async () => { await db.end(); await instance.stop(); });
  await applyCanonicalAoFixture(db);

  async function seed({ clientId = 1, aoId = 10, resolving = true } = {}) {
    const prospectId = randomUUID();
    await db.query('INSERT INTO prospects (id, client_id, assigned_ao_id) VALUES ($1,$2,$3)',
      [prospectId, clientId, aoId]);
    const stateStore = new PostgresStateStore(db, { clientId });
    const expectation = await stateStore.createExpectation({ client_id: clientId, prospect_id: prospectId,
      ao_id: aoId, expectation_type: 'inbound_call', status: 'OVERDUE',
      expected_window: { ends_at: '2026-09-01T00:00:00.000Z' } });
    if (resolving) await seedAoActivity(db, expectation);
    return expectation;
  }
  async function evaluate(expectation, stateDb = db, triggerType = 'NEW_EVIDENCE') {
    const stateStore = new PostgresStateStore(stateDb, { clientId: 1 });
    return evaluateOperationalDecision({ clientId: 1, trigger: { type: triggerType, payload: {} }, expectation,
      stateStore,
      decisionStore: new PostgresDecisionStore(db, { clientId: 1 }),
      attentionStore: new PostgresAttentionStore(db, { clientId: 1 }),
      now: new Date('2026-10-05T08:05:00.000Z') });
  }
  async function assertDurableFailure(result) {
    assert.notEqual(result.decision.execution_status, 'EXECUTED');
    assert.equal(result.decision.verification_status, 'VERIFICATION_FAILED');
    assert.equal(result.decision.execution_status, result.intent.execution_status);
    const durableDecision = (await db.query('SELECT * FROM max_operational_decisions WHERE id=$1', [result.decision.id])).rows[0];
    const durableIntent = (await db.query('SELECT * FROM max_operational_action_intents WHERE id=$1', [result.intent.id])).rows[0];
    assert.equal(durableDecision.execution_status, result.intent.execution_status);
    assert.equal(durableDecision.verification_status, 'VERIFICATION_FAILED');
    assert.equal(durableIntent.execution_status, result.intent.execution_status);
    assert.equal(durableIntent.verification_status, 'VERIFICATION_FAILED');
    assert.equal(durableIntent.error_code, result.intent.error_code);
    const attention = (await db.query('SELECT * FROM max_attention_items WHERE source_decision_id=$1', [result.decision.id])).rows;
    assert.equal(attention.length, 1);
    assert.equal(attention[0].status, 'ACTIVE');
    assert.deepEqual(attention[0].resolution_evidence, []);
  }

  await t.test('persisted resolution and fresh readback, prior follow-up closure, exact replay after restart', async () => {
    const expectation = await seed({ resolving: false });
    const prior = await evaluate(expectation, db, 'EXPECTATION_OVERDUE');
    assert.equal(prior.decision.execution_status, 'EXECUTED');
    await seedAoActivity(db, expectation);
    const calls = [];
    const stateDb = { query: async (sql, args) => { calls.push(sql); return db.query(sql, args); } };
    const result = await evaluate(expectation, stateDb);
    assert.equal(result.decision.verification_status, 'VERIFIED');
    const row = (await db.query('SELECT * FROM max_open_expectations WHERE id=$1', [expectation.id])).rows[0];
    assert.equal(row.status, 'RESOLVED');
    assert.ok(row.resolved_at instanceof Date);
    const writeIndex = calls.findIndex(sql => /UPDATE max_open_expectations/.test(sql));
    assert.ok(writeIndex >= 0);
    assert.ok(calls.slice(writeIndex + 1).some(sql => /SELECT \* FROM max_open_expectations/.test(sql)), 'verification must read after the persisted write');
    assert.equal((await db.query('SELECT execution_status FROM max_operational_decisions WHERE id=$1', [prior.decision.id])).rows[0].execution_status, 'SUPERSEDED');
    assert.equal((await db.query('SELECT status FROM max_ao_follow_up_tasks WHERE decision_id=$1', [prior.decision.id])).rows[0].status, 'superseded');
    const replay = await evaluate(expectation, { query: async (sql, args) => {
      assert.doesNotMatch(sql, /UPDATE max_open_expectations/);
      return db.query(sql, args);
    } });
    assert.equal(replay.duplicate, true);
    assert.equal(replay.decision.id, result.decision.id);
    assert.equal(replay.decision.verification_status, 'VERIFIED');
    assert.equal((await db.query('SELECT count(*)::int AS count FROM max_operational_action_intents WHERE decision_id=$1', [result.decision.id])).rows[0].count, 1);
    const restarted = new PostgresStateStore(db, { clientId: 1 });
    const scope = { clientId: 1, prospectId: expectation.prospect_id, aoId: 10 };
    const before = await restarted.readExpectation(expectation.id, scope);
    const repeated = await restarted.updateExpectation(expectation.id, { status: 'RESOLVED' }, scope);
    assert.equal(repeated.resolved_at.toISOString(), before.resolved_at.toISOString());
    assert.equal(repeated.updated_at.toISOString(), before.updated_at.toISOString());
  });

  for (const failure of ['changed state', 'missing row', 'read error', 'legacy unscoped receipt', 'missing intent']) {
    await t.test(`duplicate resolution fails closed without repeating effects: ${failure}`, async () => {
      const expectation = await seed();
      const first = await evaluate(expectation);
      if (failure === 'changed state') await db.query("UPDATE max_open_expectations SET status='OVERDUE', resolved_at=NULL WHERE id=$1", [expectation.id]);
      if (failure === 'missing row') await db.query('DELETE FROM max_open_expectations WHERE id=$1', [expectation.id]);
      if (failure === 'legacy unscoped receipt') {
        await db.query(`UPDATE max_operational_decisions SET canonical_state_snapshot =
          canonical_state_snapshot #- '{expectation,client_id}' WHERE id=$1`, [first.decision.id]);
      }
      if (failure === 'missing intent') await db.query('DELETE FROM max_operational_action_intents WHERE decision_id=$1', [first.decision.id]);
      let reads = 0;
      const result = await evaluate(expectation, { query: async (sql, args) => {
        assert.doesNotMatch(sql, /UPDATE max_open_expectations/, 'replay must never execute another resolution');
        if (/SELECT \* FROM max_open_expectations/.test(sql)) {
          reads += 1;
          if (failure === 'read error') throw Error('injected unavailable readback');
        }
        return db.query(sql, args);
      } });
      assert.equal(result.duplicate, true);
      assert.notEqual(result.decision.execution_status, 'EXECUTED');
      assert.equal(result.decision.verification_status, 'VERIFICATION_FAILED');
      assert.equal(reads, failure === 'changed state' ? 2 : 1, 'refresh precedes replay readback when canonical scope is available');
      const saved = (await db.query('SELECT * FROM max_operational_decisions WHERE id=$1', [first.decision.id])).rows[0];
      assert.equal(saved.verification_status, 'VERIFICATION_FAILED');
      const attention = (await db.query('SELECT * FROM max_attention_items WHERE source_decision_id=$1', [first.decision.id])).rows;
      assert.equal(attention.length, 1, 'reuse the resolved attention fingerprint instead of inserting a duplicate');
      assert.equal(attention[0].status, 'ACTIVE');
      assert.equal(attention[0].resolved_at, null);
      assert.deepEqual(attention[0].resolution_evidence, []);
      assert.equal((await db.query('SELECT count(*)::int AS count FROM max_operational_action_intents WHERE decision_id=$1', [first.decision.id])).rows[0].count, failure === 'missing intent' ? 0 : 1);
    });
  }

  await t.test('actual adapter enforces tenant, prospect and AO on reads and updates', async () => {
    const other = await seed({ clientId: 2, aoId: 11 });
    const own = await seed();
    const store = new PostgresStateStore(db, { clientId: 1 });
    assert.equal(await store.readExpectation(other.id), null);
    assert.equal(await store.readExpectation(other.id, { clientId: 2 }), null);
    assert.equal(await store.updateExpectation(other.id, { status: 'RESOLVED' }, { clientId: 2, prospectId: other.prospect_id, aoId: 11 }), null);
    for (const scope of [{ clientId: 1, prospectId: randomUUID(), aoId: 10 }, { clientId: 1, prospectId: own.prospect_id, aoId: 11 }]) {
      assert.equal(await store.readExpectation(own.id, scope), null);
      assert.equal(await store.updateExpectation(own.id, { status: 'RESOLVED' }, scope), null);
    }
    assert.equal((await db.query('SELECT status FROM max_open_expectations WHERE id=$1', [other.id])).rows[0].status, 'OVERDUE');
    assert.equal((await db.query('SELECT status FROM max_open_expectations WHERE id=$1', [own.id])).rows[0].status, 'OVERDUE');
  });

  for (const failure of ['missing', 'wrong tenant', 'wrong AO', 'zero-row write', 'write error', 'readback error', 'stale readback']) {
    await t.test(`persisted failure and unresolved attention: ${failure}`, async () => {
      const expectation = await seed();
      if (failure === 'missing') await db.query('DELETE FROM max_open_expectations WHERE id=$1', [expectation.id]);
      if (failure === 'wrong tenant') await db.query('UPDATE max_open_expectations SET client_id=2 WHERE id=$1', [expectation.id]);
      if (failure === 'wrong AO') await db.query('UPDATE max_open_expectations SET ao_id=11 WHERE id=$1', [expectation.id]);
      let writeCompleted = false;
      const stateDb = { query: async (sql, args) => {
        if (/UPDATE max_open_expectations/.test(sql)) {
          if (failure === 'zero-row write') return { rows: [], rowCount: 0 };
          if (failure === 'write error') throw Error('injected persistence failure');
          if (failure === 'stale readback') {
            // An acknowledgement cannot substitute for persisted state.
            return db.query("SELECT *, 'RESOLVED'::text AS status, NOW() AS resolved_at FROM max_open_expectations WHERE id=$1", [expectation.id]);
          }
          const result = await db.query(sql, args);
          writeCompleted = true;
          return result;
        }
        if (writeCompleted && failure === 'readback error' && /SELECT \* FROM max_open_expectations/.test(sql)) {
          throw Error('injected readback failure');
        }
        return db.query(sql, args);
      } };
      const result = await evaluate(expectation, stateDb);
      if (['missing', 'wrong tenant', 'wrong AO'].includes(failure)) {
        assert.equal(result.decision.selected_action.action_type, 'ABSTAIN_INSUFFICIENT_EVIDENCE');
        assert.equal(result.decision.execution_status, 'INSUFFICIENT_EVIDENCE');
        assert.equal(result.intent, null, 'missing or mismatched canonical pre-read must never reach resolution');
        const saved = (await db.query('SELECT * FROM max_operational_decisions WHERE id=$1', [result.decision.id])).rows[0];
        assert.equal(saved.execution_status, 'INSUFFICIENT_EVIDENCE');
        assert.equal((await db.query('SELECT count(*)::int AS count FROM max_operational_action_intents WHERE decision_id=$1', [result.decision.id])).rows[0].count, 0);
        assert.equal((await db.query('SELECT status FROM max_attention_items WHERE source_decision_id=$1', [result.decision.id])).rows[0].status, 'ACTIVE');
      } else {
        await assertDurableFailure(result);
      }
    });
  }

  await t.test('failed resolution never supersedes existing persisted follow-up', async () => {
    const expectation = await seed({ resolving: false });
    const prior = await evaluate(expectation, db, 'EXPECTATION_OVERDUE');
    await seedAoActivity(db, expectation);
    const result = await evaluate(expectation, { query: async (sql, args) => {
      if (/UPDATE max_open_expectations/.test(sql)) return { rows: [], rowCount: 0 };
      return db.query(sql, args);
    } });
    await assertDurableFailure(result);
    assert.equal((await db.query('SELECT execution_status FROM max_operational_decisions WHERE id=$1', [prior.decision.id])).rows[0].execution_status, 'EXECUTED');
    assert.equal((await db.query('SELECT status FROM max_ao_follow_up_tasks WHERE decision_id=$1', [prior.decision.id])).rows[0].status, 'open');
  });
});
