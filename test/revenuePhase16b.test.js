'use strict';

const assert = require('node:assert/strict');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const test = require('node:test');
const { Pool } = require('pg');
const {
  RETIRED_AUTHORIZATION_IDS,
  canonicalAuthorizationHash,
  validatePhase16bAuthorization,
} = require('../utils/revenuePhase16b');
const { deriveTimestampFromHistoricalDate } = require('../utils/historicalTimestamp');
const {
  executePhase16b,
  forceAndProveWritesDisabled,
  verifyWritesDisabledOnFreshConnection,
} = require('../services/revenuePhase16bRunner');
const { verifyDeterministicReconstruction } = require('../services/revenueReconstruction');
const operations = require('../services/revenueOperations');
const {
  prepareRevenueDatabase,
  resetRevenueService,
  startDisposablePostgres,
} = require('./helpers/disposablePostgres');

const root = path.join(__dirname, '..');
const baseDraft = JSON.parse(fs.readFileSync(
  path.join(root, 'artifacts', 'revenue', 'phase16b-production-authorization-draft.json'),
  'utf8'
));
const fixedNow = new Date('2026-07-21T17:30:00Z');

function evidenceFor(authorization, overrides = {}) {
  const read = name => JSON.parse(fs.readFileSync(path.join(root, 'artifacts', 'revenue', name), 'utf8'));
  return {
    observed_at: fixedNow.toISOString(),
    protected_main_commit: authorization.release.protected_main_commit,
    railway_deployment: {
      id: authorization.release.railway_deployment_id,
      status: 'SUCCESS',
      commit: authorization.release.deployed_commit,
      service: authorization.release.railway_service,
      environment: authorization.release.railway_environment,
    },
    migration_checksums: structuredClone(authorization.migration_checksums),
    phase16a: {
      closure: read('phase16a-durable-backup-closure.json'),
      backup: read('phase16a-backup-evidence.json'),
      restore: read('phase16a-restore-evidence.json'),
    },
    ...overrides,
  };
}

function signedAuthorization() {
  const authorization = structuredClone(baseDraft);
  authorization.draft_status = 'finalized_signed_executable';
  // Tests pin their own two-hour window around fixedNow so they stay
  // independent of the operational window recorded in the committed draft.
  authorization.window.start = '2026-07-21T17:00:00Z';
  authorization.window.end = '2026-07-21T19:00:00Z';
  authorization.authorized_operator.signature = 'I, Jacob Maynard, attest to this exact authorization.';
  authorization.approving_authority.signature = 'I, Jacob Maynard, Founder, approve this exact authorization.';
  authorization.approved_at = '2026-07-21T17:05:00Z';
  const date = localDate => ({
    local_date: localDate,
    timezone: 'America/New_York',
    precision: 'day',
    operator_confirmed: true,
  });
  const runtime = authorization.canary.operator_only_runtime_values;
  runtime.scheduled_start = date('2026-07-15');
  runtime.completion_date = date('2026-07-15');
  runtime.payment_received_at = date('2026-07-15');
  runtime.payment_method = 'cash';
  authorization.remaining_operator_only_values = [];
  authorization.approved = true;
  authorization.production_execution_permitted = true;
  authorization.executable = true;
  authorization.execution_readiness.ready_to_sign = true;
  authorization.execution_readiness.ready_to_execute = true;
  authorization.authorization_hash = canonicalAuthorizationHash(authorization);
  return authorization;
}

function rehash(authorization) {
  authorization.authorization_hash = canonicalAuthorizationHash(authorization);
  return authorization;
}

test('Phase 1.6B validator fails closed on every authorization and evidence drift class', () => {
  const authorization = signedAuthorization();
  assert.equal(validatePhase16bAuthorization(authorization, {
    now: fixedNow,
    observed: evidenceFor(authorization),
  }).valid, true);

  const cases = [
    ['expired authorization', draft => { draft.window.start = '2026-07-21T14:00:00Z'; draft.window.end = '2026-07-21T16:00:00Z'; rehash(draft); }],
    ['unsigned authorization', draft => { draft.authorized_operator.signature = null; rehash(draft); }],
    ['hash mismatch', draft => { draft.authorization_hash = '0'.repeat(64); }],
    ['immutable authorization ID', draft => { draft.authorization_id = crypto.randomUUID(); rehash(draft); }],
    ['tenant mismatch', draft => { draft.client_id = 11; rehash(draft); }],
    ['one-outcome enforcement', draft => { draft.canary.maximum_canary_outcomes = 2; rehash(draft); }],
    ['one-cent mismatch', draft => { draft.canary.collected_revenue_cents = 14999; rehash(draft); }],
    ['unknown timestamp precision', draft => { draft.canary.operator_only_runtime_values.completion_date.precision = 'unknown'; rehash(draft); }],
    ['invalid calendar date', draft => { draft.canary.operator_only_runtime_values.completion_date.local_date = '2026-02-31'; rehash(draft); }],
    ['missing stop condition', draft => { draft.stop_conditions.pop(); rehash(draft); }],
    ['remaining unresolved value', draft => { draft.remaining_operator_only_values = ['payment method']; rehash(draft); }],
  ];
  for (const [name, mutate] of cases) {
    const changed = signedAuthorization();
    mutate(changed);
    assert.equal(validatePhase16bAuthorization(changed, {
      now: fixedNow,
      observed: evidenceFor(changed),
    }).valid, false, name);
  }

  const evidenceCases = [
    ['deployment drift', observed => { observed.railway_deployment.commit = 'f'.repeat(40); }],
    ['stale deployment observation', observed => { observed.observed_at = '2026-07-21T16:00:00Z'; }],
    ['checksum drift', observed => { observed.migration_checksums.phase1.sha256 = 'f'.repeat(64); }],
    ['backup evidence mismatch', observed => { observed.phase16a.restore.schema_fingerprint = 'f'.repeat(64); }],
  ];
  for (const [name, mutate] of evidenceCases) {
    const observed = evidenceFor(authorization);
    mutate(observed);
    assert.equal(validatePhase16bAuthorization(authorization, {
      now: fixedNow,
      observed,
    }).valid, false, name);
  }
});

test('historical day precision derives stable local noon without claiming an observed clock time', () => {
  const value = {
    local_date: '2026-07-15',
    timezone: 'America/New_York',
    precision: 'day',
    operator_confirmed: true,
  };
  const first = deriveTimestampFromHistoricalDate(value);
  const second = deriveTimestampFromHistoricalDate(value);
  assert.equal(first.timestamp, '2026-07-15T16:00:00.000Z');
  assert.deepEqual(first, second);
  assert.equal(first.provenance.derived, true);
  assert.equal(first.provenance.clock_time_observed, false);
  assert.equal(first.provenance.precision, 'day');
});

test('a consumed authorization is permanently retired; a new authorization is required after a failed production attempt', async () => {
  // Rebuilding the consumed authorization perfectly — correct window, correct
  // signatures, self-consistent hash — must still fail closed forever.
  const retired = signedAuthorization();
  retired.authorization_id = RETIRED_AUTHORIZATION_IDS[0];
  rehash(retired);
  const result = validatePhase16bAuthorization(retired, {
    now: fixedNow,
    observed: evidenceFor(retired),
  });
  assert.equal(result.valid, false);
  assert.ok(result.failures.some(failure => /permanently retired/.test(failure)),
    JSON.stringify(result.failures));

  let opened = false;
  const blocked = await executePhase16b(retired, {
    observeIdentity: async () => evidenceFor(retired),
    openDatabase: async () => { opened = true; throw new Error('must not open'); },
  }, { mode: 'production', productionEnabled: true, now: fixedNow });
  assert.equal(blocked.verdict, 'BLOCKED');
  assert.equal(blocked.stage, 'authorization_validation');
  assert.equal(opened, false, 'a retired authorization must never reach production');

  // The current authorization uses entirely fresh idempotency keys and a
  // fresh correlation ID; nothing from the consumed attempt is reusable.
  const current = signedAuthorization();
  const runtime = current.canary.operator_only_runtime_values;
  assert.notEqual(runtime.correlation_id, 'fd528a1a-091b-4f7b-9210-9a613ffcb9c5');
  for (const key of Object.values(runtime.idempotency_keys)) {
    assert.ok(!/f6cc1c57|dc88d132|a6e94426|f34f98d0|e9da161a|283e4506|c8d6f93f|81d9e480/.test(key),
      `idempotency key reuses consumed material: ${key}`);
  }
});

test('runner rejects invalid or production-disabled authorization before database connectivity', async () => {
  let opened = false;
  const unsigned = structuredClone(baseDraft);
  const invalid = await executePhase16b(unsigned, {
    observeIdentity: async () => evidenceFor(unsigned),
    openDatabase: async () => { opened = true; throw new Error('must not open'); },
  }, { mode: 'rehearsal', now: fixedNow });
  assert.equal(invalid.verdict, 'BLOCKED');
  assert.equal(invalid.stage, 'authorization_validation');
  assert.equal(opened, false);

  const signed = signedAuthorization();
  const productionDisabled = await executePhase16b(signed, {
    observeIdentity: async () => evidenceFor(signed),
    openDatabase: async () => { opened = true; throw new Error('must not open'); },
  }, { mode: 'production', now: fixedNow });
  assert.equal(productionDisabled.verdict, 'BLOCKED');
  assert.equal(productionDisabled.stage, 'production_disabled');
  assert.equal(opened, false);
});

test('Phase 1.6B runner and all failure injections pass on disposable PostgreSQL with no skips', {
  timeout: 240000,
}, async t => {
  const postgres = await startDisposablePostgres();
  t.after(() => postgres.stop());

  async function resetDatabase() {
    const admin = new Pool({ connectionString: postgres.connectionString });
    await admin.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public');
    await admin.end();
    return prepareRevenueDatabase(postgres.connectionString, root);
  }

  async function scenario(options = {}, custom = {}) {
    const runnerPool = await resetDatabase();
    const service = resetRevenueService(postgres.connectionString);
    const authorization = custom.authorization || signedAuthorization();
    const dependencies = {
      observeIdentity: async () => custom.observed || evidenceFor(authorization),
      openDatabase: async () => runnerPool.connect(),
      assertDisposableDatabase: async () => true,
      readMigration: async relative => fs.readFileSync(path.join(root, relative), 'utf8'),
      revenue: custom.revenue || service.revenue,
      operations: custom.operations || operations,
    };
    try {
      const result = await executePhase16b(authorization, dependencies, {
        mode: 'rehearsal',
        now: fixedNow,
        ...options,
      });
      return { result, runnerPool, servicePool: service.pool, authorization, dependencies };
    } catch (error) {
      await runnerPool.end();
      await service.pool.end();
      throw error;
    }
  }

  async function close(state) {
    await state.runnerPool.end();
    await state.servicePool.end();
  }

  // Wraps the runner's primary connection so its explicit ROLLBACK fails,
  // simulating a rollback failure after a failed migration statement. The
  // underlying client is still rolled back on release so the pool stays sane.
  function breakRollbackClient(client) {
    const originalQuery = client.query.bind(client);
    const originalRelease = client.release.bind(client);
    return new Proxy(client, {
      get(target, property) {
        if (property === 'query') {
          return (text, ...args) => {
            if (typeof text === 'string' && text.trim() === 'ROLLBACK') {
              const error = new Error('simulated rollback failure');
              error.code = 'XX000';
              return Promise.reject(error);
            }
            return originalQuery(text, ...args);
          };
        }
        if (property === 'release') {
          return async (...args) => {
            await originalQuery('ROLLBACK').catch(() => {});
            return originalRelease(...args);
          };
        }
        const value = target[property];
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
  }

  async function migrationFailureScenario(preSql, extra = {}) {
    const runnerPool = await resetDatabase();
    await runnerPool.query(preSql);
    const service = resetRevenueService(postgres.connectionString);
    const authorization = signedAuthorization();
    let firstConnection = true;
    const dependencies = {
      observeIdentity: async () => evidenceFor(authorization),
      openDatabase: async () => {
        const client = await runnerPool.connect();
        if (extra.breakRollback && firstConnection) {
          firstConnection = false;
          return breakRollbackClient(client);
        }
        return client;
      },
      assertDisposableDatabase: async () => true,
      readMigration: async relative => fs.readFileSync(path.join(root, relative), 'utf8'),
      revenue: service.revenue,
      operations,
    };
    const result = await executePhase16b(authorization, dependencies, {
      mode: 'rehearsal',
      now: fixedNow,
    });
    return { result, runnerPool, servicePool: service.pool, authorization, dependencies };
  }

  await t.test('production failure reproduction: consumed migration raises 42830 on production-faithful schema, aborts the session, defeats the legacy prover, and is fixed by the corrected migration', async () => {
    const pool = await resetDatabase();
    const client = await pool.connect();
    const consumedSql = fs.readFileSync(
      path.join(root, 'test', 'fixtures', 'consumedPhase16bMigrationPhase1.sql'), 'utf8');

    // Exact initiating production failure: composite tenant FK against
    // companies, which lacks UNIQUE (client_id, id) in production.
    let initiating;
    try { await client.query(consumedSql); } catch (error) { initiating = error; }
    assert.ok(initiating, 'the consumed migration must fail on production-faithful schema');
    assert.equal(initiating.code, '42830');
    assert.match(initiating.message,
      /no unique constraint matching given keys for referenced table "companies"/);

    // The migration file's explicit BEGIN leaves the session in an aborted
    // transaction: every later statement on the SAME connection raises 25P02,
    // which is exactly how the production evidence lost the initiating error.
    let secondary;
    try { await client.query('SELECT 1'); } catch (error) { secondary = error; }
    assert.equal(secondary.code, '25P02');

    // Old defect: the legacy same-connection prover cannot prove anything.
    await assert.rejects(
      forceAndProveWritesDisabled(client, 'regression-test'),
      error => error.code === '25P02'
    );

    // Corrected behavior: a fresh connection proves writes were never
    // enable-able because the feature-flag table does not exist.
    const proof = await verifyWritesDisabledOnFreshConnection(() => pool.connect(), 'regression-test');
    assert.equal(proof.writes_disabled, true);
    assert.equal(proof.reason, 'feature_flag_table_absent_pre_migration');
    assert.equal(proof.verified_on_fresh_connection, true);

    await client.query('ROLLBACK');
    const partial = await client.query(`
      SELECT to_regclass('public.customers') IS NOT NULL AS customers,
             to_regclass('public.revenue_events') IS NOT NULL AS events
    `);
    assert.deepEqual(partial.rows[0], { customers: false, events: false },
      'the transactional migration must leave no partial schema');
    client.release();

    // The corrected certified migrations apply cleanly on the same schema and
    // provision the tenant composite keys themselves.
    for (const name of [
      '2026-07-18-anchor-closed-loop-revenue-phase1.sql',
      '2026-07-18-anchor-closed-loop-revenue-phase15.sql',
    ]) {
      await pool.query(fs.readFileSync(path.join(root, 'migrations', name), 'utf8'));
    }
    const keys = await pool.query(`
      SELECT COUNT(*)::int AS count
      FROM pg_constraint c
      WHERE c.contype IN ('p','u')
        AND c.conrelid IN ('public.companies'::regclass, 'public.prospects'::regclass)
        AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
             FROM unnest(c.conkey) AS k(attnum)
             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum)
            = ARRAY['client_id','id']
    `);
    assert.equal(keys.rows[0].count, 2);
    assert.equal((await pool.query(
      `SELECT to_regclass('public.revenue_feature_flags') IS NOT NULL AS present`
    )).rows[0].present, true);
    await pool.end();
  });

  await t.test('first-statement migration failure: initiating error stays primary, immediate rollback, fresh-connection shutdown proof, no partial schema', async () => {
    const state = await migrationFailureScenario('CREATE TABLE customers (existing INTEGER)');
    const result = state.result;
    assert.equal(result.verdict, 'BLOCKED');
    assert.equal(result.failure.code, '42P07', JSON.stringify(result.failure));
    assert.equal(result.failure_is_first_database_error, true);
    assert.equal(result.failure.migration.migration, 'phase1');
    assert.ok(result.failure.migration.sha256);
    assert.equal(result.rollback.attempted, true);
    assert.equal(result.rollback.succeeded, true);
    assert.equal(result.writes_disabled, true);
    assert.equal(result.write_shutdown.verified_on_fresh_connection, true);
    assert.equal(result.write_shutdown.reason, 'feature_flag_table_absent_pre_migration');
    const failed = result.migration_checkpoints.find(entry => entry.stage === 'phase1_failed');
    assert.ok(failed, 'a failed-migration checkpoint must be recorded');
    assert.equal(failed.transaction_state, 'aborted');
    assert.equal(failed.result.code, '42P07');
    await close(state);
  });

  await t.test('mid-migration failure aborts transactionally: first error primary, rollback succeeds, no partial schema survives', async () => {
    const state = await migrationFailureScenario('CREATE TABLE revenue_payments (existing INTEGER)');
    const result = state.result;
    assert.equal(result.verdict, 'BLOCKED');
    assert.equal(result.failure.code, '42P07');
    assert.equal(result.failure_is_first_database_error, true);
    assert.notEqual(result.failure.code, '25P02',
      'the aborted-transaction symptom must never replace the initiating error');
    assert.equal(result.rollback.succeeded, true);
    assert.equal(result.writes_disabled, true);
    assert.equal(result.write_shutdown.reason, 'feature_flag_table_absent_pre_migration');
    const partial = await state.runnerPool.query(`
      SELECT to_regclass('public.customers') IS NOT NULL AS customers,
             to_regclass('public.opportunities') IS NOT NULL AS opportunities,
             to_regclass('public.revenue_events') IS NOT NULL AS events
    `);
    assert.deepEqual(partial.rows[0], { customers: false, opportunities: false, events: false },
      'tables created before the failing statement must be rolled back');
    await close(state);
  });

  await t.test('rollback failure is reported separately and never displaces the initiating error', async () => {
    const state = await migrationFailureScenario(
      'CREATE TABLE revenue_payments (existing INTEGER)', { breakRollback: true });
    const result = state.result;
    assert.equal(result.verdict, 'BLOCKED');
    assert.equal(result.failure.code, '42P07', 'initiating error must remain primary');
    assert.equal(result.rollback.attempted, true);
    assert.equal(result.rollback.succeeded, false);
    assert.match(result.rollback.error.message, /simulated rollback failure/);
    assert.equal(result.writes_disabled, true,
      'shutdown proof must succeed on a fresh connection even when rollback fails');
    assert.equal(result.write_shutdown.verified_on_fresh_connection, true);
    await close(state);
  });

  await t.test('ambiguous migration state fails closed before applying anything', async () => {
    const state = await migrationFailureScenario('CREATE TABLE revenue_events (event_id UUID)');
    const result = state.result;
    assert.equal(result.verdict, 'BLOCKED');
    assert.equal(result.failure.code, 'PHASE16B_AMBIGUOUS_MIGRATION_STATE');
    const baseline = result.migration_checkpoints.find(
      entry => entry.stage === 'pre_migration_baseline_captured');
    assert.deepEqual(baseline.result, { phase1_exists: true, phase15_exists: false });
    assert.equal(result.writes_disabled, true);
    await close(state);
  });

  await t.test('fresh-connection shutdown verifier: flag table present with writes off, unexpectedly on, and enabled for another tenant', async () => {
    const state = await scenario();
    assert.equal(state.result.verdict, 'COMPLETE');
    const open = () => state.runnerPool.connect();

    let proof = await verifyWritesDisabledOnFreshConnection(open, 'verifier-test');
    assert.equal(proof.writes_disabled, true);
    assert.equal(proof.reason, 'verified_off');

    await state.runnerPool.query(
      'UPDATE revenue_feature_flags SET revenue_operator_writes_enabled=TRUE WHERE client_id=10');
    proof = await verifyWritesDisabledOnFreshConnection(open, 'verifier-test');
    assert.equal(proof.writes_disabled, true);
    assert.equal(proof.reason, 'anchor_write_flag_forced_off');
    assert.equal(proof.anchor_flag_forced_off, true);
    assert.equal((await state.runnerPool.query(
      'SELECT revenue_operator_writes_enabled FROM revenue_feature_flags WHERE client_id=10'
    )).rows[0].revenue_operator_writes_enabled, false);

    await state.runnerPool.query(`
      INSERT INTO revenue_feature_flags (client_id,revenue_schema_enabled,revenue_operator_writes_enabled,updated_by)
      VALUES (11,TRUE,TRUE,'verifier-test')
      ON CONFLICT (client_id) DO UPDATE
        SET revenue_schema_enabled=TRUE, revenue_operator_writes_enabled=TRUE
    `);
    proof = await verifyWritesDisabledOnFreshConnection(open, 'verifier-test');
    assert.equal(proof.writes_disabled, false,
      'a non-Anchor tenant with writes enabled must be reported, never silently mutated');
    assert.equal(proof.reason, 'non_anchor_client_writes_enabled');
    assert.deepEqual(proof.other_clients_with_writes_enabled, [11]);
    assert.equal((await state.runnerPool.query(
      'SELECT revenue_operator_writes_enabled FROM revenue_feature_flags WHERE client_id=11'
    )).rows[0].revenue_operator_writes_enabled, true,
      'the verifier must not write to other tenants');
    await close(state);
  });

  await t.test('exact 12-event lifecycle, one outcome, reconstruction, date provenance, and Max rejection', async () => {
    const state = await scenario();
    assert.equal(state.result.verdict, 'COMPLETE', JSON.stringify({
      failure: state.result.failure,
      reconstruction: state.result.reconstruction,
    }));
    assert.equal(state.result.writes_disabled, true);
    assert.equal(state.result.write_shutdown.verified_on_fresh_connection, true);
    assert.equal(state.result.write_shutdown.reason, 'verified_off');
    const stages = state.result.migration_checkpoints.map(entry => entry.stage);
    for (const stage of [
      'production_identity_verified',
      'pre_migration_baseline_captured',
      'phase1_transaction_opened',
      'phase1_committed',
      'phase15_transaction_opened',
      'phase15_committed',
      'post_migration_structural_verification_completed',
    ]) {
      assert.ok(stages.includes(stage), `missing migration checkpoint: ${stage}`);
    }
    const runtimeValues = state.authorization.canary.operator_only_runtime_values;
    for (const entry of state.result.migration_checkpoints) {
      assert.ok(entry.at, 'checkpoint timestamp required');
      assert.equal(entry.correlation_id, runtimeValues.correlation_id);
      assert.ok(entry.database_role, 'checkpoint database role required');
      assert.ok(entry.search_path, 'checkpoint search path required');
    }
    for (const phase of ['phase1', 'phase15']) {
      const committed = state.result.migration_checkpoints.find(
        entry => entry.stage === `${phase}_committed`);
      assert.equal(committed.transaction_state, 'committed');
      assert.equal(committed.migration_sha256,
        state.authorization.migration_checksums[phase].sha256);
    }
    assert.equal(state.result.reconciliation.ledger_event_count, 12);
    assert.equal(state.result.reconciliation.projected_outcome_count, 1);
    assert.equal(state.result.reconstruction.status, 'passed');
    assert.equal(state.result.reconstruction.non_destructive, true);
    assert.equal(state.result.reconstruction.first_reconstruction_hash,
      state.result.reconstruction.second_reconstruction_hash);
    assert.equal(state.result.reconstruction.first_reconstruction_hash,
      state.result.reconstruction.persisted_projection_hash);
    const counts = await state.runnerPool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM revenue_events WHERE client_id=10) AS events,
        (SELECT COUNT(*)::int FROM revenue_outcomes WHERE client_id=10) AS outcomes,
        (SELECT COUNT(*)::int FROM revenue_payments WHERE client_id=10) AS payments,
        (SELECT revenue_operator_writes_enabled FROM revenue_feature_flags WHERE client_id=10) AS writes
    `);
    assert.deepEqual(counts.rows[0], { events: 12, outcomes: 1, payments: 1, writes: false });
    const precision = await state.runnerPool.query(`
      SELECT payload_json->'temporal_precision' AS precision
      FROM revenue_events
      WHERE client_id=10 AND event_type='payment_succeeded'
    `);
    assert.equal(precision.rows[0].precision.precision, 'day');
    assert.equal(precision.rows[0].precision.derived, true);
    assert.equal(precision.rows[0].precision.clock_time_observed, false);
    await state.runnerPool.query(
      'UPDATE revenue_outcomes SET collected_revenue_cents=14999 WHERE client_id=10'
    );
    const corruptedBefore = await state.runnerPool.query(
      'SELECT collected_revenue_cents FROM revenue_outcomes WHERE client_id=10'
    );
    const mismatch = await verifyDeterministicReconstruction(
      state.runnerPool,
      state.result.reconstruction.boundary
    );
    const corruptedAfter = await state.runnerPool.query(
      'SELECT collected_revenue_cents FROM revenue_outcomes WHERE client_id=10'
    );
    assert.equal(mismatch.status, 'failed');
    assert.ok(mismatch.field_differences.some(diff => diff.field.endsWith('.collected_revenue_cents')));
    assert.deepEqual(corruptedAfter.rows, corruptedBefore.rows,
      'reconstruction verification must never mutate the persisted projection');
    await state.runnerPool.query(
      'UPDATE revenue_outcomes SET collected_revenue_cents=15000 WHERE client_id=10'
    );
    await assert.rejects(
      state.dependencies.revenue.createCustomer(10, { displayName: 'Forbidden Max write' }, {
        idempotencyKey: 'max-forbidden',
        actorType: 'max',
      }),
      error => error.code === 'MAX_REVENUE_MUTATION_FORBIDDEN'
    );
    await close(state);
  });

  await t.test('duplicate/replayed execution is blocked without a second effect', async () => {
    const state = await scenario();
    assert.equal(state.result.verdict, 'COMPLETE', JSON.stringify({
      failure: state.result.failure,
      reconstruction: state.result.reconstruction,
    }));
    const replay = await executePhase16b(state.authorization, state.dependencies, {
      mode: 'rehearsal',
      now: fixedNow,
    });
    assert.equal(replay.verdict, 'BLOCKED');
    const counts = await state.runnerPool.query(
      'SELECT COUNT(*)::int AS outcomes FROM revenue_outcomes WHERE client_id=10'
    );
    assert.equal(counts.rows[0].outcomes, 1);
    await close(state);
  });

  await t.test('concurrent runners allow at most one execution', async () => {
    const runnerPool = await resetDatabase();
    const service = resetRevenueService(postgres.connectionString);
    const authorization = signedAuthorization();
    const dependencies = {
      observeIdentity: async () => evidenceFor(authorization),
      openDatabase: async () => runnerPool.connect(),
      assertDisposableDatabase: async () => true,
      readMigration: async relative => fs.readFileSync(path.join(root, relative), 'utf8'),
      revenue: service.revenue,
      operations,
    };
    const results = await Promise.all([
      executePhase16b(authorization, dependencies, { mode: 'rehearsal', now: fixedNow }),
      executePhase16b(authorization, dependencies, { mode: 'rehearsal', now: fixedNow }),
    ]);
    assert.equal(results.filter(item => item.verdict === 'COMPLETE').length, 1);
    assert.equal(results.filter(item => item.verdict === 'BLOCKED').length, 1);
    assert.equal((await runnerPool.query(
      'SELECT COUNT(*)::int AS count FROM revenue_outcomes WHERE client_id=10'
    )).rows[0].count, 1);
    await runnerPool.end();
    await service.pool.end();
  });

  await t.test('one-cent runtime mismatch blocks and shuts writes down', async () => {
    const runnerPool = await resetDatabase();
    const service = resetRevenueService(postgres.connectionString);
    const revenue = {
      ...service.revenue,
      recordPayment(clientId, input, context) {
        return service.revenue.recordPayment(clientId, { ...input, amountCents: input.amountCents - 1 }, context);
      },
    };
    const authorization = signedAuthorization();
    const result = await executePhase16b(authorization, {
      observeIdentity: async () => evidenceFor(authorization),
      openDatabase: async () => runnerPool.connect(),
      assertDisposableDatabase: async () => true,
      readMigration: async relative => fs.readFileSync(path.join(root, relative), 'utf8'),
      revenue,
      operations,
    }, { mode: 'rehearsal', now: fixedNow });
    assert.equal(result.verdict, 'BLOCKED');
    assert.equal(result.writes_disabled, true);
    assert.match(result.failure.message, /financial mismatch/);
    await runnerPool.end();
    await service.pool.end();
  });

  const failurePoints = [
    'after_migration_application',
    'after_schema_verification',
    'after_read_flag_enablement',
    'after_write_flag_enablement',
    'after_customer_creation',
    'after_opportunity_creation',
    'after_job_completion',
    'after_payment_recording',
    'after_projection_creation',
    'after_write_disablement',
    'after_reconciliation',
  ];
  for (const point of failurePoints) {
    await t.test(`failure injection ${point} disables writes and prevents a second effect`, async () => {
      const state = await scenario({ failAt: point });
      assert.equal(state.result.verdict, 'BLOCKED');
      assert.equal(state.result.failure.code, 'PHASE16B_INJECTED_FAILURE');
      assert.equal(state.result.writes_disabled, true);
      assert.equal(state.result.write_shutdown.verified_on_fresh_connection, true);
      const replay = await executePhase16b(state.authorization, state.dependencies, {
        mode: 'rehearsal',
        now: fixedNow,
      });
      assert.ok(['BLOCKED', 'COMPLETE'].includes(replay.verdict));
      const counts = await state.runnerPool.query(`
        SELECT
          (SELECT COUNT(*)::int FROM revenue_outcomes WHERE client_id=10) AS outcomes,
          (SELECT COUNT(*)::int FROM revenue_payments WHERE client_id=10 AND status='succeeded') AS payments,
          (SELECT revenue_operator_writes_enabled FROM revenue_feature_flags WHERE client_id=10) AS writes
      `);
      assert.ok(counts.rows[0].outcomes <= 1);
      assert.ok(counts.rows[0].payments <= 1);
      assert.equal(counts.rows[0].writes, false);
      await close(state);
    });
  }
});
