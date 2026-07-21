'use strict';

const assert = require('node:assert/strict');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const test = require('node:test');
const { Pool } = require('pg');
const {
  canonicalAuthorizationHash,
  validatePhase16bAuthorization,
} = require('../utils/revenuePhase16b');
const { deriveTimestampFromHistoricalDate } = require('../utils/historicalTimestamp');
const { executePhase16b } = require('../services/revenuePhase16bRunner');
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

  await t.test('exact 12-event lifecycle, one outcome, reconstruction, date provenance, and Max rejection', async () => {
    const state = await scenario();
    assert.equal(state.result.verdict, 'COMPLETE', JSON.stringify({
      failure: state.result.failure,
      reconstruction: state.result.reconstruction,
    }));
    assert.equal(state.result.writes_disabled, true);
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
