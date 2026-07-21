'use strict';

const fs = require('fs');
const crypto = require('crypto');
const {
  CLIENT_ID,
  IDEMPOTENCY_KEYS,
  validatePhase16bAuthorization,
} = require('../utils/revenuePhase16b');
const { deriveTimestampFromHistoricalDate } = require('../utils/historicalTimestamp');
const {
  captureReconstructionBoundary,
  verifyDeterministicReconstruction,
} = require('./revenueReconstruction');

const RUNNER_LOCK_ID = 1610010;
const REVENUE_TABLES = Object.freeze([
  'customers',
  'opportunities',
  'revenue_events',
  'revenue_feature_flags',
  'revenue_jobs',
  'revenue_operator_audit',
  'revenue_outcomes',
  'revenue_payments',
  'revenue_projection_rebuilds',
  'revenue_reconciliation_runs',
]);

function checkpoint(options, name, evidence) {
  evidence.checkpoints.push({ name, at: new Date().toISOString() });
  if (options.failAt === name) {
    const error = new Error(`Injected failure at ${name}`);
    error.code = 'PHASE16B_INJECTED_FAILURE';
    throw error;
  }
}

function operationContext(runtime, key, authorization) {
  return {
    idempotencyKey: runtime.idempotency_keys[key],
    sourceSystem: 'revenue_phase16b',
    correlationId: runtime.correlation_id,
    actorType: 'operator',
    actorId: authorization.authorized_operator.identity,
    followupRecommendationsEnabled: false,
  };
}

async function setFlags(db, values, actor) {
  const assignments = Object.keys(values).map((name, index) => `${name}=$${index + 2}`);
  const result = await db.query(`
    UPDATE revenue_feature_flags
    SET ${assignments.join(',')}, updated_at=NOW(), updated_by=$${assignments.length + 2}
    WHERE client_id=$1
  `, [CLIENT_ID, ...Object.values(values), actor]);
  if (result.rowCount !== 1) throw new Error('Anchor feature-flag update did not affect exactly one row');
}

async function disableWrites(db, actor) {
  const result = await db.query(`
    UPDATE revenue_feature_flags
    SET revenue_operator_writes_enabled=FALSE, updated_at=NOW(), updated_by=$2
    WHERE client_id=$1
  `, [CLIENT_ID, actor]);
  if (result.rowCount !== 1) throw new Error('Anchor write-flag shutdown did not affect exactly one row');
}

async function forceAndProveWritesDisabled(db, actor) {
  const table = await db.query(
    `SELECT to_regclass('public.revenue_feature_flags') IS NOT NULL AS present`
  );
  if (table.rows[0]?.present !== true) return true;
  await db.query(`
    UPDATE revenue_feature_flags
    SET revenue_operator_writes_enabled=FALSE, updated_at=NOW(), updated_by=$2
    WHERE client_id=$1
  `, [CLIENT_ID, actor]);
  const { rows } = await db.query(
    'SELECT revenue_operator_writes_enabled FROM revenue_feature_flags WHERE client_id=$1',
    [CLIENT_ID]
  );
  return rows.length === 0 || rows[0].revenue_operator_writes_enabled === false;
}

async function flagState(db) {
  const { rows } = await db.query(`
    SELECT client_id,revenue_schema_enabled,revenue_operator_reads_enabled,
      revenue_operator_writes_enabled,revenue_max_reads_enabled,
      revenue_followup_recommendations_enabled
    FROM revenue_feature_flags
    ORDER BY client_id
  `);
  return rows;
}

async function assertInitialSchemaState(db) {
  const tables = await db.query(`
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_name=ANY($1::text[])
    ORDER BY table_name
  `, [REVENUE_TABLES]);
  if (tables.rows.length !== REVENUE_TABLES.length) throw new Error('Revenue schema verification failed');
  const trigger = await db.query(`
    SELECT 1 FROM pg_trigger
    WHERE tgrelid='revenue_events'::regclass
      AND tgname='revenue_events_no_update_delete'
      AND NOT tgisinternal
  `);
  if (trigger.rowCount !== 1) throw new Error('Revenue append-only trigger verification failed');
  const constraints = await db.query(`
    SELECT
      COUNT(*) FILTER (WHERE contype='f')::int AS foreign_keys,
      COUNT(*) FILTER (WHERE contype='c')::int AS checks
    FROM pg_constraint
    WHERE connamespace='public'::regnamespace
      AND conrelid IN (
        'customers'::regclass,
        'opportunities'::regclass,
        'revenue_jobs'::regclass,
        'revenue_payments'::regclass,
        'revenue_events'::regclass,
        'revenue_outcomes'::regclass,
        'revenue_feature_flags'::regclass
      )
  `);
  if (constraints.rows[0].foreign_keys < 10 || constraints.rows[0].checks < 10) {
    throw new Error('Revenue foreign-key or check-constraint verification failed');
  }
  const indexes = await db.query(`
    SELECT indexname,indexdef
    FROM pg_indexes
    WHERE schemaname='public'
      AND tablename IN ('opportunities','revenue_jobs','revenue_events','revenue_outcomes')
  `);
  const indexDefinitions = indexes.rows.map(row => row.indexdef);
  if (!indexDefinitions.some(definition => /\(client_id, source_system, idempotency_key\)/i.test(definition))
    || !indexDefinitions.some(definition => /\(client_id, job_id\)/i.test(definition))
    || !indexes.rows.some(row => row.indexname === 'revenue_events_client_occurred_idx')) {
    throw new Error('Revenue uniqueness or lookup-index verification failed');
  }
  const publicWrites = await db.query(`
    SELECT COUNT(*)::int AS count
    FROM information_schema.role_table_grants
    WHERE table_schema='public'
      AND table_name='revenue_events'
      AND grantee='PUBLIC'
      AND privilege_type IN ('UPDATE','DELETE','TRUNCATE')
  `);
  if (publicWrites.rows[0].count !== 0) throw new Error('PUBLIC may mutate the revenue ledger');
  const health = await db.query('SELECT 1 AS healthy');
  if (health.rows[0]?.healthy !== 1) throw new Error('Database health verification failed');
  const flags = await flagState(db);
  const anchor = flags.find(row => Number(row.client_id) === CLIENT_ID);
  if (!anchor) throw new Error('Anchor revenue feature-flag row is missing');
  if (flags.some(row => Object.entries(row)
    .filter(([key]) => key.startsWith('revenue_') && key.endsWith('_enabled'))
    .some(([, value]) => value !== false))) {
    throw new Error('Revenue flags must all be off after migration');
  }
  const counts = await db.query(`
    SELECT
      (SELECT COUNT(*)::int FROM revenue_events WHERE client_id=$1) AS events,
      (SELECT COUNT(*)::int FROM revenue_outcomes WHERE client_id=$1) AS outcomes
  `, [CLIENT_ID]);
  if (counts.rows[0].events !== 0 || counts.rows[0].outcomes !== 0) {
    throw new Error('Unexpected pre-existing Anchor revenue data');
  }
  return {
    tables: tables.rows.map(row => row.table_name),
    constraints: constraints.rows[0],
    indexes: indexes.rows.map(row => row.indexname),
    public_ledger_mutation_privileges: publicWrites.rows[0].count,
    health: 'healthy',
    flags,
    counts: counts.rows[0],
  };
}

function assertCanaryResult(report, authorization) {
  const expected = authorization.reconciliation.expected_totals;
  if (report.status !== 'passed'
    || report.ledger_event_count !== 12
    || report.projected_outcome_count !== 1
    || report.source_outcome_count !== 1
    || report.mismatches.length !== 0
    || report.unexplained_events.length !== 0) {
    throw new Error('Canary reconciliation cardinality mismatch');
  }
  for (const key of [
    'booked_revenue_cents',
    'delivered_revenue_cents',
    'collected_revenue_cents',
    'refunded_revenue_cents',
    'net_collected_revenue_cents',
  ]) {
    if (Number(report.source_totals[key]) !== expected[key]
      || Number(report.ledger_totals[key]) !== expected[key]
      || Number(report.projection_totals[key]) !== expected[key]) {
      throw new Error(`Canary reconciliation financial mismatch: ${key}`);
    }
  }
}

async function verifyLifecycleLedger(db, runtime) {
  const { rows } = await db.query(`
    SELECT event_type,idempotency_key,correlation_id::text AS correlation_id
    FROM revenue_events
    WHERE client_id=$1 AND source_system='revenue_phase16b'
    ORDER BY recorded_at,event_id
  `, [CLIENT_ID]);
  const expectedTypes = [
    'customer_created',
    'job_completed',
    'job_created',
    'job_started',
    'opportunity_contacted',
    'opportunity_created',
    'opportunity_qualified',
    'opportunity_quoted',
    'payment_succeeded',
    'revenue_outcome_updated',
    'revenue_outcome_updated',
    'revenue_outcome_updated',
  ].sort();
  const actualTypes = rows.map(row => row.event_type).sort();
  if (rows.length !== 12 || JSON.stringify(actualTypes) !== JSON.stringify(expectedTypes)) {
    throw new Error('Canary ledger event sequence is not the authorized 12-event lifecycle');
  }
  if (rows.some(row => row.correlation_id !== runtime.correlation_id)) {
    throw new Error('Canary ledger correlation ID mismatch');
  }
  for (const key of Object.values(runtime.idempotency_keys)) {
    if (rows.filter(row => row.idempotency_key === key).length !== 1) {
      throw new Error(`Canary ledger idempotency key mismatch: ${key}`);
    }
  }
  return {
    event_count: rows.length,
    event_types: actualTypes,
    correlation_id: runtime.correlation_id,
    base_idempotency_keys_verified: 8,
  };
}

async function ensureNoPriorAttempt(db) {
  const keys = Object.values(IDEMPOTENCY_KEYS);
  const { rows } = await db.query(`
    SELECT COUNT(*)::int AS count
    FROM revenue_events
    WHERE client_id=$1
      AND source_system='revenue_phase16b'
      AND split_part(idempotency_key, ':', 1)=ANY($2::text[])
  `, [CLIENT_ID, keys]);
  if (rows[0].count !== 0) {
    const error = new Error('Phase 1.6B canary was already attempted; replay is prohibited');
    error.code = 'PHASE16B_REPLAY_PROHIBITED';
    throw error;
  }
}

function redactEvidence(evidence) {
  const copy = structuredClone(evidence);
  delete copy.authorization;
  return copy;
}

async function executePhase16b(authorization, dependencies, options = {}) {
  if (!dependencies?.observeIdentity || !dependencies?.openDatabase) {
    throw new Error('observeIdentity and openDatabase dependencies are required');
  }
  const observed = await dependencies.observeIdentity();
  const validation = validatePhase16bAuthorization(authorization, {
    now: options.now,
    observed,
  });
  if (!validation.valid) {
    return {
      verdict: 'BLOCKED',
      stage: 'authorization_validation',
      validation,
      production_access_opened: false,
      automatic_continuation: false,
    };
  }
  const environment = options.environment || process.env;
  const enabledEnvironmentFlags = [
    'REVENUE_SCHEMA_ENABLED',
    'REVENUE_OPERATOR_READS_ENABLED',
    'REVENUE_OPERATOR_WRITES_ENABLED',
    'REVENUE_MAX_READS_ENABLED',
    'REVENUE_FOLLOWUP_RECOMMENDATIONS_ENABLED',
  ].filter(name => String(environment[name] || '').trim().toLowerCase() === 'true');
  if (enabledEnvironmentFlags.length) {
    return {
      verdict: 'BLOCKED',
      stage: 'environment_flags',
      validation,
      enabled_environment_flags: enabledEnvironmentFlags,
      production_access_opened: false,
      automatic_continuation: false,
    };
  }
  if (options.mode !== 'rehearsal' && options.productionEnabled !== true) {
    return {
      verdict: 'BLOCKED',
      stage: 'production_disabled',
      validation,
      production_access_opened: false,
      automatic_continuation: false,
    };
  }

  const evidence = {
    phase: authorization.phase,
    authorization_id: authorization.authorization_id,
    authorization_hash: authorization.authorization_hash,
    mode: options.mode === 'rehearsal' ? 'rehearsal' : 'production',
    started_at: new Date().toISOString(),
    verdict: 'BLOCKED',
    identity: observed,
    validation,
    checkpoints: [],
    writes_disabled: null,
    automatic_continuation: false,
  };

  let db;
  try {
    db = await dependencies.openDatabase();
  } catch (error) {
    evidence.failure = { code: error.code || 'DATABASE_CONNECT_FAILED', message: error.message };
    evidence.failed_at = new Date().toISOString();
    evidence.production_access_opened = false;
    return redactEvidence(evidence);
  }
  evidence.production_access_opened = true;
  let lockAcquired = false;
  let writesWereEnabled = false;
  try {
    if (options.mode === 'rehearsal') {
      if (!dependencies.assertDisposableDatabase
        || await dependencies.assertDisposableDatabase(db) !== true) {
        throw new Error('Rehearsal database was not positively identified as disposable');
      }
    }
    const lock = await db.query('SELECT pg_try_advisory_lock($1) AS acquired', [RUNNER_LOCK_ID]);
    lockAcquired = lock.rows[0]?.acquired === true;
    if (!lockAcquired) {
      const error = new Error('Another Phase 1.6B runner holds the execution lock');
      error.code = 'PHASE16B_CONCURRENT_EXECUTION';
      throw error;
    }

    const existingSchema = await db.query(`
      SELECT to_regclass('public.revenue_events') IS NOT NULL AS phase1,
             to_regclass('public.revenue_feature_flags') IS NOT NULL AS phase15
    `);
    const phase1Exists = existingSchema.rows[0].phase1;
    const phase15Exists = existingSchema.rows[0].phase15;
    if (phase1Exists !== phase15Exists) {
      throw new Error('Partial or ambiguous revenue migration state');
    }
    if (!phase1Exists) {
      for (const migration of ['phase1', 'phase15']) {
        const descriptor = authorization.migration_checksums[migration];
        const sql = dependencies.readMigration
          ? await dependencies.readMigration(descriptor.path)
          : fs.readFileSync(descriptor.path, 'utf8');
        const actualHash = crypto.createHash('sha256').update(sql).digest('hex');
        if (actualHash !== descriptor.sha256) {
          throw new Error(`Migration content checksum drift: ${migration}`);
        }
        await db.query(sql);
      }
    }
    await db.query(`
      INSERT INTO revenue_feature_flags (
        client_id,revenue_schema_enabled,revenue_operator_reads_enabled,
        revenue_operator_writes_enabled,revenue_max_reads_enabled,
        revenue_followup_recommendations_enabled,updated_by
      ) VALUES ($1,FALSE,FALSE,FALSE,FALSE,FALSE,$2)
      ON CONFLICT (client_id) DO NOTHING
    `, [CLIENT_ID, authorization.authorized_operator.identity]);
    checkpoint(options, 'after_migration_application', evidence);

    evidence.initial_schema = await assertInitialSchemaState(db);
    checkpoint(options, 'after_schema_verification', evidence);

    await setFlags(db, {
      revenue_schema_enabled: true,
      revenue_operator_reads_enabled: true,
      revenue_max_reads_enabled: true,
      revenue_operator_writes_enabled: false,
      revenue_followup_recommendations_enabled: false,
    }, authorization.authorized_operator.identity);
    evidence.read_flags = await flagState(db);
    if (evidence.read_flags.some(row => Number(row.client_id) !== CLIENT_ID
      && Object.entries(row)
        .filter(([key]) => key.startsWith('revenue_') && key.endsWith('_enabled'))
        .some(([, value]) => value !== false))) {
      throw new Error('A non-Anchor revenue capability is enabled');
    }
    checkpoint(options, 'after_read_flag_enablement', evidence);

    const baseline = await dependencies.operations.reconcileTenant(db, CLIENT_ID);
    if (baseline.status !== 'passed'
      || baseline.ledger_event_count !== 0
      || baseline.projected_outcome_count !== 0
      || baseline.source_outcome_count !== 0) {
      throw new Error('Baseline reconciliation failed');
    }
    evidence.baseline_reconciliation = { ...baseline, snapshots: undefined };
    evidence.baseline_event_boundary = { client_id: CLIENT_ID, ledger_event_count: 0 };
    await ensureNoPriorAttempt(db);

    await setFlags(db, { revenue_operator_writes_enabled: true },
      authorization.authorized_operator.identity);
    writesWereEnabled = true;
    checkpoint(options, 'after_write_flag_enablement', evidence);

    const runtime = authorization.canary.operator_only_runtime_values;
    const scheduled = deriveTimestampFromHistoricalDate(runtime.scheduled_start);
    const completed = deriveTimestampFromHistoricalDate(runtime.completion_date);
    const paid = deriveTimestampFromHistoricalDate(runtime.payment_received_at);
    const revenue = dependencies.revenue;

    const customerResult = await revenue.createCustomer(CLIENT_ID, {
      displayName: authorization.canary.customer_name,
      customerType: authorization.canary.customer_type,
      primaryEmail: runtime.customer_primary_email,
      primaryPhone: runtime.customer_primary_phone,
    }, operationContext(runtime, 'customer_create', authorization));
    checkpoint(options, 'after_customer_creation', evidence);

    let opportunityResult = await revenue.createOpportunity(CLIENT_ID, {
      customerId: customerResult.customer.id,
      serviceType: authorization.canary.service_type,
      estimatedValueCents: authorization.canary.booked_revenue_cents,
      estimatedCostCents: runtime.estimated_direct_cost_cents,
      source: authorization.canary.lead_source,
      attributionStatus: authorization.canary.attribution_status,
      humanOwner: runtime.human_owner,
    }, operationContext(runtime, 'opportunity_create', authorization));
    checkpoint(options, 'after_opportunity_creation', evidence);

    for (const [stage, key] of [
      ['contacted', 'opportunity_contacted'],
      ['qualified', 'opportunity_qualified'],
      ['quoted', 'opportunity_quoted'],
    ]) {
      opportunityResult = await revenue.updateOpportunity(
        CLIENT_ID,
        opportunityResult.opportunity.id,
        { stage },
        operationContext(runtime, key, authorization)
      );
    }

    const jobResult = await revenue.createJob(CLIENT_ID, {
      opportunityId: opportunityResult.opportunity.id,
      customerId: customerResult.customer.id,
      serviceType: authorization.canary.service_type,
      serviceAddress: runtime.service_address,
      scheduledStart: scheduled.timestamp,
      scheduledStartPrecision: scheduled.provenance,
      quotedAmountCents: authorization.canary.booked_revenue_cents,
      estimatedDirectCostCents: runtime.estimated_direct_cost_cents,
    }, operationContext(runtime, 'job_create', authorization));

    await revenue.completeJob(CLIENT_ID, jobResult.job.id, {
      completionConfirmed: true,
      completionDate: completed.timestamp,
      completionDatePrecision: completed.provenance,
      fullyCompleted: true,
      finalAmountCents: authorization.canary.delivered_revenue_cents,
      estimatedDirectCostCents: runtime.estimated_direct_cost_cents,
      actualDirectCostCents: runtime.actual_direct_cost_cents,
      completionNotes: authorization.canary.job_note,
    }, operationContext(runtime, 'job_complete', authorization));
    checkpoint(options, 'after_job_completion', evidence);

    const paymentResult = await revenue.recordPayment(CLIENT_ID, {
      jobId: jobResult.job.id,
      amountCents: authorization.canary.collected_revenue_cents,
      status: authorization.canary.payment_status,
      provider: authorization.canary.payment_provider,
      externalPaymentId: authorization.canary.external_payment_id,
      paymentMethod: runtime.payment_method,
      receivedAt: paid.timestamp,
      receivedAtPrecision: paid.provenance,
    }, operationContext(runtime, 'payment_succeeded', authorization));
    checkpoint(options, 'after_payment_recording', evidence);

    const projected = await db.query(
      'SELECT COUNT(*)::int AS count FROM revenue_outcomes WHERE client_id=$1',
      [CLIENT_ID]
    );
    if (projected.rows[0].count !== 1) throw new Error('Exactly one projection is required');
    evidence.ledger = await verifyLifecycleLedger(db, runtime);
    checkpoint(options, 'after_projection_creation', evidence);

    evidence.lifecycle = {
      customer_id: customerResult.customer.id,
      opportunity_id: opportunityResult.opportunity.id,
      job_id: jobResult.job.id,
      payment_id: paymentResult.payment.id,
      timestamp_precision: {
        scheduled_start: scheduled.provenance,
        completion_date: completed.provenance,
        payment_received_at: paid.provenance,
      },
    };

    await disableWrites(db, authorization.authorized_operator.identity);
    writesWereEnabled = false;
    checkpoint(options, 'after_write_disablement', evidence);

    const reconciliation = await dependencies.operations.reconcileTenant(db, CLIENT_ID);
    assertCanaryResult(reconciliation, authorization);
    evidence.reconciliation = { ...reconciliation, snapshots: undefined };
    checkpoint(options, 'after_reconciliation', evidence);

    if (dependencies.operations.rebuildProjections) {
      evidence.recorded_reconciliation = await dependencies.operations.rebuildProjections(db, {
        clientId: CLIENT_ID,
        compareOnly: true,
        record: true,
        correlationId: cryptoRandomUuidForRecord(runtime.correlation_id),
        actorType: 'operator',
        actorId: authorization.authorized_operator.identity,
        env: {
          REVENUE_SCHEMA_ENABLED: 'true',
          REVENUE_OPERATOR_READS_ENABLED: 'true',
          REVENUE_OPERATOR_WRITES_ENABLED: 'false',
          REVENUE_MAX_READS_ENABLED: 'true',
          REVENUE_FOLLOWUP_RECOMMENDATIONS_ENABLED: 'false',
        },
      });
      if (evidence.recorded_reconciliation.status !== 'passed') {
        throw new Error('Recorded reconciliation failed');
      }
    }

    const boundary = await captureReconstructionBoundary(db, CLIENT_ID);
    evidence.reconstruction = await verifyDeterministicReconstruction(db, boundary);
    if (evidence.reconstruction.status !== 'passed') {
      throw new Error('Deterministic reconstruction failed');
    }

    const maxAudit = await db.query(
      `SELECT COUNT(*)::int AS count FROM revenue_operator_audit WHERE client_id=$1 AND lower(actor_type)='max'`,
      [CLIENT_ID]
    );
    if (maxAudit.rows[0].count !== 0) throw new Error('Max mutation detected');
    evidence.max_read_only = { mutation_audit_count: 0, verified: true };
    evidence.verdict = 'COMPLETE';
    evidence.completed_at = new Date().toISOString();
  } catch (error) {
    evidence.failure = { code: error.code || 'PHASE16B_BLOCKED', message: error.message };
    evidence.failed_at = new Date().toISOString();
  } finally {
    if (writesWereEnabled || lockAcquired) {
      try {
        evidence.writes_disabled = await forceAndProveWritesDisabled(
          db,
          authorization.authorized_operator.identity
        );
        if (!evidence.writes_disabled) throw new Error('Anchor write flag remains enabled');
      } catch (error) {
        evidence.writes_disabled = false;
        evidence.write_disable_error = error.message;
        evidence.verdict = 'BLOCKED';
        evidence.failure = {
          code: error.code || 'WRITE_FLAG_SHUTDOWN_FAILED',
          message: `Write-flag shutdown could not be proven: ${error.message}`,
        };
      }
    }
    if (lockAcquired) await db.query('SELECT pg_advisory_unlock($1)', [RUNNER_LOCK_ID]).catch(() => {});
    if (typeof db.release === 'function') db.release();
  }
  return redactEvidence(evidence);
}

function cryptoRandomUuidForRecord(correlationId) {
  const crypto = require('crypto');
  const hex = crypto.createHash('sha256').update(`${correlationId}:reconciliation`).digest('hex').slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20)}`;
}

module.exports = {
  RUNNER_LOCK_ID,
  assertCanaryResult,
  disableWrites,
  executePhase16b,
  forceAndProveWritesDisabled,
  setFlags,
  verifyLifecycleLedger,
};
