'use strict';

require('dotenv').config();
const crypto = require('node:crypto');
const pool = require('../db');
const {
  ingestNormalizedSignal,
  loadClientOrchestrationConfig,
  stableSignalId,
} = require('../utils/maxSignalIngestion');
const { validateSchema } = require('./validateMaxOrchestrationSchema');
const { loadMaxOrchestrationConfig } = require('../config/maxOrchestration');
const { assertAllowed, optionalPositiveInteger, optionalUuid, tokenizeArgs } = require('../utils/maxCli');
const { classifyOperationalMutations, diffOperationalSnapshots } = require('../utils/maxMutationAttribution');

const UUID_PATTERN = '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

const LEGACY_OPERATIONAL_FIELDS = Object.freeze([
  'status', 'do_not_contact', 'last_contacted_at', 'next_touch_at',
  'email_sequence_completed_at', 'setter_status', 'setter_visible',
  'closer_status', 'booked_at',
]);

const ORCHESTRATION_FIELDS = Object.freeze([
  'lifecycle_state', 'previous_lifecycle_state', 'state_changed_at',
  'warmth_score', 'warmth_score_updated_at', 'warmth_score_version',
  'state_reason_codes', 'state_reason_summary', 'next_best_action',
  'next_action_due_at', 'next_action_status', 'operator_required',
  'operator_priority', 'operator_reason', 'last_meaningful_signal_at',
  'last_human_open_at', 'last_reply_at', 'last_positive_reply_at',
  'recycle_eligible_at', 'recycle_reason', 'active_sequence_type',
  'active_sequence_id', 'downgrade_candidate_since',
]);

const OPERATIONAL_FIELDS = Object.freeze([...LEGACY_OPERATIONAL_FIELDS, ...ORCHESTRATION_FIELDS]);

// Shadow scoring may update warmth audit fields inside the transaction. These
// fields must never change even temporarily because they represent operational
// lifecycle, routing, task, or sequence state.
const IN_TRANSACTION_IMMUTABLE_FIELDS = Object.freeze([
  ...LEGACY_OPERATIONAL_FIELDS,
  'lifecycle_state', 'previous_lifecycle_state', 'state_changed_at',
  'state_reason_codes', 'state_reason_summary', 'next_best_action',
  'next_action_due_at', 'next_action_status', 'operator_required',
  'operator_priority', 'operator_reason', 'recycle_eligible_at',
  'recycle_reason', 'active_sequence_type', 'active_sequence_id',
]);

const OPERATIONAL_COUNT_KEYS = Object.freeze([
  'agent_actions', 'cal_queue', 'touchpoint_sends', 'email_event_sends',
  'agent_log_sends', 'enrichment_activity',
]);

const REQUIRED_INVARIANT_COLUMNS = Object.freeze({
  prospects: ['id', 'client_id', ...OPERATIONAL_FIELDS],
  agent_actions: ['client_id', 'payload'],
  cal_queue: ['client_id', 'prospect_id'],
  touchpoints: ['client_id', 'prospect_id', 'channel', 'action_type'],
  email_events: ['client_id', 'prospect_id', 'event_type'],
  agent_log: ['client_id', 'prospect_id', 'payload', 'action'],
  prospect_signal_events: ['client_id', 'prospect_id', 'source', 'source_record_id', 'metadata'],
  max_decisions: ['id', 'client_id', 'prospect_id', 'trigger_event_id'],
  prospect_state_transitions: ['client_id', 'prospect_id', 'decision_id', 'is_shadow', 'applied'],
  max_actions: ['client_id', 'prospect_id', 'decision_id', 'action_status', 'error_code'],
});

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--prospect-id', '--client-id'] });
  const prospectId = optionalUuid(parsed.values.get('--prospect-id'), '--prospect-id');
  const clientId = optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id');
  if (!prospectId || !clientId) throw new Error('--prospect-id and --client-id are required');
  return { prospectId, clientId };
}

function selectedSnapshot(row, fields = OPERATIONAL_FIELDS) {
  return Object.fromEntries(fields.map(field => [field, row?.[field] ?? null]));
}

function operationalSnapshot(row) {
  return selectedSnapshot(row, OPERATIONAL_FIELDS);
}

function sameSnapshot(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function failAssertion(message, details = null) {
  const error = new Error(`SMOKE_ASSERTION_FAILED: ${message}`);
  error.code = 'SMOKE_ASSERTION_FAILED';
  error.details = details;
  throw error;
}

async function validateInvariantSources(db) {
  const result = await db.query(`
    SELECT table_name, column_name, data_type, udt_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = ANY($1::text[])
  `, [Object.keys(REQUIRED_INVARIANT_COLUMNS)]);
  const rows = result.rows || [];
  const byTable = new Map();
  for (const row of rows) {
    if (!byTable.has(row.table_name)) byTable.set(row.table_name, new Map());
    byTable.get(row.table_name).set(row.column_name, row);
  }
  const unavailable = [];
  for (const [table, columns] of Object.entries(REQUIRED_INVARIANT_COLUMNS)) {
    const actual = byTable.get(table);
    if (!actual) {
      unavailable.push(`${table}: table missing`);
      continue;
    }
    const missing = columns.filter(column => !actual.has(column));
    if (missing.length) unavailable.push(`${table}: missing ${missing.join(', ')}`);
  }
  const payloadType = byTable.get('agent_actions')?.get('payload')?.udt_name;
  if (payloadType && !['json', 'jsonb'].includes(payloadType)) {
    unavailable.push(`agent_actions.payload: expected json/jsonb, found ${payloadType}`);
  }
  if (unavailable.length) {
    const error = new Error(`UNAVAILABLE_INVARIANT: ${unavailable.join('; ')}`);
    error.code = 'UNAVAILABLE_INVARIANT';
    error.unavailable = unavailable;
    throw error;
  }
  return { available: true, tables: Object.keys(REQUIRED_INVARIANT_COLUMNS) };
}

const AGENT_ACTION_PROSPECT_SQL = `CASE
  WHEN jsonb_typeof(aa.payload::jsonb) = 'object'
    AND (aa.payload::jsonb ->> 'prospect_id') ~* '${UUID_PATTERN}'
  THEN (aa.payload::jsonb ->> 'prospect_id')::uuid
  ELSE NULL
END`;

const AGENT_LOG_PROSPECT_SQL = `COALESCE(al.prospect_id, CASE
  WHEN jsonb_typeof(al.payload::jsonb) = 'object'
    AND (al.payload::jsonb ->> 'prospect_id') ~* '${UUID_PATTERN}'
  THEN (al.payload::jsonb ->> 'prospect_id')::uuid
  ELSE NULL
END)`;

async function captureInvariantCounts(db, prospectId, clientId) {
  const result = await db.query(`
    SELECT
      (SELECT COUNT(*)::int FROM agent_actions aa
        WHERE aa.client_id=$2 AND ${AGENT_ACTION_PROSPECT_SQL}=$1::uuid) AS agent_actions,
      (SELECT COUNT(*)::int FROM cal_queue cq
        WHERE cq.client_id=$2 AND cq.prospect_id=$1::uuid) AS cal_queue,
      (SELECT COUNT(*)::int FROM touchpoints t
        WHERE t.client_id=$2 AND t.prospect_id=$1::uuid
          AND t.channel IN ('email','sms')
          AND t.action_type IN ('outbound','email_warm','send')) AS touchpoint_sends,
      (SELECT COUNT(*)::int FROM email_events ee
        WHERE ee.client_id=$2 AND ee.prospect_id=$1::uuid
          AND ee.event_type='sent') AS email_event_sends,
      (SELECT COUNT(*)::int FROM agent_log al
        WHERE al.client_id=$2 AND ${AGENT_LOG_PROSPECT_SQL}=$1::uuid
          AND al.action IN ('email_sent','send_sms','batch_sms')) AS agent_log_sends,
      (SELECT COUNT(*)::int FROM agent_log al
        WHERE al.client_id=$2 AND ${AGENT_LOG_PROSPECT_SQL}=$1::uuid
          AND al.action IN ('enrichment_attempt','retry_enrichment','enrichment_retry')) AS enrichment_activity,
      (SELECT COUNT(*)::int FROM prospect_signal_events s
        WHERE s.client_id=$2 AND s.prospect_id=$1::uuid) AS max_signals,
      (SELECT COUNT(*)::int FROM max_decisions d
        WHERE d.client_id=$2 AND d.prospect_id=$1::uuid) AS max_decisions,
      (SELECT COUNT(*)::int FROM prospect_state_transitions st
        WHERE st.client_id=$2 AND st.prospect_id=$1::uuid) AS max_transitions,
      (SELECT COUNT(*)::int FROM max_actions ma
        WHERE ma.client_id=$2 AND ma.prospect_id=$1::uuid) AS max_actions
  `, [prospectId, clientId]);
  const row = result.rows[0];
  if (!row) throw new Error('UNAVAILABLE_INVARIANT: invariant count query returned no row');
  return Object.fromEntries(Object.entries(row).map(([key, value]) => [key, Number(value)]));
}

async function captureProspectState(db, prospectId, clientId, { lock = false } = {}) {
  const result = await db.query(`
    SELECT to_jsonb(p) AS row
    FROM prospects p
    WHERE p.id=$1::uuid AND p.client_id=$2
    ${lock ? 'FOR UPDATE' : ''}
  `, [prospectId, clientId]);
  if (!result.rows[0]) throw new Error(`Designated prospect not found for client ${clientId}`);
  return operationalSnapshot(result.rows[0].row);
}

async function captureInvariantSnapshot(db, prospectId, clientId, options = {}) {
  return {
    prospect: await captureProspectState(db, prospectId, clientId, options),
    counts: await captureInvariantCounts(db, prospectId, clientId),
  };
}

function countDeltas(before, after) {
  return Object.fromEntries(Object.keys(before).map(key => [key, Number(after[key]) - Number(before[key])]));
}

async function captureSmokeResiduals(db, { clientId, prospectId, smokeId, signalId }) {
  const result = await db.query(`
    WITH smoke_signals AS (
      SELECT id FROM prospect_signal_events
      WHERE client_id=$1 AND prospect_id=$2::uuid
        AND source='max_shadow_smoke'
        AND (source_record_id=$3 OR metadata->>'smoke_test_id'=$3 OR id=$4)
    ), smoke_decisions AS (
      SELECT id FROM max_decisions
      WHERE client_id=$1 AND prospect_id=$2::uuid
        AND trigger_event_id IN (SELECT id FROM smoke_signals)
    )
    SELECT
      (SELECT COUNT(*)::int FROM smoke_signals) AS signals,
      (SELECT COUNT(*)::int FROM smoke_decisions) AS decisions,
      (SELECT COUNT(*)::int FROM prospect_state_transitions
        WHERE client_id=$1 AND prospect_id=$2::uuid
          AND decision_id IN (SELECT id FROM smoke_decisions)) AS transitions,
      (SELECT COUNT(*)::int FROM max_actions
        WHERE client_id=$1 AND prospect_id=$2::uuid
          AND decision_id IN (SELECT id FROM smoke_decisions)) AS actions
  `, [clientId, prospectId, smokeId, signalId]);
  const row = result.rows[0] || {};
  return {
    signals: Number(row.signals || 0),
    decisions: Number(row.decisions || 0),
    transitions: Number(row.transitions || 0),
    actions: Number(row.actions || 0),
  };
}

function totalResiduals(residuals) {
  return Object.values(residuals).reduce((sum, value) => sum + Number(value || 0), 0);
}

async function run(options = parseArgs(), db = pool, env = process.env, dependencies = {}) {
  const validateSchemaFn = dependencies.validateSchema || validateSchema;
  const validateInvariantSourcesFn = dependencies.validateInvariantSources || validateInvariantSources;
  const loadClientConfigFn = dependencies.loadClientOrchestrationConfig || loadClientOrchestrationConfig;
  const loadConfigFn = dependencies.loadMaxOrchestrationConfig || loadMaxOrchestrationConfig;
  const ingestFn = dependencies.ingestNormalizedSignal || ingestNormalizedSignal;
  const evaluateFn = dependencies.evaluateProspectShadow
    || (args => require('../utils/maxOrchestration').evaluateProspectShadow(args));
  const randomUuidFn = dependencies.randomUUID || crypto.randomUUID;

  const schema = await validateSchemaFn(db);
  if (!schema.valid) throw new Error(`Schema validation failed: ${JSON.stringify(schema)}`);
  await validateInvariantSourcesFn(db);
  const clientConfig = await loadClientConfigFn(db, options.clientId);
  const config = loadConfigFn({ env, clientOverrides: clientConfig.max_orchestration_config });
  const enabledActionFlags = Object.entries(config.flags)
    .filter(([name, value]) => !['max_shadow_mode', 'max_scoring_enabled'].includes(name) && value === true);
  if (!config.enabled || !config.flags.max_scoring_enabled || !config.flags.max_shadow_mode || enabledActionFlags.length) {
    throw new Error(`Unsafe smoke-test flags: enabled=${config.enabled}, shadow=${config.flags.max_shadow_mode}, scoring=${config.flags.max_scoring_enabled}, enabled_action_flags=${enabledActionFlags.map(([name]) => name).join(',')}`);
  }

  const smokeId = `shadow-smoke:${randomUuidFn()}`;
  const signalId = stableSignalId({
    source: 'max_shadow_smoke', sourceRecordId: smokeId,
    eventType: 'email_positive_reply', prospectId: options.prospectId,
  });
  const client = await db.connect();
  let before = null;
  let transactionStarted = false;
  let rolledBack = false;
  let inTransaction = null;
  try {
    await client.query('BEGIN');
    transactionStarted = true;
    before = await captureInvariantSnapshot(client, options.prospectId, options.clientId, { lock: true });
    const result = await ingestFn({
      client_id: options.clientId,
      prospect_id: options.prospectId,
      event_type: 'email_positive_reply',
      event_timestamp: new Date(),
      source: 'max_shadow_smoke',
      source_record_id: smokeId,
      metadata: { synthetic: true, provenance: 'synthetic_smoke', contact_prohibited: true, smoke_test_id: smokeId },
    }, {
      db: client,
      env,
      evaluate: true,
      transactionContext: { client, transactionManagedByCaller: true },
      evaluateProspectFn: evaluateFn,
    });

    if (!result.signal_id) failAssertion('synthetic normalized signal was not created');
    if (!result.score || !Number.isFinite(Number(result.score.score))) failAssertion('shadow score was not generated');
    if (!result.decision?.id) failAssertion('shadow decision was not generated');

    const auditResult = await client.query(`
      SELECT d.id, d.is_shadow, d.warmth_score,
             COUNT(a.id)::int AS action_count,
             COALESCE(BOOL_AND(a.action_status='skipped' AND a.error_code='SHADOW_MODE'), FALSE) AS all_actions_skipped
      FROM max_decisions d
      LEFT JOIN max_actions a ON a.decision_id=d.id
      WHERE d.id=$1 AND d.client_id=$2 AND d.prospect_id=$3::uuid
      GROUP BY d.id,d.is_shadow,d.warmth_score
    `, [result.decision.id, options.clientId, options.prospectId]);
    const audit = auditResult.rows[0] || {};
    if (audit.is_shadow !== true) failAssertion('persisted decision is not shadow');
    if (!Number.isFinite(Number(audit.warmth_score))) failAssertion('persisted decision has no warmth score');
    if (Number(audit.action_count) < 1 || audit.all_actions_skipped !== true) {
      failAssertion('recommended actions were not all skipped with SHADOW_MODE', audit);
    }

    const transitionResult = await client.query(`
      SELECT COUNT(*)::int AS transition_count,
             COALESCE(BOOL_AND(is_shadow=TRUE AND applied=FALSE), FALSE) AS all_shadow_unapplied
      FROM prospect_state_transitions
      WHERE decision_id=$1 AND client_id=$2 AND prospect_id=$3::uuid
    `, [result.decision.id, options.clientId, options.prospectId]);
    const transitionAudit = transitionResult.rows[0] || {};
    if (Number(transitionAudit.transition_count) < 1 || transitionAudit.all_shadow_unapplied !== true) {
      failAssertion('shadow lifecycle transition recommendation missing or applied', transitionAudit);
    }

    inTransaction = await captureInvariantSnapshot(client, options.prospectId, options.clientId);
    const immutableBefore = selectedSnapshot(before.prospect, IN_TRANSACTION_IMMUTABLE_FIELDS);
    const immutableAfter = selectedSnapshot(inTransaction.prospect, IN_TRANSACTION_IMMUTABLE_FIELDS);
    const deltas = countDeltas(before.counts, inTransaction.counts);
    const sideEffectDeltas = Object.fromEntries(OPERATIONAL_COUNT_KEYS.map(key => [key, deltas[key]]));
    const fieldMutations = diffOperationalSnapshots(immutableBefore, immutableAfter, {
      entity: 'prospect', entityId: options.prospectId, clientId: options.clientId,
      correlationId: smokeId, transactionOwner: 'max', maxDecisionId: result.decision.id,
    });
    const countMutations = Object.entries(sideEffectDeltas)
      .filter(([, delta]) => Number(delta) !== 0)
      .map(([field, delta]) => ({
        entity: 'operational_count', entity_id: options.prospectId, client_id: options.clientId,
        field, before: before.counts[field], after: inTransaction.counts[field], delta,
        correlation_id: smokeId, transaction_owner: 'max', max_decision_id: result.decision.id,
      }));
    const mutationAttribution = classifyOperationalMutations([...fieldMutations, ...countMutations], {
      maxDecisionIds: [result.decision.id], maxCorrelationIds: [smokeId],
    });
    if (mutationAttribution.stop_required) {
      failAssertion('Max-attributable operational mutation detected inside smoke transaction', mutationAttribution);
    }
    if (deltas.max_signals !== 1 || deltas.max_decisions !== 1 || deltas.max_transitions < 1 || deltas.max_actions < 1) {
      failAssertion('expected shadow audit records were not created exactly once', deltas);
    }

    await client.query('ROLLBACK');
    rolledBack = true;
    const afterRollback = await captureInvariantSnapshot(db, options.prospectId, options.clientId);
    const residuals = await captureSmokeResiduals(db, {
      clientId: options.clientId, prospectId: options.prospectId, smokeId, signalId,
    });
    if (!sameSnapshot(before, afterRollback)) {
      failAssertion('pre-test state was not restored after rollback', { before, after: afterRollback });
    }
    if (totalResiduals(residuals) !== 0) failAssertion('synthetic smoke records remain after rollback', residuals);

    return {
      valid: true,
      mode: 'transactional_rollback',
      smoke_test_id: smokeId,
      schema_valid: schema.valid,
      shadow_mode: true,
      synthetic_signal_normalized: true,
      shadow_score_generated: true,
      shadow_score: Number(result.score.score),
      shadow_decision_persisted: true,
      shadow_transition_recommended: true,
      recommended_actions: Number(audit.action_count),
      actions_skipped_with_shadow_mode: true,
      operational_state_unchanged: true,
      pre_test: before,
      in_transaction: inTransaction,
      post_rollback: afterRollback,
      in_transaction_deltas: deltas,
      side_effect_deltas: sideEffectDeltas,
      mutation_attribution: mutationAttribution,
      rolled_back: true,
      synthetic_records_remaining: totalResiduals(residuals),
      residuals,
    };
  } catch (error) {
    if (transactionStarted && !rolledBack) {
      await client.query('ROLLBACK').catch(rollbackError => {
        error.rollback_error = rollbackError.message;
      });
      rolledBack = !error.rollback_error;
    }
    if (before && rolledBack) {
      try {
        const afterRollback = await captureInvariantSnapshot(db, options.prospectId, options.clientId);
        const residuals = await captureSmokeResiduals(db, {
          clientId: options.clientId, prospectId: options.prospectId, smokeId, signalId,
        });
        error.smoke_failure = {
          smoke_test_id: smokeId,
          rolled_back: true,
          state_restored: sameSnapshot(before, afterRollback),
          synthetic_records_remaining: totalResiduals(residuals),
          residuals,
        };
        error.message = `${error.message}; rollback_verification=${JSON.stringify(error.smoke_failure)}`;
      } catch (verificationError) {
        error.message = `${error.message}; rollback_verification_unavailable=${verificationError.message}`;
      }
    }
    throw error;
  } finally {
    client.release();
  }
}

module.exports = {
  AGENT_ACTION_PROSPECT_SQL,
  AGENT_LOG_PROSPECT_SQL,
  IN_TRANSACTION_IMMUTABLE_FIELDS,
  LEGACY_OPERATIONAL_FIELDS,
  OPERATIONAL_COUNT_KEYS,
  OPERATIONAL_FIELDS,
  ORCHESTRATION_FIELDS,
  REQUIRED_INVARIANT_COLUMNS,
  captureInvariantCounts,
  captureInvariantSnapshot,
  captureSmokeResiduals,
  countDeltas,
  operationalSnapshot,
  parseArgs,
  run,
  sameSnapshot,
  selectedSnapshot,
  totalResiduals,
  validateInvariantSources,
};

if (require.main === module) {
  run().then(report => {
    console.log(JSON.stringify(report, null, 2));
    process.exitCode = report.valid ? 0 : 1;
  }).catch(error => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }).finally(() => pool.end());
}
