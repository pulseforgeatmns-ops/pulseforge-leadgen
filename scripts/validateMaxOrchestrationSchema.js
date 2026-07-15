require('dotenv').config();

const pool = require('../db');

const REQUIRED_COLUMNS = Object.freeze({
  prospects: [
    'lifecycle_state','previous_lifecycle_state','state_changed_at','warmth_score',
    'warmth_score_updated_at','warmth_score_version','state_reason_codes','state_reason_summary',
    'next_best_action','next_action_due_at','next_action_status','operator_required',
    'operator_priority','operator_reason','last_meaningful_signal_at','last_human_open_at',
    'last_reply_at','last_positive_reply_at','recycle_eligible_at','recycle_reason',
    'active_sequence_type','active_sequence_id','downgrade_candidate_since',
  ],
  clients: ['max_orchestration_config'],
  max_recommendation_reviews: ['client_id','decision_id','prospect_id','reviewer_identity','review_outcome','reviewed_at',
    'score_component_explanation','source_data_trustworthy','source_data_notes'],
  max_rollout_readiness_config: [
    'client_id','phase3_allowlisted','minimum_reviewed_samples','minimum_total_reviews',
    'shadow_observation_enabled','minimum_reviews_by_transition','terminal_review_requirement',
    'minimum_agreement_rate','maximum_failure_rate','maximum_oscillation_rate',
    'rollback_documented','rollback_reference','rollback_reference_verified',
    'recovery_snapshot_reference','recovery_snapshot_verified','created_at','updated_at',
    'recovery_artifact_found','recovery_hash_verified','recovery_archive_readable',
    'recovery_restore_procedure_documented','recovery_durable_storage_verified',
    'decay_schedule_configured','decay_schedule_verified','decay_schedule_reference',
    'decay_schedule_command','decay_schedule_frequency','decay_schedule_timezone',
  ],
  max_meeting_outcome_events: [
    'client_id','prospect_id','company_id','event_type','source','source_record_id',
    'event_timestamp','original_event_timestamp','confidence','correction_of_event_id','metadata','created_at',
  ],
  max_decay_run_events: [
    'run_id','mode','status','started_at','completed_at','lock_acquired','client_scope',
    'batch_limit','start_cursor','end_cursor','candidates_found','prospects_evaluated',
    'scores_changed','downgrade_candidates','recommendations_created','decisions_created',
    'errors','error_stage','error_code','error_summary','retryable','operational_effects',
    'deployment_commit','details',
  ],
});

const REQUIRED_TABLES = Object.freeze([
  'prospect_signal_events','max_decisions','prospect_state_transitions','max_actions',
  'manual_lifecycle_overrides','max_orchestration_metrics',
  'max_recommendation_reviews','max_rollout_readiness_config','max_decay_run_events',
  'max_meeting_outcome_events',
]);

const REQUIRED_INDEXES = Object.freeze([
  'prospects_lifecycle_state_idx','prospect_signal_events_prospect_time_idx',
  'prospect_signal_events_source_type_uidx','prospect_signal_events_decay_candidates_idx',
  'max_decisions_prospect_created_idx','max_decisions_recommended_state_idx',
  'prospect_state_transitions_funnel_idx','max_actions_status_idx',
  'max_recommendation_reviews_client_time_idx','max_recommendation_reviews_outcome_idx',
  'max_decay_run_events_run_time_idx','max_decay_run_events_recent_idx',
  'max_meeting_outcome_events_prospect_time_idx',
]);

const REQUIRED_CONSTRAINTS = Object.freeze([
  'prospects_lifecycle_state_check','prospects_previous_lifecycle_state_check',
  'prospects_warmth_score_check','prospects_next_action_status_check',
  'prospects_operator_priority_check','prospects_active_sequence_type_check',
  'prospect_signal_events_client_fk','max_decisions_client_fk','prospect_state_transitions_client_fk',
  'max_actions_client_fk','manual_lifecycle_overrides_client_fk','max_orchestration_metrics_client_fk',
]);

const REQUIRED_TRIGGERS = Object.freeze([
  'prospect_signal_events_append_only','max_decisions_append_only',
  'prospect_state_transitions_append_only','max_actions_append_only',
  'manual_lifecycle_overrides_append_only','max_orchestration_metrics_append_only',
  'max_recommendation_reviews_append_only',
  'max_decay_run_events_append_only',
  'max_meeting_outcome_events_append_only',
]);

const REQUIRED_COLUMN_TYPES = Object.freeze({
  'clients.id': 'integer',
  'prospects.id': 'uuid',
  'companies.id': 'uuid',
  'prospect_signal_events.client_id': 'integer',
  'prospect_signal_events.prospect_id': 'uuid',
  'prospect_signal_events.metadata': 'jsonb',
  'max_decisions.config_snapshot': 'jsonb',
  'max_recommendation_reviews.id': 'uuid',
  'max_rollout_readiness_config.client_id': 'integer',
  'max_decay_run_events.id': 'uuid',
  'max_decay_run_events.run_id': 'uuid',
  'max_meeting_outcome_events.id': 'uuid',
  'max_meeting_outcome_events.metadata': 'jsonb',
});

const REQUIRED_JSON_DEFAULTS = Object.freeze([
  'clients.max_orchestration_config','prospects.state_reason_codes','prospect_signal_events.metadata',
  'max_decisions.score_components','max_decisions.reason_codes','max_decisions.actions',
  'max_decisions.config_snapshot','max_actions.input_payload','max_actions.output_payload',
  'max_orchestration_metrics.dimensions',
  'max_rollout_readiness_config.minimum_reviews_by_transition',
  'max_decay_run_events.operational_effects','max_decay_run_events.details',
  'max_meeting_outcome_events.metadata',
]);

async function validateSchema(db = pool) {
  // Run sequentially so Railway's proxy does not need six simultaneous catalog
  // connections for a read-only smoke check.
  const columns = await db.query(`SELECT table_name, column_name, data_type, udt_name, column_default FROM information_schema.columns WHERE table_schema='public'`);
  const tables = await db.query(`SELECT table_name FROM information_schema.tables WHERE table_schema='public'`);
  const indexes = await db.query(`SELECT indexname FROM pg_indexes WHERE schemaname='public'`);
  const constraints = await db.query(`
    SELECT con.conname AS constraint_name, con.contype, con.convalidated,
           pg_get_constraintdef(con.oid) AS definition
    FROM pg_constraint con JOIN pg_namespace ns ON ns.oid=con.connamespace
    WHERE ns.nspname='public'
  `);
  const triggers = await db.query(`SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema='public'`);
  const statusFingerprint = await db.query(`
    SELECT COUNT(*)::int AS total,
           COUNT(*) FILTER (WHERE status IS NULL)::int AS null_status,
           ARRAY_AGG(DISTINCT status ORDER BY status) AS values
    FROM prospects
  `);
  const columnSet = new Set(columns.rows.map(row => `${row.table_name}.${row.column_name}`));
  const tableSet = new Set(tables.rows.map(row => row.table_name));
  const indexSet = new Set(indexes.rows.map(row => row.indexname));
  const constraintSet = new Set(constraints.rows.map(row => row.constraint_name));
  const triggerSet = new Set(triggers.rows.map(row => row.trigger_name));
  const columnsByName = new Map(columns.rows.map(row => [`${row.table_name}.${row.column_name}`, row]));
  const missingColumns = Object.entries(REQUIRED_COLUMNS).flatMap(([table, names]) => names
    .filter(name => !columnSet.has(`${table}.${name}`)).map(name => `${table}.${name}`));
  const missingTables = REQUIRED_TABLES.filter(name => !tableSet.has(name));
  const missingIndexes = REQUIRED_INDEXES.filter(name => !indexSet.has(name));
  const missingConstraints = REQUIRED_CONSTRAINTS.filter(name => !constraintSet.has(name));
  const requiredClientFks = REQUIRED_CONSTRAINTS.filter(name => name.endsWith('_client_fk'));
  const invalidForeignKeys = constraints.rows.filter(row => requiredClientFks.includes(row.constraint_name)
    && (row.contype !== 'f' || row.convalidated !== true || !String(row.definition || '').includes('REFERENCES clients(id)')))
    .map(row => ({ name: row.constraint_name, validated: row.convalidated, definition: row.definition || null }));
  const missingTriggers = REQUIRED_TRIGGERS.filter(name => !triggerSet.has(name));
  const typeMismatches = Object.entries(REQUIRED_COLUMN_TYPES).flatMap(([name, expected]) => {
    const row = columnsByName.get(name);
    if (!row) return [];
    const actual = row.data_type || row.udt_name;
    return actual === expected ? [] : [{ column: name, expected, actual }];
  });
  const jsonDefaultMismatches = REQUIRED_JSON_DEFAULTS.filter(name => {
    const value = columnsByName.get(name)?.column_default || '';
    return !value.includes("'{}'::jsonb") && !value.includes("'[]'::jsonb");
  });
  return {
    valid: !missingColumns.length && !missingTables.length && !missingIndexes.length && !missingConstraints.length && !missingTriggers.length && !typeMismatches.length && !jsonDefaultMismatches.length && !invalidForeignKeys.length,
    missing_columns: missingColumns,
    missing_tables: missingTables,
    missing_indexes: missingIndexes,
    missing_constraints: missingConstraints,
    missing_triggers: missingTriggers,
    type_mismatches: typeMismatches,
    json_default_mismatches: jsonDefaultMismatches,
    invalid_foreign_keys: invalidForeignKeys,
    operational_status_fingerprint: statusFingerprint.rows[0] || {},
    status_mutated: false,
  };
}

module.exports = { REQUIRED_COLUMNS, REQUIRED_COLUMN_TYPES, REQUIRED_CONSTRAINTS, REQUIRED_INDEXES, REQUIRED_JSON_DEFAULTS, REQUIRED_TABLES, REQUIRED_TRIGGERS, validateSchema };

if (require.main === module) {
  pool.options.connectionTimeoutMillis = 10000;
  pool.options.query_timeout = 15000;
  pool.query(`SELECT current_database() AS database, current_user AS user, inet_server_addr()::text AS server_address, version() AS version`)
  .then(async connection => {
    const report = await validateSchema();
    console.log(JSON.stringify({ connection: connection.rows[0], ...report }, null, 2));
    process.exitCode = report.valid ? 0 : 1;
  }).catch(error => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }).finally(() => pool.end());
}
