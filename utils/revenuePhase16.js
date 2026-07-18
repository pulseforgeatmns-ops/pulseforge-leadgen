'use strict';

const crypto = require('crypto');

const REQUIRED_ACTIONS = Object.freeze([
  'apply_revenue_migrations',
  'enable_operator_reads',
  'enable_operator_writes_for_canary',
  'execute_single_canary',
  'run_reconciliation',
]);

function canonicalHash(value) {
  return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
function isUtc(value) {
  return typeof value === 'string' && /Z$/.test(value) && !Number.isNaN(Date.parse(value));
}
function validateAuthorization(input, now = new Date()) {
  const failures = [];
  if (!input || input.phase !== 'revenue-phase-1.6') failures.push('phase must be revenue-phase-1.6');
  if (Number(input.client_id) !== 10) failures.push('client_id must be Anchor client 10');
  if (!input.authorized_operator || !input.rollback_owner || !input.approved_by) failures.push('named operator, rollback owner, and approver are required');
  if (!isUtc(input.approved_at) || !isUtc(input.window_start) || !isUtc(input.window_end)) failures.push('approval and window timestamps must be UTC ISO-8601 values');
  if (isUtc(input.window_start) && isUtc(input.window_end) && Date.parse(input.window_start) >= Date.parse(input.window_end)) failures.push('window_start must be before window_end');
  if (isUtc(input.window_start) && isUtc(input.window_end) && (now < new Date(input.window_start) || now > new Date(input.window_end))) failures.push('authorization window is not currently active');
  if (input.maximum_canary_outcomes !== 1) failures.push('maximum_canary_outcomes must equal 1');
  if (input.external_sends_allowed !== false || input.refunds_allowed !== false || input.max_mutations_allowed !== false) failures.push('external sends, refunds, and Max mutations must be false');
  if (!Array.isArray(input.authorized_actions) || input.authorized_actions.length !== REQUIRED_ACTIONS.length || REQUIRED_ACTIONS.some(action => !input.authorized_actions.includes(action))) failures.push('authorized_actions must contain exactly the required Phase 1.6 actions');
  if (!Array.isArray(input.stop_conditions) || input.stop_conditions.length === 0) failures.push('stop_conditions must be explicit');
  const authorizationHash = failures.length ? null : canonicalHash({ ...input, authorization_hash: undefined });
  return { valid: failures.length === 0, failures, authorizationHash };
}

async function safeQuery(db, name, sql, params = []) {
  try { return { name, ok: true, rows: (await db.query(sql, params)).rows }; }
  catch (error) { return { name, ok: false, error: error.code || error.message, rows: [] }; }
}

async function productionPreflight(db, authorization, environment = process.env) {
  const auth = validateAuthorization(authorization);
  const clientId = 10;
  const checks = [
    await safeQuery(db, 'database_identity', 'SELECT current_database() AS database, current_user AS actor, NOW() AS checked_at'),
    await safeQuery(db, 'revenue_tables', `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('revenue_events','revenue_outcomes','revenue_feature_flags','revenue_operator_audit','revenue_reconciliation_runs') ORDER BY table_name`),
    await safeQuery(db, 'feature_flags', 'SELECT * FROM revenue_feature_flags WHERE client_id=$1', [clientId]),
    await safeQuery(db, 'anchor_safety', 'SELECT id,enabled_agents,autosend_enabled FROM clients WHERE id=$1', [clientId]),
    await safeQuery(db, 'append_only_trigger', `SELECT tgname FROM pg_trigger WHERE tgrelid='revenue_events'::regclass AND NOT tgisinternal`),
    await safeQuery(db, 'unexpected_revenue_events', 'SELECT COUNT(*)::int AS count FROM revenue_events WHERE client_id=$1', [clientId]),
    await safeQuery(db, 'unexpected_projections', 'SELECT COUNT(*)::int AS count FROM revenue_outcomes WHERE client_id=$1', [clientId]),
    await safeQuery(db, 'active_connections', `SELECT COUNT(*)::int AS count FROM pg_stat_activity WHERE datname=current_database() AND pid <> pg_backend_pid()`),
  ];
  const flags = checks.find(check => check.name === 'feature_flags');
  const anchor = checks.find(check => check.name === 'anchor_safety');
  const schema = checks.find(check => check.name === 'revenue_tables');
  const failures = [...auth.failures];
  if (environment.NODE_ENV !== 'production') failures.push('NODE_ENV is not production');
  for (const name of ['REVENUE_SCHEMA_ENABLED','REVENUE_OPERATOR_READS_ENABLED','REVENUE_OPERATOR_WRITES_ENABLED','REVENUE_MAX_READS_ENABLED','REVENUE_FOLLOWUP_RECOMMENDATIONS_ENABLED']) {
    if (String(environment[name] || '').toLowerCase() === 'true') failures.push(`${name} is enabled before certification`);
  }
  if (!schema.ok || schema.rows.length !== 5) failures.push('required revenue tables are not present');
  if (!flags.ok || flags.rows.length !== 1) failures.push('Anchor revenue feature flag row is unavailable');
  else if (Object.entries(flags.rows[0]).filter(([key]) => key.startsWith('revenue_') && key.endsWith('_enabled')).some(([, value]) => value !== false)) failures.push('one or more revenue feature flags are enabled before certification');
  if (!anchor.ok || anchor.rows.length !== 1) failures.push('Anchor client safety state is unavailable');
  else if (anchor.rows[0].autosend_enabled !== false || JSON.stringify(anchor.rows[0].enabled_agents) !== JSON.stringify(['scout'])) failures.push('Anchor is not Scout-only with autosend disabled');
  if (checks.some(check => !check.ok)) failures.push('one or more required read-only checks failed');
  return {
    phase: 'revenue-phase-1.6', checkedAt: new Date().toISOString(), authorizationHash: auth.authorizationHash,
    status: failures.length ? 'blocked' : 'ready_for_authorized_execution', failures, checks,
    productionExecutionPermitted: false,
  };
}

module.exports = { REQUIRED_ACTIONS, canonicalHash, productionPreflight, validateAuthorization };
