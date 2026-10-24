'use strict';

const crypto = require('crypto');

const PHASE = 'revenue-phase-1.6b-production-migration-controlled-anchor-canary';
const AUTHORIZATION_ID = '3808a9f7-b8e4-467f-917f-5021dfb7d485';
const CLIENT_ID = 10;
const TWO_HOURS_MS = 2 * 60 * 60 * 1000;
const MAX_IDENTITY_AGE_MS = 15 * 60 * 1000;

// Authorizations consumed by a production attempt (successful or failed) are
// permanently non-reusable. ce9005f1… was consumed by the blocked
// 2026-07-21T04:02Z attempt (initiating SQLSTATE 42830; see
// docs/REVENUE_PHASE16B_IMPLEMENTATION_REPORT.md).
const RETIRED_AUTHORIZATION_IDS = Object.freeze([
  'ce9005f1-6d6f-46fd-a52d-4081a79ed02f',
]);

const CERTIFIED_MIGRATIONS = Object.freeze({
  phase1: Object.freeze({
    path: 'migrations/2026-07-18-anchor-closed-loop-revenue-phase1.sql',
    sha256: 'c11740daa17a4d8495daa428134effdffaa0d64d8b343e044739a23d28fa6495',
  }),
  phase15: Object.freeze({
    path: 'migrations/2026-07-18-anchor-closed-loop-revenue-phase15.sql',
    sha256: '9c8e1ed7b44e5fd4be9944193b1cfd1a9c1d54113795b7accd2d7d8d36fe23ed',
  }),
  phase15_operational_rollback: Object.freeze({
    path: 'migrations/2026-07-18-anchor-closed-loop-revenue-phase15.rollback.sql',
    sha256: '2ae6e4a87638722bbb2ad40d6c218a7593cfd369920b2c67dbafa233955c3045',
  }),
});

const IDEMPOTENCY_KEYS = Object.freeze({
  customer_create: 'phase16b-eliza-customer-create-75948e35-53b2-4f91-afdd-58b3475f34e9',
  opportunity_create: 'phase16b-eliza-opportunity-create-536fc409-2ca0-4b74-8009-9ceb9ee33451',
  opportunity_contacted: 'phase16b-eliza-opportunity-contacted-dafd1487-c57a-4306-a03a-23afbe8084c2',
  opportunity_qualified: 'phase16b-eliza-opportunity-qualified-5cdc5160-b9e5-40e0-94e4-56a02422ab70',
  opportunity_quoted: 'phase16b-eliza-opportunity-quoted-bd54bca4-39ac-4b7f-98f8-0654cb00a45a',
  job_create: 'phase16b-eliza-job-create-81de3325-c294-46b7-acc3-aa1a5b5cff11',
  job_complete: 'phase16b-eliza-job-complete-9712d4b9-0eb1-410d-b430-2ec02ab16f2b',
  payment_succeeded: 'phase16b-eliza-payment-succeeded-9aca5b37-85b2-41dd-823c-6b1d0dacf8c9',
});

const REQUIRED_ACTIONS = Object.freeze([
  'validate_exact_protected_main_deployment_commit_and_migration_checksums',
  'apply_certified_phase1_revenue_migration',
  'apply_certified_phase15_revenue_migration',
  'verify_schema_constraints_permissions_triggers_and_health_with_all_flags_off',
  'enable_revenue_schema_for_client_10_only',
  'enable_revenue_operator_reads_for_client_10_only',
  'enable_revenue_max_reads_for_client_10_only',
  'temporarily_enable_revenue_operator_writes_for_client_10_for_one_named_canary_only',
  'execute_exactly_one_eliza_bulger_canary_outcome',
  'disable_revenue_operator_writes_immediately_after_canary',
  'run_recorded_compare_only_reconciliation_for_client_10',
  'run_non_destructive_deterministic_projection_reconstruction_comparison',
  'produce_phase16b_certification_evidence',
]);

const REQUIRED_FALSE = Object.freeze([
  'refunds_allowed',
  'second_canary_allowed',
  'other_client_writes_allowed',
  'max_mutations_allowed',
  'external_communications_allowed',
  'provider_activity_allowed',
  'followup_sends_allowed',
  'followup_recommendations_enabled',
  'retention_automation_allowed',
  'historical_bulk_backfill_allowed',
  'automatic_continuation_beyond_phase16b_allowed',
]);

const REQUIRED_STOP_CONDITIONS = Object.freeze([
  'authorization validation fails or authorization is unsigned, unhashed, not yet active, or expired',
  'protected-main commit, deployed commit, Railway deployment identity, migration path, or migration checksum differs',
  'Phase 1.6A durable backup, readability, retention, Keychain, restore, or independent-verification evidence is unavailable or differs',
  'production environment, database, client 10, or authorized actor cannot be conclusively identified',
  'any revenue flag is enabled before its authorized sequence step or any non-client-10 revenue flag changes',
  'schema, constraint, permission, trigger, append-only, route, application, database, or connection health verification fails',
  'any one-cent discrepancy appears in booked, delivered, collected, refunded, or net-collected revenue',
  'ledger event count is not exactly 12 or projected/source outcome count is not exactly 1',
  'tenant mismatch, duplicate financial effect, duplicate outcome attempt, idempotency mismatch, or unauthorized record appears',
  'append-only violation, unexplained event, source/ledger/projection mismatch, or reconciliation failure occurs',
  'deterministic reconstruction hashes differ from each other or from the persisted projection',
  'refund, external communication, provider activity, follow-up creation or send, retention activity, or Max mutation occurs',
  'operator writes are not disabled immediately after the one canary attempt, including on failure',
  'application or database health degrades or migration result is ambiguous',
  'any request is made to continue automatically beyond Revenue Phase 1.6B',
]);

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonicalize(value[key])]));
  }
  return value;
}

function canonicalAuthorizationHash(input) {
  const copy = structuredClone(input);
  delete copy.authorization_hash;
  return crypto.createHash('sha256').update(JSON.stringify(canonicalize(copy))).digest('hex');
}

function isUuid(value) {
  return typeof value === 'string'
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function isUtc(value) {
  return typeof value === 'string'
    && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value)
    && !Number.isNaN(Date.parse(value));
}

function unresolved(value) {
  return typeof value !== 'string'
    || !value.trim()
    || /(?:<[^>]+>|tbd|todo|placeholder|unresolved|non[_ -]?executable)/i.test(value);
}

function validateHistoricalDate(value, label, failures) {
  if (!value || typeof value !== 'object') {
    failures.push(`${label} must be an operator-confirmed historical date representation`);
    return;
  }
  const dateMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value.local_date || '');
  const parsed = dateMatch ? new Date(`${value.local_date}T00:00:00Z`) : null;
  if (!dateMatch
    || Number.isNaN(parsed.getTime())
    || parsed.toISOString().slice(0, 10) !== value.local_date) {
    failures.push(`${label}.local_date must be a real ISO calendar date`);
  }
  if (value.timezone !== 'America/New_York') failures.push(`${label}.timezone must be America/New_York`);
  if (value.precision !== 'day') failures.push(`${label}.precision must be day`);
  if (value.operator_confirmed !== true) failures.push(`${label}.operator_confirmed must be true`);
}

function sameSet(actual, expected) {
  return Array.isArray(actual)
    && actual.length === expected.length
    && expected.every(value => actual.includes(value));
}

function validateEvidence(input, observed, failures, now) {
  const release = input.release || {};
  if (!observed || typeof observed !== 'object') {
    failures.push('fresh release and backup evidence is required');
    return;
  }
  if (!isUtc(observed.observed_at)
    || Math.abs(now.getTime() - Date.parse(observed.observed_at)) > MAX_IDENTITY_AGE_MS) {
    failures.push('deployment identity observation is stale');
  }
  if (observed.protected_main_commit !== release.protected_main_commit) {
    failures.push('protected-main commit differs from authorization');
  }
  const deployment = observed.railway_deployment || {};
  if (deployment.status !== 'SUCCESS') failures.push('Railway deployment is not successful');
  if (deployment.id !== release.railway_deployment_id) failures.push('Railway deployment ID drift');
  if (deployment.commit !== release.deployed_commit
    || deployment.commit !== release.protected_main_commit) {
    failures.push('deployed commit drift');
  }
  if (deployment.service !== release.railway_service
    || deployment.environment !== release.railway_environment) {
    failures.push('Railway service or environment drift');
  }

  for (const name of ['phase1', 'phase15', 'phase15_operational_rollback']) {
    const authorized = input.migration_checksums?.[name];
    const current = observed.migration_checksums?.[name];
    const certified = CERTIFIED_MIGRATIONS[name];
    if (!authorized || !current || !certified
      || authorized.path !== certified.path
      || authorized.sha256 !== certified.sha256
      || authorized.path !== current.path
      || authorized.sha256 !== current.sha256) {
      failures.push(`migration checksum drift: ${name}`);
    }
  }

  const prerequisite = input.phase16a_prerequisite || {};
  const closure = observed.phase16a?.closure || {};
  const backup = observed.phase16a?.backup || {};
  const restore = observed.phase16a?.restore || {};
  const evidenceMatches = [
    [closure.status, prerequisite.status],
    [closure.verdict, prerequisite.verdict],
    [closure.backup_identifier, prerequisite.backup_identifier],
    [closure.persistent_local_copy?.sha256, prerequisite.backup_sha256],
    [Number(closure.persistent_local_copy?.size_bytes), Number(prerequisite.backup_size_bytes)],
    [closure.drive?.folderId, prerequisite.drive_folder_id],
    [closure.drive?.backupFileId, prerequisite.drive_file_id],
    [backup.backup_sha256, prerequisite.backup_sha256],
    [backup.backup_readability_result, 'passed'],
    [backup.durable_location?.independent_verification, true],
    [backup.durable_location?.keychain_passphrase_present, true],
    [restore.restore_target_id, prerequisite.restore_target],
    [restore.restore_result, prerequisite.restore_result],
    [Number(restore.schema_table_count), Number(prerequisite.restore_schema_table_count)],
    [restore.schema_fingerprint, prerequisite.restore_schema_fingerprint],
    [restore.isolation_confirmed, true],
  ];
  if (evidenceMatches.some(([actual, expected]) => actual !== expected)
    || closure.keychain?.passphrase_exposed !== false
    || closure.productionExecutionPermitted !== false
    || closure.continues_to_phase16b !== false) {
    failures.push('Phase 1.6A backup or restore evidence mismatch');
  }
}

function validatePhase16bAuthorization(input, options = {}) {
  const failures = [];
  const now = options.now instanceof Date ? options.now : new Date(options.now || Date.now());
  if (!input || input.phase !== PHASE) failures.push(`phase must be ${PHASE}`);
  if (RETIRED_AUTHORIZATION_IDS.includes(input?.authorization_id)) {
    failures.push('authorization was consumed by a prior production attempt and is permanently retired; a new authorization is required');
  } else if (input?.authorization_id !== (options.expectedAuthorizationId || AUTHORIZATION_ID)) {
    failures.push('authorization_id differs from the immutable authorization ID');
  }
  if (Number(input?.client_id) !== CLIENT_ID) failures.push('client_id must be 10');
  if (input?.environment !== 'production') failures.push('environment must be production');
  if (/draft|unsigned|non[_ -]?executable|unresolved|awaiting/i.test(String(input?.draft_status || ''))) {
    failures.push('draft_status is not a finalized executable authorization');
  }

  const operator = input?.authorized_operator || {};
  const approver = input?.approving_authority || {};
  if (unresolved(operator.name) || unresolved(operator.identity) || unresolved(operator.signature)) {
    failures.push('operator identity and attestation are required');
  }
  if (unresolved(approver.name) || unresolved(approver.identity)
    || unresolved(approver.title) || unresolved(approver.signature)) {
    failures.push('approver identity, title, and attestation are required');
  }
  if (!isUtc(input?.approved_at) || Date.parse(input.approved_at) > now.getTime()) {
    failures.push('approved_at must be an observed UTC timestamp not in the future');
  }

  const start = input?.window?.start;
  const end = input?.window?.end;
  if (!isUtc(start) || !isUtc(end) || Date.parse(end) - Date.parse(start) !== TWO_HOURS_MS) {
    failures.push('authorization window must be exactly two hours in UTC');
  } else if (now < new Date(start) || now > new Date(end)) {
    failures.push('authorization window is not active');
  }

  if (!sameSet(input?.authorized_actions, REQUIRED_ACTIONS)) {
    failures.push('authorized_actions must contain exactly the required Phase 1.6B actions');
  }
  for (const key of REQUIRED_FALSE) {
    if (input?.required_false?.[key] !== false) failures.push(`required_false.${key} must remain false`);
  }
  if (!sameSet(input?.stop_conditions, REQUIRED_STOP_CONDITIONS)) {
    failures.push('every required stop condition must be present exactly once');
  }

  const canary = input?.canary || {};
  if (canary.maximum_canary_outcomes !== 1 || canary.expected_outcome_count !== 1) {
    failures.push('canary outcome maximum and expectation must both equal one');
  }
  if (canary.expected_ledger_event_count !== 12
    || input?.reconciliation?.expected_ledger_event_count !== 12) {
    failures.push('expected revenue-ledger event count must equal 12');
  }
  if (canary.customer_name !== 'Eliza Bulger' || canary.customer_type !== 'residential'
    || canary.customer_linkage !== 'create_new_customer' || canary.prospect_id !== null
    || canary.lead_source !== 'yelp' || canary.attribution_status !== 'confirmed') {
    failures.push('fixed Eliza canary identity or attribution facts differ');
  }
  const financials = {
    booked_revenue_cents: 15000,
    delivered_revenue_cents: 15000,
    collected_revenue_cents: 15000,
    refunded_revenue_cents: 0,
    net_collected_revenue_cents: 15000,
  };
  for (const [key, expected] of Object.entries(financials)) {
    if (canary[key] !== expected || input?.reconciliation?.expected_totals?.[key] !== expected) {
      failures.push(`inconsistent financial total: ${key}`);
    }
  }
  if (canary.payment_status !== 'succeeded' || canary.payment_provider !== 'manual') {
    failures.push('payment must be a manual succeeded payment');
  }

  const runtime = canary.operator_only_runtime_values || {};
  if (runtime.human_owner !== 'Jacob Maynard') failures.push('human_owner must be Jacob Maynard');
  for (const field of ['customer_primary_email', 'customer_primary_phone', 'service_address',
    'estimated_direct_cost_cents', 'actual_direct_cost_cents']) {
    if (runtime[field] !== null || !/^APPROVED_NULL:/.test(runtime[`${field}_status`] || '')) {
      failures.push(`${field} must be explicitly approved null`);
    }
  }
  validateHistoricalDate(runtime.scheduled_start, 'scheduled_start', failures);
  validateHistoricalDate(runtime.completion_date, 'completion_date', failures);
  validateHistoricalDate(runtime.payment_received_at, 'payment_received_at', failures);
  if (unresolved(runtime.payment_method)) failures.push('payment_method is unresolved');
  if (!isUuid(runtime.correlation_id)) failures.push('correlation_id must be a UUID');
  const keys = runtime.idempotency_keys || {};
  for (const [name, expected] of Object.entries(IDEMPOTENCY_KEYS)) {
    if (keys[name] !== expected) failures.push(`immutable idempotency key differs: ${name}`);
  }
  if (Object.keys(keys).length !== Object.keys(IDEMPOTENCY_KEYS).length) {
    failures.push('exactly eight immutable idempotency keys are required');
  }

  if (input?.approved !== true || input?.production_execution_permitted !== true
    || input?.executable !== true || input?.automatic_continuation !== false) {
    failures.push('authorization is not explicitly signed and executable for Phase 1.6B only');
  }
  if (input?.rollback_policy?.no_automatic_rollback_or_continuation !== true) {
    failures.push('automatic rollback or continuation must remain prohibited');
  }
  if (!Array.isArray(input?.remaining_operator_only_values)
    || input.remaining_operator_only_values.length !== 0) {
    failures.push('remaining_operator_only_values must be empty');
  }
  if (input?.deterministic_reconstruction?.required_equal !== true) {
    failures.push('deterministic reconstruction equality must be required');
  }

  validateEvidence(input || {}, options.observed, failures, now);
  const expectedHash = input ? canonicalAuthorizationHash(input) : null;
  if (!input?.authorization_hash || input.authorization_hash !== expectedHash) {
    failures.push('canonical authorization hash mismatch');
  }
  return {
    valid: failures.length === 0,
    failures,
    authorizationHash: expectedHash,
    clientId: CLIENT_ID,
  };
}

module.exports = {
  AUTHORIZATION_ID,
  CERTIFIED_MIGRATIONS,
  CLIENT_ID,
  IDEMPOTENCY_KEYS,
  PHASE,
  RETIRED_AUTHORIZATION_IDS,
  REQUIRED_ACTIONS,
  REQUIRED_FALSE,
  REQUIRED_STOP_CONDITIONS,
  canonicalAuthorizationHash,
  canonicalize,
  validateHistoricalDate,
  validatePhase16bAuthorization,
};
