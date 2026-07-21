#!/usr/bin/env node
'use strict';

// Offline finalizer for the Revenue Phase 1.6B production authorization.
//
// This tool NEVER touches a database, Railway, or the network. It loads the
// immutable unsigned draft, verifies that every immutable value is unaltered,
// applies exactly the operator-supplied facts (attestations, approval
// timestamp, three historical local dates, payment method), recomputes the
// canonical authorization hash, validates the signed document structurally
// without database connectivity, and writes a SEPARATE signed file. The
// unsigned draft is never modified in place.

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const {
  AUTHORIZATION_ID,
  CERTIFIED_MIGRATIONS,
  IDEMPOTENCY_KEYS,
  REQUIRED_FALSE,
  REQUIRED_STOP_CONDITIONS,
  canonicalAuthorizationHash,
  validatePhase16bAuthorization,
} = require('../utils/revenuePhase16b');
const { deriveTimestampFromHistoricalDate } = require('../utils/historicalTimestamp');

const ROOT = path.join(__dirname, '..');
const DEFAULT_DRAFT = path.join(ROOT, 'artifacts', 'revenue', 'phase16b-production-authorization-draft.json');
const DEFAULT_OUTPUT = path.join(ROOT, 'artifacts', 'revenue', 'phase16b-production-authorization-signed.json');

// Immutable unsigned-draft canonical hash. Any draft whose recomputed
// canonical hash differs from this value has been altered and is rejected.
const EXPECTED_DRAFT_HASH = 'add3646e275aeba07969b8e21b32d52e9b7831085efc10cad1f9da5a63ac447f';
const EXPECTED_CORRELATION_ID = 'fd528a1a-091b-4f7b-9210-9a613ffcb9c5';
const EXPECTED_FINANCIALS = Object.freeze({
  booked_revenue_cents: 15000,
  delivered_revenue_cents: 15000,
  collected_revenue_cents: 15000,
  refunded_revenue_cents: 0,
  net_collected_revenue_cents: 15000,
});

function parseArguments(argv) {
  const options = { draft: DEFAULT_DRAFT, output: DEFAULT_OUTPUT };
  const takesValue = {
    '--operator-attestation-file': 'operatorAttestationFile',
    '--approver-attestation-file': 'approverAttestationFile',
    '--approved-at': 'approvedAt',
    '--scheduled-date': 'scheduledDate',
    '--completion-date': 'completionDate',
    '--payment-date': 'paymentDate',
    '--payment-method': 'paymentMethod',
    '--draft': 'draft',
    '--output': 'output',
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    const key = takesValue[flag];
    if (!key) throw new Error(`Unknown argument: ${flag}`);
    const value = argv[index + 1];
    if (value === undefined || value.startsWith('--')) throw new Error(`${flag} requires a value`);
    options[key] = flag.endsWith('-file') || flag === '--draft' || flag === '--output'
      ? path.resolve(value)
      : value;
    index += 1;
  }
  for (const [flag, key] of Object.entries(takesValue)) {
    if (key === 'approvedAt' || key === 'draft' || key === 'output') continue;
    if (!options[key]) throw new Error(`${flag} is required`);
  }
  return options;
}

function placeholderLike(value) {
  return typeof value !== 'string'
    || !value.trim()
    || /(?:<[^>]+>|tbd|todo|placeholder|unresolved)/i.test(value);
}

function readAttestation(filePath, label, requiredName) {
  let content;
  try {
    content = fs.readFileSync(filePath, 'utf8').trim();
  } catch (error) {
    throw new Error(`${label} attestation file is unreadable: ${filePath}`, { cause: error });
  }
  if (placeholderLike(content)) throw new Error(`${label} attestation is empty or a placeholder`);
  if (!content.includes(requiredName)) {
    throw new Error(`${label} attestation must name ${requiredName}`);
  }
  return content;
}

function historicalDate(localDate, label) {
  const value = {
    local_date: localDate,
    timezone: 'America/New_York',
    precision: 'day',
    operator_confirmed: true,
  };
  try {
    // Throws on malformed or impossible calendar dates and enforces
    // day-level precision with a deterministic derived timestamp.
    deriveTimestampFromHistoricalDate(value);
  } catch (error) {
    throw new Error(`${label} is not a valid operator-confirmed day-precision date: ${error.message}`);
  }
  return value;
}

function assertImmutableDraft(draft) {
  const failures = [];
  if (draft.authorization_id !== AUTHORIZATION_ID) {
    failures.push('authorization_id was altered');
  }
  const runtime = draft.canary?.operator_only_runtime_values || {};
  if (runtime.correlation_id !== EXPECTED_CORRELATION_ID) {
    failures.push('correlation_id was altered');
  }
  const keys = runtime.idempotency_keys || {};
  for (const [name, expected] of Object.entries(IDEMPOTENCY_KEYS)) {
    if (keys[name] !== expected) failures.push(`idempotency key was altered: ${name}`);
  }
  if (Object.keys(keys).length !== Object.keys(IDEMPOTENCY_KEYS).length) {
    failures.push('idempotency key set was altered');
  }
  for (const [name, expected] of Object.entries(EXPECTED_FINANCIALS)) {
    if (draft.canary?.[name] !== expected
      || draft.reconciliation?.expected_totals?.[name] !== expected) {
      failures.push(`financial value was altered: ${name}`);
    }
  }
  if (draft.canary?.expected_ledger_event_count !== 12
    || draft.reconciliation?.expected_ledger_event_count !== 12
    || draft.canary?.maximum_canary_outcomes !== 1
    || draft.canary?.expected_outcome_count !== 1) {
    failures.push('ledger-event or outcome expectation was altered');
  }
  for (const name of REQUIRED_FALSE) {
    if (draft.required_false?.[name] !== false) failures.push(`prohibition was altered: ${name}`);
  }
  const stopConditions = draft.stop_conditions;
  if (!Array.isArray(stopConditions)
    || stopConditions.length !== REQUIRED_STOP_CONDITIONS.length
    || REQUIRED_STOP_CONDITIONS.some(condition => !stopConditions.includes(condition))) {
    failures.push('stop conditions were altered');
  }
  if (draft.approved !== false || draft.executable !== false
    || draft.production_execution_permitted !== false
    || draft.authorized_operator?.signature !== null
    || draft.approving_authority?.signature !== null) {
    failures.push('input is not the unsigned non-executable draft');
  }
  if (failures.length) {
    throw new Error(`Immutable draft verification failed:\n- ${failures.join('\n- ')}`);
  }
  const recomputed = canonicalAuthorizationHash(draft);
  if (draft.authorization_hash !== EXPECTED_DRAFT_HASH || recomputed !== EXPECTED_DRAFT_HASH) {
    throw new Error('Unsigned draft canonical hash mismatch; the draft was altered');
  }
}

function sha256File(relativePath) {
  return {
    path: relativePath,
    sha256: crypto.createHash('sha256').update(fs.readFileSync(path.join(ROOT, relativePath))).digest('hex'),
  };
}

// Offline structural evidence: local migration files are re-hashed for real;
// deployment identity mirrors the values recorded in the authorization. A
// FRESH identity observation remains mandatory at execution time and is
// performed by the production runner, never by this finalizer.
function offlineObservedEvidence(authorization, observedAt) {
  const read = name => JSON.parse(
    fs.readFileSync(path.join(ROOT, 'artifacts', 'revenue', name), 'utf8')
  );
  return {
    observed_at: observedAt,
    validation_mode: 'offline_structural_no_database',
    protected_main_commit: authorization.release.protected_main_commit,
    railway_deployment: {
      id: authorization.release.railway_deployment_id,
      status: 'SUCCESS',
      commit: authorization.release.deployed_commit,
      service: authorization.release.railway_service,
      environment: authorization.release.railway_environment,
    },
    migration_checksums: {
      phase1: sha256File(CERTIFIED_MIGRATIONS.phase1.path),
      phase15: sha256File(CERTIFIED_MIGRATIONS.phase15.path),
      phase15_operational_rollback: sha256File(CERTIFIED_MIGRATIONS.phase15_operational_rollback.path),
    },
    phase16a: {
      closure: read('phase16a-durable-backup-closure.json'),
      backup: read('phase16a-backup-evidence.json'),
      restore: read('phase16a-restore-evidence.json'),
    },
  };
}

function finalizeAuthorization(options) {
  const draftBytes = fs.readFileSync(options.draft);
  const draft = JSON.parse(draftBytes.toString('utf8'));
  assertImmutableDraft(draft);

  if (path.resolve(options.output) === path.resolve(options.draft)) {
    throw new Error('The signed output must be a separate file; the unsigned draft is immutable');
  }
  if (fs.existsSync(options.output)) {
    throw new Error(`Refusing to overwrite existing signed authorization: ${options.output}`);
  }

  const approvedAt = options.approvedAt || new Date().toISOString();
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(approvedAt)
    || Number.isNaN(Date.parse(approvedAt))) {
    throw new Error('approved_at must be a UTC ISO-8601 timestamp');
  }
  if (Date.parse(approvedAt) > Date.now() + 60000) {
    throw new Error('approved_at must not be in the future');
  }

  const scheduled = historicalDate(options.scheduledDate, 'scheduled date');
  const completion = historicalDate(options.completionDate, 'completion date');
  const payment = historicalDate(options.paymentDate, 'payment date');
  if (options.completionDate < options.scheduledDate || options.paymentDate < options.completionDate) {
    throw new Error('dates must be non-decreasing: scheduled <= completion <= payment');
  }
  if (placeholderLike(options.paymentMethod)) throw new Error('payment method is unresolved');

  const operatorAttestation = readAttestation(
    options.operatorAttestationFile, 'operator', draft.authorized_operator.name);
  const approverAttestation = readAttestation(
    options.approverAttestationFile, 'approver', draft.approving_authority.name);

  const signed = structuredClone(draft);
  signed.draft_status = 'finalized_signed_executable';
  signed.authorized_operator.signature = operatorAttestation;
  signed.approving_authority.signature = approverAttestation;
  signed.approved_at = approvedAt;
  const runtime = signed.canary.operator_only_runtime_values;
  runtime.scheduled_start = scheduled;
  runtime.completion_date = completion;
  runtime.payment_received_at = payment;
  runtime.payment_method = options.paymentMethod;
  signed.remaining_operator_only_values = [];
  signed.approved = true;
  signed.production_execution_permitted = true;
  signed.executable = true;
  signed.execution_readiness.ready_to_sign = true;
  signed.execution_readiness.ready_to_execute = true;
  signed.execution_readiness.ready_to_sign_blockers = 'none';
  signed.execution_readiness.repository_gaps_requiring_offline_implementation_and_review_before_signing = [];
  signed.finalization = {
    finalized_at: new Date().toISOString(),
    finalized_from_draft_hash: EXPECTED_DRAFT_HASH,
    finalizer: 'scripts/finalizeRevenuePhase16bAuthorization.js',
    validation_mode: 'offline_structural_no_database',
  };
  signed.authorization_hash = canonicalAuthorizationHash(signed);

  // Structural validation without database connectivity, evaluated at a
  // simulated instant inside the authorization window. This proves the
  // signed document itself is complete and internally consistent.
  const windowStart = Date.parse(signed.window.start);
  const inWindowInstant = new Date(windowStart + 60000);
  const structural = validatePhase16bAuthorization(signed, {
    now: inWindowInstant,
    observed: offlineObservedEvidence(signed, inWindowInstant.toISOString()),
  });
  if (!structural.valid) {
    throw new Error(`Signed authorization failed offline validation:\n- ${structural.failures.join('\n- ')}`);
  }

  // Independent proof that the signed file stays non-executable outside its
  // window: validation must fail both before the start and after the end.
  const beforeWindow = new Date(windowStart - 60000);
  const afterWindow = new Date(Date.parse(signed.window.end) + 60000);
  const outsideResults = [beforeWindow, afterWindow].map(instant =>
    validatePhase16bAuthorization(signed, {
      now: instant,
      observed: offlineObservedEvidence(signed, instant.toISOString()),
    }));
  if (outsideResults.some(result => result.valid)) {
    throw new Error('Signed authorization validated outside its window; refusing to emit it');
  }

  fs.mkdirSync(path.dirname(options.output), { recursive: true });
  fs.writeFileSync(options.output, `${JSON.stringify(signed, null, 2)}\n`, { mode: 0o600, flag: 'wx' });

  const draftBytesAfter = fs.readFileSync(options.draft);
  if (!draftBytes.equals(draftBytesAfter)) {
    throw new Error('The unsigned draft changed during finalization; investigate immediately');
  }

  const now = new Date();
  return {
    status: 'finalized',
    signed_authorization_path: options.output,
    unsigned_draft_path: options.draft,
    unsigned_draft_hash: EXPECTED_DRAFT_HASH,
    unsigned_draft_unchanged: true,
    final_authorization_hash: signed.authorization_hash,
    approved_at: approvedAt,
    offline_structural_validation: { valid: true, failures: [] },
    non_executable_outside_window: true,
    window: { start: signed.window.start, end: signed.window.end },
    window_active_at_finalization: now >= new Date(signed.window.start) && now <= new Date(signed.window.end),
    database_connectivity_used: false,
  };
}

function main() {
  const options = parseArguments(process.argv.slice(2));
  const result = finalizeAuthorization(options);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}

module.exports = {
  DEFAULT_DRAFT,
  DEFAULT_OUTPUT,
  EXPECTED_DRAFT_HASH,
  assertImmutableDraft,
  finalizeAuthorization,
  offlineObservedEvidence,
  parseArguments,
};
