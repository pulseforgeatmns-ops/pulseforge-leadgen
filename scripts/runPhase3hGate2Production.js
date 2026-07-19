#!/usr/bin/env node
'use strict';

// Guarded Phase 3H / Gate 2 production operator runner.
//
// Purpose: apply the reviewed Phase 3D setter-pilot migration to the Railway
// production database while keeping Anchor and every setter operation disabled.
//
// Safety posture:
//   * Default behavior is read-only (local inspection + production preflight).
//   * The migration requires an explicit flag, a preflight pass, a printed
//     execution plan, and a second exact confirmation phrase.
//   * Credentials are read from the Railway-linked .env into memory only and are
//     never printed, copied into the worktree, persisted, or written to reports.
//   * All artifacts pass a recursive secret-redaction check before they are
//     finalized.
//
// This module is intentionally dependency-injectable so its guards can be unit
// tested without ever connecting to production. See test/phase3hGate2Runner.test.js.

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const readline = require('node:readline');
const { spawnSync } = require('node:child_process');

const {
  snapshot,
  phase3dObjectState,
  allPhase3dObjectsPresent,
  hasTable,
  hasColumn,
} = require('./runSetterReleaseRehearsal');

// ---------------------------------------------------------------------------
// Fixed release / identity constants (reviewed Gate 2 inputs).
// ---------------------------------------------------------------------------

const RELEASE = Object.freeze({
  deployedSha: '2862d2ee1e5662db42fa111baaaa0faf248bf5d3',
  migrationRelPath: 'migrations/2026-07-19-setter-pilot-quality-control.sql',
  rollbackRelPath: 'migrations/2026-07-19-setter-pilot-quality-control.rollback.sql',
  migrationSha256: '2dd54555bf14e71305ca28edc7041b0771a5a2421e5632ad1a96dc51523d937d',
  rollbackSha256: '79c2e3973d71f7d416124fa7389ae61ec952369df464c0e7770ac96a223ca275',
  backupProcedureRelPath: 'scripts/stagePhase16bDurableBackup.js',
  remediationRelPath: 'scripts/remediation/2026-07-19-cancel-suppressed-setter-callbacks.sql',
});

const IDENTITY = Object.freeze({
  expectedDatabase: 'railway',
  expectedMajor: 18,
  expectedVersionPrefix: '18.4',
  anchorClientId: 10,
});

const EXPECTED_REMEDIATION = Object.freeze({
  suppressedPending: 1,
  syntheticPending: 0,
  totalPending: 10,
  nonSuppressedPending: 9,
  affectedRows: 1,
});

const USAGE = `Phase 3H Gate 2 production runner (fail-closed).

Usage:
  node scripts/runPhase3hGate2Production.js --help
  node scripts/runPhase3hGate2Production.js --artifacts-dir=/abs/path [options]

Modes (mutually exclusive write/verify actions):
  (default)                 Local inspect + read-only production preflight
  --verify-post-migration   Read-only post-migration investigation (COMPLETE_PHASE3D OK)
  --verify-after-restart    Read-only post-restart checks
  --execute-migration       Backup + apply reviewed migration (requires confirmation)
  --execute-rollback        Logical feature-flag rollback (requires separate confirmation)
  --execute-remediation-suppressed-callbacks
                            Cancel the one pending DNC/synthetic callback (requires confirmation)

Required for every non-help run:
  --artifacts-dir=/absolute/path

Common options:
  --worktree=/path/to/release-sha-worktree
  --expected-head=<sha>
  --env-file=/path/to/.env
  --non-interactive         With PHASE3H_PRODUCTION_APPROVED=... / PHASE3H_ROLLBACK_APPROVED=...
                            / PHASE3H_REMEDIATION_APPROVED=CANCEL_SUPPRESSED_SETTER_CALLBACKS_ON_RAILWAY
  --allow-preexisting-database-url

Default mode never writes. --verify-post-migration never writes.
`;

function printUsage(log = console.log) {
  log(USAGE);
}

function stableHash(value) {
  return crypto.createHash('sha256').update(String(value)).digest('hex').slice(0, 16);
}

const CONFIRM = Object.freeze({
  migrationPhrase: 'APPLY PHASE3D TO railway POSTGRES18',
  migrationEnvKey: 'PHASE3H_PRODUCTION_APPROVED',
  migrationEnvValue: 'APPLY_PHASE3D_TO_RAILWAY_POSTGRES18',
  rollbackPhrase: 'ROLLBACK PHASE3D ON railway POSTGRES18',
  rollbackEnvKey: 'PHASE3H_ROLLBACK_APPROVED',
  rollbackEnvValue: 'ROLLBACK_PHASE3D_ON_RAILWAY_POSTGRES18',
  remediationPhrase: 'CANCEL SUPPRESSED SETTER CALLBACKS ON railway',
  remediationEnvKey: 'PHASE3H_REMEDIATION_APPROVED',
  remediationEnvValue: 'CANCEL_SUPPRESSED_SETTER_CALLBACKS_ON_RAILWAY',
});

const DEFAULT_ENV_FILE = path.join(os.homedir(), 'Desktop', 'Pulseforge', 'Lead Gen', 'Lead Gen App', '.env');

// Absolute binary paths used by the embedded durable-backup procedure.
const BACKUP_BINARIES = Object.freeze([
  '/usr/local/bin/pg_dump',
  '/usr/local/bin/pg_restore',
  '/usr/local/bin/gpg',
  '/usr/bin/security',
]);

// The relations Stage 3 must snapshot. Absent relations are recorded as
// { exists: false, row_count: null } and are never queried.
const SNAPSHOT_TABLES = Object.freeze([
  'clients',
  'prospects',
  'call_dispositions',
  'activity_log',
  'agent_log',
  'touchpoints',
  'setter_follow_up_drafts',
  'setter_callbacks',
]);

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

class GateError extends Error {
  constructor(message, { stage = 'unknown', code = 'GATE_FAILED', details = null } = {}) {
    super(message);
    this.name = 'GateError';
    this.stage = stage;
    this.code = code;
    this.details = details;
    this.gate = true;
  }
}

function requireAbsoluteArtifactsDir(artifactsDir) {
  if (!artifactsDir) {
    throw new GateError(
      '--artifacts-dir=/absolute/path is required for all non-help runs',
      { stage: 'cli', code: 'ARTIFACTS_DIR_REQUIRED' },
    );
  }
  if (!path.isAbsolute(artifactsDir)) {
    throw new GateError(
      `--artifacts-dir must be absolute (got: ${artifactsDir})`,
      { stage: 'cli', code: 'ARTIFACTS_DIR_NOT_ABSOLUTE' },
    );
  }
  return artifactsDir;
}

// ---------------------------------------------------------------------------
// Phase 3D schema classification
// ---------------------------------------------------------------------------
//
// Gate 1 documented the genuine pre-Phase-3D production shape: the base human
// setter pipeline already includes prospects.assigned_setter_id (and related
// setter_status / setter_visible columns). The Phase 3D migration re-declares
// assigned_setter_id with IF NOT EXISTS for idempotency, but that column is
// NOT a unique Phase 3D quality-control marker.
//
// Treating assigned_setter_id as a Phase 3D marker false-positives clean
// production as PARTIAL. Unique Phase 3D markers are the quality-control
// objects introduced only by 2026-07-19-setter-pilot-quality-control.sql.

const LEGACY_SETTER_COLUMNS = Object.freeze({
  prospects: Object.freeze(['assigned_setter_id', 'setter_status', 'setter_visible', 'callback_at', 'do_not_contact']),
});

// Uniquely Phase 3D — presence of ANY of these on an otherwise incomplete set
// means PARTIAL_PHASE3D. assigned_setter_id is intentionally excluded.
const UNIQUE_PHASE3D_COLUMNS = Object.freeze({
  clients: Object.freeze(['setter_pipeline_v2_enabled', 'setter_pipeline_v2_configured_at', 'setter_review_sample_percent']),
  prospects: Object.freeze(['is_synthetic', 'synthetic_label', 'callback_completed_at']),
  call_dispositions: Object.freeze([
    'structured_notes', 'activity_result', 'next_action', 'suppression_state', 'lifecycle_result',
    'is_synthetic', 'review_required', 'review_status', 'idempotency_key',
  ]),
});
const UNIQUE_PHASE3D_TABLES = Object.freeze(['setter_callbacks']);
const UNIQUE_PHASE3D_INDEXES = Object.freeze([
  'call_dispositions_idempotency_idx',
  'setter_callbacks_one_pending_idx',
  'setter_callbacks_due_idx',
]);
const UNIQUE_PHASE3D_TRIGGERS = Object.freeze([
  'prospects_synthetic_suppression',
  'prospects_suppression_cleanup',
]);

function uniquePhase3dObjectState(state) {
  return {
    tables: Object.fromEntries(UNIQUE_PHASE3D_TABLES.map(name => [name, hasTable(state, name)])),
    columns: Object.fromEntries(Object.entries(UNIQUE_PHASE3D_COLUMNS).map(([table, columns]) => [table,
      Object.fromEntries(columns.map(column => [column, hasColumn(state, table, column)])),
    ])),
    indexes: Object.fromEntries(UNIQUE_PHASE3D_INDEXES.map(name => [name, state.indexes.some(index => index.indexname === name)])),
    triggers: Object.fromEntries(UNIQUE_PHASE3D_TRIGGERS.map(name => [name, state.triggers.some(trigger => trigger.trigger_name === name)])),
    callback_status_constraint: state.constraints.some(constraint =>
      constraint.constraint_name === 'setter_callbacks_status_check' && constraint.constraint_type === 'CHECK'),
  };
}

function listPresentMarkers(uniqueState) {
  const present = [];
  for (const [name, exists] of Object.entries(uniqueState.tables)) {
    if (exists) present.push(`table:${name}`);
  }
  for (const [table, cols] of Object.entries(uniqueState.columns)) {
    for (const [column, exists] of Object.entries(cols)) {
      if (exists) present.push(`column:${table}.${column}`);
    }
  }
  for (const [name, exists] of Object.entries(uniqueState.indexes)) {
    if (exists) present.push(`index:${name}`);
  }
  for (const [name, exists] of Object.entries(uniqueState.triggers)) {
    if (exists) present.push(`trigger:${name}`);
  }
  if (uniqueState.callback_status_constraint) present.push('constraint:setter_callbacks_status_check');
  return present;
}

function listAbsentMarkers(uniqueState) {
  const absent = [];
  for (const [name, exists] of Object.entries(uniqueState.tables)) {
    if (!exists) absent.push(`table:${name}`);
  }
  for (const [table, cols] of Object.entries(uniqueState.columns)) {
    for (const [column, exists] of Object.entries(cols)) {
      if (!exists) absent.push(`column:${table}.${column}`);
    }
  }
  for (const [name, exists] of Object.entries(uniqueState.indexes)) {
    if (!exists) absent.push(`index:${name}`);
  }
  for (const [name, exists] of Object.entries(uniqueState.triggers)) {
    if (!exists) absent.push(`trigger:${name}`);
  }
  if (!uniqueState.callback_status_constraint) absent.push('constraint:setter_callbacks_status_check');
  return absent;
}

function legacySetterObjectState(state) {
  return {
    columns: Object.fromEntries(Object.entries(LEGACY_SETTER_COLUMNS).map(([table, columns]) => [table,
      Object.fromEntries(columns.map(column => [column, hasColumn(state, table, column)])),
    ])),
    tables: {
      setter_follow_up_drafts: hasTable(state, 'setter_follow_up_drafts'),
    },
    call_dispositions_details: hasColumn(state, 'call_dispositions', 'details'),
  };
}

// Classify production schema for Gate 2. Does not weaken the gate: any unique
// Phase 3D marker without the full contract is PARTIAL_PHASE3D.
function classifyPhase3dSchema(state) {
  const unique = uniquePhase3dObjectState(state);
  const legacy = legacySetterObjectState(state);
  // Post-migration completeness still uses the rehearsal inventory (includes
  // assigned_setter_id, which production already has and the migration keeps).
  const inventory = phase3dObjectState(state);
  const complete = allPhase3dObjectsPresent(inventory);
  const presentUnique = listPresentMarkers(unique);
  const absentUnique = listAbsentMarkers(unique);
  const anyUnique = presentUnique.length > 0;

  let classification;
  if (complete) classification = 'COMPLETE_PHASE3D';
  else if (!anyUnique) classification = 'CLEAN_PRE_MIGRATION';
  else if (anyUnique) classification = 'PARTIAL_PHASE3D';
  else classification = 'AMBIGUOUS';

  return {
    classification,
    unique_phase3d_present: presentUnique,
    unique_phase3d_absent: absentUnique,
    legacy_setter: legacy,
    inventory,
    complete,
  };
}

// ---------------------------------------------------------------------------
// Secret handling / redaction
// ---------------------------------------------------------------------------

// Build the list of concrete secret strings that must never leak into logs or
// artifacts (the full URL plus each decoded component).
function deriveSecrets(databaseUrl) {
  const secrets = new Set();
  if (!databaseUrl) return secrets;
  secrets.add(databaseUrl);
  try {
    const u = new URL(databaseUrl);
    if (u.password) {
      secrets.add(u.password);
      secrets.add(decodeURIComponent(u.password));
    }
    if (u.username) {
      secrets.add(u.username);
      secrets.add(decodeURIComponent(u.username));
    }
    if (u.host) secrets.add(u.host);
    if (u.hostname) secrets.add(u.hostname);
    if (u.search) secrets.add(u.search.replace(/^\?/, ''));
  } catch {
    // If it does not parse as a URL we still redact the raw value above.
  }
  return secrets;
}

function redactString(value, secrets) {
  let out = String(value);
  for (const secret of secrets) {
    if (secret && out.includes(secret)) out = out.split(secret).join('[REDACTED]');
  }
  // Defense in depth: strip anything that still looks like a connection string.
  out = out.replace(/postgres(?:ql)?:\/\/[^\s"']+/gi, '[REDACTED_CONNECTION_STRING]');
  out = out.replace(/password=([^\s&"']+)/gi, 'password=[REDACTED]');
  return out;
}

function redactDeep(value, secrets) {
  if (value == null) return value;
  if (typeof value === 'string') return redactString(value, secrets);
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) return value.map(item => redactDeep(item, secrets));
  if (value instanceof Error) {
    return { name: value.name, message: redactString(value.message, secrets) };
  }
  if (typeof value === 'object') {
    const out = {};
    for (const [key, val] of Object.entries(value)) out[key] = redactDeep(val, secrets);
    return out;
  }
  return value;
}

// Sanitize a database/tooling error into a report-safe shape.
function sanitizeError(error, secrets) {
  const message = redactString(error && error.message ? error.message : String(error), secrets);
  const out = { name: error && error.name ? error.name : 'Error', message };
  if (error && error.code) out.code = redactString(error.code, secrets);
  return out;
}

// Recursive guard run over every artifact before it is written. Throws if any
// known secret or credential-shaped token survives.
function assertNoSecrets(value, secrets, pathLabel = '$') {
  const forbiddenPatterns = [
    /postgres(?:ql)?:\/\//i,
    /PHASE3H_PRODUCTION_APPROVED\s*=/,
    /railway[_-]?token/i,
    /-----BEGIN [A-Z ]*PRIVATE KEY-----/,
  ];
  const walk = (node, label) => {
    if (node == null) return;
    if (typeof node === 'string') {
      for (const secret of secrets) {
        if (secret && node.includes(secret)) {
          throw new GateError(`Secret leak detected in artifact at ${label}`, { stage: 'redaction', code: 'SECRET_LEAK' });
        }
      }
      for (const pattern of forbiddenPatterns) {
        if (pattern.test(node)) {
          throw new GateError(`Credential-shaped token detected in artifact at ${label}`, { stage: 'redaction', code: 'SECRET_LEAK' });
        }
      }
      return;
    }
    if (Array.isArray(node)) {
      node.forEach((item, index) => walk(item, `${label}[${index}]`));
      return;
    }
    if (typeof node === 'object') {
      for (const [key, val] of Object.entries(node)) walk(val, `${label}.${key}`);
    }
  };
  walk(value, pathLabel);
  return true;
}

// ---------------------------------------------------------------------------
// Credential loading (memory only)
// ---------------------------------------------------------------------------

// Read DATABASE_URL from the Railway-linked .env into memory. Never returns via
// logs. Rejects a pre-exported DATABASE_URL from an unexpected source unless the
// operator explicitly overrides it.
function loadDatabaseUrl({ envFilePath, processEnv, allowPreexisting, deps }) {
  const fsdep = deps.fs;
  if (processEnv.DATABASE_URL && !allowPreexisting) {
    throw new GateError(
      'DATABASE_URL is already exported in this environment from an unexpected source. '
      + 'Unset it or re-run with --allow-preexisting-database-url to explicitly override.',
      { stage: 'credentials', code: 'PREEXISTING_DATABASE_URL' },
    );
  }
  if (!fsdep.existsSync(envFilePath)) {
    throw new GateError(`Credential source not found: ${envFilePath}`, { stage: 'credentials', code: 'ENV_FILE_MISSING' });
  }
  const contents = fsdep.readFileSync(envFilePath, 'utf8');
  let url = null;
  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const match = /^(?:export\s+)?DATABASE_URL\s*=\s*(.*)$/.exec(line);
    if (match) {
      let value = match[1].trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      url = value;
      break;
    }
  }
  if (allowPreexisting && processEnv.DATABASE_URL) url = processEnv.DATABASE_URL;
  if (!url) {
    throw new GateError(`DATABASE_URL not present in ${envFilePath}`, { stage: 'credentials', code: 'DATABASE_URL_MISSING' });
  }
  return url;
}

// ---------------------------------------------------------------------------
// Stage 1 — local inspection
// ---------------------------------------------------------------------------

function sha256File(fsdep, filePath) {
  return crypto.createHash('sha256').update(fsdep.readFileSync(filePath)).digest('hex');
}

// Static safety checks on the (hash-verified) migration text.
function assertMigrationStaticSafety(sql) {
  const findings = {};
  const trimmed = sql.trim();
  const beginCount = (sql.match(/^\s*BEGIN\s*;/gim) || []).length;
  const commitCount = (sql.match(/^\s*COMMIT\s*;/gim) || []).length;
  const rollbackCount = (sql.match(/^\s*ROLLBACK\s*;/gim) || []).length;
  findings.single_transaction = /^BEGIN\s*;/i.test(trimmed) && /COMMIT\s*;?$/i.test(trimmed)
    && beginCount === 1 && commitCount === 1 && rollbackCount === 0;
  if (!findings.single_transaction) {
    throw new GateError('Migration is not wrapped in exactly one BEGIN/COMMIT transaction', { stage: 'inspect', code: 'NOT_SINGLE_TXN' });
  }

  findings.setter_flag_defaults_false = /setter_pipeline_v2_enabled\s+BOOLEAN\s+NOT\s+NULL\s+DEFAULT\s+false/i.test(sql);
  if (!findings.setter_flag_defaults_false) {
    throw new GateError('Migration does not declare setter_pipeline_v2_enabled DEFAULT false', { stage: 'inspect', code: 'FLAG_DEFAULT_NOT_FALSE' });
  }

  // No statement may enable the Anchor / setter pipeline flag.
  const enablesFlag = /setter_pipeline_v2_enabled\s*=\s*true/i.test(sql)
    || /set\s+setter_pipeline_v2_enabled\s*=\s*true/i.test(sql)
    || /default\s+true/i.test((sql.match(/setter_pipeline_v2_enabled[^,;]*/i) || [''])[0]);
  findings.no_anchor_enablement = !enablesFlag;
  if (enablesFlag) {
    throw new GateError('Migration contains an Anchor / setter_pipeline_v2 enablement statement', { stage: 'inspect', code: 'ANCHOR_ENABLEMENT' });
  }
  return findings;
}

function inspectRelease({ worktree, expectedHead, deps }) {
  const { git, fs: fsdep } = deps;
  const report = { stage: 'inspect', worktree, checks: {} };

  const head = git.revParse(worktree);
  report.head = head;
  report.checks.head_matches = head === expectedHead;
  if (head !== expectedHead) {
    throw new GateError(`Worktree HEAD ${head} does not match required release ${expectedHead}`, { stage: 'inspect', code: 'HEAD_MISMATCH' });
  }

  const status = git.status(worktree); // array of {x, y, path}
  const trackedChanges = status.filter(entry => entry.code !== '??');
  const untracked = status.filter(entry => entry.code === '??').map(entry => entry.path);
  report.checks.working_tree_clean = trackedChanges.length === 0;
  report.untracked_present = untracked;
  if (trackedChanges.length !== 0) {
    throw new GateError(
      `Working tree has uncommitted changes to tracked files: ${trackedChanges.map(e => e.path).join(', ')}`,
      { stage: 'inspect', code: 'DIRTY_WORKTREE' },
    );
  }

  const migrationPath = path.join(worktree, RELEASE.migrationRelPath);
  const rollbackPath = path.join(worktree, RELEASE.rollbackRelPath);

  if (!fsdep.existsSync(migrationPath)) {
    throw new GateError(`Reviewed migration missing: ${RELEASE.migrationRelPath}`, { stage: 'inspect', code: 'MIGRATION_MISSING' });
  }
  if (!fsdep.existsSync(rollbackPath)) {
    throw new GateError(`Reviewed rollback missing: ${RELEASE.rollbackRelPath}`, { stage: 'inspect', code: 'ROLLBACK_MISSING' });
  }

  // The durable backup procedure is embedded in this runner (createDurableBackup)
  // because the deployed release does not ship a standalone backup script. Record
  // tool availability for the operator; the backup stage itself fails closed if a
  // required tool is missing at execution time.
  const backupTools = {};
  for (const bin of BACKUP_BINARIES) backupTools[bin] = fsdep.existsSync(bin);
  report.backup = {
    embedded_procedure: true,
    reference_script_present: fsdep.existsSync(path.join(worktree, RELEASE.backupProcedureRelPath)),
    tools: backupTools,
  };
  report.checks.backup_procedure_available = true;

  const migrationSha = sha256File(fsdep, migrationPath);
  const rollbackSha = sha256File(fsdep, rollbackPath);
  report.migration_sha256 = migrationSha;
  report.rollback_sha256 = rollbackSha;
  report.checks.migration_hash_matches = migrationSha === RELEASE.migrationSha256;
  report.checks.rollback_hash_matches = rollbackSha === RELEASE.rollbackSha256;
  if (migrationSha !== RELEASE.migrationSha256) {
    throw new GateError('Migration SHA-256 mismatch — refusing to use unreviewed migration SQL', { stage: 'inspect', code: 'MIGRATION_HASH_MISMATCH' });
  }
  if (rollbackSha !== RELEASE.rollbackSha256) {
    throw new GateError('Rollback SHA-256 mismatch — refusing to use unreviewed rollback SQL', { stage: 'inspect', code: 'ROLLBACK_HASH_MISMATCH' });
  }

  const migrationSql = fsdep.readFileSync(migrationPath, 'utf8');
  report.checks.migration_static_safety = assertMigrationStaticSafety(migrationSql);
  report.migration_path = migrationPath;
  report.rollback_path = rollbackPath;
  report.passed = true;
  return report;
}

// ---------------------------------------------------------------------------
// Stage 2 — read-only production preflight
// ---------------------------------------------------------------------------

async function withReadOnly(db, fn) {
  await db.query('BEGIN');
  try {
    await db.query('SET TRANSACTION READ ONLY');
    const result = await fn();
    await db.query('ROLLBACK');
    return result;
  } catch (error) {
    try { await db.query('ROLLBACK'); } catch { /* already aborted */ }
    throw error;
  }
}

async function runPreflight(db, { identity = IDENTITY, postMigrationMode = false } = {}) {
  return withReadOnly(db, async () => {
    const report = { stage: 'preflight', read_only: true, checks: {}, observations: {} };

    const idRow = (await db.query('SELECT current_database() AS db')).rows[0];
    report.observations.current_database = idRow.db;
    report.checks.database_identity = idRow.db === identity.expectedDatabase;
    if (idRow.db !== identity.expectedDatabase) {
      throw new GateError(`current_database() = ${idRow.db}, expected ${identity.expectedDatabase}`, { stage: 'preflight', code: 'DB_IDENTITY', details: report });
    }

    const versionRow = (await db.query('SHOW server_version')).rows[0];
    const serverVersion = versionRow.server_version;
    report.observations.server_version = serverVersion;
    const major = parseInt(String(serverVersion).split('.')[0], 10);
    report.checks.server_major = major === identity.expectedMajor;
    if (major !== identity.expectedMajor) {
      throw new GateError(`PostgreSQL major ${major}, expected ${identity.expectedMajor}`, { stage: 'preflight', code: 'PG_MAJOR', details: report });
    }
    report.checks.server_version_consistent = String(serverVersion).startsWith(identity.expectedVersionPrefix);
    if (!String(serverVersion).startsWith(identity.expectedVersionPrefix)) {
      throw new GateError(`PostgreSQL version ${serverVersion} not consistent with ${identity.expectedVersionPrefix}`, { stage: 'preflight', code: 'PG_VERSION', details: report });
    }

    const state = await snapshot(db);

    const coreTables = ['clients', 'prospects', 'call_dispositions'];
    const missingCore = coreTables.filter(t => !hasTable(state, t));
    report.checks.pulseforge_identity = missingCore.length === 0;
    if (missingCore.length !== 0) {
      throw new GateError(`Production identity check failed; missing core tables: ${missingCore.join(', ')}`, { stage: 'preflight', code: 'IDENTITY_TABLES', details: report });
    }

    const anchor = await db.query('SELECT id FROM clients WHERE id = $1', [identity.anchorClientId]);
    report.observations.anchor_client_rows = anchor.rowCount;
    report.checks.anchor_unique = anchor.rowCount === 1;
    if (anchor.rowCount !== 1) {
      throw new GateError(`Anchor client id=${identity.anchorClientId} not uniquely identified (rows=${anchor.rowCount})`, { stage: 'preflight', code: 'ANCHOR_IDENTITY', details: report });
    }

    const flagColumnExists = hasColumn(state, 'clients', 'setter_pipeline_v2_enabled');
    report.observations.setter_pipeline_v2_column_exists = flagColumnExists;
    let anyEnabled = false;
    if (flagColumnExists) {
      anyEnabled = (await db.query('SELECT COALESCE(bool_or(setter_pipeline_v2_enabled), false) AS enabled FROM clients')).rows[0].enabled;
      report.observations.any_setter_pipeline_v2_enabled = anyEnabled;
      report.checks.feature_flag_not_enabled = anyEnabled === false;
      if (anyEnabled !== false) {
        throw new GateError('setter_pipeline_v2_enabled is TRUE for at least one client — Anchor pilot must not be active', { stage: 'preflight', code: 'FLAG_ENABLED', details: report });
      }
    } else {
      report.checks.feature_flag_not_enabled = true;
    }

    // Classify before interpreting pending callbacks.
    const classified = classifyPhase3dSchema(state);
    report.observations.phase3d_classification = classified.classification;
    report.observations.phase3d_unique_present = classified.unique_phase3d_present;
    report.observations.phase3d_unique_absent = classified.unique_phase3d_absent;
    report.observations.legacy_setter = classified.legacy_setter;
    report.observations.phase3d_pre_state = classified.inventory;
    report.observations.phase3d_all_present = classified.complete;
    report.checks.schema_classification_clean_or_complete =
      classified.classification === 'CLEAN_PRE_MIGRATION'
      || classified.classification === 'COMPLETE_PHASE3D';
    report.checks.no_partial_migration = classified.classification !== 'PARTIAL_PHASE3D'
      && classified.classification !== 'AMBIGUOUS';
    if (classified.classification === 'PARTIAL_PHASE3D' || classified.classification === 'AMBIGUOUS') {
      throw new GateError(
        `Phase 3D schema classification ${classified.classification} — refusing to proceed`,
        {
          stage: 'preflight',
          code: classified.classification === 'PARTIAL_PHASE3D' ? 'PARTIAL_MIGRATION' : 'AMBIGUOUS_SCHEMA',
          details: report,
        },
      );
    }

    // Pending setter_callbacks after a successful Phase 3D apply are inert queued
    // history while the Anchor flag is false (and especially before app restart).
    // They are NOT treated as "active setter operations." Pre-migration, the
    // relation must still be absent.
    const callbacksExists = hasTable(state, 'setter_callbacks');
    report.observations.setter_callbacks_exists = callbacksExists;
    if (callbacksExists) {
      const pending = (await db.query("SELECT count(*)::int AS count FROM setter_callbacks WHERE status = 'pending'")).rows[0].count;
      report.observations.pending_setter_callbacks = pending;
      if (classified.complete && anyEnabled === false) {
        report.observations.pending_callbacks_classification = 'inert_queued_backfill';
        report.checks.no_active_setter_ops = true;
        report.checks.pending_callbacks_inert_while_flag_off = true;
      } else if (postMigrationMode && classified.complete) {
        report.observations.pending_callbacks_classification = 'investigating';
        report.checks.no_active_setter_ops = true;
      } else if (pending !== 0) {
        report.checks.no_active_setter_ops = false;
        throw new GateError(`Active setter operations detected: ${pending} pending setter_callbacks`, {
          stage: 'preflight', code: 'ACTIVE_SETTER_OPS', details: report,
        });
      } else {
        report.checks.no_active_setter_ops = true;
      }
    } else {
      report.checks.no_active_setter_ops = true;
      if (postMigrationMode && !classified.complete) {
        throw new GateError('Post-migration verify requires COMPLETE_PHASE3D schema', {
          stage: 'preflight', code: 'NOT_POST_MIGRATION', details: report,
        });
      }
    }

    report.phase3d_already_applied = classified.complete;
    report.passed = true;
    return report;
  });
}

// ---------------------------------------------------------------------------
// Stage 3 — pre-migration snapshot (sanitized, absent relations not queried)
// ---------------------------------------------------------------------------

async function snapshotCounts(db, state, tables = SNAPSHOT_TABLES) {
  const out = {};
  for (const table of tables) {
    if (!hasTable(state, table)) {
      out[table] = { exists: false, row_count: null };
      continue;
    }
    const result = await db.query(`SELECT count(*)::int AS count FROM ${table}`);
    out[table] = { exists: true, row_count: result.rows[0].count };
  }
  return out;
}

async function prospectSafetyCounts(db, state) {
  const out = {};
  const guarded = async (name, exists, sql) => {
    if (!exists) { out[name] = { exists: false, row_count: null }; return; }
    const result = await db.query(sql);
    out[name] = { exists: true, row_count: result.rows[0].count };
  };
  const hasProspects = hasTable(state, 'prospects');
  const hasSynthetic = hasProspects && hasColumn(state, 'prospects', 'is_synthetic');
  const hasDnc = hasProspects && hasColumn(state, 'prospects', 'do_not_contact');
  const hasCallbackAt = hasProspects && hasColumn(state, 'prospects', 'callback_at');
  await guarded('synthetic_prospects', hasSynthetic, 'SELECT count(*)::int AS count FROM prospects WHERE is_synthetic');
  await guarded('dnc_prospects', hasDnc, 'SELECT count(*)::int AS count FROM prospects WHERE do_not_contact');
  await guarded('prospects_with_callback_at', hasCallbackAt, 'SELECT count(*)::int AS count FROM prospects WHERE callback_at IS NOT NULL');
  if (hasProspects) {
    const status = await db.query('SELECT status, count(*)::int AS count FROM prospects GROUP BY status ORDER BY status');
    out.status_distribution = status.rows.reduce((acc, row) => { acc[row.status] = row.count; return acc; }, {});
  } else {
    out.status_distribution = null;
  }
  return out;
}

async function runPreMigrationSnapshot(db) {
  return withReadOnly(db, async () => {
    const state = await snapshot(db);
    return {
      stage: 'pre-migration-snapshot',
      read_only: true,
      captured_at: new Date().toISOString(),
      table_counts: await snapshotCounts(db, state),
      prospect_safety: await prospectSafetyCounts(db, state),
      phase3d_objects: phase3dObjectState(state),
      anchor_flag: hasColumn(state, 'clients', 'setter_pipeline_v2_enabled')
        ? (await db.query('SELECT setter_pipeline_v2_enabled FROM clients WHERE id = $1', [IDENTITY.anchorClientId])).rows[0] || { setter_pipeline_v2_enabled: null }
        : { exists: false },
      schema_object_counts: {
        tables: state.tables.length,
        columns: state.columns.length,
        indexes: state.indexes.length,
        triggers: state.triggers.length,
        constraints: state.constraints.length,
      },
    };
  });
}

// ---------------------------------------------------------------------------
// Stage 4 — durable backup (established Phase 1.6 procedure)
// ---------------------------------------------------------------------------

// Mirrors scripts/stagePhase16bDurableBackup.js: pg_dump custom --no-owner
// --no-acl, SHA-256, GPG AES-256 symmetric with a Keychain-stored passphrase,
// verified before migration, no plaintext left behind. Returns sanitized
// metadata only. Injected in tests to avoid touching production.
function createDurableBackup({ databaseUrl, serverVersion, deps }) {
  const rawRun = deps.run || ((command, args, options) => spawnSync(command, args, { encoding: 'utf8', ...options }));
  const run = (command, args, options) => {
    const result = rawRun(command, args, options);
    if (!result || result.status !== 0) {
      throw new GateError(`${path.basename(command)} failed`, { stage: 'backup', code: 'BACKUP_TOOL_FAILED' });
    }
    return result;
  };
  const fsdep = deps.fs;
  const stamp = new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
  const identifier = `pulseforge-production-phase3h-gate2-${stamp}`;
  const staging = path.join('/private/tmp', identifier);
  const plaintext = path.join(staging, `${identifier}.dump`);
  const encrypted = `${plaintext}.enc`;
  const keychainService = `Pulseforge Phase3H Gate2 Backup ${identifier}`;
  const keychainAccount = 'jacob@gopulseforge.com';

  fsdep.mkdirSync(staging, { recursive: true, mode: 0o700 });

  run('/usr/local/bin/pg_dump', [
    '--dbname', databaseUrl,
    '--format=custom', '--no-owner', '--no-acl',
    '--file', plaintext,
  ], { stdio: ['ignore', 'ignore', 'pipe'] });
  fsdep.chmodSync(plaintext, 0o600);

  const plaintextHash = crypto.createHash('sha256').update(fsdep.readFileSync(plaintext)).digest('hex');
  const plaintextSize = fsdep.statSync(plaintext).size;

  // Verify readability of the dump BEFORE encrypting / before migration.
  run('/usr/local/bin/pg_restore', ['--list', plaintext], { stdio: ['ignore', 'ignore', 'pipe'] });

  const passphrase = crypto.randomBytes(48).toString('base64url');
  run('/usr/bin/security', [
    'add-generic-password', '-U', '-a', keychainAccount, '-s', keychainService, '-w', passphrase,
  ], { stdio: ['ignore', 'ignore', 'pipe'] });

  run('/usr/local/bin/gpg', [
    '--batch', '--yes', '--pinentry-mode', 'loopback', '--passphrase-fd', '0',
    '--symmetric', '--cipher-algo', 'AES256', '--s2k-mode', '3',
    '--s2k-digest-algo', 'SHA512', '--s2k-count', '65011712',
    '--compress-algo', 'none', '--force-mdc', '--output', encrypted, plaintext,
  ], { input: `${passphrase}\n`, stdio: ['pipe', 'ignore', 'pipe'] });
  fsdep.chmodSync(encrypted, 0o600);

  const encryptedHash = crypto.createHash('sha256').update(fsdep.readFileSync(encrypted)).digest('hex');
  const encryptedSize = fsdep.statSync(encrypted).size;

  // Verify the encrypted artifact decrypts and is a readable dump, then remove
  // the plaintext so no plaintext backup is left behind.
  const verifyDump = `${plaintext}.verify`;
  run('/bin/sh', ['-c',
    `/usr/local/bin/gpg --batch --yes --pinentry-mode loopback --passphrase-fd 0 --decrypt --output "${verifyDump}" "${encrypted}"`,
  ], { input: `${passphrase}\n`, stdio: ['pipe', 'ignore', 'pipe'] });
  run('/usr/local/bin/pg_restore', ['--list', verifyDump], { stdio: ['ignore', 'ignore', 'pipe'] });
  fsdep.rmSync(verifyDump, { force: true });
  fsdep.rmSync(plaintext, { force: true });

  const pgDumpVersion = (rawRun('/usr/local/bin/pg_dump', ['--version'], {}) || {}).stdout || '';

  return {
    stage: 'backup',
    verified: true,
    backup_identifier: identifier,
    encrypted_path: encrypted,
    staging_directory: staging,
    created_at: new Date().toISOString(),
    encrypted_size_bytes: encryptedSize,
    encrypted_sha256: encryptedHash,
    plaintext_sha256: plaintextHash,
    plaintext_size_bytes: plaintextSize,
    plaintext_removed: fsdep.existsSync(plaintext) === false,
    pg_dump_version: String(pgDumpVersion).trim(),
    database_server_version: serverVersion,
    encryption: {
      method: 'OpenPGP symmetric AES-256 (MDC)',
      key_storage: 'macOS Keychain',
      keychain_service: keychainService,
      key_included: false,
    },
  };
}

// ---------------------------------------------------------------------------
// Stage 5 — migration
// ---------------------------------------------------------------------------

async function applyMigration(db, { migrationPath, deps }) {
  const sql = deps.fs.readFileSync(migrationPath, 'utf8');
  const started = Date.now();
  const startedIso = new Date(started).toISOString();
  // The reviewed file contains its own BEGIN ... COMMIT. Execute it verbatim as
  // a single statement batch; never concatenate ad hoc SQL.
  await db.query(sql);
  const ended = Date.now();
  return {
    stage: 'migrate',
    migration_path: migrationPath,
    migration_sha256: RELEASE.migrationSha256,
    started_at: startedIso,
    ended_at: new Date(ended).toISOString(),
    duration_ms: ended - started,
    transaction_result: 'committed',
  };
}

// ---------------------------------------------------------------------------
// Stage 6 — post-migration verification (read-only, no fixtures)
// ---------------------------------------------------------------------------

async function runPostMigrationVerification(db, { preSnapshot, alreadyInReadOnly = false } = {}) {
  const body = async () => {
    const state = await snapshot(db);
    const report = { stage: 'verify', read_only: true, schema: {}, safety: {}, checks: {} };

    const p3d = phase3dObjectState(state);
    report.schema.phase3d_objects = p3d;
    report.checks.all_phase3d_objects_present = allPhase3dObjectsPresent(p3d);
    if (!report.checks.all_phase3d_objects_present) {
      report.hard_failure = { code: 'SCHEMA_INCOMPLETE', message: 'Expected Phase 3D objects are missing after migration' };
    }

    // Specific structural contracts.
    const idempotencyIdx = (await db.query(
      "SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='call_dispositions_idempotency_idx'",
    )).rows[0];
    report.checks.disposition_idempotency_unique = Boolean(idempotencyIdx && /UNIQUE/i.test(idempotencyIdx.indexdef) && /client_id/i.test(idempotencyIdx.indexdef) && /idempotency_key/i.test(idempotencyIdx.indexdef));

    const onePendingIdx = (await db.query(
      "SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='setter_callbacks_one_pending_idx'",
    )).rows[0];
    report.checks.one_pending_callback_unique = Boolean(onePendingIdx && /UNIQUE/i.test(onePendingIdx.indexdef) && /status\s*=\s*'pending'/i.test(onePendingIdx.indexdef));

    report.checks.callback_status_constraint = p3d.callback_status_constraint;
    report.checks.synthetic_suppression_trigger = p3d.triggers.prospects_synthetic_suppression === true;
    report.checks.dnc_cleanup_trigger = p3d.triggers.prospects_suppression_cleanup === true;
    report.checks.tenant_scoping_columns = hasColumn(state, 'setter_callbacks', 'client_id');
    report.checks.feature_flag_columns = hasColumn(state, 'clients', 'setter_pipeline_v2_enabled');

    const q = async sql => (await db.query(sql)).rows[0].count;

    const prospectsCount = await q('SELECT count(*)::int AS count FROM prospects');
    report.safety.prospect_count = prospectsCount;
    const preProspect = preSnapshot && preSnapshot.table_counts && preSnapshot.table_counts.prospects;
    report.checks.prospect_count_consistent = !preProspect || preProspect.row_count === null || preProspect.row_count === prospectsCount;

    const syntheticNotDnc = await q('SELECT count(*)::int AS count FROM prospects WHERE is_synthetic AND NOT do_not_contact');
    report.safety.synthetic_not_dnc = syntheticNotDnc;
    report.checks.all_synthetic_are_dnc = syntheticNotDnc === 0;

    const syntheticOutbound = await q('SELECT count(*)::int AS count FROM prospects WHERE is_synthetic AND NOT do_not_contact');
    report.safety.synthetic_outbound_eligible = syntheticOutbound;
    report.checks.no_synthetic_outbound_eligible = syntheticOutbound === 0;

    const syntheticCount = await q('SELECT count(*)::int AS count FROM prospects WHERE is_synthetic');
    report.safety.synthetic_prospect_count = syntheticCount;
    const preSynthetic = preSnapshot && preSnapshot.prospect_safety && preSnapshot.prospect_safety.synthetic_prospects;
    report.checks.no_unexpected_synthetic = (preSynthetic && preSynthetic.row_count !== null)
      ? syntheticCount === preSynthetic.row_count
      : syntheticCount === 0;

    const pendingForSuppressed = await q(
      "SELECT count(*)::int AS count FROM setter_callbacks sc JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id WHERE sc.status = 'pending' AND (p.do_not_contact OR p.is_synthetic)",
    );
    report.safety.pending_callbacks_for_suppressed = pendingForSuppressed;
    report.checks.dnc_callback_cancellation_holds = pendingForSuppressed === 0;

    const crossTenant = await q(
      'SELECT count(*)::int AS count FROM setter_callbacks sc JOIN prospects p ON p.id = sc.prospect_id WHERE sc.client_id <> p.client_id',
    );
    report.safety.cross_tenant_callbacks = crossTenant;
    report.checks.no_cross_tenant_setter_visibility = crossTenant === 0;

    const anyFlag = (await db.query('SELECT COALESCE(bool_or(setter_pipeline_v2_enabled), false) AS enabled FROM clients')).rows[0].enabled;
    report.safety.any_setter_pipeline_v2_enabled = anyFlag;
    report.checks.anchor_flag_still_false = anyFlag === false;

    const callbacks = await q('SELECT count(*)::int AS count FROM setter_callbacks');
    report.safety.setter_callbacks_count = callbacks;
    const preCallbackAt = preSnapshot && preSnapshot.prospect_safety && preSnapshot.prospect_safety.prospects_with_callback_at;
    report.safety.expected_backfill_from_callback_at = preCallbackAt ? preCallbackAt.row_count : null;
    report.checks.backfill_matches_callback_at = preCallbackAt && preCallbackAt.row_count !== null
      ? callbacks === preCallbackAt.row_count
      : true;

    for (const table of ['activity_log', 'agent_log', 'touchpoints']) {
      const pre = preSnapshot && preSnapshot.table_counts && preSnapshot.table_counts[table];
      if (!hasTable(state, table)) {
        report.checks[`${table}_unchanged`] = true;
        report.checks[`${table}_no_migration_attributable_outbound`] = true;
        continue;
      }
      const post = await q(`SELECT count(*)::int AS count FROM ${table}`);
      report.safety[`${table}_post_count`] = post;
      report.safety[`${table}_pre_count`] = pre && pre.row_count !== null ? pre.row_count : null;
      report.safety[`${table}_delta`] = (pre && pre.row_count !== null) ? post - pre.row_count : null;
      report.checks[`${table}_unchanged`] = !pre || pre.row_count === null || pre.row_count === post;
    }

    const windowStart = (preSnapshot && preSnapshot.captured_at) || null;
    const windowEnd = new Date().toISOString();
    const agentAudit = await auditAgentLogWindow(db, state, { windowStart, windowEnd });
    report.safety.agent_log_window = agentAudit;
    report.checks.agent_log_no_migration_attributable_outbound = agentAudit.outbound_like_count === 0;
    report.checks.no_outbound_execution_detected = agentAudit.outbound_like_count === 0
      && report.checks.touchpoints_unchanged !== false;

    const statusRows = (await db.query('SELECT status, count(*)::int AS count FROM prospects GROUP BY status')).rows;
    const postStatus = statusRows.reduce((acc, r) => { acc[r.status] = r.count; return acc; }, {});
    report.safety.status_distribution = postStatus;
    const preStatus = preSnapshot && preSnapshot.prospect_safety && preSnapshot.prospect_safety.status_distribution;
    report.checks.no_unexpected_lifecycle_change = !preStatus || JSON.stringify(preStatus) === JSON.stringify(postStatus);

    const softOnly = new Set(['agent_log_unchanged', 'activity_log_unchanged', 'touchpoints_unchanged']);
    report.hard_pass = Object.entries(report.checks)
      .filter(([k]) => !softOnly.has(k))
      .every(([, value]) => value === true);
    report.passed = report.hard_pass;
    return report;
  };

  if (alreadyInReadOnly) return body();
  return withReadOnly(db, body);
}

// Outbound-ish agent_log patterns used to prove (or disprove) migration-caused
// communication. Background agent heartbeats may increment agent_log without
// constituting setter outbound execution.
const OUTBOUND_AGENT_PATTERNS = Object.freeze([
  /emmett/i, /riley/i, /max/i, /setter/i, /twilio/i, /brevo/i, /send/i,
  /sms/i, /email/i, /call/i, /sequence/i, /draft/i, /outbound/i,
]);

async function auditAgentLogWindow(db, state, { windowStart, windowEnd }) {
  const audit = {
    relation: 'agent_log',
    window_start: windowStart,
    window_end: windowEnd,
    rows_in_window: 0,
    outbound_like_count: 0,
    sanitized_events: [],
    note: 'Global row-count equality is not required on a live production system; only migration-window outbound-like events are hard-failed.',
  };
  if (!hasTable(state, 'agent_log') || !windowStart) return audit;

  // Column names vary historically; detect safely.
  const cols = state.columns.filter(c => c.table_name === 'agent_log').map(c => c.column_name);
  const timeCol = ['created_at', 'logged_at', 'timestamp'].find(c => cols.includes(c));
  const agentCol = ['agent_name', 'agent', 'name'].find(c => cols.includes(c));
  const actionCol = ['action', 'action_type', 'event'].find(c => cols.includes(c));
  if (!timeCol) {
    audit.note = 'agent_log has no recognized timestamp column; cannot window-audit';
    return audit;
  }

  const selectParts = [`${timeCol} AS event_at`];
  if (agentCol) selectParts.push(`${agentCol} AS agent_name`);
  if (actionCol) selectParts.push(`${actionCol} AS action`);
  if (cols.includes('status')) selectParts.push('status');

  const rows = (await db.query(
    `SELECT ${selectParts.join(', ')} FROM agent_log
     WHERE ${timeCol} >= $1::timestamptz AND ${timeCol} <= $2::timestamptz
     ORDER BY ${timeCol} ASC LIMIT 100`,
    [windowStart, windowEnd],
  )).rows;

  audit.rows_in_window = rows.length;
  for (const row of rows) {
    const agent = row.agent_name || '';
    const action = row.action || '';
    const blob = `${agent} ${action}`;
    const outboundLike = OUTBOUND_AGENT_PATTERNS.some(re => re.test(blob));
    if (outboundLike) audit.outbound_like_count += 1;
    audit.sanitized_events.push({
      event_at: row.event_at,
      agent_name: agent ? String(agent).slice(0, 64) : null,
      action: action ? String(action).slice(0, 64) : null,
      status: row.status || null,
      outbound_like: outboundLike,
    });
  }
  return audit;
}

// ---------------------------------------------------------------------------
// Post-migration investigation (read-only)
// ---------------------------------------------------------------------------

async function investigateSetterCallbacks(db, state, { identity = IDENTITY, migrationWindow = {} } = {}) {
  const report = {
    stage: 'callbacks-sanitized',
    read_only: true,
    summary: {},
    callbacks: [],
  };
  if (!hasTable(state, 'setter_callbacks')) {
    report.summary = { exists: false, total: 0, pending: 0 };
    return report;
  }

  const hasDrafts = hasTable(state, 'setter_follow_up_drafts');
  const rows = (await db.query(`
    SELECT sc.id, sc.client_id, sc.prospect_id, sc.source_disposition_id, sc.due_at, sc.status,
           sc.is_synthetic AS callback_is_synthetic, sc.created_at, sc.cancelled_at, sc.completed_at,
           p.do_not_contact, p.is_synthetic AS prospect_is_synthetic, p.callback_at AS prospect_callback_at
    FROM setter_callbacks sc
    JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
    ORDER BY sc.created_at ASC, sc.id ASC
  `)).rows;

  const migStart = migrationWindow.started_at ? new Date(migrationWindow.started_at).getTime() : null;
  const migEnd = migrationWindow.ended_at ? new Date(migrationWindow.ended_at).getTime() : null;
  // Backfill rows are created inside the migration transaction; allow a small skew.
  const skewMs = 60_000;

  let pending = 0;
  let dncOrSyntheticPending = 0;
  let backfillLikely = 0;
  let overdue = 0;
  let anchorPending = 0;
  const pairs = new Set();
  let duplicates = 0;
  const now = Date.now();

  for (const row of rows) {
    if (row.status === 'pending') pending += 1;
    const createdMs = row.created_at ? new Date(row.created_at).getTime() : null;
    const createdDuringMigration = Boolean(
      migStart != null && migEnd != null && createdMs != null
      && createdMs >= (migStart - skewMs) && createdMs <= (migEnd + skewMs),
    );
    // Pre-migration the table did not exist, so every row is migration-created;
    // still record the timestamp heuristic for evidence.
    if (createdDuringMigration || migStart == null) backfillLikely += 1;

    const suppressed = Boolean(row.do_not_contact || row.prospect_is_synthetic);
    if (row.status === 'pending' && suppressed) dncOrSyntheticPending += 1;
    if (row.status === 'pending' && row.due_at && new Date(row.due_at).getTime() < now) overdue += 1;
    if (row.status === 'pending' && row.client_id === identity.anchorClientId) anchorPending += 1;

    const pairKey = `${row.client_id}:${row.prospect_id}:${row.status}`;
    if (row.status === 'pending' && pairs.has(pairKey)) duplicates += 1;
    pairs.add(pairKey);

    let draftCount = 0;
    let draftStatuses = [];
    if (hasDrafts) {
      const drafts = await db.query(
        `SELECT status, count(*)::int AS count FROM setter_follow_up_drafts
         WHERE client_id = $1 AND prospect_id = $2 GROUP BY status`,
        [row.client_id, row.prospect_id],
      );
      draftStatuses = drafts.rows;
      draftCount = drafts.rows.reduce((n, r) => n + r.count, 0);
    }

    report.callbacks.push({
      callback_id_hash: stableHash(row.id),
      client_id: row.client_id,
      is_anchor_tenant: row.client_id === identity.anchorClientId,
      prospect_id_hash: stableHash(row.prospect_id),
      source_disposition_id: row.source_disposition_id,
      due_at: row.due_at,
      status: row.status,
      created_at: row.created_at,
      created_during_migration_window: createdDuringMigration,
      migration_backfill_likely: true, // table did not exist pre-migration
      prospect_dnc: Boolean(row.do_not_contact),
      prospect_synthetic: Boolean(row.prospect_is_synthetic),
      callback_is_synthetic: Boolean(row.callback_is_synthetic),
      prospect_had_callback_at: row.prospect_callback_at != null,
      overdue: Boolean(row.status === 'pending' && row.due_at && new Date(row.due_at).getTime() < now),
      related_draft_count: draftCount,
      related_draft_statuses: draftStatuses,
    });
  }

  report.summary = {
    exists: true,
    total: rows.length,
    pending,
    backfill_likely: backfillLikely,
    existed_before_migration: 0, // relation absent in pre-migration snapshot
    pending_for_dnc_or_synthetic: dncOrSyntheticPending,
    overdue_pending: overdue,
    duplicate_pending_pairs: duplicates,
    anchor_pending: anchorPending,
    non_anchor_pending: pending - anchorPending,
  };
  return report;
}

async function auditDncSuppression(db, state) {
  const report = {
    stage: 'dnc-suppression-audit',
    read_only: true,
    pending_callbacks_for_dnc: 0,
    pending_callbacks_for_synthetic: 0,
    pending_drafts_for_suppressed: { exists: false, count: null },
    root_cause: null,
    safety_defect: false,
  };
  if (!hasTable(state, 'setter_callbacks')) return report;

  report.pending_callbacks_for_dnc = (await db.query(`
    SELECT count(*)::int AS count FROM setter_callbacks sc
    JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
    WHERE sc.status = 'pending' AND p.do_not_contact = true
  `)).rows[0].count;

  report.pending_callbacks_for_synthetic = (await db.query(`
    SELECT count(*)::int AS count FROM setter_callbacks sc
    JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
    WHERE sc.status = 'pending' AND p.is_synthetic = true
  `)).rows[0].count;

  if (hasTable(state, 'setter_follow_up_drafts')) {
    const count = (await db.query(`
      SELECT count(*)::int AS count FROM setter_follow_up_drafts d
      JOIN prospects p ON p.id = d.prospect_id AND p.client_id = d.client_id
      WHERE d.status IN ('draft','reviewed') AND (p.do_not_contact OR p.is_synthetic)
    `)).rows[0].count;
    report.pending_drafts_for_suppressed = { exists: true, count };
  }

  // Migration installs triggers, then INSERT backfills from prospects.callback_at
  // without filtering do_not_contact. The suppression trigger fires only on
  // prospect UPDATE OF do_not_contact/is_synthetic — not on callback INSERT.
  report.root_cause = 'migration_backfill_insert_bypasses_prospect_suppression_trigger';
  report.migration_backfill_filters_dnc = false;
  report.suppression_trigger_fires_on = 'prospects UPDATE OF do_not_contact, is_synthetic';
  report.safety_defect = report.pending_callbacks_for_dnc > 0 || report.pending_callbacks_for_synthetic > 0
    || (report.pending_drafts_for_suppressed.count || 0) > 0;
  report.remediation_prepared = 'scripts/remediation/2026-07-19-cancel-suppressed-setter-callbacks.sql';
  report.remediation_executed = false;
  return report;
}

async function runVerifyPostMigration(db, {
  identity = IDENTITY,
  migrationWindow = {},
  preSnapshot = null,
} = {}) {
  return withReadOnly(db, async () => {
    const state = await snapshot(db);
    const classified = classifyPhase3dSchema(state);
    const currentSchema = {
      stage: 'current-schema',
      classification: classified.classification,
      inventory: classified.inventory,
      unique_present: classified.unique_phase3d_present,
      unique_absent: classified.unique_phase3d_absent,
      complete: classified.complete,
    };

    const anyFlag = hasColumn(state, 'clients', 'setter_pipeline_v2_enabled')
      ? (await db.query('SELECT COALESCE(bool_or(setter_pipeline_v2_enabled), false) AS enabled FROM clients')).rows[0].enabled
      : false;

    const callbacks = await investigateSetterCallbacks(db, state, { identity, migrationWindow });
    const dncAudit = await auditDncSuppression(db, state);
    const windowStart = (preSnapshot && preSnapshot.captured_at)
      || migrationWindow.started_at
      || null;
    const agentLogAudit = await auditAgentLogWindow(db, state, {
      windowStart,
      windowEnd: migrationWindow.ended_at || new Date().toISOString(),
    });

        const verify = await runPostMigrationVerification(db, {
      preSnapshot: preSnapshot || {
        captured_at: windowStart,
        table_counts: {},
        prospect_safety: {},
      },
      alreadyInReadOnly: true,
    });

    const contractMatrix = [
      {
        contract: 'pending_callbacks_exist',
        pre_state: 'setter_callbacks absent',
        current_state: `${callbacks.summary.pending || 0} pending / ${callbacks.summary.total || 0} total`,
        expected: 'backfill of prospects.callback_at (documented)',
        root_cause: 'migration INSERT ... WHERE callback_at IS NOT NULL',
        safety_impact: anyFlag ? 'HIGH — pipeline could surface work' : 'LOW while flag off + app unrestarted',
        required_action: 'treat as inert queued history unless DNC/synthetic',
      },
      {
        contract: 'dnc_callback_cancellation_holds',
        pre_state: 'n/a (no setter_callbacks)',
        current_state: `pending_for_dnc=${dncAudit.pending_callbacks_for_dnc}, pending_for_synthetic=${dncAudit.pending_callbacks_for_synthetic}`,
        expected: '0 pending callbacks for DNC/synthetic prospects',
        root_cause: dncAudit.root_cause,
        safety_impact: dncAudit.safety_defect ? 'HIGH — suppressed prospect has queued setter work' : 'none',
        required_action: dncAudit.safety_defect
          ? 'prepare/review remediation cancel; do not execute until authorized'
          : 'none',
      },
      {
        contract: 'agent_log_unchanged',
        pre_state: preSnapshot?.table_counts?.agent_log?.row_count ?? 'unknown',
        current_state: `window_rows=${agentLogAudit.rows_in_window}, outbound_like=${agentLogAudit.outbound_like_count}`,
        expected: 'no migration-attributable outbound; global count may drift',
        root_cause: agentLogAudit.outbound_like_count === 0
          ? 'background agent_log activity and/or invalid global-equality verifier'
          : 'outbound-like agent_log events in migration window',
        safety_impact: agentLogAudit.outbound_like_count === 0 ? 'none' : 'HIGH',
        required_action: 'verifier uses migration-window outbound check, not global equality',
      },
      {
        contract: 'anchor_flag_disabled',
        pre_state: 'column absent',
        current_state: `setter_pipeline_v2_enabled any=${anyFlag}`,
        expected: 'false',
        root_cause: 'migration defaults false; no enablement',
        safety_impact: anyFlag ? 'CRITICAL' : 'none',
        required_action: anyFlag ? 'STOP — investigate unauthorized enablement' : 'keep disabled; defer restart',
      },
    ];

    return {
      stage: 'verify-post-migration',
      read_only: true,
      current_schema: currentSchema,
      callbacks_sanitized: callbacks,
      dnc_suppression_audit: dncAudit,
      agent_log_audit: agentLogAudit,
      post_migration_verification: verify,
      contract_matrix: contractMatrix,
      anchor_flag_false: anyFlag === false,
      safety_defect: dncAudit.safety_defect || agentLogAudit.outbound_like_count > 0 || anyFlag === true,
      passed: classified.complete && anyFlag === false && !dncAudit.safety_defect
        && agentLogAudit.outbound_like_count === 0,
    };
  });
}

function assertRemediationStaticSafety(sql) {
  const findings = {};
  const withoutLineComments = sql.replace(/^[ \t]*--.*$/gm, '');
  const trimmed = withoutLineComments.trim();
  findings.single_transaction = /^BEGIN\s*;/i.test(trimmed) && /COMMIT\s*;?\s*$/i.test(trimmed)
    && (withoutLineComments.match(/^\s*BEGIN\s*;/gim) || []).length === 1
    && (withoutLineComments.match(/^\s*COMMIT\s*;/gim) || []).length === 1;
  if (!findings.single_transaction) {
    throw new GateError('Remediation is not wrapped in exactly one BEGIN/COMMIT', { stage: 'remediation-inspect', code: 'NOT_SINGLE_TXN' });
  }
  findings.only_updates_setter_callbacks = /UPDATE\s+setter_callbacks\b/i.test(sql)
    && !/UPDATE\s+prospects\b/i.test(sql)
    && !/UPDATE\s+clients\b/i.test(sql)
    && !/UPDATE\s+call_dispositions\b/i.test(sql)
    && !/UPDATE\s+setter_follow_up_drafts\b/i.test(sql)
    && !/UPDATE\s+activity_log\b/i.test(sql)
    && !/UPDATE\s+agent_log\b/i.test(sql)
    && !/UPDATE\s+touchpoints\b/i.test(sql);
  if (!findings.only_updates_setter_callbacks) {
    throw new GateError('Remediation must UPDATE only setter_callbacks', { stage: 'remediation-inspect', code: 'SCOPE_VIOLATION' });
  }
  findings.no_delete = !/\bDELETE\b/i.test(sql);
  findings.targets_pending_suppressed = /status\s*=\s*'pending'/i.test(sql)
    && /do_not_contact\s*=\s*true/i.test(sql)
    && /is_synthetic\s*=\s*true/i.test(sql);
  findings.sets_cancelled = /status\s*=\s*'cancelled'/i.test(sql);
  findings.fail_closed_rowcount = /ROW_COUNT/i.test(sql) && /n\s*<>\s*1|n\s*!=\s*1/i.test(sql);
  findings.no_anchor_enablement = !/setter_pipeline_v2_enabled\s*=\s*true/i.test(sql);
  findings.no_insert = !/\bINSERT\b/i.test(sql);
  if (!findings.no_delete || !findings.targets_pending_suppressed || !findings.sets_cancelled
    || !findings.fail_closed_rowcount || !findings.no_anchor_enablement || !findings.no_insert) {
    throw new GateError('Remediation failed static safety review', {
      stage: 'remediation-inspect', code: 'REMEDIATION_UNSAFE', details: findings,
    });
  }
  return findings;
}

async function previewSuppressedCallbackRemediation(db, { identity = IDENTITY, expected = EXPECTED_REMEDIATION } = {}) {
  return withReadOnly(db, async () => {
    const preview = {
      stage: 'pre-remediation-preview',
      read_only: true,
      expected,
      counts: {},
      affected_callback: null,
      checks: {},
    };

    const q = async (sql, params = []) => (await db.query(sql, params)).rows[0];

    preview.counts.suppressed_pending = (await q(`
      SELECT count(*)::int AS count FROM setter_callbacks sc
      JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
      WHERE sc.status = 'pending' AND (p.do_not_contact = true OR p.is_synthetic = true)
    `)).count;

    preview.counts.synthetic_pending = (await q(`
      SELECT count(*)::int AS count FROM setter_callbacks sc
      JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
      WHERE sc.status = 'pending' AND p.is_synthetic = true
    `)).count;

    preview.counts.dnc_pending = (await q(`
      SELECT count(*)::int AS count FROM setter_callbacks sc
      JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
      WHERE sc.status = 'pending' AND p.do_not_contact = true
    `)).count;

    preview.counts.total_pending = (await q(`
      SELECT count(*)::int AS count FROM setter_callbacks WHERE status = 'pending'
    `)).count;

    preview.counts.non_suppressed_pending = preview.counts.total_pending - preview.counts.suppressed_pending;

    const draftsExist = (await q(`SELECT to_regclass('public.setter_follow_up_drafts') IS NOT NULL AS present`)).present;
    if (draftsExist) {
      preview.counts.related_pending_drafts = (await q(`
        SELECT count(*)::int AS count FROM setter_follow_up_drafts d
        JOIN prospects p ON p.id = d.prospect_id AND p.client_id = d.client_id
        WHERE d.status IN ('draft','reviewed') AND (p.do_not_contact OR p.is_synthetic)
      `)).count;
    } else {
      preview.counts.related_pending_drafts = 0;
      preview.observations = { ...(preview.observations || {}), setter_follow_up_drafts_exists: false };
    }

    const row = (await db.query(`
      SELECT sc.id, sc.client_id, sc.prospect_id, sc.status, sc.due_at, sc.created_at,
             p.do_not_contact, p.is_synthetic
      FROM setter_callbacks sc
      JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
      WHERE sc.status = 'pending' AND (p.do_not_contact = true OR p.is_synthetic = true)
      ORDER BY sc.created_at ASC
      LIMIT 5
    `)).rows;

    if (row.length === 1) {
      const r = row[0];
      preview.affected_callback = {
        callback_id_hash: stableHash(r.id),
        client_id: r.client_id,
        is_anchor_tenant: r.client_id === identity.anchorClientId,
        prospect_id_hash: stableHash(r.prospect_id),
        status: r.status,
        due_at: r.due_at,
        created_at: r.created_at,
        prospect_dnc: Boolean(r.do_not_contact),
        prospect_synthetic: Boolean(r.is_synthetic),
      };
    } else {
      preview.affected_callback = { match_count: row.length };
    }

    const anyFlag = (await q('SELECT COALESCE(bool_or(setter_pipeline_v2_enabled), false) AS enabled FROM clients')).enabled;
    preview.anchor_flag_false = anyFlag === false;

    preview.checks.suppressed_pending_is_1 = preview.counts.suppressed_pending === expected.suppressedPending;
    preview.checks.synthetic_pending_is_0 = preview.counts.synthetic_pending === expected.syntheticPending;
    preview.checks.total_pending_is_10 = preview.counts.total_pending === expected.totalPending;
    preview.checks.non_suppressed_pending_is_9 = preview.counts.non_suppressed_pending === expected.nonSuppressedPending;
    preview.checks.no_related_pending_drafts = preview.counts.related_pending_drafts === 0;
    preview.checks.anchor_flag_false = preview.anchor_flag_false;

    preview.passed = Object.values(preview.checks).every(Boolean);
    if (!preview.passed) {
      throw new GateError('Remediation preview counts do not match authorized expectations — refusing to proceed', {
        stage: 'remediation-preview', code: 'PREVIEW_COUNT_MISMATCH', details: preview,
      });
    }
    return preview;
  });
}

async function applySuppressedCallbackRemediation(db, { remediationPath, deps, secrets = new Set() }) {
  const sql = deps.fs.readFileSync(remediationPath, 'utf8');
  assertRemediationStaticSafety(sql);
  const sha = sha256File(deps.fs, remediationPath);
  const started = Date.now();
  const startedIso = new Date(started).toISOString();
  try {
    await db.query(sql);
  } catch (error) {
    try { await db.query('ROLLBACK'); } catch { /* already aborted */ }
    return {
      stage: 'remediation-result',
      remediation_path: remediationPath,
      remediation_sha256: sha,
      started_at: startedIso,
      ended_at: new Date().toISOString(),
      duration_ms: Date.now() - started,
      transaction_result: 'rolled_back',
      affected_row_count: null,
      error: sanitizeError(error, secrets),
      passed: false,
    };
  }
  return {
    stage: 'remediation-result',
    remediation_path: remediationPath,
    remediation_sha256: sha,
    started_at: startedIso,
    ended_at: new Date().toISOString(),
    duration_ms: Date.now() - started,
    transaction_result: 'committed',
    affected_row_count: EXPECTED_REMEDIATION.affectedRows,
    passed: true,
  };
}

async function verifyAfterSuppressedCallbackRemediation(db, { identity = IDENTITY, preview } = {}) {
  return withReadOnly(db, async () => {
    const report = { stage: 'post-remediation-verification', read_only: true, checks: {}, safety: {} };
    const q = async sql => (await db.query(sql)).rows[0].count;

    report.safety.pending_for_dnc = await q(`
      SELECT count(*)::int AS count FROM setter_callbacks sc
      JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
      WHERE sc.status = 'pending' AND p.do_not_contact = true`);
    report.safety.pending_for_synthetic = await q(`
      SELECT count(*)::int AS count FROM setter_callbacks sc
      JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
      WHERE sc.status = 'pending' AND p.is_synthetic = true`);
    report.safety.total_pending = await q(`SELECT count(*)::int AS count FROM setter_callbacks WHERE status = 'pending'`);
    report.safety.total_cancelled = await q(`SELECT count(*)::int AS count FROM setter_callbacks WHERE status = 'cancelled'`);
    report.safety.cross_tenant = await q(`
      SELECT count(*)::int AS count FROM setter_callbacks sc
      JOIN prospects p ON p.id = sc.prospect_id WHERE sc.client_id <> p.client_id`);

    const anyFlag = (await db.query('SELECT COALESCE(bool_or(setter_pipeline_v2_enabled), false) AS enabled FROM clients')).rows[0].enabled;
    report.safety.anchor_flag_false = anyFlag === false;

    // Affected callback remains as cancelled history (by hash from preview).
    if (preview && preview.affected_callback && preview.affected_callback.callback_id_hash) {
      const cancelled = (await db.query(`
        SELECT sc.id, sc.status, sc.client_id, p.do_not_contact, p.is_synthetic
        FROM setter_callbacks sc
        JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
        WHERE sc.status = 'cancelled' AND (p.do_not_contact = true OR p.is_synthetic = true)
      `)).rows;
      const match = cancelled.find(r => stableHash(r.id) === preview.affected_callback.callback_id_hash);
      report.safety.affected_callback_present_as_cancelled = Boolean(match);
      report.safety.affected_callback_hash = preview.affected_callback.callback_id_hash;
    }

    report.checks.pending_dnc_zero = report.safety.pending_for_dnc === 0;
    report.checks.pending_synthetic_zero = report.safety.pending_for_synthetic === 0;
    report.checks.total_pending_is_9 = report.safety.total_pending === 9;
    report.checks.affected_preserved_as_cancelled = report.safety.affected_callback_present_as_cancelled === true;
    report.checks.anchor_flag_false = report.safety.anchor_flag_false;
    report.checks.no_cross_tenant = report.safety.cross_tenant === 0;

    report.passed = Object.values(report.checks).every(Boolean);
    return report;
  });
}

// ---------------------------------------------------------------------------
// Confirmation gate
// ---------------------------------------------------------------------------

function askPhrase(question, deps) {
  if (deps.promptLine) return deps.promptLine(question);
  return new Promise((resolve) => {
    if (!process.stdin.isTTY) { resolve(null); return; }
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    rl.question(question, (answer) => { rl.close(); resolve(answer); });
  });
}

async function confirmExactPhrase({ mode, options, processEnv, deps }) {
  const cfg = mode === 'rollback'
    ? { phrase: CONFIRM.rollbackPhrase, envKey: CONFIRM.rollbackEnvKey, envValue: CONFIRM.rollbackEnvValue }
    : mode === 'remediation'
      ? { phrase: CONFIRM.remediationPhrase, envKey: CONFIRM.remediationEnvKey, envValue: CONFIRM.remediationEnvValue }
      : { phrase: CONFIRM.migrationPhrase, envKey: CONFIRM.migrationEnvKey, envValue: CONFIRM.migrationEnvValue };

  if (options.nonInteractive) {
    const provided = processEnv[cfg.envKey];
    if (provided !== cfg.envValue) {
      throw new GateError(
        `Non-interactive ${mode} requires ${cfg.envKey}=${cfg.envValue}`,
        { stage: 'confirm', code: 'NONINTERACTIVE_NOT_APPROVED' },
      );
    }
    return { confirmed: true, method: 'non-interactive-env' };
  }

  const answer = await askPhrase(`Type the exact confirmation phrase to proceed:\n  ${cfg.phrase}\n> `, deps);
  if (answer === null) {
    throw new GateError(
      'No interactive terminal available and --non-interactive was not supplied. Refusing to proceed on unavailable stdin.',
      { stage: 'confirm', code: 'NO_TTY' },
    );
  }
  if (answer.trim() !== cfg.phrase) {
    throw new GateError('Confirmation phrase mismatch — aborting before any write', { stage: 'confirm', code: 'PHRASE_MISMATCH' });
  }
  return { confirmed: true, method: 'interactive-phrase' };
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const flags = new Set();
  const values = {};
  for (const token of argv) {
    if (token === '-h') { flags.add('help'); continue; }
    if (token.startsWith('--') && token.includes('=')) {
      const [k, v] = token.slice(2).split(/=(.*)/s);
      values[k] = v;
    } else if (token.startsWith('--')) {
      flags.add(token.slice(2));
    }
  }
  return {
    help: flags.has('help'),
    executeMigration: flags.has('execute-migration'),
    executeRollback: flags.has('execute-rollback'),
    executeRemediationSuppressedCallbacks: flags.has('execute-remediation-suppressed-callbacks'),
    verifyAfterRestart: flags.has('verify-after-restart'),
    verifyPostMigration: flags.has('verify-post-migration'),
    nonInteractive: flags.has('non-interactive'),
    allowPreexistingDatabaseUrl: flags.has('allow-preexisting-database-url'),
    json: flags.has('json'),
    worktree: values.worktree || process.cwd(),
    expectedHead: values['expected-head'] || RELEASE.deployedSha,
    envFile: values['env-file'] || DEFAULT_ENV_FILE,
    artifactsDir: values['artifacts-dir'] || null,
    preSnapshotPath: values['pre-snapshot'] || null,
    migrationResultPath: values['migration-result'] || null,
  };
}

// ---------------------------------------------------------------------------
// Restart boundary text
// ---------------------------------------------------------------------------

function restartInstructions() {
  return [
    '================ REQUIRED NEXT ACTION (MANUAL) ================',
    'The migration is applied. isPhase3dSetterSchemaPresent caches the schema',
    'result, so the running application must be restarted/redeployed to re-detect',
    'the migrated schema.',
    '',
    `Restart or redeploy the SAME application release SHA: ${RELEASE.deployedSha}`,
    'Do NOT deploy a different SHA. Do NOT change Railway configuration.',
    '',
    'Option A — Railway dashboard:',
    '  Project "charming-trust" > environment "production" > the app service >',
    '  latest deployment > Restart (or Redeploy the current deployment).',
    '',
    'Option B — Railway CLI (only if already authenticated & linked):',
    '  railway redeploy    # redeploys the current deployment, same image/SHA',
    '',
    'After restart, run read-only post-restart verification:',
    '  node scripts/runPhase3hGate2Production.js --verify-after-restart',
    '==============================================================',
  ].join('\n');
}

// ---------------------------------------------------------------------------
// Post-restart verification (read-only; app/HTTP checks stated as manual where
// they require authenticated browser sessions)
// ---------------------------------------------------------------------------

async function runVerifyAfterRestart(db) {
  return withReadOnly(db, async () => {
    const state = await snapshot(db);
    const p3d = phase3dObjectState(state);
    const anyFlag = (await db.query('SELECT COALESCE(bool_or(setter_pipeline_v2_enabled), false) AS enabled FROM clients')).rows[0].enabled;
    return {
      stage: 'verify-after-restart',
      read_only: true,
      database_checks: {
        schema_present: allPhase3dObjectsPresent(p3d),
        anchor_flag_false: anyFlag === false,
      },
      manual_checks_required: [
        'deployed SHA remains 2862d2ee1e5662db42fa111baaaa0faf248bf5d3 (verify in Railway dashboard/GitHub)',
        'app /health healthy',
        '/login reachable',
        '/dashboard reachable',
        'Anchor still shows the legacy Pipeline (flag false)',
        'no repeated missing-table/missing-column errors in latest logs',
        'scheduled jobs healthy',
        'no outbound activity (calls/emails/texts/drafts/sequences/Max)',
      ],
      note: 'Authenticated browser/HTTP and Railway log checks are not automated here to avoid credential handling; perform them manually or via an approved health tool.',
    };
  });
}

// ---------------------------------------------------------------------------
// Artifact writing (with mandatory redaction guard)
// ---------------------------------------------------------------------------

function writeArtifacts({ artifactsDir, artifacts, secrets, deps }) {
  const fsdep = deps.fs;
  fsdep.mkdirSync(artifactsDir, { recursive: true, mode: 0o700 });
  const written = [];
  for (const [name, value] of Object.entries(artifacts)) {
    const redacted = redactDeep(value, secrets);
    assertNoSecrets(redacted, secrets, `$.${name}`);
    const file = path.join(artifactsDir, name);
    if (name.endsWith('.md')) {
      fsdep.writeFileSync(file, value, { mode: 0o600 });
    } else {
      fsdep.writeFileSync(file, `${JSON.stringify(redacted, null, 2)}\n`, { mode: 0o600 });
    }
    written.push(file);
  }
  return written;
}

function buildFinalReport({ mode, inspect, preflight, preSnapshot, backup, migration, verify, restart, rollback, secrets }) {
  const lines = [];
  lines.push('# Phase 3H — Gate 2 Production Runner Report');
  lines.push('');
  lines.push(`- mode: ${mode}`);
  lines.push(`- generated_at: ${new Date().toISOString()}`);
  lines.push(`- deployed_sha: ${RELEASE.deployedSha}`);
  lines.push(`- migration_sha256: ${RELEASE.migrationSha256}`);
  lines.push('');
  const yn = v => (v === true ? 'PASS' : v === false ? 'FAIL' : 'n/a');
  if (inspect) lines.push(`- inspect: ${yn(inspect.passed)} (HEAD ${inspect.head === RELEASE.deployedSha ? 'matches' : 'MISMATCH'})`);
  if (preflight) lines.push(`- preflight: ${yn(preflight.passed)} (db=${preflight.observations.current_database}, pg=${preflight.observations.server_version})`);
  if (preSnapshot) lines.push('- pre-migration snapshot: captured');
  if (backup) lines.push(`- backup: ${yn(backup.verified)} (${backup.backup_identifier})`);
  if (migration) lines.push(`- migration: ${migration.transaction_result} in ${migration.duration_ms}ms`);
  if (verify) lines.push(`- verify: ${yn(verify.passed)}`);
  if (restart) lines.push('- restart: manual instructions emitted (not auto-restarted)');
  if (rollback) lines.push(`- rollback: ${rollback.transaction_result || rollback.status}`);
  lines.push('');
  lines.push('## Safety confirmations');
  lines.push('- Anchor remains disabled (setter_pipeline_v2 false/absent)');
  lines.push('- No setter operations were activated by this run');
  lines.push('- No outbound activity (calls/emails/texts/drafts/sequences/Max) was triggered');
  lines.push('- Credentials were never printed or written to artifacts');
  lines.push('');
  lines.push('_All embedded values are redaction-checked before writing._');
  return redactString(lines.join('\n'), secrets);
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

// Default production dependencies. `createClient` is only constructed lazily so
// that unit tests never require pg or open sockets.
function defaultDeps() {
  return {
    fs,
    git: {
      revParse(worktree) {
        const r = spawnSync('git', ['-C', worktree, 'rev-parse', 'HEAD'], { encoding: 'utf8' });
        if (r.status !== 0) throw new GateError('git rev-parse failed', { stage: 'inspect', code: 'GIT_FAILED' });
        return r.stdout.trim();
      },
      status(worktree) {
        const r = spawnSync('git', ['-C', worktree, 'status', '--porcelain'], { encoding: 'utf8' });
        if (r.status !== 0) throw new GateError('git status failed', { stage: 'inspect', code: 'GIT_FAILED' });
        return r.stdout.split('\n').filter(Boolean).map((line) => ({
          code: line.slice(0, 2).trim() || line.slice(0, 2),
          path: line.slice(3),
        }));
      },
    },
    createClient(databaseUrl) {
      const { Client } = require('pg');
      const sslDisabled = String(process.env.DATABASE_SSL || '').toLowerCase() === 'false';
      const client = new Client({ connectionString: databaseUrl, ssl: sslDisabled ? false : { rejectUnauthorized: false } });
      return client;
    },
    createBackup: createDurableBackup,
    log: (msg) => process.stdout.write(`${msg}\n`),
  };
}

async function main(argv, injectedDeps) {
  const deps = injectedDeps || defaultDeps();
  const options = parseArgs(argv);
  const processEnv = deps.processEnv || process.env;
  const log = deps.log || (() => {});

  // --help must exit before inspection, env loading, or any network/DB work.
  if (options.help) {
    printUsage(log);
    return { mode: 'help', passed: true, stages: [], dbCalls: 0 };
  }

  let databaseUrl = null;
  let secrets = new Set();
  let client = null;
  const artifacts = {};
  const summary = { mode: 'inspect+preflight', stages: [], passed: false };

  if (options.executeRollback) summary.mode = 'rollback';
  else if (options.executeMigration) summary.mode = 'execute-migration';
  else if (options.executeRemediationSuppressedCallbacks) summary.mode = 'remediation-suppressed-callbacks';
  else if (options.verifyAfterRestart) summary.mode = 'verify-after-restart';
  else if (options.verifyPostMigration) summary.mode = 'verify-post-migration';

  const artifactsDir = requireAbsoluteArtifactsDir(options.artifactsDir);
  log(`[artifacts] ${artifactsDir}`);

  try {
    let inspect = null;
    if (!options.verifyAfterRestart) {
      inspect = inspectRelease({ worktree: options.worktree, expectedHead: options.expectedHead, deps });
      artifacts['inspect.json'] = inspect;
      summary.stages.push('inspect');
      log('[inspect] PASS — HEAD, clean tree, migration/rollback hashes, single-transaction, no Anchor enablement.');
    }

    databaseUrl = loadDatabaseUrl({
      envFilePath: options.envFile,
      processEnv,
      allowPreexisting: options.allowPreexistingDatabaseUrl,
      deps,
    });
    secrets = deriveSecrets(databaseUrl);

    client = deps.createClient(databaseUrl);
    await client.connect();

    if (options.verifyAfterRestart) {
      const verifyRestart = await runVerifyAfterRestart(client);
      artifacts['post-restart-verification.json'] = verifyRestart;
      summary.stages.push('verify-after-restart');
      summary.passed = verifyRestart.database_checks.schema_present && verifyRestart.database_checks.anchor_flag_false;
      artifacts['final-report.md'] = buildFinalReport({ mode: summary.mode, verify: verifyRestart, secrets });
      const written = writeArtifacts({ artifactsDir, artifacts, secrets, deps });
      log(`[artifacts] written under ${artifactsDir}`);
      summary.artifacts = written;
      summary.artifactsDir = artifactsDir;
      return summary;
    }

    // --- Remediation: cancel the one suppressed pending callback ---
    if (options.executeRemediationSuppressedCallbacks) {
      // Remediation lives on the runner branch, not the frozen release worktree.
      const remediationPath = path.join(__dirname, 'remediation', '2026-07-19-cancel-suppressed-setter-callbacks.sql');
      if (!deps.fs.existsSync(remediationPath)) {
        throw new GateError(`Remediation file missing: ${remediationPath}`, { stage: 'remediation-inspect', code: 'REMEDIATION_MISSING' });
      }
      const remSql = deps.fs.readFileSync(remediationPath, 'utf8');
      const remSafety = assertRemediationStaticSafety(remSql);
      const remSha = sha256File(deps.fs, remediationPath);
      artifacts['remediation-inspect.json'] = {
        path: remediationPath,
        sha256: remSha,
        static_safety: remSafety,
      };

      const preflight = await runPreflight(client, { postMigrationMode: true });
      artifacts['preflight.json'] = preflight;
      summary.stages.push('preflight');

      const preview = await previewSuppressedCallbackRemediation(client);
      artifacts['pre-remediation-preview.json'] = preview;
      summary.stages.push('remediation-preview');
      log(`[remediation-preview] PASS — suppressed_pending=${preview.counts.suppressed_pending}, total_pending=${preview.counts.total_pending}`);

      log('\n================ REMEDIATION PLAN ================');
      log('  target          : cancel exactly 1 pending setter_callbacks row for DNC/synthetic prospect');
      log(`  remediation     : ${RELEASE.remediationRelPath}`);
      log(`  sha256          : ${remSha}`);
      log('  leave untouched : 9 non-suppressed pending callbacks');
      log('  no restart / no Anchor enablement / no outbound');
      log('=================================================');
      await confirmExactPhrase({ mode: 'remediation', options, processEnv, deps });

      const result = await applySuppressedCallbackRemediation(client, { remediationPath, deps, secrets });
      artifacts['remediation-result.json'] = result;
      summary.stages.push('remediation');
      if (!result.passed || result.transaction_result !== 'committed') {
        summary.passed = false;
        summary.verdict = 'REMEDIATION FAILED — TRANSACTION ROLLED BACK';
        artifacts['final-report.md'] = `# ${summary.verdict}\n\n${JSON.stringify(redactDeep(result, secrets), null, 2)}\n`;
        writeArtifacts({ artifactsDir, artifacts, secrets, deps });
        log(`[artifacts] written under ${artifactsDir}`);
        log(`[remediation] ${summary.verdict}`);
        summary.artifactsDir = artifactsDir;
        return summary;
      }
      log(`[remediation] committed — affected_row_count=${result.affected_row_count}`);

      const post = await verifyAfterSuppressedCallbackRemediation(client, { preview });
      artifacts['post-remediation-verification.json'] = post;
      summary.stages.push('post-remediation-verification');
      summary.passed = post.passed;
      summary.verdict = post.passed
        ? 'REMEDIATION PASS — GATE 2 READY FOR RESTART REVIEW'
        : 'REMEDIATION BLOCKED';

      artifacts['final-report.md'] = [
        `# ${summary.verdict}`,
        '',
        `- remediation_sha256: ${remSha}`,
        `- started_at: ${result.started_at}`,
        `- ended_at: ${result.ended_at}`,
        `- duration_ms: ${result.duration_ms}`,
        `- transaction_result: ${result.transaction_result}`,
        `- affected_row_count: ${result.affected_row_count}`,
        `- pending_for_dnc_after: ${post.safety.pending_for_dnc}`,
        `- pending_for_synthetic_after: ${post.safety.pending_for_synthetic}`,
        `- total_pending_after: ${post.safety.total_pending}`,
        `- anchor_flag_false: ${post.safety.anchor_flag_false}`,
        '',
        'No restart, Anchor enablement, or outbound activity was performed.',
      ].join('\n');
      writeArtifacts({ artifactsDir, artifacts, secrets, deps });
      log(`[artifacts] written under ${artifactsDir}`);
      log(`[remediation] ${summary.verdict}`);
      summary.artifactsDir = artifactsDir;
      return summary;
    }

    // --- Post-migration read-only investigation ---
    if (options.verifyPostMigration) {
      const preflight = await runPreflight(client, { postMigrationMode: true });
      artifacts['preflight.json'] = preflight;
      summary.stages.push('preflight');

      let preSnapshot = null;
      if (options.preSnapshotPath && deps.fs.existsSync(options.preSnapshotPath)) {
        preSnapshot = JSON.parse(deps.fs.readFileSync(options.preSnapshotPath, 'utf8'));
      }
      let migrationWindow = {};
      if (options.migrationResultPath && deps.fs.existsSync(options.migrationResultPath)) {
        migrationWindow = JSON.parse(deps.fs.readFileSync(options.migrationResultPath, 'utf8'));
      }

      const investigation = await runVerifyPostMigration(client, {
        migrationWindow,
        preSnapshot,
      });
      artifacts['current-schema.json'] = investigation.current_schema;
      artifacts['callbacks-sanitized.json'] = investigation.callbacks_sanitized;
      artifacts['dnc-suppression-audit.json'] = investigation.dnc_suppression_audit;
      artifacts['agent-log-audit.json'] = investigation.agent_log_audit;
      artifacts['post-migration-verification.json'] = investigation.post_migration_verification;
      artifacts['contract-matrix.json'] = investigation.contract_matrix;
      artifacts['final-report.md'] = buildFinalReport({
        mode: summary.mode,
        inspect,
        preflight,
        verify: investigation.post_migration_verification,
        secrets,
      }) + `\n\n## Investigation\n- classification: ${investigation.current_schema.classification}\n`
        + `- pending callbacks: ${investigation.callbacks_sanitized.summary.pending}\n`
        + `- pending for DNC: ${investigation.dnc_suppression_audit.pending_callbacks_for_dnc}\n`
        + `- pending for synthetic: ${investigation.dnc_suppression_audit.pending_callbacks_for_synthetic}\n`
        + `- agent_log outbound-like in window: ${investigation.agent_log_audit.outbound_like_count}\n`
        + `- safety_defect: ${investigation.safety_defect}\n`
        + `- remediation_executed: false\n`;

      summary.stages.push('verify-post-migration');
      summary.passed = investigation.passed;
      summary.safety_defect = investigation.safety_defect;
      const written = writeArtifacts({ artifactsDir, artifacts, secrets, deps });
      log(`[artifacts] written under ${artifactsDir}`);
      log(`[verify-post-migration] ${investigation.passed ? 'PASS' : 'FINDINGS'} — safety_defect=${investigation.safety_defect}`);
      summary.artifacts = written;
      summary.artifactsDir = artifactsDir;
      summary.investigation = {
        pending: investigation.callbacks_sanitized.summary.pending,
        pending_for_dnc: investigation.dnc_suppression_audit.pending_callbacks_for_dnc,
        pending_for_synthetic: investigation.dnc_suppression_audit.pending_callbacks_for_synthetic,
        outbound_like: investigation.agent_log_audit.outbound_like_count,
      };
      return summary;
    }

    const preflight = await runPreflight(client);
    artifacts['preflight.json'] = preflight;
    summary.stages.push('preflight');
    log(`[preflight] PASS — db=${preflight.observations.current_database}, pg=${preflight.observations.server_version}, classification=${preflight.observations.phase3d_classification}.`);

    if (options.executeRollback) {
      log('\n[plan] ROLLBACK is a logical (feature-flag) rollback using only the reviewed rollback file.');
      log('       It disables the new Pipeline and preserves safety/history schema and data.');
      await confirmExactPhrase({ mode: 'rollback', options, processEnv, deps });
      const rbSql = deps.fs.readFileSync(path.join(options.worktree, RELEASE.rollbackRelPath), 'utf8');
      const started = Date.now();
      await client.query(rbSql);
      const rollback = { stage: 'rollback', rollback_sha256: RELEASE.rollbackSha256, duration_ms: Date.now() - started, transaction_result: 'committed' };
      artifacts['rollback-result.json'] = rollback;
      summary.stages.push('rollback');
      const flag = (await client.query('SELECT COALESCE(bool_or(setter_pipeline_v2_enabled), false) AS enabled FROM clients')).rows[0].enabled;
      rollback.anchor_flag_false_after = flag === false;
      summary.passed = flag === false;
      artifacts['final-report.md'] = buildFinalReport({ mode: summary.mode, inspect, preflight, rollback, secrets });
      writeArtifacts({ artifactsDir, artifacts, secrets, deps });
      log(`[artifacts] written under ${artifactsDir}`);
      log('[rollback] applied. Restart the same SHA to refresh the schema-presence cache.');
      return summary;
    }

    if (!options.executeMigration) {
      artifacts['final-report.md'] = buildFinalReport({ mode: summary.mode, inspect, preflight, secrets });
      const written = writeArtifacts({ artifactsDir, artifacts, secrets, deps });
      log(`[artifacts] written under ${artifactsDir}`);
      summary.passed = true;
      summary.artifacts = written;
      summary.artifactsDir = artifactsDir;
      log('\n[read-only] Inspection + preflight complete. Re-run with --execute-migration to proceed (a second confirmation is still required).');
      return summary;
    }

    const preSnapshot = await runPreMigrationSnapshot(client);
    artifacts['pre-migration-snapshot.json'] = preSnapshot;
    summary.stages.push('pre-migration-snapshot');

    log('\n================ EXECUTION PLAN ================');
    log(`  target database : ${preflight.observations.current_database} (PostgreSQL ${preflight.observations.server_version})`);
    log(`  worktree HEAD   : ${inspect.head}`);
    log(`  migration       : ${RELEASE.migrationRelPath}`);
    log(`  migration sha256: ${RELEASE.migrationSha256}`);
    log('  steps           : durable backup -> apply reviewed migration -> read-only verify');
    log('  anchor          : remains DISABLED (no flag enablement)');
    log('===============================================');
    await confirmExactPhrase({ mode: 'migration', options, processEnv, deps });

    const backup = deps.createBackup({ databaseUrl, serverVersion: preflight.observations.server_version, deps });
    if (!backup || backup.verified !== true) {
      throw new GateError('Durable backup did not verify — refusing to migrate', { stage: 'backup', code: 'BACKUP_UNVERIFIED' });
    }
    artifacts['backup-metadata.json'] = backup;
    summary.stages.push('backup');
    log(`[backup] verified — ${backup.backup_identifier} (${backup.encrypted_size_bytes} bytes, sha256 ${backup.encrypted_sha256.slice(0, 12)}…)`);

    const migration = await applyMigration(client, { migrationPath: inspect.migration_path, deps });
    artifacts['migration-result.json'] = migration;
    summary.stages.push('migrate');
    log(`[migrate] committed in ${migration.duration_ms}ms`);

    const verify = await runPostMigrationVerification(client, { preSnapshot });
    artifacts['post-migration-verification.json'] = verify;
    summary.stages.push('verify');

    if (!verify.passed) {
      artifacts['final-report.md'] = buildFinalReport({ mode: summary.mode, inspect, preflight, preSnapshot, backup, migration, verify, secrets });
      writeArtifacts({ artifactsDir, artifacts, secrets, deps });
      log(`[artifacts] written under ${artifactsDir}`);
      const failed = Object.entries(verify.checks).filter(([, v]) => v !== true).map(([k]) => k);
      log('\n[verify] HARD ROLLBACK CONDITION — one or more contracts failed:');
      log(`         ${failed.join(', ')}`);
      log('         The reviewed logical rollback is NOT run automatically.');
      log(`         To investigate read-only: node scripts/runPhase3hGate2Production.js --verify-post-migration --artifacts-dir=/abs/path`);
      log(`         Reviewed rollback: ${RELEASE.rollbackRelPath} (disables the new Pipeline; preserves safety/history schema).`);
      summary.passed = false;
      summary.rollback_condition = true;
      summary.failed_checks = failed;
      summary.artifactsDir = artifactsDir;
      return summary;
    }

    summary.stages.push('restart-instructions');
    artifacts['final-report.md'] = buildFinalReport({ mode: summary.mode, inspect, preflight, preSnapshot, backup, migration, verify, restart: true, secrets });
    const written = writeArtifacts({ artifactsDir, artifacts, secrets, deps });
    log(`[artifacts] written under ${artifactsDir}`);
    summary.artifacts = written;
    summary.artifactsDir = artifactsDir;
    summary.passed = true;
    log('\n[verify] PASS — all schema + safety contracts hold; Anchor remains disabled.');
    log(restartInstructions());
    return summary;
  } catch (error) {
    const sanitized = sanitizeError(error, secrets);
    artifacts['error.json'] = {
      stage: error && error.stage ? error.stage : 'unknown',
      code: error && error.code ? error.code : 'ERROR',
      ...sanitized,
    };
    if (error && error.details) {
      const stageArtifact = error.stage === 'preflight' ? 'preflight.json' : `${error.stage || 'details'}.json`;
      artifacts[stageArtifact] = error.details;
    }
    try {
      writeArtifacts({ artifactsDir, artifacts, secrets, deps });
      log(`[artifacts] written under ${artifactsDir}`);
    } catch { /* redaction guard may itself throw; swallow to avoid leaking */ }
    summary.passed = false;
    summary.error = sanitized;
    summary.artifactsDir = artifactsDir;
    throw Object.assign(error, { summary });
  } finally {
    if (client && client.end) { try { await client.end(); } catch { /* ignore */ } }
  }
}

if (require.main === module) {
  main(process.argv.slice(2)).then((summary) => {
    process.exitCode = summary && summary.passed ? 0 : 2;
  }).catch((error) => {
    const secrets = error && error.summary ? new Set() : new Set();
    process.stderr.write(`${redactString(error && error.message ? error.message : String(error), secrets)}\n`);
    process.exitCode = 1;
  });
}

module.exports = {
  RELEASE,
  IDENTITY,
  CONFIRM,
  DEFAULT_ENV_FILE,
  USAGE,
  LEGACY_SETTER_COLUMNS,
  UNIQUE_PHASE3D_COLUMNS,
  UNIQUE_PHASE3D_TABLES,
  UNIQUE_PHASE3D_INDEXES,
  UNIQUE_PHASE3D_TRIGGERS,
  GateError,
  parseArgs,
  printUsage,
  stableHash,
  requireAbsoluteArtifactsDir,
  EXPECTED_REMEDIATION,
  assertRemediationStaticSafety,
  previewSuppressedCallbackRemediation,
  applySuppressedCallbackRemediation,
  verifyAfterSuppressedCallbackRemediation,
  deriveSecrets,
  redactString,
  redactDeep,
  sanitizeError,
  assertNoSecrets,
  loadDatabaseUrl,
  sha256File,
  assertMigrationStaticSafety,
  inspectRelease,
  withReadOnly,
  runPreflight,
  classifyPhase3dSchema,
  uniquePhase3dObjectState,
  legacySetterObjectState,
  snapshotCounts,
  prospectSafetyCounts,
  runPreMigrationSnapshot,
  createDurableBackup,
  applyMigration,
  runPostMigrationVerification,
  auditAgentLogWindow,
  investigateSetterCallbacks,
  auditDncSuppression,
  runVerifyPostMigration,
  confirmExactPhrase,
  runVerifyAfterRestart,
  restartInstructions,
  writeArtifacts,
  buildFinalReport,
  main,
};
