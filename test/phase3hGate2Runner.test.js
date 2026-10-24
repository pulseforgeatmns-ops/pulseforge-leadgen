'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');

const runner = require('../scripts/runPhase3hGate2Production');

const WORKTREE = path.join(__dirname, '..');
const REAL_MIGRATION = path.join(WORKTREE, runner.RELEASE.migrationRelPath);
const REAL_ROLLBACK = path.join(WORKTREE, runner.RELEASE.rollbackRelPath);

// GateError codes are the stable contract; Node matches regex validators against
// the message, so assert on `.code` explicitly.
const isCode = code => err => err.code === code;

// --------------------------------------------------------------------------
// Fixtures / test doubles
// --------------------------------------------------------------------------

const PHASE3D_COLUMNS = {
  clients: ['setter_pipeline_v2_enabled', 'setter_pipeline_v2_configured_at', 'setter_review_sample_percent'],
  prospects: ['is_synthetic', 'synthetic_label', 'callback_completed_at', 'assigned_setter_id'],
  call_dispositions: ['structured_notes', 'activity_result', 'next_action', 'suppression_state', 'lifecycle_result', 'is_synthetic', 'review_required', 'review_status', 'idempotency_key'],
};
const PHASE3D_INDEXES = ['call_dispositions_idempotency_idx', 'setter_callbacks_one_pending_idx', 'setter_callbacks_due_idx'];
const PHASE3D_TRIGGERS = ['prospects_synthetic_suppression', 'prospects_suppression_cleanup'];

function baseColumns() {
  return [
    { table_name: 'prospects', column_name: 'do_not_contact' },
    { table_name: 'prospects', column_name: 'callback_at' },
    { table_name: 'prospects', column_name: 'status' },
  ];
}

function stateConfig({ phase3d = false, partial = false } = {}) {
  const tables = ['clients', 'prospects', 'call_dispositions', 'touchpoints', 'setter_follow_up_drafts'];
  const columns = baseColumns();
  const indexes = [];
  const triggers = [];
  const constraints = [];
  if (phase3d || partial) {
    columns.push(...Object.entries(PHASE3D_COLUMNS).flatMap(([t, cols]) => cols.map(c => ({ table_name: t, column_name: c }))));
  }
  if (phase3d) {
    tables.push('setter_callbacks');
    columns.push(...['client_id', 'prospect_id', 'status', 'due_at', 'is_synthetic'].map(column_name => ({ table_name: 'setter_callbacks', column_name })));
    indexes.push(...PHASE3D_INDEXES.map(indexname => ({ tablename: 'x', indexname, indexdef: indexdefFor(indexname) })));
    triggers.push(...PHASE3D_TRIGGERS.map(trigger_name => ({ table_name: 'prospects', trigger_name })));
    constraints.push({ table_name: 'setter_callbacks', constraint_name: 'setter_callbacks_status_check', constraint_type: 'CHECK' });
  }
  return { tables, columns, indexes, triggers, constraints };
}

function indexdefFor(name) {
  if (name === 'call_dispositions_idempotency_idx') return 'CREATE UNIQUE INDEX call_dispositions_idempotency_idx ON public.call_dispositions (client_id, idempotency_key) WHERE idempotency_key IS NOT NULL';
  if (name === 'setter_callbacks_one_pending_idx') return "CREATE UNIQUE INDEX setter_callbacks_one_pending_idx ON public.setter_callbacks (client_id, prospect_id) WHERE status = 'pending'";
  return `CREATE INDEX ${name} ON public.setter_callbacks (client_id, status, due_at)`;
}

// A mock pg client driven by a config object. Records every SQL executed.
function makeDb(config = {}) {
  const cfg = {
    database: 'railway',
    serverVersion: '18.4',
    anchorRows: 1,
    flagEnabled: false,
    pendingCallbacks: 0,
    counts: {},
    statusDistribution: [],
    ...config,
  };
  const st = cfg.state || stateConfig();
  const queries = [];
  const rows = r => ({ rows: r, rowCount: r.length });
  async function query(sql, params) {
    queries.push({ sql, params });
    const s = String(sql);
    if (/^\s*(BEGIN|SET TRANSACTION READ ONLY|ROLLBACK|COMMIT)/i.test(s)) return rows([]);
    if (/current_database\(\)/.test(s)) return rows([{ db: cfg.database }]);
    if (/SHOW server_version/.test(s)) return rows([{ server_version: cfg.serverVersion }]);
    if (/information_schema\.tables/.test(s)) return rows(st.tables.map(table_name => ({ table_name })));
    if (/information_schema\.columns/.test(s)) return rows(st.columns);
    if (/pg_indexes/.test(s) && /indexname='call_dispositions_idempotency_idx'/.test(s)) {
      const found = st.indexes.find(i => i.indexname === 'call_dispositions_idempotency_idx');
      return rows(found ? [{ indexdef: found.indexdef }] : []);
    }
    if (/pg_indexes/.test(s) && /indexname='setter_callbacks_one_pending_idx'/.test(s)) {
      const found = st.indexes.find(i => i.indexname === 'setter_callbacks_one_pending_idx');
      return rows(found ? [{ indexdef: found.indexdef }] : []);
    }
    if (/pg_indexes/.test(s)) return rows(st.indexes);
    if (/table_constraints/.test(s)) return rows(st.constraints);
    if (/pg_trigger/.test(s)) return rows(st.triggers);
    if (/FROM clients WHERE id = \$1/.test(s)) return { rows: cfg.anchorRows ? [{ id: params[0] }] : [], rowCount: cfg.anchorRows };
    if (/bool_or\(setter_pipeline_v2_enabled\)/.test(s)) return rows([{ enabled: cfg.flagEnabled }]);
    if (/setter_pipeline_v2_enabled FROM clients WHERE id/.test(s)) return rows([{ setter_pipeline_v2_enabled: cfg.flagEnabled }]);
    if (/FROM setter_callbacks WHERE status = 'pending'/.test(s)) return rows([{ count: cfg.pendingCallbacks }]);
    if (/GROUP BY status/.test(s)) return rows(cfg.statusDistribution);
    // Generic counters keyed by distinctive fragments.
    const c = cfg.counts;
    if (/is_synthetic AND NOT do_not_contact/.test(s)) return rows([{ count: c.syntheticNotDnc ?? 0 }]);
    if (/WHERE sc\.client_id <> p\.client_id/.test(s)) return rows([{ count: c.crossTenant ?? 0 }]);
    if (/status = 'pending' AND \(p\.do_not_contact OR p\.is_synthetic\)/.test(s)) return rows([{ count: c.pendingSuppressed ?? 0 }]);
    if (/FROM prospects WHERE is_synthetic\b/.test(s)) return rows([{ count: c.synthetic ?? 0 }]);
    if (/FROM prospects WHERE do_not_contact\b/.test(s)) return rows([{ count: c.dnc ?? 0 }]);
    if (/FROM prospects WHERE callback_at IS NOT NULL/.test(s)) return rows([{ count: c.callbackAt ?? 0 }]);
    if (/count\(\*\)::int AS count FROM prospects\b/.test(s)) return rows([{ count: c.prospects ?? 0 }]);
    if (/FROM setter_callbacks\b/.test(s)) return rows([{ count: c.setterCallbacks ?? 0 }]);
    if (/FROM call_dispositions\b/.test(s)) return rows([{ count: c.callDispositions ?? 0 }]);
    if (/FROM touchpoints\b/.test(s)) return rows([{ count: c.touchpoints ?? 0 }]);
    if (/FROM activity_log\b/.test(s)) return rows([{ count: c.activityLog ?? 0 }]);
    if (/FROM agent_log\b/.test(s)) return rows([{ count: c.agentLog ?? 0 }]);
    if (/FROM clients\b/.test(s)) return rows([{ count: c.clients ?? 1 }]);
    return rows([{ count: 0 }]);
  }
  return { query, queries, connect: async () => {}, end: async () => {} };
}

// fs double that serves real reviewed files but lets specific paths be overridden.
function makeFs(overrides = {}) {
  return {
    existsSync: p => (p in overrides ? overrides[p].exists !== false : fs.existsSync(p)),
    readFileSync: (p, enc) => (p in overrides && overrides[p].content != null ? overrides[p].content : fs.readFileSync(p, enc)),
  };
}

function makeGit({ head = runner.RELEASE.deployedSha, status = [] } = {}) {
  return { revParse: () => head, status: () => status };
}

// --------------------------------------------------------------------------
// Stage 1 — inspection guards
// --------------------------------------------------------------------------

test('inspect passes for correct SHA, clean tree, and matching hashes', () => {
  const report = runner.inspectRelease({
    worktree: WORKTREE,
    expectedHead: runner.RELEASE.deployedSha,
    deps: { fs: makeFs(), git: makeGit() },
  });
  assert.equal(report.passed, true);
  assert.equal(report.checks.head_matches, true);
  assert.equal(report.checks.migration_hash_matches, true);
  assert.equal(report.checks.rollback_hash_matches, true);
  assert.equal(report.checks.migration_static_safety.single_transaction, true);
  assert.equal(report.checks.migration_static_safety.no_anchor_enablement, true);
});

test('inspect rejects a wrong Git SHA', () => {
  assert.throws(() => runner.inspectRelease({
    worktree: WORKTREE,
    expectedHead: runner.RELEASE.deployedSha,
    deps: { fs: makeFs(), git: makeGit({ head: 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef' }) },
  }), /HEAD_MISMATCH|does not match required release/);
});

test('inspect rejects a dirty worktree (tracked change)', () => {
  assert.throws(() => runner.inspectRelease({
    worktree: WORKTREE,
    expectedHead: runner.RELEASE.deployedSha,
    deps: { fs: makeFs(), git: makeGit({ status: [{ code: 'M', path: 'leadgen.js' }] }) },
  }), /DIRTY_WORKTREE|uncommitted/);
});

test('inspect ignores untracked files (overlaid script + artifacts)', () => {
  const report = runner.inspectRelease({
    worktree: WORKTREE,
    expectedHead: runner.RELEASE.deployedSha,
    deps: { fs: makeFs(), git: makeGit({ status: [{ code: '??', path: 'scripts/runPhase3hGate2Production.js' }] }) },
  });
  assert.equal(report.passed, true);
});

test('inspect rejects a migration hash mismatch', () => {
  const overrides = { [REAL_MIGRATION]: { content: 'BEGIN;\nSELECT 1;\nCOMMIT;\n' } };
  assert.throws(() => runner.inspectRelease({
    worktree: WORKTREE,
    expectedHead: runner.RELEASE.deployedSha,
    deps: { fs: makeFs(overrides), git: makeGit() },
  }), isCode('MIGRATION_HASH_MISMATCH'));
});

test('inspect rejects a rollback hash mismatch', () => {
  const overrides = { [REAL_ROLLBACK]: { content: 'BEGIN;\nSELECT 2;\nCOMMIT;\n' } };
  assert.throws(() => runner.inspectRelease({
    worktree: WORKTREE,
    expectedHead: runner.RELEASE.deployedSha,
    deps: { fs: makeFs(overrides), git: makeGit() },
  }), isCode('ROLLBACK_HASH_MISMATCH'));
});

test('inspect reports the embedded backup procedure available even without a standalone script', () => {
  // The deployed release ships no standalone backup script; the procedure is
  // embedded in the runner, so inspection must still pass and record it.
  const report = runner.inspectRelease({
    worktree: WORKTREE,
    expectedHead: runner.RELEASE.deployedSha,
    deps: { fs: makeFs(), git: makeGit() },
  });
  assert.equal(report.checks.backup_procedure_available, true);
  assert.equal(report.backup.embedded_procedure, true);
});

test('static safety rejects a migration that enables the Anchor flag', () => {
  assert.throws(() => runner.assertMigrationStaticSafety(
    "BEGIN;\nALTER TABLE clients ADD COLUMN setter_pipeline_v2_enabled BOOLEAN NOT NULL DEFAULT false;\nUPDATE clients SET setter_pipeline_v2_enabled = true;\nCOMMIT;",
  ), isCode('ANCHOR_ENABLEMENT'));
});

test('static safety rejects a non-single-transaction migration', () => {
  assert.throws(() => runner.assertMigrationStaticSafety(
    'ALTER TABLE clients ADD COLUMN setter_pipeline_v2_enabled BOOLEAN NOT NULL DEFAULT false;',
  ), isCode('NOT_SINGLE_TXN'));
});

// --------------------------------------------------------------------------
// Stage 2 — preflight guards
// --------------------------------------------------------------------------

test('preflight passes on a clean pre-migration production database', async () => {
  const db = makeDb({ state: stateConfig(), database: 'railway', serverVersion: '18.4' });
  const report = await runner.runPreflight(db);
  assert.equal(report.passed, true);
  assert.equal(report.checks.database_identity, true);
  assert.equal(report.checks.feature_flag_not_enabled, true);
  assert.equal(report.checks.no_active_setter_ops, true);
  assert.equal(report.checks.no_partial_migration, true);
  assert.equal(report.phase3d_already_applied, false);
});

test('preflight fails closed on the wrong database name', async () => {
  const db = makeDb({ database: 'postgres' });
  await assert.rejects(runner.runPreflight(db), /DB_IDENTITY|expected railway/);
});

test('preflight fails closed on the wrong PostgreSQL major', async () => {
  const db = makeDb({ serverVersion: '17.5' });
  await assert.rejects(runner.runPreflight(db), isCode('PG_MAJOR'));
});

test('preflight fails closed when the Anchor flag is already enabled', async () => {
  const db = makeDb({ state: stateConfig({ phase3d: true }), flagEnabled: true });
  await assert.rejects(runner.runPreflight(db), isCode('FLAG_ENABLED'));
});

test('preflight fails closed on ambiguous / partial migration state', async () => {
  const db = makeDb({ state: stateConfig({ partial: true }) });
  await assert.rejects(runner.runPreflight(db), isCode('PARTIAL_MIGRATION'));
});

test('production pre-Phase-3D shape with legacy assigned_setter_id is CLEAN_PRE_MIGRATION', async () => {
  // Exact Gate 1 documented production shape: base human-setter columns exist,
  // including assigned_setter_id, but NO unique Phase 3D quality-control objects.
  // Shape for classifyPhase3dSchema (information_schema-like rows):
  const productionPreRows = {
    tables: ['clients', 'prospects', 'call_dispositions', 'touchpoints', 'activity_log', 'setter_follow_up_drafts']
      .map(table_name => ({ table_name })),
    columns: [
      { table_name: 'prospects', column_name: 'do_not_contact' },
      { table_name: 'prospects', column_name: 'callback_at' },
      { table_name: 'prospects', column_name: 'status' },
      { table_name: 'prospects', column_name: 'setter_status' },
      { table_name: 'prospects', column_name: 'setter_visible' },
      { table_name: 'prospects', column_name: 'assigned_setter_id' },
      { table_name: 'call_dispositions', column_name: 'details' },
    ],
    indexes: [],
    triggers: [],
    constraints: [],
  };
  const classified = runner.classifyPhase3dSchema(productionPreRows);
  assert.equal(classified.classification, 'CLEAN_PRE_MIGRATION');
  assert.deepEqual(classified.unique_phase3d_present, []);
  assert.equal(classified.legacy_setter.columns.prospects.assigned_setter_id, true);
  assert.equal(classified.legacy_setter.call_dispositions_details, true);

  // makeDb stateConfig shape uses string table names (wrapper maps them).
  const productionPreDb = {
    tables: ['clients', 'prospects', 'call_dispositions', 'touchpoints', 'activity_log', 'setter_follow_up_drafts'],
    columns: productionPreRows.columns,
    indexes: [],
    triggers: [],
    constraints: [],
  };
  const db = makeDb({ state: productionPreDb, database: 'railway', serverVersion: '18.4' });
  const report = await runner.runPreflight(db);
  assert.equal(report.passed, true);
  assert.equal(report.observations.phase3d_classification, 'CLEAN_PRE_MIGRATION');
  assert.equal(report.phase3d_already_applied, false);
});

test('classifyPhase3dSchema marks complete inventory as COMPLETE_PHASE3D', () => {
  // Convert stateConfig strings into information_schema-like row objects.
  const raw = stateConfig({ phase3d: true });
  const shaped = {
    tables: raw.tables.map(table_name => ({ table_name })),
    columns: raw.columns,
    indexes: raw.indexes,
    triggers: raw.triggers,
    constraints: raw.constraints,
  };
  const classified = runner.classifyPhase3dSchema(shaped);
  assert.equal(classified.classification, 'COMPLETE_PHASE3D');
});

test('classifyPhase3dSchema marks a single unique Phase 3D column as PARTIAL_PHASE3D', () => {
  const partial = {
    tables: ['clients', 'prospects', 'call_dispositions'].map(table_name => ({ table_name })),
    columns: [
      { table_name: 'prospects', column_name: 'assigned_setter_id' },
      { table_name: 'prospects', column_name: 'is_synthetic' }, // unique Phase 3D marker alone
    ],
    indexes: [],
    triggers: [],
    constraints: [],
  };
  const classified = runner.classifyPhase3dSchema(partial);
  assert.equal(classified.classification, 'PARTIAL_PHASE3D');
  assert.ok(classified.unique_phase3d_present.includes('column:prospects.is_synthetic'));
});

test('snapshot helper never overlaps queries on a single client', async () => {
  const { snapshot } = require('../scripts/runSetterReleaseRehearsal');
  let inFlight = 0;
  let maxInFlight = 0;
  const db = {
    async query(sql) {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      if (inFlight > 1) throw new Error(`overlapping client.query detected (inFlight=${inFlight}) for: ${sql.slice(0, 60)}`);
      await new Promise(resolve => setImmediate(resolve));
      inFlight -= 1;
      if (/information_schema\.tables/.test(sql)) return { rows: [] };
      if (/information_schema\.columns/.test(sql)) return { rows: [] };
      if (/pg_indexes/.test(sql)) return { rows: [] };
      if (/table_constraints/.test(sql)) return { rows: [] };
      if (/pg_trigger/.test(sql)) return { rows: [] };
      return { rows: [] };
    },
  };
  await snapshot(db);
  assert.equal(maxInFlight, 1, 'snapshot must never run concurrent queries on one client');
});

test('preflight fails closed when Anchor cannot be uniquely identified', async () => {
  const db = makeDb({ anchorRows: 0 });
  await assert.rejects(runner.runPreflight(db), isCode('ANCHOR_IDENTITY'));
});

test('preflight treats pending callbacks as inert when Phase 3D is complete and flag is off', async () => {
  const db = makeDb({ state: stateConfig({ phase3d: true }), pendingCallbacks: 10, flagEnabled: false });
  const report = await runner.runPreflight(db);
  assert.equal(report.passed, true);
  assert.equal(report.observations.pending_setter_callbacks, 10);
  assert.equal(report.observations.pending_callbacks_classification, 'inert_queued_backfill');
  assert.equal(report.checks.no_active_setter_ops, true);
});

test('preflight postMigrationMode does not reject merely because setter_callbacks has rows', async () => {
  const db = makeDb({ state: stateConfig({ phase3d: true }), pendingCallbacks: 10, flagEnabled: false });
  const report = await runner.runPreflight(db, { postMigrationMode: true });
  assert.equal(report.passed, true);
  assert.equal(report.phase3d_already_applied, true);
});

// --------------------------------------------------------------------------
// Stage 3 — pre-migration snapshot never queries absent relations
// --------------------------------------------------------------------------

test('pre-migration snapshot records missing objects without querying them', async () => {
  const db = makeDb({ state: stateConfig(), counts: { prospects: 5, callbackAt: 2 } });
  const snap = await runner.runPreMigrationSnapshot(db);
  assert.deepEqual(snap.table_counts.setter_callbacks, { exists: false, row_count: null });
  assert.deepEqual(snap.prospect_safety.synthetic_prospects, { exists: false, row_count: null });
  assert.equal(db.queries.some(q => /FROM setter_callbacks/.test(q.sql)), false);
  assert.equal(db.queries.some(q => /FROM prospects WHERE is_synthetic/.test(q.sql)), false);
});

// --------------------------------------------------------------------------
// Stage 4 — backup failure stops the run
// --------------------------------------------------------------------------

test('durable backup surfaces a tool failure as a GateError', () => {
  const failingRun = (command) => {
    if (/pg_dump/.test(command)) return { status: 1, stdout: '', stderr: 'redacted failure' };
    return { status: 0, stdout: '' };
  };
  const fakeFs = { mkdirSync() {}, chmodSync() {}, statSync: () => ({ size: 1 }), readFileSync: () => Buffer.from('x'), rmSync() {}, existsSync: () => false };
  assert.throws(() => runner.createDurableBackup({
    databaseUrl: 'postgres://u:p@h:5432/railway',
    serverVersion: '18.4',
    deps: { fs: fakeFs, run: failingRun },
  }), isCode('BACKUP_TOOL_FAILED'));
});

// --------------------------------------------------------------------------
// Confirmation gate
// --------------------------------------------------------------------------

test('confirmation phrase mismatch aborts before any write', async () => {
  await assert.rejects(runner.confirmExactPhrase({
    mode: 'migration',
    options: { nonInteractive: false },
    processEnv: {},
    deps: { promptLine: async () => 'apply phase3d' },
  }), isCode('PHRASE_MISMATCH'));
});

test('exact confirmation phrase is accepted', async () => {
  const res = await runner.confirmExactPhrase({
    mode: 'migration',
    options: { nonInteractive: false },
    processEnv: {},
    deps: { promptLine: async () => runner.CONFIRM.migrationPhrase },
  });
  assert.equal(res.confirmed, true);
});

test('non-interactive migration without env approval is rejected', async () => {
  await assert.rejects(runner.confirmExactPhrase({
    mode: 'migration',
    options: { nonInteractive: true },
    processEnv: {},
    deps: {},
  }), isCode('NONINTERACTIVE_NOT_APPROVED'));
});

test('non-interactive migration requires the exact env acknowledgment value', async () => {
  const res = await runner.confirmExactPhrase({
    mode: 'migration',
    options: { nonInteractive: true },
    processEnv: { [runner.CONFIRM.migrationEnvKey]: runner.CONFIRM.migrationEnvValue },
    deps: {},
  });
  assert.equal(res.confirmed, true);
});

test('rollback requires a separate authorization phrase (migration phrase is not accepted)', async () => {
  await assert.rejects(runner.confirmExactPhrase({
    mode: 'rollback',
    options: { nonInteractive: false },
    processEnv: {},
    deps: { promptLine: async () => runner.CONFIRM.migrationPhrase },
  }), isCode('PHRASE_MISMATCH'));
  const res = await runner.confirmExactPhrase({
    mode: 'rollback',
    options: { nonInteractive: true },
    processEnv: { [runner.CONFIRM.rollbackEnvKey]: runner.CONFIRM.rollbackEnvValue },
    deps: {},
  });
  assert.equal(res.confirmed, true);
});

// --------------------------------------------------------------------------
// Credential loading + redaction
// --------------------------------------------------------------------------

test('loadDatabaseUrl rejects a pre-exported DATABASE_URL from an unexpected source', () => {
  assert.throws(() => runner.loadDatabaseUrl({
    envFilePath: '/nonexistent/.env',
    processEnv: { DATABASE_URL: 'postgres://u:p@h:5432/railway' },
    allowPreexisting: false,
    deps: { fs: makeFs() },
  }), isCode('PREEXISTING_DATABASE_URL'));
});

test('loadDatabaseUrl reads the URL from the env file into memory', () => {
  const envPath = '/tmp/phase3h-test.env';
  const url = runner.loadDatabaseUrl({
    envFilePath: envPath,
    processEnv: {},
    allowPreexisting: false,
    deps: { fs: {
      existsSync: () => true,
      readFileSync: () => 'FOO=bar\nDATABASE_URL="postgres://user:secretpw@proxy.example.net:43478/railway"\n',
    } },
  });
  assert.match(url, /^postgres:\/\//);
});

test('redaction removes credentials and the guard rejects any leak', () => {
  const url = 'postgres://user:secretpw@proxy.example.net:43478/railway';
  const secrets = runner.deriveSecrets(url);
  const report = { note: `connection to ${url} failed`, error: { message: 'password "secretpw" rejected' } };
  const redacted = runner.redactDeep(report, secrets);
  const serialized = JSON.stringify(redacted);
  assert.equal(serialized.includes('secretpw'), false);
  assert.equal(serialized.includes('proxy.example.net'), false);
  assert.equal(runner.assertNoSecrets(redacted, secrets), true);
  assert.throws(() => runner.assertNoSecrets({ raw: url }, secrets), isCode('SECRET_LEAK'));
});

test('sanitizeError never returns raw connection details', () => {
  const url = 'postgres://user:secretpw@h:5432/railway';
  const secrets = runner.deriveSecrets(url);
  const out = runner.sanitizeError(new Error(`could not connect to ${url}`), secrets);
  assert.equal(out.message.includes('secretpw'), false);
  assert.equal(/postgres:\/\//.test(out.message), false);
});

// --------------------------------------------------------------------------
// Stage 6 — post-migration verification
// --------------------------------------------------------------------------

function goodPreSnapshot() {
  return {
    table_counts: {
      prospects: { exists: true, row_count: 10 },
      activity_log: { exists: false, row_count: null },
      agent_log: { exists: true, row_count: 4 },
      touchpoints: { exists: true, row_count: 7 },
    },
    prospect_safety: {
      synthetic_prospects: { exists: false, row_count: null },
      prospects_with_callback_at: { exists: true, row_count: 3 },
      status_distribution: { new: 6, warm: 4 },
    },
  };
}

test('post-migration verification passes and confirms the Anchor flag is still false', async () => {
  const db = makeDb({
    state: stateConfig({ phase3d: true }),
    flagEnabled: false,
    counts: {
      prospects: 10, synthetic: 0, syntheticNotDnc: 0, pendingSuppressed: 0,
      crossTenant: 0, setterCallbacks: 3, agentLog: 4, touchpoints: 7,
    },
    statusDistribution: [{ status: 'new', count: 6 }, { status: 'warm', count: 4 }],
  });
  const report = await runner.runPostMigrationVerification(db, { preSnapshot: goodPreSnapshot() });
  assert.equal(report.checks.all_phase3d_objects_present, true);
  assert.equal(report.checks.anchor_flag_still_false, true);
  assert.equal(report.checks.no_unexpected_synthetic, true);
  assert.equal(report.checks.backfill_matches_callback_at, true);
  assert.equal(report.checks.no_unexpected_lifecycle_change, true);
  assert.equal(report.passed, true);
});

test('post-migration verification fails if the feature flag became true', async () => {
  const db = makeDb({
    state: stateConfig({ phase3d: true }),
    flagEnabled: true,
    counts: { prospects: 10, setterCallbacks: 3, agentLog: 4, touchpoints: 7, callbackAt: 3 },
    statusDistribution: [{ status: 'new', count: 6 }, { status: 'warm', count: 4 }],
  });
  const report = await runner.runPostMigrationVerification(db, { preSnapshot: goodPreSnapshot() });
  assert.equal(report.checks.anchor_flag_still_false, false);
  assert.equal(report.passed, false);
});

// --------------------------------------------------------------------------
// Read-only transaction wrapper
// --------------------------------------------------------------------------

test('withReadOnly wraps work in a read-only transaction and rolls back', async () => {
  const db = makeDb();
  await runner.withReadOnly(db, async () => db.query('SELECT 1'));
  const sqls = db.queries.map(q => q.sql);
  assert.ok(sqls.some(s => /^BEGIN/i.test(s)));
  assert.ok(sqls.some(s => /SET TRANSACTION READ ONLY/i.test(s)));
  assert.ok(sqls.some(s => /^ROLLBACK/i.test(s)));
});

// --------------------------------------------------------------------------
// Arg parsing / restart boundary
// --------------------------------------------------------------------------

test('parseArgs keeps read-only default and exposes explicit execution flags', () => {
  assert.equal(runner.parseArgs([]).executeMigration, false);
  const a = runner.parseArgs(['--execute-migration', '--non-interactive', '--worktree=/tmp/x']);
  assert.equal(a.executeMigration, true);
  assert.equal(a.nonInteractive, true);
  assert.equal(a.worktree, '/tmp/x');
  assert.equal(runner.parseArgs(['--execute-rollback']).executeRollback, true);
});

test('restart instructions never auto-restart and pin the deployed SHA', () => {
  const text = runner.restartInstructions();
  assert.match(text, /2862d2ee1e5662db42fa111baaaa0faf248bf5d3/);
  assert.match(text, /Do NOT deploy a different SHA/);
});

test('--help prints usage and performs zero DB / credential work', async () => {
  const lines = [];
  const summary = await runner.main(['--help'], {
    log: (msg) => lines.push(String(msg)),
    createClient: () => { throw new Error('createClient must not be called for --help'); },
    fs: {
      existsSync: () => { throw new Error('fs must not be touched for --help'); },
      readFileSync: () => { throw new Error('fs must not be touched for --help'); },
    },
    git: {
      revParse: () => { throw new Error('git must not be touched for --help'); },
      status: () => { throw new Error('git must not be touched for --help'); },
    },
  });
  assert.equal(summary.mode, 'help');
  assert.equal(summary.passed, true);
  assert.ok(lines.join('\n').includes('--verify-post-migration'));
  assert.ok(lines.join('\n').includes('--artifacts-dir'));
});

test('requireAbsoluteArtifactsDir rejects missing and relative paths', () => {
  assert.throws(() => runner.requireAbsoluteArtifactsDir(null), isCode('ARTIFACTS_DIR_REQUIRED'));
  assert.throws(() => runner.requireAbsoluteArtifactsDir('relative/path'), isCode('ARTIFACTS_DIR_NOT_ABSOLUTE'));
  assert.equal(runner.requireAbsoluteArtifactsDir('/tmp/abs'), '/tmp/abs');
});

test('auditDncSuppression flags pending callbacks for DNC prospects as a safety defect', async () => {
  const st = stateConfig({ phase3d: true });
  // makeDb returns string tables; convert for hasTable via investigate helpers using db.query.
  const db = makeDb({
    state: st,
    counts: {},
  });
  // Override queries for the DNC audit SQL shapes.
  const orig = db.query.bind(db);
  db.query = async (sql, params) => {
    if (/do_not_contact = true/.test(sql) && /setter_callbacks/.test(sql)) return { rows: [{ count: 1 }], rowCount: 1 };
    if (/is_synthetic = true/.test(sql) && /setter_callbacks/.test(sql)) return { rows: [{ count: 0 }], rowCount: 1 };
    return orig(sql, params);
  };
  // Build information_schema-like state for hasTable
  const shaped = {
    tables: st.tables.map(table_name => ({ table_name })),
    columns: st.columns,
    indexes: st.indexes,
    triggers: st.triggers,
    constraints: st.constraints,
  };
  const audit = await runner.auditDncSuppression(db, shaped);
  assert.equal(audit.pending_callbacks_for_dnc, 1);
  assert.equal(audit.safety_defect, true);
  assert.match(audit.root_cause, /backfill_insert_bypasses/);
  assert.equal(audit.remediation_executed, false);
});

test('agent_log global delta is soft; outbound-like window events are hard', async () => {
  const pre = goodPreSnapshot();
  pre.table_counts.agent_log = { exists: true, row_count: 51295 };
  pre.captured_at = '2026-07-19T21:51:01.136Z';
  const st = stateConfig({ phase3d: true });
  st.tables.push('agent_log');
  st.columns.push(
    { table_name: 'agent_log', column_name: 'created_at' },
    { table_name: 'agent_log', column_name: 'agent_name' },
    { table_name: 'agent_log', column_name: 'action' },
  );
  const db = makeDb({
    state: st,
    flagEnabled: false,
    counts: {
      prospects: 10, synthetic: 0, syntheticNotDnc: 0, pendingSuppressed: 0,
      crossTenant: 0, setterCallbacks: 3, agentLog: 51296, touchpoints: 7,
    },
    statusDistribution: [{ status: 'new', count: 6 }, { status: 'warm', count: 4 }],
  });
  const orig = db.query.bind(db);
  db.query = async (sql, params) => {
    if (/FROM agent_log/.test(sql) && /created_at/.test(sql)) {
      return {
        rows: [{ event_at: '2026-07-19T21:52:00Z', agent_name: 'healthcheck', action: 'tick', status: 'ok' }],
        rowCount: 1,
      };
    }
    return orig(sql, params);
  };
  const report = await runner.runPostMigrationVerification(db, { preSnapshot: pre });
  assert.equal(report.checks.agent_log_unchanged, false); // soft signal: delta exists
  assert.equal(report.checks.agent_log_no_migration_attributable_outbound, true);
  assert.equal(report.safety.agent_log_delta, 1);
  assert.equal(report.passed, true); // hard pass ignores soft unchanged
});

test('stableHash is deterministic and non-reversible PII stand-in', () => {
  assert.equal(runner.stableHash('abc'), runner.stableHash('abc'));
  assert.notEqual(runner.stableHash('abc'), 'abc');
  assert.equal(runner.stableHash('abc').length, 16);
});

test('remediation SQL passes static safety and is single-transaction', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const sql = fs.readFileSync(path.join(__dirname, '../scripts/remediation/2026-07-19-cancel-suppressed-setter-callbacks.sql'), 'utf8');
  const findings = runner.assertRemediationStaticSafety(sql);
  assert.equal(findings.single_transaction, true);
  assert.equal(findings.only_updates_setter_callbacks, true);
  assert.equal(findings.no_delete, true);
  assert.equal(findings.fail_closed_rowcount, true);
});

test('remediation confirmation rejects migration phrase', async () => {
  await assert.rejects(runner.confirmExactPhrase({
    mode: 'remediation',
    options: { nonInteractive: false },
    processEnv: {},
    deps: { promptLine: async () => runner.CONFIRM.migrationPhrase },
  }), isCode('PHRASE_MISMATCH'));
});

test('remediation non-interactive requires dedicated env approval', async () => {
  await assert.rejects(runner.confirmExactPhrase({
    mode: 'remediation',
    options: { nonInteractive: true },
    processEnv: {},
    deps: {},
  }), isCode('NONINTERACTIVE_NOT_APPROVED'));
  const ok = await runner.confirmExactPhrase({
    mode: 'remediation',
    options: { nonInteractive: true },
    processEnv: { [runner.CONFIRM.remediationEnvKey]: runner.CONFIRM.remediationEnvValue },
    deps: {},
  });
  assert.equal(ok.confirmed, true);
});

test('remediation preview fails closed on count mismatch', async () => {
  const st = stateConfig({ phase3d: true });
  const db = makeDb({ state: st, flagEnabled: false });
  const orig = db.query.bind(db);
  db.query = async (sql, params) => {
    if (/^\s*(BEGIN|SET TRANSACTION READ ONLY|ROLLBACK|COMMIT)/i.test(sql)) return { rows: [], rowCount: 0 };
    if (/pending' AND \(p\.do_not_contact = true OR p\.is_synthetic = true\)/.test(sql) && /count/.test(sql)) {
      return { rows: [{ count: 2 }], rowCount: 1 }; // wrong — expect 1
    }
    if (/is_synthetic = true/.test(sql) && /count/.test(sql)) return { rows: [{ count: 0 }], rowCount: 1 };
    if (/do_not_contact = true/.test(sql) && /count/.test(sql) && !/is_synthetic/.test(sql)) return { rows: [{ count: 2 }], rowCount: 1 };
    if (/FROM setter_callbacks WHERE status = 'pending'/.test(sql)) return { rows: [{ count: 10 }], rowCount: 1 };
    if (/to_regclass\('public\.setter_follow_up_drafts'\)/.test(sql)) return { rows: [{ present: false }], rowCount: 1 };
    if (/bool_or\(setter_pipeline_v2_enabled\)/.test(sql)) return { rows: [{ enabled: false }], rowCount: 1 };
    if (/LIMIT 5/.test(sql)) return { rows: [{ id: 'x', client_id: 10, prospect_id: 'y', status: 'pending', do_not_contact: true, is_synthetic: false }], rowCount: 1 };
    return orig(sql, params);
  };
  await assert.rejects(runner.previewSuppressedCallbackRemediation(db), isCode('PREVIEW_COUNT_MISMATCH'));
});
