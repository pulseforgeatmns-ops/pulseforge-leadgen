'use strict';

// Phase 3H Gate 1 — pre-migration production schema compatibility.
//
// Proves that the human-setter release tolerates a GENUINE pre-Phase-3D
// production schema: application startup performs no Phase 3D schema creation,
// legacy behaviour keeps working while the Anchor setter flag is disabled, DNC
// and setter safety checks fail closed, and — only after the explicit forward
// migration — full Phase 3D behaviour becomes available.
//
// Gated on MAX_SMOKE_DISPOSABLE_PG (set in the hosted Phase 3F release gate).
// Self-skips locally when the flag or PostgreSQL binaries are unavailable.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { execFileSync } = require('node:child_process');
const test = require('node:test');
const { Pool } = require('pg');

const {
  ensureCallDispositionSchema,
  ensureLegacyCallDispositionSchema,
  isPhase3dSetterSchemaPresent,
  notSyntheticSql,
  applyProspectDisposition,
  resetPhase3dSchemaCache,
} = require('../utils/callDispositions');

const MIGRATION_PATH = path.join(
  __dirname,
  '..',
  'migrations',
  '2026-07-19-setter-pilot-quality-control.sql',
);

const PHASE3D_PROSPECT_COLUMNS = ['is_synthetic', 'synthetic_label', 'callback_completed_at'];
const PHASE3D_CLIENT_COLUMNS = ['setter_pipeline_v2_enabled', 'setter_pipeline_v2_configured_at', 'setter_review_sample_percent'];
const PHASE3D_DISPOSITION_COLUMNS = ['structured_notes', 'is_synthetic', 'idempotency_key', 'review_status'];

let ctx = null;
let dbClient = null;

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

async function seedPreMigrationSchema(db) {
  // A faithful genuine pre-Phase-3D production schema: the base human-setter
  // pipeline (setter_status, setter_visible, assigned_setter_id, activity_log,
  // legacy call_dispositions) is already present, exactly as production runs it,
  // but NONE of the Phase 3D quality-control objects exist.
  await db.query(`
    CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      name TEXT,
      email TEXT,
      role TEXT,
      active BOOLEAN DEFAULT true,
      client_id INTEGER
    );
    CREATE TABLE clients (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      slug TEXT,
      active BOOLEAN DEFAULT true
    );
    CREATE TABLE companies (
      id SERIAL PRIMARY KEY,
      name TEXT,
      client_id INTEGER
    );
    CREATE TABLE prospects (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      company_id INTEGER,
      client_id INTEGER NOT NULL REFERENCES clients(id),
      first_name TEXT,
      last_name TEXT,
      email TEXT UNIQUE,
      phone TEXT,
      job_title TEXT,
      decision_maker BOOLEAN DEFAULT false,
      linkedin_url TEXT,
      facebook_url TEXT,
      source TEXT DEFAULT 'manual',
      icp_score INTEGER DEFAULT 0,
      service_area_match TEXT,
      do_not_contact BOOLEAN DEFAULT false,
      status TEXT DEFAULT 'cold',
      is_hot BOOLEAN DEFAULT false,
      notes TEXT,
      callback_at TIMESTAMPTZ,
      setter_status TEXT DEFAULT 'new',
      setter_visible BOOLEAN DEFAULT false,
      setter_updated_at TIMESTAMPTZ,
      assigned_setter_id INTEGER REFERENCES users(id),
      enrichment_attempted BOOLEAN DEFAULT false,
      last_contacted_at TIMESTAMPTZ,
      booked_at TIMESTAMPTZ,
      closed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE touchpoints (
      id SERIAL PRIMARY KEY,
      prospect_id UUID,
      channel TEXT,
      action_type TEXT,
      content_summary TEXT,
      outcome TEXT,
      sentiment TEXT,
      agent_id TEXT,
      external_ref TEXT,
      client_id INTEGER,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE activity_log (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      lead_id UUID REFERENCES prospects(id) ON DELETE CASCADE,
      action_type TEXT NOT NULL,
      notes TEXT,
      setter_id TEXT,
      client_id INTEGER NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE call_dispositions (
      id SERIAL PRIMARY KEY,
      prospect_id UUID REFERENCES prospects(id),
      client_id INTEGER,
      call_duration_seconds INTEGER,
      disposition TEXT,
      notes TEXT,
      cal_queue_id INTEGER,
      setter_id INTEGER,
      source TEXT NOT NULL DEFAULT 'cal',
      callback_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX call_dispositions_client_created_idx ON call_dispositions (client_id, created_at DESC);
    CREATE INDEX call_dispositions_prospect_idx ON call_dispositions (prospect_id, created_at DESC);
  `);
  await db.query(`INSERT INTO clients (id, name, slug) VALUES (1, 'Pulseforge', 'pulseforge'), (10, 'Anchor Cleaning', 'anchor')`);
  await db.query(`INSERT INTO users (id, name, role, active, client_id) VALUES (500, 'Setter One', 'setter', true, 10)`);
}

async function columnSet(db, table) {
  const { rows } = await db.query(
    `SELECT column_name FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
      ORDER BY column_name`,
    [table],
  );
  return rows.map(row => row.column_name);
}

async function phase3dSnapshot(db) {
  const columns = await db.query(`
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND (
        (table_name = 'prospects' AND column_name = ANY($1))
        OR (table_name = 'clients' AND column_name = ANY($2))
        OR (table_name = 'call_dispositions' AND column_name = ANY($3))
      )
  `, [PHASE3D_PROSPECT_COLUMNS, PHASE3D_CLIENT_COLUMNS, PHASE3D_DISPOSITION_COLUMNS]);
  const setterCallbacks = await db.query(`SELECT to_regclass('public.setter_callbacks') IS NOT NULL AS present`);
  const triggers = await db.query(`
    SELECT tgname FROM pg_trigger
    WHERE tgname IN ('prospects_synthetic_suppression', 'prospects_suppression_cleanup')
  `);
  return {
    phase3dColumns: columns.rows.map(r => `${r.table_name}.${r.column_name}`).sort(),
    setterCallbacks: Boolean(setterCallbacks.rows[0].present),
    triggers: triggers.rows.map(r => r.tgname).sort(),
  };
}

test.before(async () => {
  if (process.env.MAX_SMOKE_DISPOSABLE_PG !== 'true') return;
  for (const binary of ['/usr/local/bin/initdb', '/usr/local/bin/pg_ctl']) {
    if (!fs.existsSync(binary)) return;
  }
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'setter-premigration-pg-'));
  const port = await freePort();
  const logFile = path.join(directory, 'postgres.log');
  const options = { stdio: 'ignore', env: { ...process.env, LANG: 'C', LC_ALL: 'C' } };
  execFileSync('/usr/local/bin/initdb', ['-A', 'trust', '-U', 'postgres', '-D', directory], options);
  try {
    // Pin unix_socket_directories to the disposable data dir; PostgreSQL 18
    // cannot write the default socket path on GitHub Actions runners.
    execFileSync('/usr/local/bin/pg_ctl', [
      '-D', directory,
      '-l', logFile,
      '-o', `-p ${port} -h 127.0.0.1 -k ${directory}`,
      '-w',
      'start',
    ], options);
  } catch (error) {
    const diagnostics = fs.existsSync(logFile) ? fs.readFileSync(logFile, 'utf8') : 'no server log';
    throw new Error(`Temporary PostgreSQL failed to start:\n${diagnostics}`, { cause: error });
  }

  // Point the shared db singleton at the disposable cluster BEFORE requiring
  // dbClient, so the real shipped functions run against this schema.
  process.env.DATABASE_URL = `postgresql://postgres@127.0.0.1:${port}/postgres`;
  process.env.DATABASE_SSL = 'false';
  process.env.ACTIVE_CLIENT_ID = '10';

  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  await seedPreMigrationSchema(pool);
  resetPhase3dSchemaCache();
  dbClient = require('../dbClient');
  ctx = { pool, directory, options };
});

test.after(async () => {
  if (!ctx) return;
  await ctx.pool.end();
  try {
    const dbPool = require('../db');
    await dbPool.end();
  } catch { /* singleton may already be closed */ }
  execFileSync('/usr/local/bin/pg_ctl', ['-D', ctx.directory, '-m', 'fast', '-w', 'stop'], ctx.options);
  fs.rmSync(ctx.directory, { recursive: true, force: true });
});

function guard(t) {
  if (!ctx) {
    t.skip('set MAX_SMOKE_DISPOSABLE_PG=true with PostgreSQL binaries for pre-migration compatibility tests');
    return false;
  }
  return true;
}

// A + B — startup performs no Phase 3D schema creation and leaves schema unchanged.
test('pre-Phase-3D startup creates no Phase 3D schema and leaves schema unchanged', { timeout: 60000 }, async t => {
  if (!guard(t)) return;
  const db = ctx.pool;
  resetPhase3dSchemaCache();

  const before = await phase3dSnapshot(db);
  assert.deepEqual(before.phase3dColumns, [], 'no Phase 3D columns exist before startup');
  assert.equal(before.setterCallbacks, false, 'setter_callbacks absent before startup');
  assert.deepEqual(before.triggers, [], 'no Phase 3D suppression triggers before startup');

  const columnsBefore = {
    prospects: await columnSet(db, 'prospects'),
    clients: await columnSet(db, 'clients'),
    call_dispositions: await columnSet(db, 'call_dispositions'),
  };

  // This is exactly the schema path exercised at application startup
  // (server.js -> calBatchAgent -> ensureCallDispositionSchema).
  const result = await ensureCallDispositionSchema(db);
  assert.deepEqual(result, { phase3d: false }, 'startup ensure reports Phase 3D absent and does not reconcile');

  const after = await phase3dSnapshot(db);
  assert.deepEqual(after.phase3dColumns, [], 'startup created no Phase 3D columns');
  assert.equal(after.setterCallbacks, false, 'startup did not create setter_callbacks');
  assert.deepEqual(after.triggers, [], 'startup created no Phase 3D suppression triggers');

  assert.deepEqual(await columnSet(db, 'prospects'), columnsBefore.prospects, 'prospects columns unchanged by startup');
  assert.deepEqual(await columnSet(db, 'clients'), columnsBefore.clients, 'clients columns unchanged by startup');
  assert.deepEqual(await columnSet(db, 'call_dispositions'), columnsBefore.call_dispositions, 'call_dispositions columns unchanged by startup');

  // The legacy-only helper must never introduce Phase 3D schema either.
  await ensureLegacyCallDispositionSchema(db);
  const afterLegacy = await phase3dSnapshot(db);
  assert.deepEqual(afterLegacy.phase3dColumns, [], 'legacy ensure created no Phase 3D columns');
  assert.equal(afterLegacy.setterCallbacks, false, 'legacy ensure did not create setter_callbacks');
});

// C — legacy pipeline behaviour still works with the Anchor flag disabled.
test('legacy pipeline behaviour works on the pre-migration schema', { timeout: 60000 }, async t => {
  if (!guard(t)) return;
  const db = ctx.pool;
  resetPhase3dSchemaCache();

  const warm = await db.query(`
    INSERT INTO prospects (client_id, first_name, email, phone, status, icp_score, do_not_contact)
    VALUES (10, 'Warm', 'warm-legacy@example.com', '+16035550100', 'warm', 72, false)
    RETURNING id`);
  const suppressed = await db.query(`
    INSERT INTO prospects (client_id, first_name, email, phone, status, icp_score, do_not_contact)
    VALUES (10, 'Suppressed', 'dnc-legacy@example.com', '+16035550101', 'warm', 80, true)
    RETURNING id`);
  const warmId = warm.rows[0].id;
  const suppressedId = suppressed.rows[0].id;

  // Real dbClient.getProspectsByStatus must run and exclude DNC pre-migration.
  const active = await dbClient.getProspectsByStatus('warm');
  const activeIds = active.map(row => row.id);
  assert.ok(activeIds.includes(warmId), 'active warm prospect is selected');
  assert.ok(!activeIds.includes(suppressedId), 'do_not_contact prospect is excluded');

  // Real dbClient.addProspect must not insert Phase 3D columns pre-migration.
  const newId = await dbClient.addProspect({
    first_name: 'Fresh',
    last_name: 'Lead',
    email: 'fresh-legacy@example.com',
    phone: '+16035550102',
    source: 'manual',
    icp_score: 55,
    client_id: 10,
  });
  assert.ok(newId, 'addProspect succeeds on the pre-migration schema');

  // The Cal candidate guard (notSyntheticSql) must yield valid pre-migration SQL.
  const syntheticGuard = await notSyntheticSql(db, 'p.is_synthetic');
  assert.equal(syntheticGuard, 'TRUE', 'synthetic guard is a no-op before the migration');
  const candidates = await db.query(
    `SELECT p.id FROM prospects p
      LEFT JOIN companies c ON p.company_id = c.id
      WHERE p.status = 'warm' AND p.do_not_contact = false AND ${syntheticGuard}
        AND p.phone IS NOT NULL AND p.phone != '' AND p.icp_score >= 60`);
  const candidateIds = candidates.rows.map(row => row.id);
  assert.ok(candidateIds.includes(warmId), 'cal candidate query returns the eligible warm prospect');
  assert.ok(!candidateIds.includes(suppressedId), 'cal candidate query excludes suppressed prospects');

  // Legacy disposition write path is unaffected by absent Phase 3D schema.
  const updated = await applyProspectDisposition(db, {
    prospectId: warmId,
    clientId: 10,
    disposition: 'answered_interested',
    callbackAt: null,
  });
  assert.equal(updated.status, 'warm');
  assert.equal(updated.setter_status, 'follow_up');
  assert.equal(updated.is_hot, true);
});

// D — DNC checks fail closed without Phase 3D columns.
test('DNC checks fail closed on the pre-migration schema', { timeout: 60000 }, async t => {
  if (!guard(t)) return;
  const db = ctx.pool;
  resetPhase3dSchemaCache();

  const allowed = await db.query(`
    INSERT INTO prospects (client_id, first_name, email, status, do_not_contact)
    VALUES (10, 'Allowed', 'allowed-dnc@example.com', 'warm', false) RETURNING id`);
  const blocked = await db.query(`
    INSERT INTO prospects (client_id, first_name, email, status, do_not_contact)
    VALUES (10, 'Blocked', 'blocked-dnc@example.com', 'warm', true) RETURNING id`);

  assert.equal(await dbClient.checkDNC(blocked.rows[0].id), true, 'do_not_contact prospect is blocked');
  assert.equal(await dbClient.checkDNC(allowed.rows[0].id), false, 'contactable prospect is allowed');
  assert.equal(
    await dbClient.checkDNC('00000000-0000-4000-8000-000000000000'),
    true,
    'missing prospect fails closed (treated as do-not-contact)',
  );
});

// E — setter endpoints are unavailable / safely degraded before the migration.
test('setter feature schema is reported absent and setter queries fail closed', { timeout: 60000 }, async t => {
  if (!guard(t)) return;
  const db = ctx.pool;
  resetPhase3dSchemaCache();

  assert.equal(await isPhase3dSetterSchemaPresent(db), false, 'Phase 3D setter schema reported absent');

  // /setter/api/features resolves the pilot flag from clients; the column does
  // not exist pre-migration, so the route query rejects (route -> 500).
  await assert.rejects(
    db.query('SELECT setter_pipeline_v2_enabled FROM clients WHERE id = 10'),
    err => err.code === '42703',
    'setter pilot flag query fails closed with undefined_column',
  );

  // Setter callback features depend on a table that does not exist yet.
  await assert.rejects(
    db.query('SELECT 1 FROM setter_callbacks LIMIT 1'),
    err => err.code === '42P01',
    'setter_callbacks access fails closed with undefined_table',
  );

  // A genuinely unrelated error still surfaces normally (not swallowed).
  await assert.rejects(
    db.query('SELECT 1 FROM prospects WHERE id = $1', ['not-a-uuid']),
    err => err.code === '22P02',
    'unrelated invalid-input errors still surface',
  );
});

// F — absence of Phase 3D schema causes no outbound / Max execution.
test('no outbound or Max action can target suppressed or synthetic work pre-migration', { timeout: 60000 }, async t => {
  if (!guard(t)) return;
  const db = ctx.pool;
  resetPhase3dSchemaCache();

  const dnc = await db.query(`
    INSERT INTO prospects (client_id, first_name, email, phone, status, icp_score, do_not_contact)
    VALUES (10, 'MaxDnc', 'max-dnc@example.com', '+16035550200', 'warm', 90, true) RETURNING id`);

  // Max/agents gate on checkDNC before acting; a suppressed prospect is skipped.
  assert.equal(await dbClient.checkDNC(dnc.rows[0].id), true, 'Max DNC gate blocks the suppressed prospect');

  // Emmett autosend candidate guard yields valid SQL and excludes suppression.
  const syntheticGuard = await notSyntheticSql(db, 'p.is_synthetic');
  const emmettCandidates = await db.query(
    `SELECT p.email FROM prospects p
      WHERE p.client_id = 10
        AND p.status IN ('cold','contacted','warm')
        AND COALESCE(p.do_not_contact, FALSE) = FALSE
        AND ${syntheticGuard}
        AND p.email IS NOT NULL AND p.email <> ''`);
  assert.ok(
    !emmettCandidates.rows.some(row => row.email === 'max-dnc@example.com'),
    'suppressed prospect is never an outbound email candidate',
  );

  // Pre-migration there is no is_synthetic column, so no synthetic row can exist
  // or be selected by any outbound path.
  await assert.rejects(
    db.query('SELECT COUNT(*) FROM prospects WHERE is_synthetic = true'),
    err => err.code === '42703',
    'synthetic marker column does not exist before the migration',
  );
});

// G — after the explicit forward migration, all normal Phase 3D behaviour works.
test('after the forward migration Phase 3D behaviour works and the Anchor flag stays disabled', { timeout: 60000 }, async t => {
  if (!guard(t)) return;
  const db = ctx.pool;

  const migrationSql = fs.readFileSync(MIGRATION_PATH, 'utf8');
  await db.query(migrationSql);
  resetPhase3dSchemaCache();

  assert.equal(await isPhase3dSetterSchemaPresent(db), true, 'Phase 3D schema is detected after the migration');

  // ensureCallDispositionSchema now reconciles idempotently without error.
  const ensured = await ensureCallDispositionSchema(db);
  assert.deepEqual(ensured, { phase3d: true }, 'post-migration ensure reconciles Phase 3D schema');

  const snapshot = await phase3dSnapshot(db);
  assert.ok(snapshot.phase3dColumns.includes('prospects.is_synthetic'), 'is_synthetic exists after migration');
  assert.equal(snapshot.setterCallbacks, true, 'setter_callbacks exists after migration');
  assert.deepEqual(
    snapshot.triggers,
    ['prospects_suppression_cleanup', 'prospects_synthetic_suppression'],
    'suppression triggers exist after migration',
  );

  // The Anchor pilot flag exists but remains disabled by default.
  const flag = await db.query('SELECT setter_pipeline_v2_enabled FROM clients WHERE id = 10');
  assert.equal(flag.rows[0].setter_pipeline_v2_enabled, false, 'Anchor setter pilot remains disabled after migration');

  // addProspect now writes the synthetic marker and the suppression trigger fires.
  resetPhase3dSchemaCache();
  const syntheticId = await dbClient.addProspect({
    first_name: 'Synthetic',
    last_name: 'Rehearsal',
    email: 'synthetic-postmigration@example.com',
    source: 'manual',
    client_id: 10,
    is_synthetic: true,
    synthetic_label: 'pilot rehearsal',
  });
  const synthetic = await db.query('SELECT is_synthetic, do_not_contact FROM prospects WHERE id = $1', [syntheticId]);
  assert.equal(synthetic.rows[0].is_synthetic, true, 'synthetic marker persisted');
  assert.equal(synthetic.rows[0].do_not_contact, true, 'suppression trigger forces do_not_contact for synthetic prospects');

  // Synthetic work is excluded from outbound selection, real work is retained.
  await db.query(`
    INSERT INTO prospects (client_id, first_name, email, phone, status, icp_score, do_not_contact, is_synthetic)
    VALUES (10, 'RealWarm', 'real-postmigration@example.com', '+16035550300', 'warm', 75, false, false)`);
  const active = await dbClient.getProspectsByStatus('warm');
  const emails = active.map(row => row.email);
  assert.ok(emails.includes('real-postmigration@example.com'), 'real warm prospect is selected post-migration');
  assert.ok(!emails.includes('synthetic-postmigration@example.com'), 'synthetic prospect is excluded post-migration');

  assert.equal(await dbClient.checkDNC(syntheticId), true, 'DNC check reads is_synthetic and blocks synthetic prospects');
});
