'use strict';

// Focused PostgreSQL coverage for GET /api/stats/today setter_id casts.
// Gated on MAX_SMOKE_DISPOSABLE_PG (same as other disposable PG tests).

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { execFileSync } = require('node:child_process');
const test = require('node:test');
const { Client } = require('pg');

const TODAY_STATS_SQL = `
  SELECT (
    SELECT COUNT(*)::int
    FROM activity_log al
    JOIN prospects p ON p.id = al.lead_id AND p.client_id = al.client_id
    WHERE al.action_type = 'call'
      AND al.setter_id = $1::text
      AND al.client_id = $2
      AND COALESCE(p.is_synthetic, false) = false
      AND al.created_at >= CURRENT_DATE
      AND al.created_at < CURRENT_DATE + INTERVAL '1 day'
  ) + (
    SELECT COUNT(*)::int
    FROM call_dispositions
    WHERE setter_id = $1::integer
      AND client_id = $2
      AND source = 'manual_setter'
      AND COALESCE(is_synthetic, false) = false
      AND created_at >= CURRENT_DATE
      AND created_at < CURRENT_DATE + INTERVAL '1 day'
  ) AS calls_today
`;

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

function resolvePgBin(name) {
  const candidates = [
    `/usr/local/bin/${name}`,
    `/opt/homebrew/bin/${name}`,
    `/usr/bin/${name}`,
  ];
  return candidates.find((p) => fs.existsSync(p)) || null;
}

test('today-stats SQL: integer/text casts, tenant scope, and safe null for bad ids', { timeout: 30000 }, async (t) => {
  // Always assert the route source keeps the cast/coercion contract.
  const source = fs.readFileSync(path.join(__dirname, '../routes/setter.js'), 'utf8');
  const block = source.slice(source.indexOf("'/api/stats/today'"), source.indexOf("'/api/stats/today'") + 1800);
  assert.match(block, /al\.setter_id = \$1::text/);
  assert.match(block, /setter_id = \$1::integer/);
  assert.match(block, /Number\.isFinite\(Number\(rawSetterId\)\)/);

  if (process.env.MAX_SMOKE_DISPOSABLE_PG !== 'true') {
    return t.skip('set MAX_SMOKE_DISPOSABLE_PG=true with PostgreSQL binaries for today-stats PG coverage');
  }

  const initdb = resolvePgBin('initdb');
  const pgCtl = resolvePgBin('pg_ctl');
  if (!initdb || !pgCtl) {
    return t.skip('PostgreSQL binaries unavailable');
  }

  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'setter-today-stats-'));
  const port = await freePort();
  const logFile = path.join(dataDir, 'postgres.log');
  const commandOptions = {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, LANG: 'C', LC_ALL: 'C' },
    encoding: 'utf8',
  };

  try {
    execFileSync(initdb, ['-A', 'trust', '-U', 'postgres', '-D', dataDir], commandOptions);
    execFileSync(pgCtl, [
      '-D', dataDir,
      '-l', logFile,
      '-o', `-p ${port} -h 127.0.0.1 -k ${dataDir}`,
      '-w',
      'start',
    ], { ...commandOptions, stdio: 'ignore' });
  } catch (error) {
    const diagnostics = [
      error.stderr,
      error.stdout,
      fs.existsSync(logFile) ? fs.readFileSync(logFile, 'utf8') : '',
    ].filter(Boolean).join('\n');
    fs.rmSync(dataDir, { recursive: true, force: true });
    // Cursor/sandbox hosts often deny shmget; CI disposable-PG runners still exercise this path.
    if (/shmget|shared memory|Operation not permitted/i.test(diagnostics || error.message)) {
      return t.skip(`disposable postgres unavailable in this environment: ${diagnostics.split('\n')[0] || error.message}`);
    }
    throw new Error(`Temporary PostgreSQL failed to start:\n${diagnostics || error.message}`, { cause: error });
  }

  const client = new Client({
    connectionString: `postgres://postgres@127.0.0.1:${port}/postgres`,
    ssl: false,
  });
  await client.connect();
  t.after(async () => {
    await client.end().catch(() => {});
    try { execFileSync(pgCtl, ['-D', dataDir, '-m', 'fast', '-w', 'stop'], commandOptions); } catch { /* ignore */ }
    fs.rmSync(dataDir, { recursive: true, force: true });
  });

  await client.query(`
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
    CREATE TABLE clients (id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE prospects (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL REFERENCES clients(id),
      is_synthetic BOOLEAN NOT NULL DEFAULT false
    );
    CREATE TABLE activity_log (
      id SERIAL PRIMARY KEY,
      lead_id UUID REFERENCES prospects(id),
      action_type TEXT,
      setter_id TEXT,
      client_id INTEGER,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE call_dispositions (
      id SERIAL PRIMARY KEY,
      prospect_id UUID REFERENCES prospects(id),
      client_id INTEGER,
      setter_id INTEGER,
      source TEXT NOT NULL DEFAULT 'manual_setter',
      is_synthetic BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    INSERT INTO clients (id, name) VALUES (10, 'Anchor'), (20, 'Other');
    INSERT INTO prospects (id, client_id) VALUES
      ('10000000-0000-4000-8000-000000000010', 10),
      ('20000000-0000-4000-8000-000000000020', 20);
    INSERT INTO activity_log (lead_id, action_type, setter_id, client_id)
      VALUES ('10000000-0000-4000-8000-000000000010', 'call', '42', 10),
             ('20000000-0000-4000-8000-000000000020', 'call', '42', 20);
    INSERT INTO call_dispositions (prospect_id, client_id, setter_id, source)
      VALUES ('10000000-0000-4000-8000-000000000010', 10, 42, 'manual_setter'),
             ('20000000-0000-4000-8000-000000000020', 20, 42, 'manual_setter');
  `);

  // Numeric setter id: counts activity_log (text) + dispositions (integer) for tenant 10 only.
  const ok = await client.query(TODAY_STATS_SQL, [42, 10]);
  assert.equal(Number(ok.rows[0].calls_today), 2);

  // Same setter on another tenant is scoped correctly (2 for tenant 20, 0 for unknown).
  const other = await client.query(TODAY_STATS_SQL, [42, 20]);
  assert.equal(Number(other.rows[0].calls_today), 2);
  const wrongTenant = await client.query(TODAY_STATS_SQL, [42, 99]);
  assert.equal(Number(wrongTenant.rows[0].calls_today), 0);

  // Non-numeric / null setter ids must not broaden results (NULL comparisons → 0).
  const nullId = await client.query(TODAY_STATS_SQL, [null, 10]);
  assert.equal(Number(nullId.rows[0].calls_today), 0);
});
