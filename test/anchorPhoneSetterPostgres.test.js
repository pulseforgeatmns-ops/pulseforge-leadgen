'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const test = require('node:test');
const { Pool } = require('pg');

const root = path.join(__dirname, '..');
const forward = path.join(root, 'migrations/2026-07-18-anchor-phone-setter-immediate-cash-v1.sql');
const rollback = path.join(root, 'migrations/2026-07-18-anchor-phone-setter-immediate-cash-v1.rollback.sql');

function binary(name) {
  try { return execFileSync('sh', ['-c', `command -v ${name}`], { encoding: 'utf8' }).trim(); } catch {
    try {
      const candidate = path.join(execFileSync('pg_config', ['--bindir'], { encoding: 'utf8' }).trim(), name);
      return fs.existsSync(candidate) ? candidate : null;
    } catch { return null; }
  }
}

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const port = server.address().port;
      server.close(() => resolve(port));
    });
  });
}

test('Anchor Phone Setter migration rehearses forward, protected rollback, clean rollback, and deterministic reapply', { timeout: 90000 }, async t => {
  if (process.env.ANCHOR_PHONE_SETTER_TEST_POSTGRES !== 'true') {
    t.skip('set ANCHOR_PHONE_SETTER_TEST_POSTGRES=true to run the required disposable PostgreSQL rehearsal');
    return;
  }
  const initdb = binary('initdb');
  const pgCtl = binary('pg_ctl');
  if (!initdb || !pgCtl) assert.fail('PostgreSQL initdb and pg_ctl are required for the disposable rehearsal');

  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'anchor-phone-setter-pg-'));
  const port = await freePort();
  const logFile = path.join(directory, 'postgres.log');
  const options = { stdio: 'ignore', env: { ...process.env, LANG: 'C', LC_ALL: 'C' } };
  execFileSync(initdb, ['-A', 'trust', '-U', 'postgres', '-D', directory], options);
  try {
    execFileSync(pgCtl, ['-D', directory, '-l', logFile, '-o', `-p ${port} -h 127.0.0.1 -k ${directory}`, '-w', 'start'], options);
  } catch (error) {
    const diagnostics = fs.existsSync(logFile) ? fs.readFileSync(logFile, 'utf8') : 'PostgreSQL did not create a server log.';
    throw new Error(`Temporary PostgreSQL failed to start:\n${diagnostics}`, { cause: error });
  }

  const db = new Pool({ connectionString: `postgresql://postgres@127.0.0.1:${port}/postgres` });
  t.after(async () => {
    await db.end();
    execFileSync(pgCtl, ['-D', directory, '-m', 'fast', '-w', 'stop'], options);
    fs.rmSync(directory, { recursive: true, force: true });
  });

  await db.query(`
    CREATE EXTENSION pgcrypto;
    CREATE TABLE clients (
      id INTEGER PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN NOT NULL DEFAULT TRUE,
      enabled_agents TEXT[] NOT NULL DEFAULT ARRAY['scout'], autosend_enabled BOOLEAN NOT NULL DEFAULT FALSE,
      target_verticals JSONB NOT NULL DEFAULT '[]', vertical_tiers JSONB NOT NULL DEFAULT '{}'
    );
    CREATE TABLE prospects (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id), vertical TEXT
    );
    CREATE TABLE call_dispositions (
      id SERIAL PRIMARY KEY, prospect_id UUID REFERENCES prospects(id), client_id INTEGER NOT NULL,
      disposition TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    INSERT INTO clients (id, name, target_verticals, vertical_tiers) VALUES
      (1, 'Client One', '[{"vertical":"legacy","tier":"B"}]', '{"legacy":"B"}'),
      (10, 'Anchor Cleaning', '[{"vertical":"office","tier":"B"}]', '{"office":"B"}');
    INSERT INTO prospects (id, client_id, vertical)
      VALUES ('10000000-0000-4000-8000-000000000010', 10, 'property_manager');
  `);
  const original = (await db.query('SELECT target_verticals, vertical_tiers FROM clients WHERE id = 10')).rows[0];
  const clientOneBefore = (await db.query('SELECT target_verticals, vertical_tiers FROM clients WHERE id = 1')).rows[0];

  await db.query(fs.readFileSync(forward, 'utf8'));
  assert.equal((await db.query("SELECT to_regclass('public.campaigns') AS name")).rows[0].name, 'campaigns');
  assert.equal((await db.query("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='call_dispositions' AND column_name='details') AS exists")).rows[0].exists, true);
  const campaign = await db.query("SELECT status, metadata FROM campaigns WHERE client_id=10 AND campaign_key='anchor_phone_setter_immediate_cash_v1'");
  assert.deepEqual(campaign.rows[0], { status: 'paused', metadata: { mode: 'manual_phone', external_sends_enabled: false, revenue_writes_enabled: false } });
  const anchorTargets = await db.query("SELECT vertical_tiers FROM clients WHERE id=10");
  assert.deepEqual(Object.keys(anchorTargets.rows[0].vertical_tiers).sort(), [
    'cleaning_company_overflow', 'commercial_office', 'property_manager', 'realtor',
    'restoration_remodeling_partner', 'str_manager',
  ]);
  assert.deepEqual((await db.query('SELECT target_verticals, vertical_tiers FROM clients WHERE id=1')).rows[0], clientOneBefore);

  await db.query(`
    INSERT INTO call_dispositions (prospect_id, client_id, disposition, details)
    VALUES ('10000000-0000-4000-8000-000000000010', 10, 'answered_callback', '{"category":"property_manager","interest_level":"warm","next_step":"callback"}');
    INSERT INTO setter_follow_up_drafts (client_id, prospect_id, channel, body, created_by)
    VALUES (10, '10000000-0000-4000-8000-000000000010', 'email', 'Rehearsal draft', 1);
  `);
  await assert.rejects(db.query(fs.readFileSync(rollback, 'utf8')), /Rollback blocked: Anchor structured call history exists/);
  await db.query('ROLLBACK');
  assert.equal((await db.query("SELECT to_regclass('public.call_dispositions_anchor_details_idx') AS name")).rows[0].name, 'call_dispositions_anchor_details_idx');
  assert.equal((await db.query("SELECT COUNT(*)::int AS count FROM campaigns WHERE client_id=10 AND campaign_key='anchor_phone_setter_immediate_cash_v1'")).rows[0].count, 1);
  assert.equal((await db.query("SELECT COUNT(*)::int AS count FROM call_dispositions WHERE client_id=10 AND details <> '{}'::jsonb")).rows[0].count, 1);

  await db.query("DELETE FROM call_dispositions WHERE client_id=10 AND details <> '{}'::jsonb");
  await db.query(fs.readFileSync(rollback, 'utf8'));
  assert.deepEqual((await db.query('SELECT target_verticals, vertical_tiers FROM clients WHERE id=10')).rows[0], original);
  assert.equal((await db.query("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='call_dispositions' AND column_name='details') AS exists")).rows[0].exists, false);
  assert.equal((await db.query("SELECT to_regclass('public.call_dispositions_anchor_details_idx') AS name")).rows[0].name, null);

  await db.query(fs.readFileSync(forward, 'utf8'));
  assert.equal((await db.query("SELECT COUNT(*)::int AS count FROM campaigns WHERE client_id=10 AND campaign_key='anchor_phone_setter_immediate_cash_v1'")).rows[0].count, 1);
  assert.equal((await db.query("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='call_dispositions' AND column_name='details') AS exists")).rows[0].exists, true);
});
