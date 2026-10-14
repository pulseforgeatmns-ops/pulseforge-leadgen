'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { refreshUsersRoleConstraint, withSchemaTransaction, refreshUsersRoleConstraintOnClient, USERS_ROLE_LOCK } = require('../utils/usersRoleConstraint');
const { USER_ROLES } = require('../utils/userRoles');

test('disposable PostgreSQL: concurrent role refresh, atomic failures, preserved checks/data and no held locks', { timeout: 60000 }, async t => {
  const instance = await startDisposablePostgres('portal-startup-');
  const pool = new Pool({ connectionString: instance.connectionString, max: 8 });
  t.after(async () => { await pool.end(); await instance.stop(); });
  await pool.query(`CREATE TABLE users (id serial PRIMARY KEY, role text NOT NULL CHECK (role IN ('admin', 'manager')), name text, CONSTRAINT keep_role_name CHECK (role <> 'admin' OR name IS NOT NULL)); INSERT INTO users(role,name) VALUES ('admin','Synthetic admin'); CREATE SCHEMA unrelated; CREATE TABLE unrelated.users (role text CONSTRAINT users_role_check CHECK(role = 'custom'));`);
  const definitions = async () => (await pool.query(`SELECT conname, pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid='users'::regclass ORDER BY conname`)).rows;
  const unrelated = (await pool.query(`SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid='unrelated.users'::regclass`)).rows;
  await Promise.all(Array.from({ length: 16 }, () => refreshUsersRoleConstraint(pool)));
  for (const role of USER_ROLES) await pool.query('INSERT INTO users(role,name) VALUES ($1,$2)', [role, `Synthetic ${role}`]);
  await assert.rejects(pool.query("INSERT INTO users(role) VALUES ('unknown')"), { code: '23514' });
  await assert.rejects(pool.query("INSERT INTO users(role) VALUES ('admin')"), { code: '23514' });
  assert.deepEqual((await pool.query(`SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid='unrelated.users'::regclass`)).rows, unrelated);
  const shadowPool = new Pool({ connectionString: instance.connectionString, options: '-c search_path=unrelated,public' });
  try { await assert.rejects(refreshUsersRoleConstraint(shadowPool), /must resolve to public.users/); } finally { await shadowPool.end(); }
  const before = await definitions();
  // Failure after DROP/ADD and subsequent DDL must restore both constraint and schema.
  await assert.rejects(withSchemaTransaction(pool, async client => {
    await refreshUsersRoleConstraintOnClient(client);
    await client.query('CREATE TABLE synthetic_partial_schema(id int)');
    await client.query('SELECT missing_startup_function()');
  }), { code: '42883' });
  assert.deepEqual(await definitions(), before);
  assert.equal((await pool.query("SELECT to_regclass('synthetic_partial_schema') AS relation")).rows[0].relation, null);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM users')).rows[0].n, USER_ROLES.length + 1);
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM pg_locks WHERE locktype = 'advisory' AND granted")).rows[0].n, 0);
  // A failure while recreating the check restores the old check (including NOT VALID).
  await pool.query('ALTER TABLE users DROP CONSTRAINT users_role_check');
  await pool.query("INSERT INTO users(role,name) VALUES ('custom','Synthetic custom')");
  await pool.query("ALTER TABLE users ADD CONSTRAINT users_role_check CHECK(role IN ('admin','manager')) NOT VALID");
  const custom = await definitions();
  await assert.rejects(refreshUsersRoleConstraint(pool), /Unrecognized users_role_check/);
  assert.deepEqual(await definitions(), custom);
  // Named check with unknown valid role must not be silently narrowed.
  await pool.query('ALTER TABLE users DROP CONSTRAINT users_role_check');
  await pool.query("ALTER TABLE users ADD CONSTRAINT users_role_check CHECK(role IN ('admin','custom')) NOT VALID");
  await assert.rejects(refreshUsersRoleConstraint(pool), /explicit operator review/);
  // Prove different pooled connections contend on the shared transaction key.
  const holder = await pool.connect();
  await holder.query('BEGIN'); await holder.query('SELECT pg_advisory_xact_lock($1::bigint)', [USERS_ROLE_LOCK]);
  let completed = false;
  const waiting = refreshUsersRoleConstraint(pool).catch(() => {}).then(() => { completed = true; });
  await new Promise(resolve => setTimeout(resolve, 100)); assert.equal(completed, false);
  await holder.query('ROLLBACK'); holder.release(); await waiting;
  assert.equal(completed, true);
});

test('actual portal initializer rolls back partial DDL and stays seed-free; repeated startup preserves history', { timeout: 60000 }, async t => {
  const instance = await startDisposablePostgres('portal-init-');
  const pool = new Pool({ connectionString: instance.connectionString, max: 6 });
  const dbPath = require.resolve('../db');
  const previous = require.cache[dbPath];
  require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: pool };
  const schemaPath = require.resolve('../utils/anchorPortalSchema'); delete require.cache[schemaPath];
  const schema = require(schemaPath);
  t.after(async () => { delete require.cache[schemaPath]; if (previous) require.cache[dbPath] = previous; else delete require.cache[dbPath]; await pool.end(); await instance.stop(); });
  await pool.query("CREATE TABLE users(id serial PRIMARY KEY, role text NOT NULL CHECK(role IN ('admin','manager')), name text)");
  // Force an error deep in the real helper after early CREATE TABLE statements.
  await pool.query('CREATE TABLE anchor_portal_issues(id serial PRIMARY KEY, location_id text)');
  await assert.rejects(schema.ensureAnchorPortalSchema());
  assert.equal((await pool.query("SELECT to_regclass('anchor_portal_locations') AS relation")).rows[0].relation, null);
  assert.match((await pool.query("SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conrelid='users'::regclass AND conname='users_role_check'")).rows[0].def, /admin.*manager/);
  assert.doesNotMatch((await pool.query("SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conrelid='users'::regclass AND conname='users_role_check'")).rows[0].def, /cleaner/);
  await pool.query('DROP TABLE anchor_portal_issues');
  await Promise.all(Array.from({ length: 6 }, () => schema.ensureAnchorPortalSchema()));
  assert.equal((await pool.query('SELECT count(*)::int n FROM users')).rows[0].n, 0);
  assert.equal((await pool.query('SELECT count(*)::int n FROM anchor_portal_locations')).rows[0].n, 0);
  await pool.query("INSERT INTO anchor_portal_locations(name,slug) VALUES ('Synthetic existing','synthetic-existing'); INSERT INTO anchor_portal_visits(location_id,status,visit_notes) VALUES (1,'completed','Preserve synthetic history')");
  await schema.ensureAnchorPortalSchema();
  assert.equal((await pool.query('SELECT visit_notes FROM anchor_portal_visits')).rows[0].visit_notes, 'Preserve synthetic history');
  await pool.query('DROP INDEX idx_anchor_portal_report_request');
  await pool.query('CREATE INDEX idx_anchor_portal_report_request ON anchor_portal_visits(request_id)');
  await assert.rejects(schema.ensureAnchorPortalSchema(), /schema mismatch: idx_anchor_portal_report_request/);
  // Never silently repair an existing incorrect object; preserve for operator review.
  assert.equal((await pool.query("SELECT indisunique FROM pg_index WHERE indexrelid='idx_anchor_portal_report_request'::regclass")).rows[0].indisunique, false);
  await pool.query('DROP INDEX idx_anchor_portal_report_request');
  await schema.ensureAnchorPortalSchema();
  await pool.query('ALTER TABLE anchor_portal_evidence ALTER COLUMN purpose DROP NOT NULL');
  await assert.rejects(schema.ensureAnchorPortalSchema(), /schema mismatch: anchor_portal_evidence.purpose/);
  assert.equal((await pool.query('SELECT visit_notes FROM anchor_portal_visits')).rows[0].visit_notes, 'Preserve synthetic history');

});
