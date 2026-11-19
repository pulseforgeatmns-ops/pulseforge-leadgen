'use strict';
// Railway -> Render reconciliation: fingerprints detect updates, deletes and
// sequence regressions that row counts miss, and never write. Disposable
// PostgreSQL only.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { fingerprint, compare, manifestTables, qualified } = require('../scripts/migration/tableFingerprints');

const SCHEMA = `
  CREATE TABLE prospects(id SERIAL PRIMARY KEY, client_id INT, email TEXT, do_not_contact BOOLEAN DEFAULT false);
  CREATE TABLE agent_log(id SERIAL PRIMARY KEY, agent_name TEXT, payload JSONB);
  CREATE TABLE "Odd ""Name"""(k TEXT);
  INSERT INTO prospects(client_id,email) SELECT 21, 'p' || g || '@example.invalid' FROM generate_series(1,50) g;
  INSERT INTO agent_log(agent_name,payload) SELECT 'scout', jsonb_build_object('n', g) FROM generate_series(1,20) g;
  INSERT INTO "Odd ""Name"""(k) VALUES ('x'),('x');`;

test('fingerprints: equal copies match; updates, deletes and sequence lag are caught; read-only', { timeout: 180000 }, async () => {
  const pg = await startDisposablePostgres('migration-fingerprints-');
  const admin = new Pool({ connectionString: pg.connectionString });
  let source; let dest;
  try {
    await admin.query('CREATE DATABASE fp_source');
    await admin.query('CREATE DATABASE fp_dest');
    source = new Pool({ connectionString: pg.connectionString.replace(/\/postgres$/, '/fp_source') });
    dest = new Pool({ connectionString: pg.connectionString.replace(/\/postgres$/, '/fp_dest') });
    await source.query(SCHEMA);
    await dest.query(SCHEMA);
    const tables = ['public.agent_log', 'public.prospects', 'public.not_there'];

    const a = await fingerprint(source, { tables, expectDatabase: 'fp_source' });
    const b = await fingerprint(dest, { tables, expectDatabase: 'fp_dest' });
    assert.equal(a.tables['public.prospects'].rows, 50);
    assert.deepEqual(a.tables['public.not_there'], { missing: true });
    const same = compare(a, b);
    assert.deepEqual(same.differences.filter(d => d.table !== 'public.not_there'), []);

    // Same row count, changed content: an update (e.g. a suppression flag).
    await dest.query(`UPDATE prospects SET do_not_contact = true WHERE id = 7`);
    // Same row count after delete + insert of a different row.
    await dest.query(`DELETE FROM agent_log WHERE id = 3`);
    await dest.query(`INSERT INTO agent_log(id, agent_name, payload) VALUES (3, 'scout', '{"n":999}')`);
    // Destination sequence behind the source would reissue ids.
    await source.query(`SELECT setval('prospects_id_seq', 500)`);
    const a2 = await fingerprint(source, { tables, expectDatabase: 'fp_source' });
    const b2 = await fingerprint(dest, { tables, expectDatabase: 'fp_dest' });
    assert.equal(a2.tables['public.prospects'].rows, b2.tables['public.prospects'].rows, 'counts alone would pass');
    const diff = compare(a2, b2);
    assert.equal(diff.equivalent, false);
    const kinds = diff.differences.map(d => `${d.kind}:${d.table || d.sequence}`);
    assert.ok(kinds.includes('content:public.prospects'));
    assert.ok(kinds.includes('content:public.agent_log'));
    assert.ok(kinds.includes('sequence_behind:public.prospects_id_seq'));

    // A deleted row changes the count.
    await dest.query(`DELETE FROM prospects WHERE id = 8`);
    const rowDiff = compare(a2, await fingerprint(dest, { tables, expectDatabase: 'fp_dest' }));
    assert.ok(rowDiff.differences.some(d => d.kind === 'row_count' && d.table === 'public.prospects'));

    // Wrong target fails closed; output carries no row data.
    await assert.rejects(fingerprint(dest, { tables, expectDatabase: 'fp_source' }), { code: 'fingerprint_target_database_mismatch' });
    await assert.rejects(fingerprint(dest, { tables }), { code: 'fingerprint_expect_database_required' });
    assert.ok(!JSON.stringify(a2).includes('example.invalid'));

    // Read-only: a write inside the fingerprint transaction is impossible.
    const statements = [];
    const recording = { connect: async () => { const c = await source.connect();
      return { query: (sql, p) => { statements.push(String(sql)); return c.query(sql, p); }, release: () => c.release() }; } };
    const before = (await source.query('SELECT count(*)::int AS n FROM prospects')).rows[0].n;
    await fingerprint(recording, { tables, expectDatabase: 'fp_source' });
    assert.equal(statements[0], 'BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    assert.equal(statements.at(-1), 'ROLLBACK');
    for (const sql of statements) assert.doesNotMatch(sql, /\b(INSERT|UPDATE|DELETE|ALTER|DROP|CREATE|TRUNCATE)\b/i);
    assert.equal((await source.query('SELECT count(*)::int AS n FROM prospects')).rows[0].n, before);
  } finally {
    await source?.end(); await dest?.end(); await admin.end(); await pg.stop();
  }
});

test('identifier quoting and manifest selection', () => {
  assert.equal(qualified('public.prospects'), '"public"."prospects"');
  assert.equal(qualified('public.a"b'), '"public"."a""b"');
  assert.throws(() => qualified('prospects'), { code: 'fingerprint_table_invalid' });
  assert.throws(() => qualified('a.b.c'), { code: 'fingerprint_table_invalid' });
  const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'fp-')), 'm.json');
  fs.writeFileSync(file, JSON.stringify({ tables: [
    { table: 'public.prospects', owner: 'PULSEFORGE_CANDIDATE' }, { table: 'public.signal_x', owner: 'SIGNAL_CANDIDATE' }] }));
  assert.deepEqual(manifestTables(file, 'PULSEFORGE_CANDIDATE'), ['public.prospects']);
  assert.throws(() => manifestTables(file, 'NONE'), { code: 'fingerprint_manifest_empty' });
  const real = manifestTables(path.join(__dirname, '..', 'docs/migration/source-ownership-candidates-20261117.json'), 'PULSEFORGE_CANDIDATE');
  assert.equal(real.length, 274);
});
