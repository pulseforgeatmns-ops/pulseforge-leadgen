#!/usr/bin/env node
'use strict';
// Read-only content fingerprints for Railway -> Render reconciliation.
//
// Row counts cannot prove equivalence: an UPDATE keeps the count and a
// DELETE+INSERT can too. For every manifest table this computes, inside ONE
// REPEATABLE READ READ ONLY transaction (a single consistent snapshot):
//   rows        = count(*)
//   fingerprint = md5 of the sorted md5(row::text) values (order-independent
//                 multiset digest; any changed, added or removed row changes it)
// plus every sequence's last_value. No row data, names or secrets are emitted.
// Compare a source and a destination run with --compare; both sides must run
// the same PostgreSQL major version (row::text output is version-specific).
//
//   DATABASE_URL=... node scripts/migration/tableFingerprints.js \
//     --expect-database=<name> --manifest=docs/migration/source-ownership-candidates-20261117.json \
//     --owner=PULSEFORGE_CANDIDATE > source.json
//   node scripts/migration/tableFingerprints.js --compare source.json destination.json

const fs = require('node:fs');

function quoteIdent(value) { return `"${String(value).replace(/"/g, '""')}"`; }
function qualified(name) {
  const [schema, table, extra] = String(name).split('.');
  if (!schema || !table || extra !== undefined) throw Object.assign(new Error(`invalid table name: ${name}`), { code: 'fingerprint_table_invalid' });
  return `${quoteIdent(schema)}.${quoteIdent(table)}`;
}

function manifestTables(path, owner) {
  const raw = JSON.parse(fs.readFileSync(path, 'utf8'));
  const list = (raw.tables || []).filter(t => !owner || t.owner === owner).map(t => t.table);
  if (!list.length) throw Object.assign(new Error('manifest selects no tables'), { code: 'fingerprint_manifest_empty' });
  return [...new Set(list)].sort();
}

async function fingerprint(pool, { tables, expectDatabase, statementTimeoutMs = 600000 }) {
  if (!expectDatabase) throw Object.assign(new Error('expectDatabase is required'), { code: 'fingerprint_expect_database_required' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await client.query(`SET LOCAL statement_timeout = ${Number(statementTimeoutMs) | 0}`);
    const id = (await client.query(`SELECT current_database() AS database,
      current_setting('server_version_num')::int AS server_version_num,
      current_setting('transaction_read_only') AS read_only, now() AS captured_at`)).rows[0];
    if (id.database !== expectDatabase) throw Object.assign(new Error('fingerprint_target_database_mismatch'), { code: 'fingerprint_target_database_mismatch' });
    if (id.read_only !== 'on') throw Object.assign(new Error('fingerprint_not_read_only'), { code: 'fingerprint_not_read_only' });
    const report = { database: id.database, serverVersionNum: id.server_version_num,
      capturedAt: new Date(id.captured_at).toISOString(), tables: {}, sequences: {} };
    for (const name of tables) {
      await client.query('SAVEPOINT fp');
      try {
        const exists = (await client.query('SELECT to_regclass($1) IS NOT NULL AS ok', [name])).rows[0].ok;
        if (!exists) { report.tables[name] = { missing: true }; await client.query('RELEASE SAVEPOINT fp'); continue; }
        const row = (await client.query(`SELECT count(*)::bigint AS rows,
          md5(coalesce(string_agg(h, '' ORDER BY h), '')) AS fingerprint
          FROM (SELECT md5(t::text) AS h FROM ${qualified(name)} t) s`)).rows[0];
        report.tables[name] = { rows: Number(row.rows), fingerprint: row.fingerprint };
        await client.query('RELEASE SAVEPOINT fp');
      } catch (error) {
        await client.query('ROLLBACK TO SAVEPOINT fp');
        report.tables[name] = { error: error.code || 'fingerprint_failed' };
      }
    }
    const seqs = (await client.query(`SELECT schemaname || '.' || sequencename AS name, last_value
      FROM pg_sequences WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1`)).rows;
    for (const s of seqs) report.sequences[s.name] = s.last_value == null ? null : String(s.last_value);
    return report;
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    client.release();
  }
}

function compare(source, destination) {
  const differences = [];
  if (Math.floor(source.serverVersionNum / 10000) !== Math.floor(destination.serverVersionNum / 10000)) {
    differences.push({ kind: 'server_major_version', source: source.serverVersionNum, destination: destination.serverVersionNum });
  }
  const names = new Set([...Object.keys(source.tables), ...Object.keys(destination.tables)]);
  for (const name of [...names].sort()) {
    const a = source.tables[name]; const b = destination.tables[name];
    if (!a || !b) differences.push({ kind: 'table_not_fingerprinted_on_both', table: name });
    else if (a.missing || b.missing || a.error || b.error) differences.push({ kind: 'table_unavailable', table: name, source: a, destination: b });
    else if (a.rows !== b.rows) differences.push({ kind: 'row_count', table: name, source: a.rows, destination: b.rows });
    else if (a.fingerprint !== b.fingerprint) differences.push({ kind: 'content', table: name, rows: a.rows });
  }
  for (const name of Object.keys(source.sequences).sort()) {
    const a = source.sequences[name]; const b = destination.sequences[name];
    // A destination sequence behind the source would reissue ids.
    if (b === undefined) differences.push({ kind: 'sequence_missing', sequence: name });
    else if (a !== null && (b === null || BigInt(b) < BigInt(a))) differences.push({ kind: 'sequence_behind', sequence: name, source: a, destination: b });
  }
  return { equivalent: differences.length === 0, tablesCompared: names.size, differences };
}

module.exports = { fingerprint, compare, manifestTables, qualified };

if (require.main === module) {
  const args = Object.fromEntries(process.argv.slice(2).filter(a => a.startsWith('--') && a.includes('='))
    .map(a => { const i = a.indexOf('='); return [a.slice(2, i), a.slice(i + 1)]; }));
  const compareIdx = process.argv.indexOf('--compare');
  if (compareIdx > 0) {
    const [a, b] = process.argv.slice(compareIdx + 1, compareIdx + 3);
    const result = compare(JSON.parse(fs.readFileSync(a, 'utf8')), JSON.parse(fs.readFileSync(b, 'utf8')));
    console.log(JSON.stringify(result, null, 2));
    process.exit(result.equivalent ? 0 : 1);
  }
  const { Pool } = require('pg');
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 1,
    ssl: String(process.env.DATABASE_SSL || '').toLowerCase() === 'false' ? false : { rejectUnauthorized: false } });
  Promise.resolve()
    .then(() => fingerprint(pool, { tables: manifestTables(args.manifest, args.owner), expectDatabase: args['expect-database'] }))
    .then(report => { console.log(JSON.stringify(report, null, 2)); return pool.end().then(() => process.exit(0)); })
    .catch(error => { console.error(`[table-fingerprints] ${error.code || error.message}`); pool.end().finally(() => process.exit(2)); });
}
