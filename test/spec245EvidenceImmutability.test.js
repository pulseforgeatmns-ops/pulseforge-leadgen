'use strict';
const assert = require('node:assert/strict');
const { before, after, it } = require('node:test');
const { randomUUID } = require('crypto');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { hash } = require('../lib/canonicalSemanticWrite');
const { createPostgresStore, createMemoryStore } = require('../services/clientIntelligenceInterview');
const sql = name => fs.readFileSync(path.join(__dirname, '../migrations', name), 'utf8');
const backfill = sql('2026-09-06-spec-245a-evidence-immutability.sql');
let postgres, pool, sessionId;
before(async () => {
  postgres = await startDisposablePostgres('spec245-');
  pool = new Pool({ connectionString: postgres.connectionString });
  await pool.query(`CREATE TABLE clients (id INTEGER PRIMARY KEY);
    CREATE TABLE tenant_workspaces (client_id INTEGER, tenant_key TEXT);
    INSERT INTO clients VALUES (1);`);
  await pool.query(sql('2026-08-06-client-intelligence-engine.sql'));
  await pool.query(sql('2026-09-01-spec-223a-canonical-semantic-persistence.sql'));
  sessionId = (await pool.query('INSERT INTO cie_interview_sessions (client_id) VALUES (1) RETURNING id')).rows[0].id;
});
after(async () => { await pool?.end(); await postgres?.stop(); });
const input = statement => ({ id: randomUUID(), client_id: 1, session_id: sessionId,
  source: 'interview', category: 'identity', statement, confidence: 0.8, type: 'EXPLICIT' });
it('fresh writes hash exact Unicode and whitespace, deterministically, in both stores', async () => {
  for (const store of [createPostgresStore(pool), createMemoryStore()]) {
    const statements = ['  café\t😀\r\n', 'café\t😀\r\n', '  café\t😀\r\n'];
    const rows = [];
    for (const statement of statements) {
      const row = await store.insertEvidence({ ...input(statement), source_text_sha256: 'incorrect' });
      assert.equal(row.statement, statement);
      assert.equal(row.source_text_sha256, hash(statement));
      assert.ok(row.immutable_at);
      rows.push(row);
    }
    assert.notEqual(rows[0].source_text_sha256, rows[1].source_text_sha256);
    assert.equal(rows[0].source_text_sha256, rows[2].source_text_sha256);
  }
  const stored = (await pool.query("SELECT *, encode(digest(statement,'sha256'),'hex') AS digest FROM cie_evidence")).rows;
  for (const row of stored) {
    assert.equal(row.source_text_sha256, row.digest);
    assert.equal(+row.immutable_at, +row.created_at);
  }
});
it('backfills all missing combinations, preserves populated rows and all other columns, and reruns without updates', async () => {
  for (const [digestPresent, timestampPresent] of [[false,false],[false,true],[true,false],[true,true]]) {
    const statement = `  historical é 😀\t${digestPresent}/${timestampPresent}\n`;
    await pool.query(`INSERT INTO cie_evidence
      (client_id,session_id,statement,category,created_at,source_text_sha256,immutable_at)
      VALUES (1,$1,$2,'identity','2020-01-01',$3,$4)`,
    [sessionId, statement, digestPresent ? hash(statement) : null, timestampPresent ? '2021-01-01' : null]);
  }
  const snapshot = async () => (await pool.query('SELECT *, xmin::text AS row_version FROM cie_evidence ORDER BY id')).rows;
  const beforeRows = await snapshot();
  await pool.query(backfill);
  const afterRows = await snapshot();
  for (let i = 0; i < beforeRows.length; i++) {
    const old = beforeRows[i], row = afterRows[i];
    assert.equal(row.source_text_sha256, hash(old.statement));
    assert.equal(+row.immutable_at, +(old.immutable_at || old.created_at));
    for (const key of Object.keys(old).filter(k => !['source_text_sha256','immutable_at','row_version'].includes(k))) {
      assert.deepEqual(row[key], old[key], key);
    }
    if (old.source_text_sha256 && old.immutable_at) assert.equal(row.row_version, old.row_version);
  }
  await pool.query(backfill);
  assert.deepEqual(await snapshot(), afterRows);
  await assert.rejects(pool.query("UPDATE cie_evidence SET statement='changed' WHERE id=$1", [afterRows[0].id]), /immutable CIE evidence/);
});
it('invalid existing digest rolls back repairs and restores trigger protection', async () => {
  const id = randomUUID();
  await pool.query(`INSERT INTO cie_evidence (id,client_id,session_id,statement,category,source_text_sha256)
    VALUES ($1,1,$2,'invalid historical','identity',$3)`, [id, sessionId, 'a'.repeat(64)]);
  const client = await pool.connect();
  try {
    await assert.rejects(client.query(backfill), /validation failed/);
    await client.query('ROLLBACK');
  } finally { client.release(); }
  assert.equal((await pool.query('SELECT immutable_at FROM cie_evidence WHERE id=$1',[id])).rows[0].immutable_at, null);
  assert.equal((await pool.query("SELECT tgenabled FROM pg_trigger WHERE tgname='canonical_cie_evidence_immutable_trigger'")).rows[0].tgenabled, 'O');
});
