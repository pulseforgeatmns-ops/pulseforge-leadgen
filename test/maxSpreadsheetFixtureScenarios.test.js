'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');
const XLSX = require('xlsx');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { extract } = require('../packages/max/composer/adapters/spreadsheet');
const { buildSpreadsheetProposal } = require('../packages/max/stateIngestion/spreadsheetProposal');
const { PostgresSpreadsheetProposalStore } = require('../packages/max/stateIngestion/spreadsheetProposalStore');
const filename = 'Anchor Cleaning Prospect List.xlsx';
const original = fs.readFileSync(path.join(__dirname, 'fixtures/anchor-cleaning-actual.xlsx'));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const expectedHash = 'b1cddfa475f244e27d8c81a381976b30911b34f53ea6c449f868a54208ba6a75';

test('hash-pinned real fixture: empty, mixed, aligned approved effects and reserialized semantic replay', { timeout: 60000 }, async t => {
  assert.equal(hash(original), expectedHash);
  const instance = await startDisposablePostgres('spreadsheet-fixture-matrix-');
  assert.equal(new URL(instance.connectionString).hostname, '127.0.0.1');
  const db = new Pool({ connectionString: instance.connectionString });
  t.after(async () => { await db.end(); await instance.stop(); });
  await db.query(fs.readFileSync(path.join(__dirname, 'fixtures/maxSpreadsheetBaseSchema.sql'), 'utf8'));
  await db.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-07-max-spreadsheet-reliability.sql'), 'utf8'));
  await db.query("INSERT INTO clients VALUES(1); INSERT INTO users(id,client_id,name,role) VALUES(10,1,'Test AO','ao'),(11,1,'Test Jake','admin')");
  const store = new PostgresSpreadsheetProposalStore(db, { clientId: 1, aoId: 10, approverUserId: 11 });
  async function plan(bytes = original, name = filename) {
    const parsed = await extract({ filename: name }, { buffer: bytes });
    assert.equal(parsed.extractionStatus, 'ready');
    const baseline = await store.snapshotContext();
    const result = buildSpreadsheetProposal({ structuredData: parsed.structuredData, snapshot: baseline,
      scope: { clientId: 1, aoId: 10, actorId: 10 }, fileHash: hash(bytes) });
    assert.equal(result.rows.length, 12);
    assert.ok(result.rows.every(row => row.evidence.length > 0));
    return { plan: result, baseline, parsed: parsed.structuredData };
  }
  const empty = await plan();
  assert.equal(empty.plan.rows.filter(row => row.accountResolution.status === 'new_candidate').length, 12);
  assert.ok(empty.plan.operations.every(operation => operation.blocked));
  assert.equal((await db.query('SELECT count(*) FROM prospects')).rows[0].count, '0');

  const rows = empty.parsed.sheets[0].rows;
  async function seed(row) {
    const company = await db.query('INSERT INTO companies(client_id,name) VALUES(1,$1) RETURNING id', [row.values.company]);
    await db.query(`INSERT INTO prospects(client_id,company_id,assigned_ao_id,phone,ao_last_touch_at)
      VALUES(1,$1,10,$2,'2026-09-01Z')`, [company.rows[0].id, row.values.phone]);
  }
  for (const row of rows.filter((_, index) => index % 2 === 0)) await seed(row);
  const mixed = await plan();
  assert.equal(mixed.plan.rows.filter(row => row.accountResolution.status === 'matched').length, 6);
  assert.equal(mixed.plan.rows.filter(row => row.accountResolution.status === 'new_candidate').length, 6);
  assert.ok(mixed.plan.rows.filter(row => row.accountResolution.status === 'new_candidate').every(row => row.operations.every(operation => operation.blocked)));
  for (const row of rows.filter((_, index) => index % 2 !== 0)) await seed(row);
  const before = await plan();
  const selected = before.plan.operations.filter(operation => !operation.blocked);
  assert.ok(selected.length > 0);
  const scope = { actorId: 10, conversationId: randomUUID(), sourceHash: expectedHash };
  const proposal = await store.createProposal({ ...scope, plan: before.plan, baseline: before.baseline });
  const receipt = await store.commitProposal({ ...scope, proposalId: proposal.id,
    selectedOperationIds: selected.map(operation => operation.id), expectedDigest: proposal.digest,
    approvedBy: 11, idempotencyKey: randomUUID() });
  assert.equal(receipt.results.length, selected.length);
  assert.ok(receipt.results.every(result => result.status === 'verified'));
  const aligned = await plan();
  assert.equal(aligned.plan.operations.filter(operation => !operation.blocked).length, 0);
  assert.ok(aligned.plan.rows.some(row => row.outcome === 'needs_review'), 'Aligned approved effects do not resolve source identities by inference');
  assert.ok(aligned.plan.rows.find(row => row.rowNumber === 7).conflicts.some(conflict => conflict.code === 'IDENTITY_CONFLICT'));
  const durable = JSON.stringify(aligned.baseline);

  const renamed = await plan(original, 'Renamed by user.xlsx');
  assert.equal(renamed.plan.operations.filter(operation => !operation.blocked).length, 0);
  assert.equal(JSON.stringify(renamed.baseline), durable);
  const workbook = XLSX.read(original, { type: 'buffer', cellStyles: true });
  const sheet = workbook.Sheets.Sheet1;
  const copies = rows.map(row => Array.from({ length: 12 }, (_, column) => structuredClone(sheet[XLSX.utils.encode_cell({ r: row.rowNumber - 1, c: column })])));
  for (let index = 0; index < 12; index++) for (let column = 0; column < 12; column++) {
    const address = XLSX.utils.encode_cell({ r: index + 2, c: column });
    const cell = copies[11 - index][column];
    if (cell) sheet[address] = cell; else delete sheet[address];
  }
  const reorderedBytes = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
  assert.notEqual(hash(reorderedBytes), expectedHash);
  const reordered = await plan(reorderedBytes, 'Reordered copy.xlsx');
  assert.equal(reordered.plan.rows[0].company, 'Grappone Ford');
  assert.equal(reordered.plan.operations.filter(operation => !operation.blocked).length, 0);
  assert.equal(JSON.stringify(reordered.baseline), durable);

  // A meaningful suffix and a corrected historical note must be proposed
  // additively, without deleting or silently rewriting original history.
  const changed = XLSX.read(original, { type: 'buffer', cellStyles: true });
  const oldText = changed.Sheets.Sheet1.L9.v;
  changed.Sheets.Sheet1.L9.v += '\n10/3: Owner requested information about backup weekend coverage.';
  const incremental = await plan(XLSX.write(changed, { type: 'buffer', bookType: 'xlsx' }), 'Incremental.xlsx');
  assert.ok(incremental.plan.rows.find(row => row.rowNumber === 9).operations.some(operation => operation.type === 'ADD_NOTE' && operation.after.text.includes('weekend coverage')));
  assert.equal(JSON.stringify(incremental.baseline), durable);
  changed.Sheets.Sheet1.L9.v = oldText.replace('very happy', 'happy');
  const corrected = await plan(XLSX.write(changed, { type: 'buffer', bookType: 'xlsx' }), 'Corrected.xlsx');
  assert.ok(corrected.plan.rows.find(row => row.rowNumber === 9).operations.some(operation => operation.type === 'ADD_NOTE' && operation.after.text !== oldText));
  assert.equal(JSON.stringify(corrected.baseline), durable);
  const history = (await db.query("SELECT notes FROM ao_prospect_activity WHERE notes=$1", [oldText])).rows;
  assert.equal(history.length, 1);
  assert.equal((await db.query("SELECT count(*) FROM prospects WHERE ao_last_touch_at <> '2026-09-01Z'::timestamptz")).rows[0].count, '0');
  assert.equal(hash(fs.readFileSync(path.join(__dirname, 'fixtures/anchor-cleaning-actual.xlsx'))), expectedHash);
});
