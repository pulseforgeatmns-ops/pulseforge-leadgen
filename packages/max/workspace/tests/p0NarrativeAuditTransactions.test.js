'use strict';
// Savepoint-based nested transactions in scripts/investigationNarrativeAudit.js:
// successful completion, nested rollback, error handling, and prevention of
// writes or escape from the outer READ ONLY transaction.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const audit = require('../../../../scripts/investigationNarrativeAudit');
const { deriveBlueprintCompatibility } = require('../../../../lib/canonicalProjection');

// Minimal Postgres transaction simulator: one connection, real-ish semantics
// for BEGIN/COMMIT/ROLLBACK, savepoints, read-only writes and aborted state.
function fakePg({ onSelect } = {}) {
  const st = { inTxn: false, readOnly: false, aborted: false, savepoints: [], log: [] };
  const fail = (msg, code) => { throw Object.assign(new Error(msg), { code }); };
  return { st, async query(sql) {
    const text = String(typeof sql === 'string' ? sql : sql.text).trim();
    st.log.push(text);
    let m;
    if ((m = text.match(/^ROLLBACK TO SAVEPOINT (\w+)$/i))) {
      const i = st.savepoints.lastIndexOf(m[1]); if (i < 0) fail('no such savepoint', '3B001');
      st.savepoints.length = i + 1; st.aborted = false; return { rows: [] };
    }
    if ((m = text.match(/^RELEASE SAVEPOINT (\w+)$/i))) {
      if (st.aborted) fail('current transaction is aborted', '25P02');
      const i = st.savepoints.lastIndexOf(m[1]); if (i < 0) fail('no such savepoint', '3B001');
      st.savepoints.length = i; return { rows: [] };
    }
    if ((m = text.match(/^SAVEPOINT (\w+)$/i))) {
      if (!st.inTxn) fail('SAVEPOINT can only be used in transaction blocks', '25P01');
      if (st.aborted) fail('current transaction is aborted', '25P02');
      st.savepoints.push(m[1]); return { rows: [] };
    }
    if (/^BEGIN/i.test(text)) { if (!st.inTxn) { st.inTxn = true; st.readOnly = /READ ONLY/i.test(text); } return { rows: [] }; }
    if (/^(COMMIT|END|ROLLBACK|ABORT)\b/i.test(text)) { Object.assign(st, { inTxn: false, readOnly: false, aborted: false, savepoints: [] }); return { rows: [] }; }
    if (st.aborted) fail('current transaction is aborted, commands ignored until end of transaction block', '25P02');
    if (/^SHOW transaction_read_only$/i.test(text)) return { rows: [{ transaction_read_only: st.inTxn && st.readOnly ? 'on' : 'off' }] };
    if (/^SET LOCAL/i.test(text)) return { rows: [] };
    if (/^(INSERT|UPDATE|DELETE)/i.test(text)) { if (st.inTxn) st.aborted = true; if (st.readOnly) fail('cannot execute in a read-only transaction', '25006'); return { rows: [], rowCount: 1 }; }
    if (/FAIL_ME/.test(text)) { if (st.inTxn) st.aborted = true; fail('relation does not exist', '42P01'); }
    return onSelect ? onSelect(text) : { rows: [] };
  }, release() { st.released = true; } };
}
async function outer(pg) {
  await pg.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
  const boundary = { reads: 0, writesBlocked: 0 };
  return { boundary, conn: audit.guardedConnection(pg, boundary) };
}
const outerIntact = pg => { assert.equal(pg.st.inTxn, true); assert.equal(pg.st.readOnly, true); };
const sentToDb = (pg, re) => pg.st.log.filter(l => re.test(l));

test('successful nested transaction: BEGIN/COMMIT become SAVEPOINT/RELEASE inside the outer read-only transaction', async () => {
  const pg = fakePg(); const { conn, boundary } = await outer(pg);
  const inner = await conn.connect();
  await inner.query('BEGIN READ ONLY');
  await inner.query('SELECT 1');
  await inner.query('COMMIT');
  inner.release();
  assert.deepEqual(pg.st.log.slice(1), ['SAVEPOINT audit_nested_1', 'SELECT 1', 'RELEASE SAVEPOINT audit_nested_1']);
  outerIntact(pg); assert.deepEqual(pg.st.savepoints, []);
  assert.equal(boundary.nested.begun, 1); assert.equal(boundary.nested.released, 1); assert.equal(boundary.writesBlocked, 0);
  assert.equal(conn.hasOpenNestedTransaction(), false);
  // Sequential sessions get fresh savepoint names.
  const again = await conn.connect(); await again.query('START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'); await again.query('END');
  assert.ok(pg.st.log.includes('SAVEPOINT audit_nested_2')); outerIntact(pg);
});

test('nested rollback rolls back to the savepoint only, never the outer transaction', async () => {
  const pg = fakePg(); const { conn, boundary } = await outer(pg);
  const inner = await conn.connect();
  await inner.query('BEGIN READ ONLY'); await inner.query('SELECT 1'); await inner.query('ROLLBACK');
  assert.deepEqual(pg.st.log.slice(1), ['SAVEPOINT audit_nested_1', 'SELECT 1', 'ROLLBACK TO SAVEPOINT audit_nested_1', 'RELEASE SAVEPOINT audit_nested_1']);
  outerIntact(pg); assert.equal(boundary.nested.rolledBack, 1);
  await conn.query('SELECT 2'); assert.equal(pg.st.log.at(-1), 'SELECT 2');
});

test('error handling: a failed nested read is recovered by the loader\'s ROLLBACK or by release(); the outer transaction stays usable', async () => {
  const pg = fakePg(); const { conn, boundary } = await outer(pg);
  const inner = await conn.connect();
  await inner.query('BEGIN READ ONLY');
  await assert.rejects(inner.query('SELECT * FROM FAIL_ME'), { code: '42P01' });
  assert.equal(pg.st.aborted, true);
  await inner.query('ROLLBACK');
  assert.equal(pg.st.aborted, false); outerIntact(pg);
  await conn.query('SELECT 3'); assert.equal(pg.st.log.at(-1), 'SELECT 3');

  // A loader that fails and releases without rolling back is cleaned up too.
  const careless = await conn.connect();
  await careless.query('BEGIN'); await assert.rejects(careless.query('SELECT FAIL_ME'));
  careless.release(); await new Promise(r => setImmediate(r));
  assert.equal(pg.st.aborted, false); outerIntact(pg); assert.equal(boundary.nested.cleanedUp, 1);
  assert.equal(conn.hasOpenNestedTransaction(), false);
  await conn.query('SELECT 4'); assert.equal(pg.st.log.at(-1), 'SELECT 4');
});

test('writes and transaction escape are prevented; rejected statements never reach the database', async () => {
  const pg = fakePg(); const { conn, boundary } = await outer(pg);
  const inner = await conn.connect();
  const rejected = ['UPDATE prospects SET notes = $1', 'INSERT INTO touchpoints VALUES (1)', 'DELETE FROM prospects',
    'BEGIN READ WRITE', 'BEGIN TRANSACTION READ WRITE', 'COMMIT', 'END', 'COMMIT AND CHAIN', 'ROLLBACK AND CHAIN',
    'SAVEPOINT x', 'RELEASE SAVEPOINT x', 'ROLLBACK TO SAVEPOINT x', 'PREPARE TRANSACTION \'t\'', 'SET TRANSACTION READ WRITE',
    'SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE', 'SELECT 1; COMMIT', 'SELECT 1; UPDATE prospects SET status = $1'];
  for (const sql of rejected) await assert.rejects(inner.query(sql), /audit_|read_only_violation/, sql);
  for (const sql of ['COMMIT', 'ROLLBACK', 'BEGIN', 'SELECT 1; COMMIT', 'UPDATE prospects SET status = $1']) {
    await assert.rejects(conn.query(sql), /audit_|read_only_violation/, `top-level ${sql}`);
  }
  // A second nested BEGIN while one is open is rejected; the open one is intact.
  await inner.query('BEGIN READ ONLY');
  const other = await conn.connect();
  await assert.rejects(other.query('BEGIN READ ONLY'), /audit_nested_transaction_already_open/);
  await assert.rejects(other.query('COMMIT'), /audit_commit_without_nested_transaction/);
  await other.query('ROLLBACK'); // not its savepoint: ignored, never sent
  await inner.query('COMMIT');
  // A stray ROLLBACK with no nested transaction is a no-op, never sent.
  await inner.query('ROLLBACK');
  assert.deepEqual(sentToDb(pg, /^(?:COMMIT|END|ROLLBACK|ABORT)\b(?! TO)/i), []);
  assert.deepEqual(sentToDb(pg, /^(?:UPDATE|INSERT|DELETE|PREPARE|SET)\b/i), []);
  assert.deepEqual(sentToDb(pg, /;/), []);
  outerIntact(pg); assert.ok(boundary.writesBlocked >= rejected.length + 5);
  assert.equal(boundary.nested.ignoredRollbacks, 2);
});

test('the real canonical projection now runs inside a savepoint and reaches the database instead of failing on its own BEGIN', async () => {
  const pg = fakePg(); const { conn, boundary } = await outer(pg);
  const facts = await deriveBlueprintCompatibility({ tenant_id: 't', snapshot_id: 's', pool: conn });
  // No canonical rows in the simulator, so it is UNAVAILABLE for a real data
  // reason, not because the audit guard rejected the transaction.
  assert.equal(facts._projection_metadata.completeness, 'UNAVAILABLE');
  assert.notEqual(facts._projection_metadata.error, 'operational_inspection_read_only_violation');
  assert.ok(pg.st.log.includes('SAVEPOINT audit_nested_1'));
  assert.ok(pg.st.log.some(l => /canonical_business_snapshots/.test(l)));
  assert.ok(pg.st.log.includes('ROLLBACK TO SAVEPOINT audit_nested_1'));
  outerIntact(pg); assert.equal(boundary.writesBlocked, 0);
});

test('main(): repeatable-read read-only outer transaction, verified intact at the end, always rolled back', async () => {
  const pg = fakePg({ onSelect: () => ({ rows: [] }) });
  const out = [];
  const code = await audit.main({ pool: { connect: async () => pg }, out: s => out.push(s), argv: ['--json'],
    deps: { clientIntelligence: { loadApprovedClientIntelligence: async () => ({ summary: null }) } } });
  assert.equal(code, 1); // empty cohort is a FAIL, but the run itself completed
  const report = JSON.parse(out[0]);
  assert.equal(report.readOnly.endState, 'read_only'); assert.equal(report.readOnly.transaction, 'REPEATABLE READ, READ ONLY');
  assert.equal(pg.st.log[0], 'BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
  assert.equal(pg.st.log.at(-1), 'ROLLBACK'); assert.equal(pg.st.released, true);
  assert.equal(pg.st.log.filter(l => /^ROLLBACK$/.test(l)).length, 1);
});

test('main(): an escaped or unverifiable outer transaction withholds the report and exits 2', async () => {
  const pg = fakePg();
  let shows = 0;
  const leaky = { ...pg, async query(sql) {
    if (sql === 'SHOW transaction_read_only' && ++shows === 2) return { rows: [{ transaction_read_only: 'off' }] };
    return pg.query(sql);
  } };
  const out = [];
  const code = await audit.main({ pool: { connect: async () => leaky }, out: s => out.push(s), argv: [],
    deps: { clientIntelligence: { loadApprovedClientIntelligence: async () => ({ summary: null }) } } });
  assert.equal(code, 2); assert.match(out[0], /TRANSACTION_ESCAPE_OR_UNVERIFIED \(escaped\)\. Report withheld/);
  assert.equal(out.length, 1);
});
