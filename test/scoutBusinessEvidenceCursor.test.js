'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createFakePool } = require('./helpers/scoutEvidenceCursorFakePool');
const { createBusinessEvidenceCursorStore, candidateKey, CURSOR_VERSION, EVENT_TYPE, LOCK_NAMESPACE,
  assertCursorTenantBindings } = require('../services/scoutBusinessEvidenceCursor');

function companies(count = 41) {
  return Array.from({ length: count }, (_, i) => ({ name: `Fixture ${i}`, website: `https://fixture${i}.example` }));
}
function cursor(pool, tenantId = '10', fingerprint = 'mission-fixture') {
  return createBusinessEvidenceCursorStore({ pool, tenantId, fingerprint });
}
async function pass(store, rows = companies(), options = {}) {
  const visited = [];
  const output = await store.withPass(async selection => {
    selection.assertTenant(options.tenantId || '10');
    selection.select(rows);
    for (const row of rows) if (selection.take(row)) visited.push(candidateKey(row));
    if (options.after) await options.after(selection);
    return { checkpointComplete: false };
  });
  return { ...output, visited };
}

test('three durable cursor passes reach every identity, preserve order, cap distinct slots and retain immutable inputs', async () => {
  const pool = createFakePool();
  const rows = companies(); rows.push({ ...rows[0], website: 'https://WWW.FIXTURE0.EXAMPLE/about' });
  const original = structuredClone(rows); const all = new Set();
  for (let i = 1; i <= 3; i++) {
    const result = await pass(cursor(pool), rows);
    assert.equal(result.visited.length, 20); assert.equal(new Set(result.visited).size, 20);
    assert.deepEqual(result.progression, { selected: 20, investigated: 20, advanced: true, sequence: i });
    assert.equal(result.result.checkpointComplete, false);
    result.visited.forEach(key => all.add(key));
    const distinctInputOrder = [...new Set(rows.map(candidateKey))].filter(key => result.visited.includes(key));
    assert.deepEqual(result.visited, distinctInputOrder);
  }
  assert.equal(all.size, 41); assert.deepEqual(rows, original); assert.equal(pool.events.size, 3);
  assert.ok(pool.calls.filter(c => /pg_try/.test(c.sql)).every(c => c.params[0] === LOCK_NAMESPACE));
  assert.notEqual(LOCK_NAMESPACE, 261020);
});

for (const size of [21, 157]) test(`cursor fairly visits ${size} immutable identities within capped passes`, async () => {
  const pool = createFakePool(); const rows = companies(size); const seen = new Set();
  for (let i = 0; i < Math.ceil(size / 20); i++) {
    const result = await pass(cursor(pool), rows);
    assert.equal(result.visited.length, 20);
    result.visited.forEach(key => seen.add(key));
  }
  assert.equal(seen.size, size);
});

test('changed candidate sets and a removed endpoint continue from the durable key and wrap', async () => {
  const pool = createFakePool(); const initial = companies();
  await pass(cursor(pool), initial);
  const endpoint = [...pool.events.values()][0].payload.afterKey;
  const nextRows = [...initial.filter(row => candidateKey(row) !== endpoint), ...companies(43).slice(41)];
  const original = structuredClone(nextRows); const seen = new Set();
  for (let i = 0; i < 3; i++) {
    const result = await pass(cursor(pool), nextRows);
    result.visited.forEach(key => seen.add(key));
  }
  assert.equal(seen.size, 42); assert.deepEqual(nextRows, original);
});

test('empty selection does not append a cursor or claim advancement', async () => {
  const pool = createFakePool(); const result = await pass(cursor(pool), []);
  assert.deepEqual(result.progression, { selected: 0, investigated: 0, advanced: false, sequence: 0 });
  assert.equal(pool.events.size, 0); assert.equal(pool.releases.length, 0);
});

test('tenant, mission, version and globally unique event identities are isolated', async () => {
  const pool = createFakePool();
  await pass(cursor(pool)); await pass(cursor(pool, '13'), companies(), { tenantId: '13' });
  await pass(cursor(pool, '10', 'another-mission'));
  assert.equal(pool.events.size, 3);
  assert.ok([...pool.events.values()].every(row => row.payload.sequence === 1 && row.event_type === EVENT_TYPE
    && row.payload.cursorVersion === CURSOR_VERSION));
  const alternateVersion = structuredClone([...pool.events.values()][0]);
  alternateVersion.id = 'alternate-version'; alternateVersion.payload.cursorVersion = 'another-version';
  alternateVersion.payload.sequence = 999;
  pool.events.set(alternateVersion.id, alternateVersion);
  assert.equal((await pass(cursor(pool))).progression.sequence, 2, 'other versions never become this cursor head');
  await assert.rejects(pass(cursor(pool, '13')), /tenant mismatch/);
});

test('small-capacity pool remains available for callback/global helper queries', async () => {
  const pool = createFakePool({ capacity: 1, onQuery: async sql => {
    assert.equal(sql, 'SYNTHETIC HELPER'); return { rows: [] };
  } });
  await pass(cursor(pool), companies(), { after: async () => {
    assert.equal(pool.active, 0); await pool.query('SYNTHETIC HELPER');
    const other = await pool.connect(); await other.query('SYNTHETIC HELPER'); other.release(false);
  } });
  assert.equal(pool.active, 0);
});

test('two snapshots may repeat bounded work but a delayed writer cannot overwrite two newer receipts', async () => {
  const pool = createFakePool(); let unblock; let started;
  const ready = new Promise(resolve => { started = resolve; });
  const wait = new Promise(resolve => { unblock = resolve; });
  const slow = pass(cursor(pool), companies(), { after: async () => { started(); await wait; } });
  const rejected = assert.rejects(slow, /snapshot is stale/);
  await ready;
  const first = await pass(cursor(pool)); const second = await pass(cursor(pool));
  assert.equal(first.progression.sequence, 1); assert.equal(second.progression.sequence, 2);
  unblock(); await rejected;
  assert.equal(pool.events.size, 2);
  const restart = await pass(cursor(pool)); assert.equal(restart.progression.sequence, 3);
});

test('interrupted callback cannot append or retain a usable pass handle', async () => {
  const pool = createFakePool(); let handle;
  await assert.rejects(pass(cursor(pool), companies(), { after: selection => {
    handle = selection; throw Error('interrupted persistence');
  } }), /interrupted persistence/);
  assert.equal(pool.events.size, 0); assert.equal(pool.releases.length, 0);
  assert.throws(() => handle.take(companies()[0]), /closed/);
  assert.equal((await pass(cursor(pool))).progression.sequence, 1);
});

for (const [fault, message] of [['busy', /commit is busy/], ['insertFailure', /insert failure/],
  ['unconfirmed', /not confirmed/], ['conflictingReceipt', /not confirmed/]]) {
  test(`${fault} rolls back without cursor advancement`, async () => {
    const pool = createFakePool(); pool.faults[fault] = true;
    await assert.rejects(pass(cursor(pool)), message);
    assert.equal(pool.events.size, 0); assert.equal(pool.active, 0);
    assert.equal(pool.calls.at(-1).sql, 'ROLLBACK');
    delete pool.faults[fault]; assert.equal((await pass(cursor(pool))).progression.sequence, 1);
  });
}

test('unknown commit result throws; restart reads durable receipt rather than resetting cursor', async () => {
  const pool = createFakePool(); pool.faults.uncertainCommit = true;
  await assert.rejects(pass(cursor(pool)), /unknown commit/);
  assert.equal(pool.events.size, 1);
  delete pool.faults.uncertainCommit;
  assert.equal((await pass(cursor(pool))).progression.sequence, 2);
});

test('failed rollback destroys client and never reports successful progression', async () => {
  const pool = createFakePool(); pool.faults.insertFailure = true; pool.faults.rollbackFailure = true;
  await assert.rejects(pass(cursor(pool)), /insert failure/);
  assert.deepEqual(pool.releases, [true]);
});

test('invalid stored cursor or read failure fails closed before candidate work', async () => {
  const pool = createFakePool(); await pass(cursor(pool));
  const row = [...pool.events.values()][0]; row.payload.afterKey = 'invalid';
  let calls = 0;
  await assert.rejects(cursor(pool).withPass(async () => { calls++; }), /Invalid.*receipt/);
  assert.equal(calls, 0);
  pool.events.clear(); pool.faults.readFailure = true;
  await assert.rejects(cursor(pool).withPass(async () => { calls++; }), /read failure/); assert.equal(calls, 0);
});

test('cursor cannot mask mismatched context, store or program tenant bindings', () => {
  assert.doesNotThrow(() => assertCursorTenantBindings({ tenantId: '10', store: { tenantId: '10', clientId: 10 },
    scoutContext: { authorizedTenantId: '10', program: { tenant_id: '10' } } }));
  for (const input of [
    { store: { tenantId: '13' }, scoutContext: {} },
    { store: {}, scoutContext: { clientId: 13 } },
    { store: {}, scoutContext: { governedContext: { transport: { tenantId: '13' } } } },
    { store: {}, scoutContext: { program: { policy: { tenantId: '13' } } } },
  ]) assert.throws(() => assertCursorTenantBindings({ tenantId: '10', ...input }), /binding mismatch/);
});

test('malformed cursor scope fails before database reads or namespace coercion', () => {
  const pool = createFakePool();
  for (const tenantId of [{}, [], true, '', ' 10 ', NaN, 0, -1, 1.5]) {
    assert.throws(() => cursor(pool, tenantId), /requires pool, tenant and mission fingerprint/);
  }
  for (const fingerprint of [{}, [], true, 10, '', ' padded ', null, 'x'.repeat(257)]) {
    assert.throws(() => cursor(pool, '10', fingerprint), /requires pool, tenant and mission fingerprint/);
  }
  assert.equal(pool.calls.length, 0);
});
