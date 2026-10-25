'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { readPreparationApprovalContext } = require('../services/anchorCrmPreparationApproval');
const revision = 'a'.repeat(40), env = { RAILWAY_GIT_COMMIT_SHA: revision };
const actor = { id: '3', role: 'admin', sessionId: 'synthetic-context-only' };
function fixture(change = () => {}) {
  const state = { session: { authenticated: true, active_client_id: 10,
    user: { id: 3, role: 'admin', client_id: null, email_verified: true, password_change_required: false } },
    user: { id: 3, role: 'admin', active: true, client_id: null, email_verified: true, password_change_required: false },
    unexpired: true, target: true, exists: true };
  change(state);
  const queries = [], releases = [];
  const pool = { async connect() { return { async query(input) {
    const sql = typeof input === 'string' ? input : input.text; queries.push(sql);
    if (/^(BEGIN READ ONLY|SET LOCAL|COMMIT|ROLLBACK)/.test(sql)) return { rows: [] };
    if (sql === 'SELECT client_id FROM users WHERE id::text=$1') return { rows: [{ client_id: state.user.client_id }] };
    if (sql.startsWith('SELECT sid,sess')) return { rows: state.exists ? [{ sid: actor.sessionId, sess: state.session }] : [] };
    if (sql.startsWith('SELECT expire')) return { rows: [{ unexpired: state.unexpired }] };
    if (sql.startsWith('SELECT id,role,active')) return { rows: [state.user] };
    if (sql.startsWith('SELECT id FROM clients')) return { rows: state.target ? [{ id: 10 }] : [] };
    assert.fail(`Unexpected query ${sql}`);
  }, release(destroy) { releases.push(destroy); } }; } };
  return { pool, queries, releases };
}
test('UI context is bounded, read-only, live-verified and excludes native session identifiers', async () => {
  const f = fixture();
  const result = await readPreparationApprovalContext(f.pool, actor, { env });
  assert.deepEqual(result, { revision, actorId: '3', tenantId: '10', role: 'admin' });
  assert.equal(f.queries[0], 'BEGIN READ ONLY');
  assert.match(f.queries[1], /lock_timeout='5s'.*statement_timeout='5s'/);
  assert.equal(f.queries.at(-1), 'COMMIT');
  assert.ok(f.queries.every(q => /^(SELECT|BEGIN READ ONLY|SET LOCAL|COMMIT)/.test(q)));
  assert.deepEqual(f.releases, [true]);
  assert.ok(!JSON.stringify(result).includes(actor.sessionId));
});
test('UI context rejects missing deployed revision and non-admin before database access', async () => {
  const pool = { connect() { assert.fail('Must fail before connection'); } };
  for (const badEnv of [{}, { RAILWAY_GIT_COMMIT_SHA: 'main' }])
    await assert.rejects(readPreparationApprovalContext(pool, actor, { env: badEnv }));
  await assert.rejects(readPreparationApprovalContext(pool, { ...actor, role: 'manager' }, { env }));
});
test('UI context never advertises authorization from a stale middleware identity', async t => {
  for (const [name, change] of [
    ['deleted session', s => { s.exists = false; }],
    ['expired session', s => { s.unexpired = false; }],
    ['wrong active tenant', s => { s.session.active_client_id = 1; }],
    ['user disabled', s => { s.user.active = false; }],
    ['role revoked', s => { s.user.role = 'ao'; }],
    ['account moved', s => { s.user.client_id = 11; }],
    ['email unverified', s => { s.user.email_verified = false; }],
    ['password change required', s => { s.user.password_change_required = true; }],
    ['target disabled', s => { s.target = false; }],
  ]) await t.test(name, async () => {
    const f = fixture(change);
    await assert.rejects(readPreparationApprovalContext(f.pool, actor, { env }));
    assert.equal(f.queries.at(-1), 'ROLLBACK'); assert.deepEqual(f.releases, [true]);
  });
});
