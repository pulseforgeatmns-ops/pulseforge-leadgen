'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const identity = require('../services/anchorCrmPreparationIdentity');
function fixture(binding = null) {
  const sessionId = 'synthetic-native-session';
  const manifest = { version: 2, operatorId: '3', operatorRole: 'admin', preparationAuthority: {
    originalAuthorizerId: '19', approverAccountClientId: binding,
    approverSessionHash: identity.sessionBindingHash(sessionId) } };
  const f = { manifest, sessionId, sess: { authenticated: true, active_client_id: 10,
    user: { id: '3', role: 'admin', client_id: binding, email_verified: true, password_change_required: false } },
  user: { id: '3', role: 'admin', active: true, client_id: binding, email_verified: true, password_change_required: false },
  unexpired: true, target: true, queries: [], expiryChecks: 0 };
  f.db = { async query(sql, values) {
    f.queries.push(sql);
    assert.match(sql, /^SELECT /, 'identity checker never writes or calls providers');
    if (sql.startsWith('SELECT sid,sess')) {
      assert.match(sql, /WHERE sid=\$1/); assert.equal(values[0], sessionId);
      return { rows: f.sess ? [{ sid: sessionId, sess: f.sess }] : [] };
    }
    if (sql.startsWith('SELECT expire')) { f.expiryChecks++; return { rows: [{ unexpired: f.unexpired }] }; }
    if (sql.startsWith('SELECT id,role')) { if (f.expireDuringAccountLock) f.unexpired = false; return { rows: f.user ? [f.user] : [] }; }
    if (sql.startsWith('SELECT id FROM clients')) return { rows: f.target ? [{ id: 10 }] : [] };
    throw Error(sql);
  } };
  return f;
}
test('NULL platform-admin and tenant-bound admin require separately pinned native session and explicit selected tenant', async () => {
  for (const binding of [null, 10]) {
    const f = fixture(binding), before = JSON.stringify(f.manifest);
    assert.equal(await identity.assertPreparationApprover(f.db, f.manifest, { sessionId: f.sessionId }), f.user);
    assert.equal(f.expiryChecks, 2, 'real expiry rechecked after account/target locks');
    assert.equal(JSON.stringify(f.manifest), before);
    assert.ok(f.queries.filter(sql => /SELECT (sid,sess|id,role|id FROM clients)/.test(sql)).every(sql => sql.endsWith(' FOR SHARE')));
  }
});
test('session, account, tenant and original approver snapshot fail closed without writes', async () => {
  const cases = [f => { f.sess = null; }, f => { f.unexpired = false; }, f => { f.expireDuringAccountLock = true; },
    f => { f.sess.authenticated = false; }, f => { f.sess.user.id = '19'; }, f => { f.sess.user.role = 'ao'; },
    f => { f.sess.active_client_id = 13; }, f => { delete f.sess.active_client_id; },
    f => { f.sess.impersonation = { active: false }; }, f => { f.sess.user.client_id = 10; },
    f => { delete f.sess.user.client_id; }, f => { f.sess.user.password_change_required = true; },
    f => { f.sess.user.email_verified = false; }, f => { f.user = null; }, f => { f.user.active = false; },
    f => { f.user.role = 'ao'; }, f => { f.user.client_id = 10; }, f => { delete f.user.client_id; },
    f => { f.user.password_change_required = true; }, f => { f.user.email_verified = false; },
    f => { f.target = false; }, f => { f.manifest.operatorId = '19'; }];
  for (const mutate of cases) {
    const f = fixture(); mutate(f);
    await assert.rejects(identity.assertPreparationApprover(f.db, f.manifest, { sessionId: f.sessionId }), /approver_invalid/);
  }
});
test('missing, wrong, forged or changed invoking session/actor rejects before DB lookup', async () => {
  for (const options of [{}, { sessionId: 'different' }, { actor: { id: '19', role: 'admin', sessionId: 'synthetic-native-session' } },
    { actor: { id: '3', role: 'ao', sessionId: 'synthetic-native-session' } },
    { actor: { id: '3', role: 'admin' } }]) {
    const f = fixture();
    await assert.rejects(identity.assertPreparationApprover(f.db, f.manifest, options), /approver_invalid/);
    assert.equal(f.queries.length, 0);
  }
});
