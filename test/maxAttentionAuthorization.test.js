'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const express = require('express');
const { resolveAttentionScope } = require('../utils/maxAttentionAuthorization');
const admin = { id: 4, role: 'admin', active: true, client_id: 1 };
const manager = { id: 5, role: 'manager', active: true, client_id: 11 };
function fixture({ actor = admin, effective, rows = [admin, manager], activeClient = 11, clients = [1, 11], body = {}, query = {} } = {}) {
  const req = { session: { user: actor, active_client_id: activeClient }, body, query };
  if (effective) req.session.impersonation = { active: true, effectiveUser: effective };
  const calls = [];
  const db = { query: async (sql, args) => {
    calls.push({ sql, args });
    if (sql.includes('FROM users')) return { rows };
    assert.match(sql, /FROM clients WHERE id = \$1 AND active = true/);
    return { rows: clients.includes(args[0]) ? [{ id: args[0] }] : [] };
  } };
  return { req, db, calls };
}
async function rejects(options, code) {
  const { req, db } = fixture(options);
  await assert.rejects(resolveAttentionScope(req, db), error => error.code === code);
}

test('admin uses active authenticated tenant, never default1; consistent selectors accepted', async () => {
  for (const options of [{}, { query: { client_id: '11' } }, { body: { clientId: 11 } },
    { body: { client_id: 11, clientId: '11' }, query: { client_id: '11', clientId: '11' } }]) {
    const { req, db } = fixture(options);
    assert.equal(await resolveAttentionScope(req, db), 11);
  }
  const { req, db } = fixture({ activeClient: undefined });
  delete req.session.active_client_id;
  assert.equal(await resolveAttentionScope(req, db), 1); // explicitly bound DB tenant, not a default
});

test('admin query/body cannot escape selected tenant; aliases cannot mask conflicts', async () => {
  for (const options of [{ body: { client_id: 1 } }, { query: { client_id: '1' } },
    { body: { client_id: 11, clientId: 1 } }, { body: { client_id: 11 }, query: { client_id: '1' } }]) {
    await rejects(options, 'tenant_scope_mismatch');
  }
  for (const value of ['', null, true, [11], {}, '11x', '01', 0, -1, 1.5, '9007199254740992']) {
    await rejects({ body: { client_id: value } }, 'invalid_client_id');
  }
});

test('manager uses current DB tenant, not stale session or stale actor binding', async () => {
  const { req, db } = fixture({ actor: { ...manager, client_id: 1 }, activeClient: 1 });
  assert.equal(await resolveAttentionScope(req, db), 11);
  await rejects({ actor: manager, activeClient: 1, query: { client_id: '1' } }, 'tenant_scope_mismatch');
  await rejects({ actor: manager, rows: [{ ...manager, client_id: null }] }, 'authenticated_tenant_required');
});

test('revoked, demoted, unbound, legacy and inactive-tenant requests fail closed', async () => {
  await rejects({ rows: [] }, 'actor_access_revoked');
  for (const active of [false, null, undefined]) {
    await rejects({ rows: [{ ...admin, active }] }, 'actor_access_revoked');
    await rejects({ effective: manager, rows: [admin, { ...manager, active }] }, 'actor_access_revoked');
  }
  await rejects({ rows: [{ ...admin, role: 'viewer' }] }, 'attention_access_denied');
  await rejects({ actor: { role: 'admin', id: null } }, 'authenticated_actor_required');
  await rejects({ activeClient: 'bad' }, 'authenticated_tenant_required');
  await rejects({ clients: [] }, 'tenant_access_revoked');
});

test('effective identity is reloaded and cannot inherit authenticating admin authority', async () => {
  const { req, db } = fixture({ effective: manager, activeClient: 1 });
  assert.equal(await resolveAttentionScope(req, db), 11);
  await rejects({ effective: manager, body: { client_id: 1 } }, 'tenant_scope_mismatch');
  await rejects({ effective: { ...manager, role: 'admin' }, rows: [admin, { ...manager, role: 'ao' }] }, 'attention_access_denied');
  await rejects({ actor: manager, effective: admin }, 'impersonation_not_authorized');
  await rejects({ effective: manager, rows: [admin] }, 'actor_access_revoked');
});

test('actual API cycle and queue routes check scope before service dispatch', async () => {
  const f = fixture();
  const calls = [];
  const service = {
    runAttentionScheduler: async id => { calls.push(['cycle', id]); return { failed: 0 }; },
    listOperatorAttention: async id => { calls.push(['queue', id]); return []; },
    getAttentionHealth: async () => { throw new Error('not exercised'); },
  };
  const module = { exports: {} };
  const filename = path.join(__dirname, '../routes/maxAttention.js');
  // Execute the real authentication and identity middleware with DB/init-only
  // imports stubbed. No production database or initialization is available.
  const authModule = { exports: {} };
  const authDeps = {
    bcryptjs: {}, '../db': f.db,
    '../utils/userRoles': require('../utils/userRoles'),
    '../utils/usersRoleConstraint': { refreshUsersRoleConstraint: async () => { throw new Error('init forbidden'); } },
    '../utils/requestIdentity': require('../utils/requestIdentity'),
  };
  vm.compileFunction(fs.readFileSync(path.join(__dirname, '../middleware/auth.js'), 'utf8'),
    ['module', 'require'], { filename: 'middleware/auth.js' })(authModule, name => {
    assert.ok(Object.hasOwn(authDeps, name), name); return authDeps[name];
  });
  const deps = { express, '../db': f.db, '../utils/maxAttentionAuthorization': { resolveAttentionScope },
    '../services/maxAttentionService': service,
    '../middleware/auth': authModule.exports };
  vm.compileFunction(fs.readFileSync(filename, 'utf8'), ['module', 'require'], { filename })(module, name => {
    assert.ok(Object.hasOwn(deps, name), name); return deps[name];
  });
  const app = express(); app.use(express.json());
  app.use((req, _res, next) => { req.session = f.req.session; next(); });
  app.use(module.exports);
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  const base = `http://127.0.0.1:${server.address().port}/api/v1/max/attention`;
  try {
    assert.equal((await fetch(`${base}/cycle?client_id=1`, { method: 'POST' })).status, 403);
    assert.equal((await fetch(`${base}/operator-queue?client_id=bad`)).status, 400);
    assert.equal(calls.length, 0);
    assert.equal((await fetch(`${base}/cycle`, { method: 'POST' })).status, 200);
    assert.equal((await fetch(`${base}/operator-queue?client_id=11`)).status, 200);
    assert.deepEqual(calls, [['cycle', 11], ['queue', 11]]);
    f.req.session.impersonation = { active: true, effectiveUser: { ...manager, client_id: 1, role: 'admin' } };
    f.req.session.active_client_id = 1;
    assert.equal((await fetch(`${base}/cycle?client_id=1`, { method: 'POST' })).status, 403);
    assert.equal(calls.length, 2);
    assert.equal((await fetch(`${base}/cycle?client_id=11`, { method: 'POST' })).status, 200);
    assert.deepEqual(calls[2], ['cycle', 11]);
    delete f.req.session.impersonation;
    f.req.session.user = { role: 'admin', id: null };
    assert.equal((await fetch(`${base}/cycle`, { method: 'POST' })).status, 403);
    assert.equal(calls.length, 3);
    f.req.session = {};
    assert.equal((await fetch(`${base}/cycle`, { method: 'POST' })).status, 401);
    f.req.session.user = { id: 5, role: 'ao' };
    assert.equal((await fetch(`${base}/cycle`, { method: 'POST' })).status, 403);
    assert.equal(calls.length, 3);
  } finally { await new Promise(resolve => server.close(resolve)); }
});
