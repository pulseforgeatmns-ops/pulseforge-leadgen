'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const express = require('express');
const http = require('node:http');
const session = require('express-session');
const PgStore = require('connect-pg-simple')(session);
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { makeHarness, fixturePassword } = require('./helpers/loginIdentityHarness');
const { createIdentityTenantSelectionRouter, FORM_PATH, canonicalOrigin } = require('../routes/identityTenantSelection');
const { createIdentityTenantSelection } = require('../services/identityTenantSelection');
const ORIGIN = 'https://identity.example.test';
const HOST = 'identity.example.test';
const clone = value => JSON.parse(JSON.stringify(value));
// Node's fetch deliberately overrides Host. Use a loopback-only HTTP client to
// exercise canonical proxy Host checks accurately without any external traffic.
function fixtureFetch(url, options = {}) {
  const parsed = new URL(url);
  assert.equal(parsed.hostname, '127.0.0.1');
  return new Promise((resolve, reject) => {
    const request = http.request(parsed, { method: options.method || 'GET', headers: options.headers }, response => {
      const chunks = []; response.on('data', chunk => chunks.push(chunk));
      response.on('end', () => resolve(new Response(Buffer.concat(chunks), {
        status: response.statusCode, headers: Object.fromEntries(Object.entries(response.headers)
          .map(([key, value]) => [key, Array.isArray(value) ? value.join(', ') : value])),
      })));
    });
    request.on('error', reject);
    if (options.body !== undefined) request.write(String(options.body));
    request.end();
  });
}


// Real PostgreSQL and native express-session cookies, but exclusively disposable
// loopback fixtures. Never imports db.js, the app server, providers or startup.
test('identity-only tenant selection: real session-store and transaction boundaries', { timeout: 90000 }, async t => {
  const instance = await startDisposablePostgres('identity-tenant-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString });
  t.after(() => closePostgresFixture(pool, instance));
  await pool.query(`CREATE TABLE "session" (sid varchar PRIMARY KEY, sess json NOT NULL, expire timestamp NOT NULL);
    CREATE TABLE users (id integer PRIMARY KEY, role text, active boolean, client_id integer,
      email_verified boolean, password_change_required boolean);
    CREATE TABLE clients (id integer PRIMARY KEY, name text, active boolean, operator_switchable boolean);
    INSERT INTO users VALUES (3,'admin',true,null,true,false);
    INSERT INTO clients VALUES (1,'Current tenant',true,true),(10,'Anchor <fixture>',true,true),(11,'Other tenant',true,true)`);
  const queries = []; const stores = []; const servers = [];
  let failAfterCommit = false;
  const tracedPool = { async connect() {
    const db = await pool.connect();
    return { release: () => db.release(), query: async (sql, args) => {
      queries.push(sql.replace(/\s+/g, ' ').trim());
      const result = await db.query(sql, args);
      if (failAfterCommit && sql === 'COMMIT') throw Error('fixture lost acknowledgement');
      return result;
    } };
  } };
  let appUrl = ORIGIN;
  async function appInstance() {
    const h = makeHarness({ user: { id: 3 } });
    const app = express(); const store = new PgStore({ pool, tableName: 'session', pruneSessionInterval: false });
    const calls = { set: 0, touch: 0, destroy: 0 };
    for (const key of Object.keys(calls)) {
      const original = store[key].bind(store);
      store[key] = (...args) => { calls[key]++; return original(...args); };
    }
    stores.push(store);
    app.use(session({ store, secret: 'disposable-fixture-not-production', resave: false,
      saveUninitialized: false, cookie: { maxAge: 86400000 } }));
    app.use(createIdentityTenantSelectionRouter({ Router: express.Router, pool: tracedPool, appUrl: () => appUrl }));
    app.use(express.json()); app.use(express.urlencoded({ extended: true }));
    // Same ordering as production: this legacy normalizer must not run on the new path.
    app.use((req, _res, next) => {
      if (req.session?.authenticated && !req.session.active_client_id) req.session.active_client_id = 1;
      next();
    });
    for (const [key, fns] of Object.entries(h.handlers)) {
      const [method, route] = key.split(' '); app[method.toLowerCase()](route, ...fns);
    }
    app.use((_req, res) => res.status(418).send('Unexpected route'));
    const server = app.listen(0, '127.0.0.1'); servers.push(server);
    await new Promise(resolve => server.once('listening', resolve));
    return { base: `http://127.0.0.1:${server.address().port}`, calls };
  }
  const first = await appInstance(); const second = await appInstance();
  t.after(async () => { for (const s of servers) await new Promise(resolve => s.close(resolve)); for (const s of stores) s.close(); });
  let cookie;
  const stored = async () => (await pool.query('SELECT sid, sess, expire FROM "session"')).rows[0];
  async function setSession(change) {
    const row = await stored(); const sess = clone(row.sess); change(sess);
    await pool.query('UPDATE "session" SET sess=$1 WHERE sid=$2', [sess, row.sid]);
  }
  async function reset() {
    appUrl = ORIGIN; failAfterCommit = false;
    await pool.query(`DELETE FROM "session";
      UPDATE users SET role='admin',active=true,client_id=null,email_verified=true,password_change_required=false WHERE id=3;
      UPDATE clients SET active=true,operator_switchable=true WHERE id=10`);
    const response = await fixtureFetch(first.base + '/login', { method: 'POST', redirect: 'manual',
      headers: { host: HOST, 'content-type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ email: 'identity@example.test', password: fixturePassword, workflow: 'identity-check' }) });
    assert.equal(response.status, 302); assert.equal(response.headers.get('location'), '/api/me');
    cookie = response.headers.get('set-cookie').split(';')[0];
    queries.length = 0;
    for (const h of [first, second]) Object.keys(h.calls).forEach(key => { h.calls[key] = 0; });
  }
  function request(method, body, extra = {}) {
    const { instance: server = first, headers = {}, query = '', ...rest } = extra;
    return fixtureFetch(server.base + FORM_PATH + query, { method, redirect: 'manual',
      headers: { host: HOST, cookie, ...(method === 'POST' ? { origin: ORIGIN, 'content-type': 'application/x-www-form-urlencoded' } : {}), ...headers },
      ...(body === undefined ? {} : { body: typeof body === 'string' ? body : new URLSearchParams(body) }), ...rest });
  }
  async function form() {
    const response = await request('GET'); assert.equal(response.status, 200, await response.clone().text());
    const html = await response.text(); const csrf = html.match(/name="csrf" value="([a-f0-9]{64})"/)[1];
    return { response, html, body: { workflow: 'identity-tenant-check', client_id: '10', confirm: 'select-anchor', csrf } };
  }
  function noStoreWrites() {
    for (const h of [first, second]) assert.deepEqual(h.calls, { set: 0, touch: 0, destroy: 0 });
  }
  async function rejectedWithoutChange(body, extra, expected) {
    const before = await stored(); const response = await request('POST', body, extra);
    assert.equal(response.status, expected, await response.clone().text()); assert.equal(response.headers.get('location'), null);
    assert.deepEqual(await stored(), before); noStoreWrites();
    return response;
  }

  await t.test('GET only issues a bounded challenge; POST commits session-only change then fixed303; /api/me reads active10', async () => {
    await reset(); await setSession(s => { s.unrelated = { preserve: true }; });
    const before = await stored(); const { response, html, body } = await form();
    assert.equal(response.headers.get('set-cookie'), null); assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.match(response.headers.get('content-security-policy'), /form-action 'self'/);
    assert.equal(response.headers.get('referrer-policy'), 'same-origin');
    assert.match(html, /Anchor &lt;fixture&gt;/); assert.doesNotMatch(html, /<script|src=|fetch\(|command-deck|dashboard\.js/);
    const issued = await stored(); assert.equal(issued.sess.active_client_id, 1);
    assert.equal(issued.expire.getTime(), before.expire.getTime());
    assert.equal(issued.sess.identityTenantSelection.digest.includes(body.csrf), false);
    assert.ok(issued.sess.identityTenantSelection.expiresAt <= issued.expire.getTime());
    const selected = await request('POST', body);
    assert.equal(selected.status, 303); assert.equal(selected.headers.get('location'), '/api/me');
    assert.equal(selected.headers.get('set-cookie'), null);
    const after = await stored(); const expected = clone(before.sess); expected.active_client_id = 10;
    assert.deepEqual(after.sess, expected); assert.equal(after.expire.getTime(), before.expire.getTime()); noStoreWrites();
    assert.ok(queries.every(q => /^(BEGIN|SET LOCAL|SELECT|UPDATE "session"|COMMIT)/.test(q)));
    assert.ok(queries.filter(q => q.startsWith('UPDATE')).every(q => q.startsWith('UPDATE "session" SET sess')));
    const me = await fixtureFetch(first.base + '/api/me', { headers: { host: HOST, cookie } });
    assert.equal(me.status, 200); const account = await me.json(); assert.equal(account.active_client_id, 10);
    assert.equal(account.user.id, 3); assert.equal(account.user.role, 'admin');
  });

  for (const [name, mutate] of [
    ['demoted current DB user', () => pool.query("UPDATE users SET role='manager' WHERE id=3")],
    ['inactive current DB user', () => pool.query('UPDATE users SET active=false WHERE id=3')],
    ['password change required', () => pool.query('UPDATE users SET password_change_required=true WHERE id=3')],
    ['email unverified', () => pool.query('UPDATE users SET email_verified=false WHERE id=3')],
    ['target inactive', () => pool.query('UPDATE clients SET active=false WHERE id=10')],
    ['target hidden', () => pool.query('UPDATE clients SET operator_switchable=false WHERE id=10')],
  ]) await t.test(`${name}: no tenant change or token consumption`, async () => {
    await reset(); const { body } = await form(); await mutate(); await rejectedWithoutChange(body, {}, 403);
  });

  await t.test('missing target and missing operator_switchable column both fail closed without initialization', async () => {
    await reset(); const { body } = await form();
    await pool.query('UPDATE clients SET id=12 WHERE id=10');
    await rejectedWithoutChange(body, {}, 403); await pool.query('UPDATE clients SET id=10 WHERE id=12');
    await pool.query('ALTER TABLE clients RENAME COLUMN operator_switchable TO fixture_hidden_column');
    try { await rejectedWithoutChange(body, {}, 503); }
    finally { await pool.query('ALTER TABLE clients RENAME COLUMN fixture_hidden_column TO operator_switchable'); }
  });
  await t.test('existing NULL operator-switchable semantics retained, no permissive schema fallback', async () => {
    await reset(); await pool.query('UPDATE clients SET operator_switchable=null WHERE id=10');
    const { body } = await form(); assert.equal((await request('POST', body)).status, 303); noStoreWrites();
  });

  for (const [name, change] of [
    ['active tenant changed', s => { s.active_client_id = 11; }],
    ['session account binding changed', s => { s.user.client_id = 11; }],
    ['authenticated actor changed', s => { s.user.id = 4; }],
    ['session role downgraded', s => { s.user.role = 'manager'; }],
    ['impersonation started', s => { s.impersonation = { active: true, effectiveUser: { id: 4, role: 'admin' } }; }],
    ['malformed impersonation present', s => { s.impersonation = { active: true }; }],
  ]) await t.test(`${name}: stale form rejected`, async () => {
    await reset(); const { body } = await form(); await setSession(change);
    const before = await stored(); const response = await request('POST', body);
    assert.ok([403, 409].includes(response.status)); assert.deepEqual(await stored(), before); noStoreWrites();
  });
  await t.test('DB client binding changed after form: stale form rejected', async () => {
    await reset(); const { body } = await form(); await pool.query('UPDATE users SET client_id=11 WHERE id=3');
    await rejectedWithoutChange(body, {}, 409);
  });
  for (const [name, change] of [
    ['legacy session', s => { delete s.user; }],
    ['missing active client', s => { delete s.active_client_id; }],
    ['invalid active client', s => { s.active_client_id = '10x'; }],
    ['invalid actor', s => { s.user.id = '3x'; }],
    ['unbound manager', s => { s.user.role = 'manager'; s.user.client_id = null; }],
    ['other-tenant client', s => { s.user.role = 'client'; s.user.client_id = 11; }],
  ]) await t.test(`${name}: GET does not repair or broaden authorization`, async () => {
    await reset(); await setSession(change); const before = await stored(); const response = await request('GET');
    assert.ok([401, 403].includes(response.status)); assert.deepEqual(await stored(), before); noStoreWrites();
  });
  await t.test('unauthenticated request creates no session and no CSRF challenge', async () => {
    await reset(); const before = await stored(); const response = await request('GET', undefined, { headers: { cookie: '' } });
    assert.equal(response.status, 403); assert.equal(response.headers.get('set-cookie'), null);
    assert.deepEqual(await stored(), before); noStoreWrites();
  });

  for (const [name, headers, status] of [
    ['missing Origin', { origin: '' }, 403], ['null Origin', { origin: 'null' }, 403],
    ['foreign Origin', { origin: 'https://attacker.example' }, 403],
    ['same-site sibling', { origin: 'https://sibling.identity.example.test' }, 403],
    ['wrong Host', { host: 'attacker.example' }, 403],
    ['spoofed forwarded origin', { origin: 'https://attacker.example', 'x-forwarded-host': HOST, 'x-forwarded-proto': 'https' }, 403],
    ['cross-site fetch', { 'sec-fetch-site': 'cross-site' }, 403],
    ['JSON instead of native form', { 'content-type': 'application/json' }, 415],
  ]) await t.test(`${name}: no session write`, async () => {
    await reset(); const { body } = await form();
    await rejectedWithoutChange(headers['content-type'] ? JSON.stringify(body) : body, { headers }, status);
  });
  for (const [name, change] of [
    ['wrong target', b => { b.client_id = '11'; }], ['numeric prefix', b => { b.client_id = '10x'; }],
    ['missing workflow', b => { delete b.workflow; }], ['wrong workflow', b => { b.workflow = 'identity-check'; }],
    ['missing explicit confirmation', b => { delete b.confirm; }], ['missing csrf', b => { delete b.csrf; }],
    ['wrong csrf', b => { b.csrf = 'f'.repeat(64); }], ['open redirect', b => { b.returnTo = 'https://attacker.example'; }],
    ['new role', b => { b.role = 'admin'; }], ['forged actor', b => { b.user_id = '4'; }],
  ]) await t.test(`${name}: explicit allowlist denies`, async () => {
    await reset(); const { body } = await form(); change(body);
    await rejectedWithoutChange(body, {}, name === 'wrong csrf' ? 409 : 400);
  });
  await t.test('duplicate/nested selectors rejected; query cannot change target or redirect', async () => {
    await reset(); const { body } = await form(); const encoded = new URLSearchParams(body).toString();
    await rejectedWithoutChange(encoded + '&client_id=11', {}, 400);
    await rejectedWithoutChange(encoded.replace('client_id=10', 'client_id%5Bx%5D=10'), {}, 400);
    await rejectedWithoutChange(body, { query: '?returnTo=https://attacker.example' }, 400);
  });
  await t.test('HEAD/PUT/OPTIONS cannot issue or consume a form', async () => {
    await reset(); const before = await stored();
    for (const method of ['HEAD', 'PUT', 'OPTIONS']) assert.equal((await request(method)).status, 405);
    assert.deepEqual(await stored(), before); noStoreWrites();
  });
  await t.test('missing/invalid configured canonical origin fails closed without host-derived fallback', async () => {
    await reset(); const before = await stored();
    for (const value of ['', 'not-a-url', 'http://identity.example.test', 'https://user:pass@identity.example.test']) {
      appUrl = value; assert.equal((await request('GET')).status, 503);
    }
    assert.deepEqual(await stored(), before); noStoreWrites(); appUrl = ORIGIN;
  });
  await t.test('new form invalidates the previous form; repeated submission cannot switch twice', async () => {
    await reset(); const old = await form(); const current = await form();
    await rejectedWithoutChange(old.body, {}, 409);
    assert.equal((await request('POST', current.body)).status, 303);
    await rejectedWithoutChange(current.body, {}, 409);
  });
  await t.test('parallel duplicate POSTs through independent routers serialize across replicas', async () => {
    await reset(); const { body } = await form();
    const results = await Promise.all([request('POST', body), request('POST', body, { instance: second })]);
    assert.deepEqual(results.map(r => r.status).sort(), [303, 409]);
    assert.equal((await stored()).sess.active_client_id, 10); noStoreWrites();
  });
  await t.test('expired nonce does not change session; expiry is checked again at final write', async () => {
    await reset(); const { body } = await form();
    await setSession(s => { s.identityTenantSelection.issuedAt -= 300001; s.identityTenantSelection.expiresAt -= 300001; });
    await rejectedWithoutChange(body, {}, 409);
  });
  await t.test('expired stored session cannot be resurrected; retained browser cookie is not authority', async () => {
    await reset(); const { body } = await form();
    await pool.query('UPDATE "session" SET expire=clock_timestamp()-interval \'1 second\'');
    const before = await stored(); const response = await request('POST', body);
    assert.equal(response.status, 403); assert.deepEqual(await stored(), before); noStoreWrites();
  });
  await t.test('session deleted after native middleware load cannot be recreated', async () => {
    await reset(); const { body } = await form(); const row = await stored();
    await pool.query('DELETE FROM "session"');
    const service = createIdentityTenantSelection({ pool: tracedPool });
    await assert.rejects(service.select({ sessionId: row.sid, actorId: 3, fromClientId: 1, sessionClientId: null,
      origin: ORIGIN, csrf: body.csrf }), { code: 'session_expired' });
    assert.equal((await pool.query('SELECT COUNT(*)::integer AS n FROM "session"')).rows[0].n, 0);
  });
  await t.test('expiry while waiting on session lock fails after the wait, leaving data unchanged', async () => {
    await reset(); const { body } = await form(); const row = await stored(); const blocker = await pool.connect();
    await blocker.query('BEGIN'); await blocker.query('SELECT sess FROM "session" WHERE sid=$1 FOR UPDATE', [row.sid]);
    const service = createIdentityTenantSelection({ pool: tracedPool });
    const pending = service.select({ sessionId: row.sid, actorId: 3, fromClientId: 1, sessionClientId: null,
      origin: ORIGIN, csrf: body.csrf });
    await new Promise(resolve => setTimeout(resolve, 30));
    await blocker.query('UPDATE "session" SET expire=clock_timestamp()-interval \'1 second\' WHERE sid=$1', [row.sid]);
    await blocker.query('COMMIT'); blocker.release();
    await assert.rejects(pending, { code: 'session_expired' });
    assert.deepEqual((await stored()).sess, row.sess);
  });
  await t.test('known precommit write failure rolls back; COMMIT acknowledgement loss is explicitly uncertain', async () => {
    await reset(); const { body } = await form(); const row = await stored();
    const options = { sessionId: row.sid, actorId: 3, fromClientId: 1, sessionClientId: null, origin: ORIGIN, csrf: body.csrf };
    const failingPool = mode => ({ async connect() {
      const db = await pool.connect(); return { release: () => db.release(), async query(sql, params) {
        if (mode === 'before' && sql.startsWith('UPDATE')) throw Error('fixture write failure');
        const result = await db.query(sql, params);
        if (mode === 'after' && sql === 'COMMIT') throw Error('fixture lost commit acknowledgement');
        return result;
      } };
    } });
    await assert.rejects(createIdentityTenantSelection({ pool: failingPool('before') }).select(options), { code: 'identity_selection_unavailable' });
    assert.deepEqual(await stored(), row);
    await assert.rejects(createIdentityTenantSelection({ pool: failingPool('after') }).select(options), { code: 'selection_outcome_unknown' });
    assert.equal((await stored()).sess.active_client_id, 10); // Never report a false rollback after uncertain COMMIT.
  });
  await t.test('HTTP COMMIT acknowledgement loss never redirects success or retries a saved selection', async () => {
    await reset(); const { body } = await form(); failAfterCommit = true;
    const response = await request('POST', body); failAfterCommit = false;
    assert.equal(response.status, 503); assert.equal(response.headers.get('location'), null);
    const result = await response.json(); assert.equal(result.error, 'selection_outcome_unknown');
    assert.match(result.message, /without resubmitting/); assert.equal((await stored()).sess.active_client_id, 10);
    noStoreWrites(); await rejectedWithoutChange(body, {}, 409);
  });
  await t.test('challenge is capped at existing session expiry without extending it', async () => {
    await reset(); await pool.query('UPDATE "session" SET expire=clock_timestamp()+interval \'1 minute\'');
    const before = await stored(); await form(); const after = await stored();
    assert.equal(after.expire.getTime(), before.expire.getTime());
    assert.ok(after.sess.identityTenantSelection.expiresAt <= before.expire.getTime() + 1);
    assert.ok(after.sess.identityTenantSelection.expiresAt - after.sess.identityTenantSelection.issuedAt < 61000);
    noStoreWrites();
  });
  await t.test('nonce expiring during a user-row lock wait is rejected by the final UPDATE', async () => {
    await reset(); const { body } = await form();
    await setSession(s => { s.identityTenantSelection.expiresAt = Date.now() + 150; });
    const row = await stored(); const blocker = await pool.connect();
    await blocker.query('BEGIN'); await blocker.query('SELECT id FROM users WHERE id=3 FOR UPDATE');
    const service = createIdentityTenantSelection({ pool: tracedPool });
    const pending = service.select({ sessionId: row.sid, actorId: 3, fromClientId: 1, sessionClientId: null,
      origin: ORIGIN, csrf: body.csrf });
    await new Promise(resolve => setTimeout(resolve, 200)); await blocker.query('COMMIT'); blocker.release();
    await assert.rejects(pending, { code: 'session_or_form_expired' });
    assert.deepEqual((await stored()).sess, row.sess);
  });
  await t.test('oversized and over-parameterized form parser failures cannot touch the session', async () => {
    await reset(); const { body } = await form();
    await rejectedWithoutChange({ ...body, csrf: 'a'.repeat(3000) }, {}, 413);
    await rejectedWithoutChange(new URLSearchParams(body).toString() + '&a=1&b=1&c=1&d=1&e=1', {}, 413);
  });
  await t.test('cross-site GET and unknown GET selector cannot rotate an existing challenge', async () => {
    await reset(); await form(); const before = await stored();
    assert.equal((await request('GET', undefined, { headers: { 'sec-fetch-site': 'cross-site' } })).status, 403);
    assert.equal((await request('GET', undefined, { query: '?client_id=11' })).status, 400);
    assert.deepEqual(await stored(), before); noStoreWrites();
  });
  await t.test('route-only replay guarantee does not conceal legacy whole-session overwrite limitation', async () => {
    await reset(); const { body } = await form(); const stale = await stored();
    assert.equal((await request('POST', body)).status, 303);
    // Deliberately model an unrelated existing route's stale store.set. This is
    // not prevented by a route-local lock and is explicitly a retained limitation.
    await pool.query('UPDATE "session" SET sess=$1 WHERE sid=$2', [stale.sess, stale.sid]);
    assert.equal((await stored()).sess.active_client_id, 1);
    assert.equal((await request('POST', body)).status, 303);
  });
});

test('mounting and source boundaries preserve default flows without importing initializers', () => {
  const root = path.join(__dirname, '..'); const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  assert.ok(server.indexOf("require('./routes/identityTenantSelection')") < server.indexOf('req.session.authenticated && !req.session.active_client_id'));
  assert.ok(server.indexOf("require('./routes/identityTenantSelection')") < server.indexOf('app.use(express.json('));
  assert.match(server, /saveUninitialized: false/); assert.doesNotMatch(server, /unset:\s*['"]destroy/);
  for (const file of ['routes/identityTenantSelection.js', 'services/identityTenantSelection.js']) {
    const src = fs.readFileSync(path.join(root, file), 'utf8');
    assert.doesNotMatch(src, /require\(['"].*(?:tenantWorkspace|clientContext|auth|db)['"]\)/);
    assert.doesNotMatch(src, /(?:CREATE|ALTER|DELETE|INSERT)\s+(?:TABLE|INTO|FROM)|UPDATE\s+(?:users|clients)/);
  }
  assert.equal(canonicalOrigin('https://identity.example.test/path'), ORIGIN);
  assert.equal(canonicalOrigin('http://127.0.0.1:8080'), 'http://127.0.0.1:8080');
  assert.equal(canonicalOrigin('javascript:alert(1)'), null);
});
