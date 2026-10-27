'use strict';
// Actual extracted login/api/me routes and native PostgreSQL sessions. Only the
// timing/failure boundary of PgStore.set is gated; no auth or store substitute.
const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const express = require('express');
const session = require('express-session');
const PgStore = require('connect-pg-simple')(session);
const { makeHarness, fixturePassword } = require('./helpers/loginIdentityHarness');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const deny = require('./helpers/denyExternalProviders.cjs');

function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
function request(origin, path, { method = 'GET', body = '', cookie } = {}) {
  let seen = false; const headers = deferred(), done = deferred();
  const req = http.request(origin + path, { method, headers: {
    ...(body ? { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) } : {}),
    ...(cookie ? { Cookie: cookie } : {}),
  } }, res => {
    seen = true; const value = { status: res.statusCode, headers: res.headers, body: '' }; headers.resolve(value);
    res.setEncoding('utf8'); res.on('data', text => { value.body += text; });
    res.on('end', () => done.resolve(value));
  });
  req.on('error', error => { headers.resolve({ error }); done.resolve({ error }); }); req.end(body);
  return { headers: headers.promise, done: done.promise, seen: () => seen };
}
async function harness(t, options = {}) {
  const pg = await startDisposablePostgres('login-persist-');
  const pool = createTrackedPostgresPool({ connectionString: pg.connectionString });
  let store, server;
  t.after(async () => {
    if (server) await new Promise(resolve => server.close(resolve));
    if (store) await store.close();
    await closePostgresFixture(pool, pg);
    assert.equal(deny.attempts(), 0);
  });
  const h = makeHarness(options);
  await pool.query(`CREATE TABLE users(id integer PRIMARY KEY,name text,email text,password_hash text,role text,active boolean,client_id integer,email_verified boolean,password_change_required boolean,last_login_at timestamptz);
    CREATE TABLE clients(id integer PRIMARY KEY,name text);
    INSERT INTO clients VALUES(1,'Synthetic home'),(10,'Synthetic tenant');
    CREATE TABLE session(sid varchar PRIMARY KEY,sess json NOT NULL,expire timestamp NOT NULL);`);
  if (h.user) await pool.query('INSERT INTO users(id,name,email,password_hash,role,active,client_id,email_verified,password_change_required) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)',
    [h.user.id,h.user.name,h.user.email,h.user.password_hash,h.user.role,h.user.active,h.user.client_id,h.user.email_verified,h.user.password_change_required]);
  h.pool.query = pool.query.bind(pool);
  store = new PgStore({ pool, createTableIfMissing: false, pruneSessionInterval: false });
  const originalSet = store.set.bind(store), entered = deferred(), release = deferred(); let saveCalls = 0;
  store.set = (sid, value, callback) => {
    saveCalls++; entered.resolve();
    release.promise.then(() => options.failSave ? callback(new Error('synthetic-private-store-error')) :
      originalSet(sid, value, error => callback(error || (options.failAcknowledgement ? new Error('synthetic-private-save-ack-error') : null))));
  };
  t.after(() => release.resolve());
  const app = express(); app.use(express.urlencoded({ extended: false }));
  app.use(session({ store, secret: 'synthetic-only-login-persistence-secret', resave: false, saveUninitialized: false, cookie: { maxAge: 86400000 } }));
  for (const [key, handlers] of Object.entries(h.handlers)) { const [method, route] = key.split(' '); app[method.toLowerCase()](route, ...handlers); }
  app.use((error, req, res, next) => { if (!res.headersSent) res.status(500).send('synthetic-route-error'); });
  server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
  return { pool, origin: `http://127.0.0.1:${server.address().port}`, entered: entered.promise, release: release.resolve, saveCalls: () => saveCalls };
}
const post = (h, body = {}) => request(h.origin, '/login', { method: 'POST', body: new URLSearchParams({ email: 'identity@example.test', password: fixturePassword, workflow: 'identity-check', ...body }).toString() });

test('native login exposes no redirect/cookie before PostgreSQL session persistence; immediate identity read is exact', { timeout: 20000 }, async t => {
  const h = await harness(t); const pending = post(h); await h.entered;
  try {
    await new Promise(resolve => setTimeout(resolve, 100));
    assert.equal((await h.pool.query('SELECT count(*)::int n FROM session')).rows[0].n, 0);
    assert.equal(pending.seen(), false, 'success headers must wait for actual PgStore.set completion');
  } finally { h.release(); }
  const result = await pending.done; assert.equal(result.status, 302); assert.equal(result.headers.location, '/api/me');
  assert.equal(h.saveCalls(), 1);
  const stored = (await h.pool.query('SELECT sess FROM session')).rows; assert.equal(stored.length, 1);
  assert.equal(stored[0].sess.user.id, 73); assert.equal(stored[0].sess.authenticated, true); assert.equal(stored[0].sess.active_client_id, 1);
  const cookie = result.headers['set-cookie'][0].split(';')[0];
  const landed = await request(h.origin, '/api/me', { cookie }).done;
  assert.equal(landed.status, 200); const payload = JSON.parse(landed.body);
  assert.equal(payload.user.id, 73); assert.equal(payload.user.role, 'admin'); assert.equal(payload.active_client_id, 1);
});

test('failed PostgreSQL session save returns bounded generic failure without success redirect, cookie or implicit retry', { timeout: 20000 }, async t => {
  const h = await harness(t, { failSave: true }); const pending = post(h); await h.entered; h.release();
  const result = await pending.done;
  assert.equal(result.status, 503); assert.equal(result.headers.location, undefined); assert.equal(result.headers['set-cookie'], undefined);
  assert.equal(result.headers['cache-control'], 'no-store'); assert.doesNotMatch(result.body, /synthetic-private|password|sessionID|stack/i);
  assert.equal(h.saveCalls(), 1); assert.equal((await h.pool.query('SELECT count(*)::int n FROM session')).rows[0].n, 0);
  assert.equal((await request(h.origin, '/api/me').done).status, 401);
});

test('lost save acknowledgement remains a failed login response even if PostgreSQL committed; never retries or claims rollback', { timeout: 20000 }, async t => {
  const h = await harness(t, { failAcknowledgement: true }); const pending = post(h); await h.entered; h.release();
  const result = await pending.done;
  assert.equal(result.status, 503); assert.equal(result.headers.location, undefined); assert.equal(result.headers['set-cookie'], undefined);
  assert.equal(result.headers['cache-control'], 'no-store'); assert.doesNotMatch(result.body, /synthetic-private|rolled back|not saved/i);
  assert.equal(h.saveCalls(), 1);
  const stored = (await h.pool.query('SELECT sess FROM session')).rows;
  assert.equal(stored.length, 1); assert.equal(stored[0].sess.user.id, 73);
});

for (const [label, options, body, target] of [
  ['password-change guard', { user: { password_change_required: true } }, {}, '/change-password'],
  ['ordinary role landing', { user: { role: 'setter' } }, { workflow: '' }, '/setter'],
  ['legacy no-user fallback', { user: null, userCount: 0 }, { workflow: '', password: 'local-legacy-fixture' }, '/command-deck'],
]) test(`${label} also persists before its unchanged success redirect`, { timeout: 20000 }, async t => {
  const h = await harness(t, options); const pending = post(h, body); await h.entered;
  try { await new Promise(resolve => setTimeout(resolve, 75)); assert.equal(pending.seen(), false); }
  finally { h.release(); }
  const result = await pending.done; assert.equal(result.status, 302); assert.equal(result.headers.location, target); assert.equal(h.saveCalls(), 1);
  assert.equal((await h.pool.query('SELECT count(*)::int n FROM session')).rows[0].n, 1);
});
