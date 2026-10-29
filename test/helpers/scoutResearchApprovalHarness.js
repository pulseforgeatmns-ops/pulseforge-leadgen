'use strict';

// Synthetic HTTP fixture for the Scout research approval page and Remember me.
// Real login handlers (extracted from server.js), real express-session +
// connect-pg-simple on disposable PostgreSQL, the real remembered-session guard,
// the real page router and the real tenant-outreach research endpoints over the
// SPEC-SCOUT-P0-003 synthetic Tenant 10 state. No provider, network or send.
const path = require('node:path');
const express = require('express');
const session = require('express-session');
const signature = require('cookie-signature');
const PgStore = require('connect-pg-simple')(session);
const { startResearchOnlyHarness } = require('./scoutResearchOnlyFixture');
const { makeHarness, fixturePassword } = require('./loginIdentityHarness');

const SECRET = 'synthetic-approval-cookie-secret';
const ADMIN = { id: '71', email: 'identity@example.test', name: 'Synthetic Admin' };
const AO = { id: '72', email: 'ao@example.test', name: 'Synthetic AO' };
const OTHER_ADMIN = { id: '73', email: 'other-admin@example.test', name: 'Other Tenant Admin' };
const root = path.join(__dirname, '../..');
// Captured before denyExternalProviders replaces globalThis.fetch; only ever
// used against this fixture's own 127.0.0.1 server.
const loopbackFetch = globalThis.fetch;

async function startApprovalHarness() {
  const h = await startResearchOnlyHarness();
  const restores = [];
  let server, store;
  async function close() {
    if (server) await new Promise(resolve => server.close(resolve));
    if (store) await store.close();
    restores.reverse().forEach(restore => restore());
    delete process.env.APP_URL;
    await h.close();
  }
  try {
    const login = makeHarness({ user: { id: ADMIN.id } });
    login.pool.query = (sql, params) => h.pool.query(sql, params);
    await h.pool.query(`ALTER TABLE users ADD COLUMN name text, ADD COLUMN email text, ADD COLUMN password_hash text,
      ADD COLUMN last_login_at timestamptz`);
    await h.pool.query('UPDATE users SET name=$2,email=$3,password_hash=$4 WHERE id=$1', [ADMIN.id, ADMIN.name, ADMIN.email, login.user.password_hash]);
    await h.pool.query(`INSERT INTO users(id,client_id,role,active,name,email,password_hash) VALUES
      ($1,10,'ao',true,$2,$3,$4), ($5,NULL,'admin',true,$6,$7,$4)`,
    [AO.id, AO.name, AO.email, login.user.password_hash, OTHER_ADMIN.id, OTHER_ADMIN.name, OTHER_ADMIN.email]);
    // The fixture's seeded admin session row predates express-session use; give it a cookie.
    await h.pool.query(`UPDATE "session" SET sess=jsonb_set(sess::jsonb,'{cookie}',$2::jsonb)::json WHERE sid=$1`,
      ['synthetic-research-admin-session', JSON.stringify({ originalMaxAge: 86400000, expires: '2099-01-01T00:00:00.000Z',
        httpOnly: true, path: '/', sameSite: 'lax' })]);

    // Keep the scheduler/status modules (loaded by tenantOutreach) inert.
    for (const [modulePath, exports] of [['../../services/tenantOutreachScheduler', {}], ['../../services/governedMissionExecutionStatus', {}]]) {
      const id = require.resolve(modulePath), before = require.cache[id];
      require.cache[id] = { id, filename: id, loaded: true, exports };
      restores.push(() => { delete require.cache[id]; if (before) require.cache[id] = before; });
    }
    for (const p of ['../../routes/tenantOutreach', '../../routes/scoutResearchApproval', '../../middleware/auth']) {
      const id = require.resolve(p), before = require.cache[id]; delete require.cache[id];
      restores.push(() => { delete require.cache[id]; if (before) require.cache[id] = before; });
    }
    const remembered = require('../../utils/rememberedSession');
    const app = express();
    const requests = [];
    app.use((req, _res, next) => { requests.push({ method: req.method, path: req.path }); next(); });
    store = new PgStore({ pool: h.pool, createTableIfMissing: false, pruneSessionInterval: false });
    app.use(session({ store, secret: SECRET, resave: false, saveUninitialized: false, proxy: true,
      cookie: { maxAge: remembered.DEFAULT_MAX_AGE_MS, httpOnly: true, sameSite: 'lax', secure: 'auto' } }));
    app.use(remembered.createRememberedSessionGuard({ pool: h.pool }));
    app.use(require('../../routes/scoutResearchApproval').createScoutResearchApprovalRouter({ pool: h.pool }));
    app.use(express.json());
    app.use(express.urlencoded({ extended: true }));
    for (const [key, handlers] of Object.entries(login.handlers)) {
      const [method, route] = key.split(' '); app[method.toLowerCase()](route, ...handlers);
    }
    // Same logout as server.js.
    app.get('/logout', (req, res) => {
      const done = () => { res.clearCookie('connect.sid', { path: '/', httpOnly: true, sameSite: 'lax' }); res.redirect('/login'); };
      if (!req.session) return done();
      req.session.destroy(() => done());
    });
    app.use(require('../../routes/tenantOutreach'));
    app.get(['/command-deck', '/ao', '/dashboard'], (req, res) => res.type('html').send(`<h1>Landing ${req.path}</h1>`));
    app.get('/favicon.ico', (_req, res) => res.status(204).end());
    app.use((error, _req, res, _next) => res.status(500).json({ error: 'synthetic_fixture_error', message: error.message }));
    server = app.listen(0, '127.0.0.1');
    await new Promise((resolve, reject) => { server.once('listening', resolve); server.once('error', reject); });
    const origin = `http://127.0.0.1:${server.address().port}`;
    process.env.APP_URL = origin;

    // A cookie for an existing store row (used for the fixture's seeded admin session).
    const cookieFor = sid => `connect.sid=${encodeURIComponent(`s:${signature.sign(sid, SECRET)}`)}`;
    async function seedSession(sid, sess, expire = '2099-01-01') {
      await h.pool.query('INSERT INTO "session"(sid,sess,expire) VALUES($1,$2,$3) ON CONFLICT (sid) DO UPDATE SET sess=$2,expire=$3', [sid, sess, expire]);
      return cookieFor(sid);
    }
    async function http(pathname, { method = 'GET', cookie, body, headers = {}, form } = {}) {
      const init = { method, redirect: 'manual', headers: { ...headers } };
      if (cookie) init.headers.cookie = cookie;
      if (form) { init.headers['content-type'] = 'application/x-www-form-urlencoded'; init.body = new URLSearchParams(form).toString(); }
      else if (body !== undefined) { init.headers['content-type'] = 'application/json'; init.body = JSON.stringify(body); init.headers.origin ??= origin; }
      const response = await loopbackFetch(origin + pathname, init);
      const text = await response.text();
      let json = null; try { json = JSON.parse(text); } catch { /* html */ }
      return { status: response.status, headers: response.headers, text, json, setCookie: response.headers.getSetCookie() };
    }
    async function loginAs(user, { remember = false, headers } = {}) {
      const response = await http('/login', { method: 'POST', headers,
        form: { email: user.email, password: fixturePassword, ...(remember ? { remember: 'on' } : {}) } });
      const raw = response.setCookie.find(c => c.startsWith('connect.sid='));
      return { response, raw, cookie: raw ? raw.split(';')[0] : null };
    }
    const adminCookie = cookieFor('synthetic-research-admin-session');
    return { ...h, origin, http, loginAs, seedSession, cookieFor, adminCookie, requests, close, fixturePassword,
      users: { ADMIN, AO, OTHER_ADMIN }, root };
  } catch (error) { await close(); throw error; }
}

module.exports = { startApprovalHarness, ADMIN, AO, OTHER_ADMIN };
