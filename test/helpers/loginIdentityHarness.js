'use strict';

// Executes the real login and /api/me route declarations without importing
// server.js, the application pool, startup workers, providers or migrations.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const express = require('express');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const identity = require('../../utils/requestIdentity');
const roles = require('../../utils/userRoles');
const root = path.join(__dirname, '../..');
const source = file => fs.readFileSync(path.join(root, file), 'utf8');
const serverSource = source('server.js');
const apiSource = source('routes/api.js');
const fixturePassword = 'local-identity-check-fixture';
const fixtureHash = bcrypt.hashSync(fixturePassword, 4);

function between(text, start, end) {
  const a = text.indexOf(start); const b = text.indexOf(end, a + start.length);
  if (a < 0 || b < 0) throw new Error(`Route boundary missing: ${start}`);
  return text.slice(a, b);
}

function loadAuth(pool) {
  const module = { exports: {} };
  vm.runInNewContext(source('middleware/auth.js'), {
    module, process: { env: {} },
    require(id) {
      if (id === 'bcryptjs') return bcrypt;
      if (id === '../db') return pool;
      if (id === '../utils/userRoles') return roles;
      if (id === '../utils/requestIdentity') return identity;
      if (id === '../utils/usersRoleConstraint') return { refreshUsersRoleConstraint: async () => {} };
      if (id === '../services/pilotOnboarding') return {
        FAILURE: { PASSWORD_CHANGE_REQUIRED: { code: 'password_change_required', message: 'Change password' } },
      };
      throw new Error(`Unapproved auth dependency: ${id}`);
    },
  }, { filename: 'middleware/auth.js' });
  return module.exports;
}

function makeHarness(options = {}) {
  const user = options.user === null ? null : {
    id: 73, name: 'Local Fixture', email: 'identity@example.test',
    password_hash: fixtureHash, role: 'admin', active: true, client_id: null,
    email_verified: true, password_change_required: false, ...options.user,
  };
  const queries = []; let initCalls = 0;
  const pool = { async query(sql, params = []) {
    const text = sql.replace(/\s+/g, ' ').trim();
    queries.push({ text, params });
    if (text.startsWith('SELECT id, name, email, password_hash')) {
      return { rows: user && String(params[0]).toLowerCase() === user.email.toLowerCase() ? [user] : [] };
    }
    if (text.startsWith('UPDATE users SET last_login_at')) return { rows: [] };
    if (text.startsWith('SELECT * FROM clients')) return { rows: [{ id: params[0], name: 'Local Tenant' }] };
    throw new Error(`Unexpected fixture DB operation: ${text}`);
  } };
  const auth = loadAuth(pool);
  const clientModule = { exports: {} };
  vm.runInNewContext(source('utils/clientContext.js'), {
    module: clientModule, process: { env: {} },
    require(id) {
      if (id === '../db') return pool;
      if (id === './canonicalVerticals') return require('../../utils/canonicalVerticals');
      throw new Error(`Unapproved client dependency: ${id}`);
    },
  }, { filename: 'utils/clientContext.js' });
  const impersonationModule = { exports: {} };
  vm.runInNewContext(source('services/aoImpersonationService.js'), {
    module: impersonationModule,
    require(id) {
      if (id === '../db') return pool;
      if (id === '../utils/requestIdentity') return identity;
      if (id === '../utils/clientContext') return clientModule.exports;
      if (id === '../utils/tenantAuthorization') return {};
      if (id === '../utils/aoAuditEvents') return { logAoAuditEvent: () => { throw new Error('Unexpected audit write'); } };
      throw new Error(`Unapproved impersonation dependency: ${id}`);
    },
  }, { filename: 'services/aoImpersonationService.js' });
  const handlers = {};
  const app = {
    get(route, ...fns) { handlers[`GET ${route}`] = fns; },
    post(route, ...fns) { handlers[`POST ${route}`] = fns; },
  };
  vm.runInNewContext(between(serverSource, "app.get('/login'", "app.get('/logout'"), {
    app, pool, bcrypt,
    process: { env: { DASHBOARD_PASSWORD: 'local-legacy-fixture' } },
    initAuth: async () => { initCalls++; if (options.initFailure) throw new Error('cached initializer failure'); },
    getUserCount: async () => options.userCount ?? 1,
  }, { filename: 'server-login-routes.js' });
  vm.runInNewContext(between(apiSource, "router.get('/api/me'", "router.get('/api/clients'"), {
    router: app, sessionAuth: auth.requireAuth,
    getRequestClientId: clientModule.exports.getRequestClientId,
    getClientConfig: clientModule.exports.getClientConfig,
    workspaceDisplayName: require('../../utils/clientFacingPresentation').workspaceDisplayName,
    require(id) {
      if (id === '../utils/requestIdentity') return identity;
      if (id === '../services/aoImpersonationService') return impersonationModule.exports;
      throw new Error(`Unapproved identity dependency: ${id}`);
    },
  }, { filename: 'api-me-route.js' });
  return { user, pool, queries, auth, handlers, initCalls: () => initCalls };
}

function response() {
  return {
    headers: {}, statusCode: 200,
    set(name, value) { this.headers[name] = value; return this; },
    status(code) { this.statusCode = code; return this; },
    send(body) { this.body = body; return this; },
    json(body) { this.body = body; return this; },
    redirect(location) { this.statusCode = 302; this.location = location; return this; },
  };
}

async function login(harness, body = {}, currentSession = {}) {
  // Unit calls have no express-session middleware. Mirror its non-enumerable
  // save contract; HTTP/native PostgreSQL tests use the real session method.
  if (typeof currentSession.save !== 'function') {
    Object.defineProperty(currentSession, 'save', { value: callback => callback(), configurable: true });
  }
  const req = { body: { email: 'identity@example.test', password: fixturePassword, ...body }, session: currentSession };
  const res = response();
  await harness.handlers['POST /login'][0](req, res);
  return { req, res };
}

async function startHttpHarness(t, options) {
  const h = makeHarness(options); const app = express();
  h.requests = []; h.savedSessions = [];
  const store = new session.MemoryStore();
  const originalSet = store.set.bind(store);
  store.set = (sid, data, callback) => { h.savedSessions.push(structuredClone(data)); originalSet(sid, data, callback); };
  app.use((req, _res, next) => { h.requests.push(`${req.method} ${req.path}`); next(); });
  app.use(express.urlencoded({ extended: true }));
  app.use(session({ store, secret: 'local-test-only-not-an-app-secret', resave: false, saveUninitialized: false, cookie: { maxAge: 86400000 } }));
  for (const [key, fns] of Object.entries(h.handlers)) {
    const [method, route] = key.split(' '); app[method.toLowerCase()](route, ...fns);
  }
  app.get('/change-password', h.auth.requireAuth, (_req, res) => res.sendFile(path.join(root, 'public/change-password.html')));
  app.get('/favicon.ico', (_req, res) => res.status(204).end());
  app.use((_req, res) => res.status(418).send('Unexpected landing route'));
  app.use((error, _req, res, _next) => res.status(500).json({ error: error.message }));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  return { ...h, origin: `http://127.0.0.1:${server.address().port}` };
}

module.exports = { makeHarness, loadAuth, login, response, source, fixturePassword, startHttpHarness };
