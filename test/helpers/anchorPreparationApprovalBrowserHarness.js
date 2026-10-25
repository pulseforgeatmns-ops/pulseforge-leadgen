'use strict';

// Synthetic browser fixture only: native login/tenant forms, persisted sessions,
// real guarded approval services and disposable PostgreSQL. No app startup,
// production credentials, providers, automatic settings changes or live records.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const express = require('express');
const session = require('express-session');
const PgStore = require('connect-pg-simple')(session);
const { createTrackedPostgresPool, closePostgresFixture } = require('./closePostgresPool');
const { startDisposablePostgres } = require('./disposablePostgres');
const { makeHarness, fixturePassword } = require('./loginIdentityHarness');
const { createIdentityTenantSelectionRouter } = require('../../routes/identityTenantSelection');
const deny = require('./denyExternalProviders.cjs');

const root = path.join(__dirname, '../..');
const pagePath = '/admin/anchor-preparation-approval';
const apiBase = '/api/v1/tenant-outreach/anchor-program/preparation-approval/';
const preparePath = '/api/v1/tenant-outreach/anchor-program/prepare-drafts';
function loadFixture() {
  const filename = path.join(root, 'test/anchorScopedResumeAuthenticatedControlsPostgres.test.js');
  const local = createRequire(filename), module = { exports: {} };
  const load = Object.assign(id => id === 'node:test' ? () => {} : local(id), { resolve: local.resolve, cache: require.cache });
  vm.runInNewContext(fs.readFileSync(filename, 'utf8') + '\nmodule.exports={fixture,h,setup,intent,stub,revision};', {
    module, exports: module.exports, __dirname: path.dirname(filename), process, console, structuredClone, setTimeout, require: load,
  }, { filename });
  return module.exports;
}

async function startHarness() {
  const fixture = loadFixture();
  const postgres = await startDisposablePostgres('anchor-browser-');
  const pool = createTrackedPostgresPool({ connectionString: postgres.connectionString, max: 14 });
  const keys = ['APP_URL', 'RAILWAY_GIT_COMMIT_SHA', 'ANCHOR_EMAIL_SCOPED_RESUME_ID', 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID',
    'ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'ANCHOR_GOVERNED_OUTBOUND_ENABLED', 'ANCHOR_RESEARCH_PROVIDERS_PAUSED'];
  const previousEnv = Object.fromEntries(keys.map(key => [key, process.env[key]]));
  const app = express(), requests = [], responses = [], faults = {};
  let server, store, f, origin, closed = false;
  const restores = [], cached = [];
  const login = makeHarness({ user: { id: '3', name: 'Synthetic Admin' } });
  // Exercise the real extracted login SQL/password verification on PostgreSQL;
  // only the existing harness's startup initializer remains isolated.
  login.pool.query = (sql, params) => pool.query(sql, params);

  async function reset() {
    f = await fixture.setup(pool, { mock: { timers: { setTime() {} } } });
    f.now = new Date();
    f.manifest.validFrom = new Date(+f.now - 5 * 60000).toISOString();
    f.manifest.expiresAt = new Date(+f.now + 60 * 60000).toISOString();
    await pool.query(`ALTER TABLE users ADD COLUMN name text, ADD COLUMN email text,
      ADD COLUMN password_hash text, ADD COLUMN last_login_at timestamptz;
      ALTER TABLE clients ADD COLUMN name text;
      INSERT INTO clients(id,name,active,operator_switchable) VALUES(1,'Synthetic home',true,true);
      UPDATE clients SET name='Anchor synthetic' WHERE id=10;`);
    await pool.query('UPDATE prospects SET ao_outreach_review_required=NULL WHERE id::text=$1', [f.manifest.admissions[0].prospectId]);
    await pool.query('UPDATE users SET name=$1,email=$2,password_hash=$3 WHERE id=\'3\'',
      [login.user.name, login.user.email, login.user.password_hash]);
    await pool.query('UPDATE acquisition_outbound_inbox_health SET last_success_at=$1', [f.now]);
    if (origin) process.env.APP_URL = origin;
    requests.length = 0; responses.length = 0;
    for (const key of Object.keys(faults)) delete faults[key];
    return f;
  }

  async function close() {
    if (closed) return; closed = true;
    if (server) await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    if (store) await store.close();
    cached.forEach(([id, before]) => { delete require.cache[id]; if (before) require.cache[id] = before; });
    restores.reverse().forEach(restore => restore());
    for (const key of keys) previousEnv[key] === undefined ? delete process.env[key] : process.env[key] = previousEnv[key];
    require('../../services/acquisitionMissionRuntime').resetAcquisitionMissionRuntime();
    await closePostgresFixture(pool, postgres);
    for (const file of [postgres.rootDirectory, postgres.socketDirectory, postgres.portLockPath]) {
      if (fs.existsSync(file)) throw Error('Synthetic browser PostgreSQL cleanup left a fixture path behind');
    }
  }
  try {
    await reset();
  } catch (error) {
    await close(); throw error;
  }
  function stub(modulePath, exports) {
    const id = require.resolve(modulePath), before = require.cache[id];
    require.cache[id] = { id, filename: id, loaded: true, exports };
    restores.push(() => { delete require.cache[id]; if (before) require.cache[id] = before; });
  }
  try {
    // Real SQL/commits throughout. The optional fault is injected only after a
    // durable preparation claim, so interrupted-path tests cannot fake success
    // or bypass the native authority/consumption transaction.
    const actionPool = { query: pool.query.bind(pool), async connect() {
      const db = await pool.connect(); let consumption = false;
      return { release: db.release.bind(db), async query(q, values) {
        const sql = typeof q === 'string' ? q : q.text;
        if (/INSERT INTO anchor_scoped_resume_consumption/.test(sql)) consumption = true;
        const result = await db.query(q, values);
        if (sql === 'COMMIT' && consumption && faults.afterConsumption) {
          consumption = false; const fault = faults.afterConsumption; delete faults.afterConsumption; await fault();
        }
        return result;
      } };
    } };
    stub('../../db', fixture.h.noSchemaWrites(actionPool));
    stub('../../services/tenantOutreachScheduler', {});
    stub('../../services/governedMissionExecutionStatus', {});
    for (const p of ['../../routes/tenantOutreach', '../../middleware/auth', '../../packages/max/workspace/TenantContextResolver',
      '../../routes/anchorPreparationApprovalPage']) {
      const id = require.resolve(p); cached.push([id, require.cache[id]]); delete require.cache[id];
    }
    store = new PgStore({ pool, createTableIfMissing: false, pruneSessionInterval: false });
    app.use(express.json({ strict: true }));
    app.use((req, res, next) => {
      requests.push({ method: req.method, path: req.path, origin: req.get('origin'), host: req.get('host'), body: req.body });
      if (req.path.startsWith(apiBase) || req.path === preparePath) {
        const send = res.json.bind(res);
        res.json = body => {
          responses.push({ path: req.path, status: res.statusCode, body });
          if (faults.response) return faults.response(req, res, body, () => send(body));
          return send(body);
        };
      }
      if (faults.before) return faults.before(req, res, next);
      next();
    });
    app.use(session({ store, secret: 'synthetic-browser-only-cookie-secret', resave: false, saveUninitialized: false,
      cookie: { httpOnly: true, sameSite: 'strict', secure: false, maxAge: 86400000 } }));
    app.use(createIdentityTenantSelectionRouter({ Router: express.Router, pool, appUrl: () => origin }));
    app.use(express.urlencoded({ extended: false }));
    for (const [key, handlers] of Object.entries(login.handlers)) {
      const [method, route] = key.split(' '); app[method.toLowerCase()](route, ...handlers);
    }
    const { createAnchorPreparationApprovalPageRouter } = require('../../routes/anchorPreparationApprovalPage');
    app.use(createAnchorPreparationApprovalPageRouter({ pool: fixture.h.noSchemaWrites(pool), env: process.env }));
    app.use(require('../../routes/tenantOutreach'));
    app.use('/public', express.static(path.join(root, 'public')));
    app.use(express.static(path.join(root, 'public')));
    app.get('/fixture-away', (_req, res) => res.type('html').send('<h1>Local synthetic navigation destination</h1>'));
    app.get('/favicon.ico', (_req, res) => res.status(204).end());
    app.use((error, req, res, next) => { res.status(500).json({ error: 'synthetic_fixture_error' }); });
    server = app.listen(0, '127.0.0.1');
    await new Promise((resolve, reject) => { server.once('listening', resolve); server.once('error', reject); });
    origin = `http://127.0.0.1:${server.address().port}`; process.env.APP_URL = origin;
    return { app, pool, store, origin, pagePath, apiBase, preparePath, login, fixturePassword, requests, responses, faults, reset, close,
      get f() { return f; }, intent: () => JSON.parse(JSON.stringify(fixture.intent(f))),
      snapshot: () => fixture.h.snapshot(pool), effects: () => fixture.h.effects(pool),
      protectedState: () => fixture.h.protectedState(pool, f),
      savedMission: id => require('../../services/acquisitionMissionPersistence').loadMissionSnapshot(id, '10', pool, { skipEnsure: true }),
      currentSessions: () => pool.query('SELECT sid,sess,expire FROM session WHERE sid<>$1', [f.sessionId]),
      async reviewed() { return (await pool.query('SELECT ao_outreach_review_required AS value FROM prospects WHERE id::text=$1',
        [f.manifest.admissions[0].prospectId])).rows[0].value; },
      async grantCount() { return (await pool.query('SELECT count(*)::int AS n FROM anchor_scoped_resume_grants')).rows[0].n; },
      deny,
    };
  } catch (error) { await close(); throw error; }
}

async function browserLogin(page, h, { selectTenant = true, password = fixturePassword } = {}) {
  await page.goto(h.origin + '/login?workflow=identity-check');
  await page.type('input[name=email]', 'identity@example.test');
  await page.type('input[name=password]', password);
  await Promise.all([page.waitForNavigation(), page.click('button[type=submit]')]);
  if (!selectTenant || !page.url().endsWith('/api/me')) return;
  await page.goto(h.origin + '/identity-check/tenant');
  await Promise.all([page.waitForNavigation(), page.click('button[type=submit]')]);
}

function launchOptions() {
  return { executablePath: process.env.CHROMIUM_PATH || '/usr/bin/chromium', headless: true, pipe: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-background-networking',
      '--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1',
      ...(process.env.PUPPETEER_TEST_SINGLE_PROCESS === '1' ? ['--no-zygote', '--single-process'] : [])] };
}

module.exports = { startHarness, browserLogin, launchOptions, pagePath, apiBase, preparePath };
