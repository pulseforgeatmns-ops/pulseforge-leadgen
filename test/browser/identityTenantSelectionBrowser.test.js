'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createTrackedPostgresPool, closePostgresFixture } = require('../helpers/closePostgresPool');
const express = require('express');
const session = require('express-session');
const PgStore = require('connect-pg-simple')(session);
const puppeteer = require('puppeteer');
const { startDisposablePostgres } = require('../helpers/disposablePostgres');
const { makeHarness, fixturePassword } = require('../helpers/loginIdentityHarness');
const { createIdentityTenantSelectionRouter, FORM_PATH } = require('../../routes/identityTenantSelection');

async function setup(t) {
  const postgres = await startDisposablePostgres('identity-browser-');
  const pool = createTrackedPostgresPool({ connectionString: postgres.connectionString });
  t.after(() => closePostgresFixture(pool, postgres));
  await pool.query(`CREATE TABLE "session" (sid varchar PRIMARY KEY, sess json NOT NULL, expire timestamp NOT NULL);
    CREATE TABLE users (id integer PRIMARY KEY, role text, active boolean, client_id integer,
      email_verified boolean, password_change_required boolean);
    CREATE TABLE clients (id integer PRIMARY KEY, name text, active boolean, operator_switchable boolean);
    INSERT INTO users VALUES (3,'admin',true,null,true,false);
    INSERT INTO clients VALUES (1,'Current fixture',true,true),(10,'Anchor fixture',true,true)`);
  const login = makeHarness({ user: { id: 3 } }); const app = express();
  const requests = []; let origin;
  app.use((req, _res, next) => { requests.push({ method: req.method, path: req.path, origin: req.get('origin') }); next(); });
  const store = new PgStore({ pool, tableName: 'session', pruneSessionInterval: false });
  t.after(() => store.close());
  app.use(session({ store, secret: 'disposable-browser-fixture-only', resave: false,
    saveUninitialized: false, cookie: { maxAge: 86400000 } }));
  app.use(createIdentityTenantSelectionRouter({ Router: express.Router, pool, appUrl: () => origin }));
  app.use(express.urlencoded({ extended: true }));
  for (const [key, fns] of Object.entries(login.handlers)) {
    const [method, route] = key.split(' '); app[method.toLowerCase()](route, ...fns);
  }
  app.get('/favicon.ico', (_req, res) => res.status(204).end());
  app.use((_req, res) => res.status(418).send('Unexpected route'));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  origin = `http://127.0.0.1:${server.address().port}`;
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || '/usr/bin/chromium',
    headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.setRequestInterception(true);
  page.on('request', request => new URL(request.url()).origin === origin ? request.continue() : request.abort());
  await page.goto(origin + '/login?workflow=identity-check');
  await page.type('input[name=email]', 'identity@example.test');
  await page.type('input[name=password]', fixturePassword);
  await Promise.all([page.waitForNavigation(), page.click('button[type=submit]')]);
  assert.equal(page.url(), origin + '/api/me'); requests.length = 0;
  return { pool, page, requests, origin, active: async () =>
    (await pool.query('SELECT sess FROM "session"')).rows[0].sess.active_client_id };
}

async function openSelectionForm(h) {
  const expectedUrl = h.origin + FORM_PATH;
  const response = await h.page.goto(expectedUrl);
  const formSelector = `form[method="POST"][action="${FORM_PATH}"]`;
  const form = await h.page.$(formSelector);
  const submit = await h.page.$(`${formSelector} button[type="submit"][name="confirm"][value="select-anchor"]`);
  const visible = submit ? await submit.isVisible() : false;
  await form?.dispose(); await submit?.dispose();
  if (response?.status() === 200 && response.url() === expectedUrl
    && h.page.url() === expectedUrl && form && visible) return response;

  // Fixture-only diagnostics: never print cookies, session IDs, CSRF tokens,
  // form field values, or raw response/DOM bodies. Do not retry a failed GET.
  const body = response ? await response.text().catch(() => '') : '';
  let errorCode = null;
  try {
    const value = JSON.parse(body).error;
    if (typeof value === 'string' && /^[a-z]+(?:_[a-z0-9]+)+$/.test(value) && value.length <= 80) errorCode = value;
  } catch (_) { /* HTML responses are described only by structural booleans. */ }
  const safeUrl = value => {
    try { const url = new URL(value); return url.origin + url.pathname.slice(0,200); }
    catch (_) { return '[unavailable]'; }
  };
  assert.fail(`Identity form precondition failed: ${JSON.stringify({
    status: response?.status() ?? null, responseUrl: safeUrl(response?.url()),
    pageUrl: safeUrl(h.page.url()), errorCode, formPresent: Boolean(form),
    submitPresent: Boolean(submit), submitVisible: visible,
    responseHasForm: body.includes(`<form method="POST" action="${FORM_PATH}">`),
    responseHasSubmit: body.includes('<button type="submit" name="confirm" value="select-anchor">'),
    requests: h.requests.slice(-12).map(({ method, path }) => ({ method, path: path.slice(0,200) })),
  })}`);
}

test('real native form preserves exact Origin, CSRF and submitter and lands only on /api/me', async t => {
  const h = await setup(t); const response = await openSelectionForm(h);
  assert.equal(response.headers()['referrer-policy'], 'same-origin');
  assert.equal(await h.active(), 1);
  await Promise.all([h.page.waitForNavigation(), h.page.click('button[type=submit]')]);
  assert.equal(h.page.url(), h.origin + '/api/me'); assert.equal(await h.active(), 10);
  const data = JSON.parse(await h.page.$eval('body', element => element.textContent));
  assert.equal(data.active_client_id, 10); assert.equal(data.user.id, 3);
  const meaningful = h.requests.filter(r => r.path !== '/favicon.ico');
  assert.deepEqual(meaningful.map(r => `${r.method} ${r.path}`), [`GET ${FORM_PATH}`, `POST ${FORM_PATH}`, 'GET /api/me']);
  assert.equal(meaningful[1].origin, h.origin);
});

test('native Cancel does not select a tenant or open dashboard', async t => {
  const h = await setup(t); await openSelectionForm(h);
  await Promise.all([h.page.waitForNavigation(), h.page.click('a[href="/api/me"]')]);
  assert.equal(h.page.url(), h.origin + '/api/me'); assert.equal(await h.active(), 1);
  assert.equal(h.requests.some(r => r.method === 'POST' || /command-deck|api\/clients/.test(r.path)), false);
});

test('native stale form after database demotion fails closed, without selecting or redirecting', async t => {
  const h = await setup(t); await openSelectionForm(h);
  await h.pool.query("UPDATE users SET role='manager' WHERE id=3");
  const [response] = await Promise.all([h.page.waitForNavigation(), h.page.click('button[type=submit]')]);
  assert.equal(response.status(), 403); assert.equal(h.page.url(), h.origin + FORM_PATH);
  assert.equal(await h.active(), 1);
  const data = JSON.parse(await h.page.$eval('body', element => element.textContent));
  assert.equal(data.error, 'current_admin_required');
});
