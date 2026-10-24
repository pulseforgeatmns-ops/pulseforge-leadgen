'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const puppeteer = require('puppeteer');
const { startHttpHarness, fixturePassword } = require('../helpers/loginIdentityHarness');

async function setup(t, options) {
  const h = await startHttpHarness(t, options);
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || '/usr/bin/chromium', headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  t.after(() => browser.close());
  const page = await browser.newPage(); const externalRequests = [];
  await page.setRequestInterception(true);
  page.on('request', req => {
    if (new URL(req.url()).origin !== h.origin) { externalRequests.push(req.url()); return req.abort(); }
    return req.continue();
  });
  return { ...h, page, externalRequests };
}

async function submit(page, password = fixturePassword) {
  await page.type('input[name=email]', 'identity@example.test');
  await page.type('input[name=password]', password);
  await Promise.all([page.waitForNavigation(), page.click('button[type=submit]')]);
}

test('native browser form keeps hidden workflow, authenticates normally and loads no dashboard scripts', async t => {
  const h = await setup(t);
  await h.page.goto(`${h.origin}/login?workflow=identity-check`);
  assert.equal(await h.page.$eval('input[name=workflow]', el => el.value), 'identity-check');
  assert.equal(h.savedSessions.length, 0);
  await submit(h.page);
  assert.equal(h.page.url(), `${h.origin}/api/me`);
  const data = JSON.parse(await h.page.$eval('body', el => el.textContent));
  assert.equal(data.user.id, 73); assert.equal(data.active_client_id, 1);
  assert.equal(data.impersonation.active, false);
  await new Promise(resolve => setTimeout(resolve, 150));
  assert.deepEqual(h.requests.filter(value => !value.endsWith('/favicon.ico')), ['GET /login', 'POST /login', 'GET /api/me']);
  assert.deepEqual(h.queries.map(q => q.text.split(' ')[0]), ['SELECT', 'UPDATE', 'SELECT']);
  assert.equal(h.requests.some(value => /command-deck|api\/clients|attention/.test(value)), false);
});

test('invalid-password retry keeps the selected workflow and later reaches only identity JSON', async t => {
  const h = await setup(t);
  await h.page.goto(`${h.origin}/login?workflow=identity-check`);
  await submit(h.page, 'wrong-local-fixture');
  assert.equal(h.page.url(), `${h.origin}/login?error=1&workflow=identity-check`);
  assert.equal(await h.page.$eval('input[name=workflow]', el => el.value), 'identity-check');
  assert.equal(h.savedSessions.length, 0);
  await submit(h.page);
  assert.equal(h.page.url(), `${h.origin}/api/me`);
  assert.equal(h.requests.some(value => /command-deck|api\/clients/.test(value)), false);
});

test('forced password change lands on its existing form without submitting or opening a dashboard', async t => {
  const h = await setup(t, { user: { password_change_required: true } });
  await h.page.goto(`${h.origin}/login?workflow=identity-check`);
  await submit(h.page);
  assert.equal(h.page.url(), `${h.origin}/change-password`);
  await h.page.waitForSelector('input[autocomplete=new-password]');
  assert.deepEqual(h.requests.filter(value => !value.endsWith('/favicon.ico')), ['GET /login', 'POST /login', 'GET /change-password']);
  assert.equal(h.requests.some(value => /api\/me\/password|command-deck|api\/clients/.test(value)), false);
});
