'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { createAnchorPortalReadiness, isAnchorPortalPath } = require('../utils/anchorPortalReadiness');

function response() {
  return { headers: {}, set(k, v) { this.headers[k] = v; return this; }, status(s) { this.code = s; return this; }, json(v) { this.body = v; return this; } };
}

test('portal gate blocks pending auth and pending schema; opens only on success', async () => {
  let authDone; let schemaDone; let calls = 0;
  const auth = new Promise(resolve => { authDone = resolve; });
  const schema = new Promise(resolve => { schemaDone = resolve; });
  const gate = createAnchorPortalReadiness({ initAuth: () => auth, ensureSchema: () => { calls++; return schema; } });
  let next = 0;
  const check = () => { const res = response(); gate.middleware({ path: '/portal/anchor' }, res, () => next++); return res; };
  assert.equal(check().code, 503); assert.equal(calls, 0);
  authDone(); await new Promise(setImmediate);
  assert.equal(calls, 1); assert.equal(check().code, 503);
  schemaDone(); await gate.initialized;
  check(); assert.equal(next, 1); assert.equal(gate.isReady(), true);
});

for (const failure of ['auth', 'schema']) test(`portal gate remains closed after ${failure} failure`, async () => {
  const errors = []; let schemaCalls = 0;
  const gate = createAnchorPortalReadiness({
    initAuth: async () => { if (failure === 'auth') throw Error('auth failure'); },
    ensureSchema: async () => { schemaCalls++; throw Error('schema failure'); }, onError: err => errors.push(err),
  });
  await gate.initialized;
  const res = response(); gate.middleware({ path: '/portal/anchor' }, res, () => assert.fail('must stay closed'));
  assert.equal(res.code, 503); assert.equal(res.headers['Cache-Control'], 'no-store');
  assert.equal(gate.isReady(), false); assert.equal(errors.length, 1);
  assert.equal(schemaCalls, failure === 'auth' ? 0 : 1);
});

test('server mounts all portal entry gates before static/session and never separately launches portal init', () => {
  const src = fs.readFileSync(require.resolve('../server'), 'utf8');
  const gate = src.indexOf('anchorPortalReadiness.middleware');
  assert.ok(gate < src.indexOf('app.use(session('));
  assert.ok(gate < src.indexOf('app.use(express.static('));
  for (const route of ['/portal/anchor', '/api/anchor-portal', '/public/anchor-portal.html', '/anchor-portal.html']) {
    assert.equal(isAnchorPortalPath(route), true);
  }
  assert.equal((src.match(/ensureAnchorPortalSchema\(\)/g) || []).length, 1);
});


test('gate recognizes encoded and normalized static aliases while unrelated routes continue', async () => {
  const gate = createAnchorPortalReadiness({ initAuth: () => new Promise(() => {}), ensureSchema: async () => {} });
  for (const url of ['/public/%61nchor-portal.html', '/public/nested/../anchor-portal.html', '/API/ANCHOR-PORTAL/me', '/portal/anchor/']) {
    const res = response(); gate.middleware({ path: url }, res, () => assert.fail(url)); assert.equal(res.code, 503);
  }
  let next = 0; gate.middleware({ path: '/dashboard' }, response(), () => next++); assert.equal(next, 1);
});

test('all three global role refresh entrypoints use shared transaction helper', () => {
  for (const file of ['../middleware/auth', '../utils/anchorPortalSchema', '../utils/closerSchema']) {
    const src = fs.readFileSync(require.resolve(file), 'utf8');
    assert.match(src, /require\(['"].*usersRoleConstraint['"]\)/);
    assert.doesNotMatch(src, /pg_advisory_lock\(|pg_advisory_unlock\(|DROP CONSTRAINT|ILIKE '%role%'/);
  }
});

test('HTTP static delivery stays blocked for normalized/encoded aliases until readiness', async t => {
  const express = require('express');
  let done; const pending = new Promise(resolve => { done = resolve; });
  const gate = createAnchorPortalReadiness({ initAuth: () => pending, ensureSchema: async () => {} });
  const app = express(); app.use(gate.middleware);
  app.get('/api/anchor-portal/visits/:id', (_req, res) => res.send('must not reach until ready'));
  app.use(express.static(require('node:path').join(__dirname, '..')));
  const server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const origin = `http://127.0.0.1:${server.address().port}`;
  for (const url of ['/public/anchor-portal.html', '/public/%61nchor-portal.html', '/public/./anchor-portal.html', '/portal/anchor', '/api/anchor-portal/me']) {
    const response = await fetch(origin + url); assert.equal(response.status, 503, url);
  }
  const escapedParameterStatus = await new Promise((resolve, reject) => {
    require('node:http').get(origin + '/api/anchor-portal/visits/%2e%2e%2f%2e%2e%2f%2e%2e%2fx', res => {
      res.resume(); res.on('end', () => resolve(res.statusCode));
    }).on('error', reject);
  });
  assert.equal(escapedParameterStatus, 503);
  done(); await gate.initialized;
  assert.equal((await fetch(origin + '/public/anchor-portal.html')).status, 200);
});
