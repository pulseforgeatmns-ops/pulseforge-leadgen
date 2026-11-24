'use strict';

/**
 * MAX-P0-002 — every cron route fails closed.
 *
 * Production (Render, 2026-11-24) ran with CRON_SECRET unset, and the legacy
 * guard `if (CRON_SECRET && secret !== CRON_SECRET)` skipped authentication.
 * A wrong secret reached `/cron/:agent`'s agent lookup (HTTP 400, not 401).
 *
 * These tests mount the real routers and enumerate every registered /cron and
 * /internal/cron path, so a new route that reintroduces the bypass fails here.
 * No handler body runs: each request is refused before any DB/provider work,
 * and the positive control uses an unknown agent, which is rejected before I/O.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const express = require('express');

const { legacyCronSecretValid } = require('../utils/cronAuth');

const SECRET_KEYS = ['CRON_SECRET', 'MAX_DECAY_CRON_SECRET'];

function withEnv(values, fn) {
  const saved = {};
  for (const key of SECRET_KEYS) saved[key] = process.env[key];
  for (const key of SECRET_KEYS) {
    if (Object.prototype.hasOwnProperty.call(values, key)) {
      if (values[key] === undefined) delete process.env[key];
      else process.env[key] = values[key];
    }
  }
  const restore = () => {
    for (const key of SECRET_KEYS) {
      if (saved[key] === undefined) delete process.env[key];
      else process.env[key] = saved[key];
    }
  };
  return Promise.resolve().then(fn).finally(restore);
}

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use(require('../routes/cron'));
  app.use(require('../routes/knowledgeAdmin'));
  return app;
}

function listCronRoutes(app) {
  const routes = [];
  const stack = (app._router || app.router).stack;
  const visit = (layers) => {
    for (const layer of layers) {
      if (layer.route) {
        const path = layer.route.path;
        if (typeof path !== 'string') continue;
        if (!/^\/(?:internal\/)?cron\//.test(path)) continue;
        for (const method of Object.keys(layer.route.methods)) {
          routes.push({ method: method.toUpperCase(), path });
        }
      } else if (layer.handle && layer.handle.stack) {
        visit(layer.handle.stack);
      }
    }
  };
  visit(stack);
  return routes;
}

function concretePath(path) {
  return path.replace(':agent', 'p0-audit-nonexistent-agent');
}

async function withServer(app, fn) {
  const server = http.createServer(app);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  try {
    return await fn(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise(resolve => server.close(resolve));
  }
}

async function call(base, method, path, { query = '', headers = {}, body } = {}) {
  const res = await fetch(`${base}${concretePath(path)}${query}`, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: method === 'GET' ? undefined : JSON.stringify(body || {}),
  });
  return res.status;
}

test('legacyCronSecretValid fails closed on missing/empty configuration and bad input', () => {
  assert.equal(legacyCronSecretValid('anything', {}), false);
  assert.equal(legacyCronSecretValid('anything', { CRON_SECRET: '' }), false);
  assert.equal(legacyCronSecretValid('', { CRON_SECRET: '' }), false);
  assert.equal(legacyCronSecretValid(undefined, { CRON_SECRET: 's3cret' }), false);
  assert.equal(legacyCronSecretValid('', { CRON_SECRET: 's3cret' }), false);
  assert.equal(legacyCronSecretValid(['s3cret'], { CRON_SECRET: 's3cret' }), false);
  assert.equal(legacyCronSecretValid('s3cre', { CRON_SECRET: 's3cret' }), false);
  assert.equal(legacyCronSecretValid('s3cret!', { CRON_SECRET: 's3cret' }), false);
  assert.equal(legacyCronSecretValid('s3cret', { CRON_SECRET: 's3cret' }), true);
});

test('route enumeration finds the legacy and governed cron surface', () => {
  const routes = listCronRoutes(buildApp());
  const paths = new Set(routes.map(r => `${r.method} ${r.path}`));
  for (const expected of [
    'POST /cron/:agent',
    'GET /cron/:agent',
    'GET /cron/execute-anchor-one-outbound',
    'POST /cron/tenant-outreach-executor',
    'GET /cron/recover-anchor-canonical-outbound',
    'POST /cron/knowledge-outbox',
    'POST /cron/max-conversation-context-cleanup',
    'POST /internal/cron/max-decay',
  ]) {
    assert.ok(paths.has(expected), `missing ${expected}`);
  }
  assert.ok(routes.length >= 35, `expected the full cron surface, found ${routes.length}`);
});

for (const scenario of [
  { name: 'CRON_SECRET unset, no secret supplied', env: { CRON_SECRET: undefined, MAX_DECAY_CRON_SECRET: undefined }, supply: null },
  { name: 'CRON_SECRET unset, arbitrary secret supplied', env: { CRON_SECRET: undefined, MAX_DECAY_CRON_SECRET: undefined }, supply: 'invalid-probe' },
  { name: 'CRON_SECRET empty, empty secret supplied', env: { CRON_SECRET: '', MAX_DECAY_CRON_SECRET: '' }, supply: '' },
  { name: 'CRON_SECRET set, wrong secret supplied', env: { CRON_SECRET: 'configured-secret', MAX_DECAY_CRON_SECRET: 'configured-decay' }, supply: 'wrong-secret' },
]) {
  test(`every cron route returns 401: ${scenario.name}`, async () => {
    await withEnv(scenario.env, async () => {
      const app = buildApp();
      const routes = listCronRoutes(app);
      const failures = [];
      await withServer(app, async (base) => {
        for (const { method, path } of routes) {
          const opts = {};
          if (scenario.supply !== null) {
            opts.query = `?secret=${encodeURIComponent(scenario.supply)}`;
            opts.headers = {
              authorization: `Bearer ${scenario.supply}`,
              'x-cron-secret': scenario.supply,
            };
            opts.body = { secret: scenario.supply };
          }
          const status = await call(base, method, path, opts);
          if (status !== 401) failures.push(`${method} ${path} -> ${status}`);
        }
      });
      assert.deepEqual(failures, [], `routes not refusing: ${failures.join(', ')}`);
    });
  });
}

test('a correct secret still passes the legacy guard (unknown agent is rejected before any I/O)', async () => {
  await withEnv({ CRON_SECRET: 'configured-secret' }, async () => {
    await withServer(buildApp(), async (base) => {
      assert.equal(await call(base, 'GET', '/cron/:agent', { query: '?secret=configured-secret' }), 400);
      assert.equal(await call(base, 'POST', '/cron/:agent', { body: { secret: 'configured-secret' } }), 400);
    });
  });
});
