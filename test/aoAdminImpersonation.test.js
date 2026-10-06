'use strict';

const assert = require('node:assert/strict');
const express = require('express');
const test = require('node:test');
const {
  bindRequestIdentity,
  getAuthenticatedActor,
  getEffectiveActor,
  isImpersonating,
} = require('../utils/requestIdentity');
const { effectiveAoOwnerId, effectiveRoleIsAo } = require('../utils/aoRequestHelpers');

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise(resolve => server.on('listening', () => resolve({
    server,
    base: `http://127.0.0.1:${server.address().port}`,
  })));
}

function stub(modulePath, exports) {
  const resolved = require.resolve(modulePath);
  const previous = require.cache[resolved];
  require.cache[resolved] = { id: resolved, filename: resolved, loaded: true, exports };
  return () => {
    delete require.cache[resolved];
    if (previous) require.cache[resolved] = previous;
  };
}

test('I1 identity helpers preserve authenticated vs effective actor', () => {
  const req = {
    session: {
      user: { id: 3, name: 'Jake', role: 'admin', client_id: 10 },
      impersonation: {
        active: true,
        authenticatedUserId: 3,
        effectiveUserId: 19,
        tenantId: 10,
        startedAt: '2026-10-06T12:00:00.000Z',
        startedByRole: 'admin',
        effectiveUser: { id: 19, name: 'Tony', role: 'ao', client_id: 10 },
      },
    },
  };
  bindRequestIdentity(req);
  assert.equal(getAuthenticatedActor(req).id, 3);
  assert.equal(getEffectiveActor(req).id, 19);
  assert.equal(isImpersonating(req), true);
  assert.equal(effectiveAoOwnerId(req), 19);
  assert.equal(effectiveRoleIsAo(req), true);
});

test('I9 exit clears effective actor', () => {
  const req = {
    session: {
      user: { id: 3, role: 'admin' },
      impersonation: {
        active: true,
        effectiveUser: { id: 19, role: 'ao', client_id: 10 },
      },
    },
  };
  bindRequestIdentity(req);
  delete req.session.impersonation;
  bindRequestIdentity(req);
  assert.equal(getEffectiveActor(req).id, 3);
  assert.equal(isImpersonating(req), false);
});

test('I2 AO cannot start impersonation via API', async () => {
  const restores = [
    stub('../services/aoImpersonationService', {
      startImpersonation: async () => assert.fail('should not be called'),
      stopImpersonation: async () => ({ ok: true }),
      listImpersonationTargets: async () => ({ ok: true, targets: [] }),
      publicImpersonationState: () => ({ active: false }),
    }),
  ];
  let running;
  try {
    delete require.cache[require.resolve('../routes/aoImpersonation')];
    const router = require('../routes/aoImpersonation');
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.session = { user: { id: 19, role: 'ao', client_id: 10 } };
      req.user = req.session.user;
      bindRequestIdentity(req);
      next();
    });
    app.use(router);
    running = await listen(app);
    const res = await fetch(`${running.base}/api/v1/admin/impersonation/start`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ user_id: 20, client_id: 10 }),
    });
    assert.equal(res.status, 403);
  } finally {
    if (running) await new Promise(r => running.server.close(r));
    delete require.cache[require.resolve('../routes/aoImpersonation')];
    restores.forEach(r => r());
  }
});

test('I3 cross-tenant start blocked', async () => {
  const restores = [
    stub('../services/aoImpersonationService', {
      startImpersonation: async () => ({ error: 'cross_tenant_target', status: 403 }),
      stopImpersonation: async () => ({ ok: true }),
      listImpersonationTargets: async () => ({ ok: true, targets: [] }),
      publicImpersonationState: () => ({ active: false }),
    }),
  ];
  let running;
  try {
    delete require.cache[require.resolve('../routes/aoImpersonation')];
    const router = require('../routes/aoImpersonation');
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.session = { user: { id: 3, role: 'admin' }, active_client_id: 10 };
      req.user = req.session.user;
      bindRequestIdentity(req);
      next();
    });
    app.use(router);
    running = await listen(app);
    const res = await fetch(`${running.base}/api/v1/admin/impersonation/start`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ user_id: 99, client_id: 10 }),
    });
    assert.equal(res.status, 403);
  } finally {
    if (running) await new Promise(r => running.server.close(r));
    delete require.cache[require.resolve('../routes/aoImpersonation')];
    restores.forEach(r => r());
  }
});

test('I11 query param cannot activate impersonation', async () => {
  let running;
  try {
    delete require.cache[require.resolve('../routes/aoImpersonation')];
    const router = require('../routes/aoImpersonation');
    const app = express();
    app.use((req, _res, next) => {
      req.session = { user: { id: 3, role: 'admin' } };
      req.user = req.session.user;
      bindRequestIdentity(req);
      next();
    });
    app.use(router);
    running = await listen(app);
    const res = await fetch(`${running.base}/api/v1/admin/impersonation?impersonate_user_id=19`);
    const data = await res.json();
    assert.equal(data.impersonation.active, false);
  } finally {
    if (running) await new Promise(r => running.server.close(r));
    delete require.cache[require.resolve('../routes/aoImpersonation')];
  }
});

test('max composer actor uses effective AO when impersonating', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const src = fs.readFileSync(path.join(__dirname, '..', 'routes', 'maxStateIngestion.js'), 'utf8');
  assert.match(src, /getEffectiveActor/);
  assert.match(src, /impersonationProvenance/);
});

test('admin impersonation routes are registered', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const serverSrc = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  const routeSrc = fs.readFileSync(path.join(__dirname, '..', 'routes', 'aoImpersonation.js'), 'utf8');
  assert.match(serverSrc, /routes\/aoImpersonation/);
  assert.match(routeSrc, /\/api\/v1\/admin\/impersonation\/start/);
});
