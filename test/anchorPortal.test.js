'use strict';

const { describe, it, before } = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const session = require('express-session');
const anchorPortalRouter = require('../routes/anchorPortal');

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise((resolve) => {
    server.on('listening', () => {
      const { port } = server.address();
      resolve({
        server,
        base: `http://127.0.0.1:${port}`,
        async close() {
          await new Promise((r) => server.close(r));
        },
      });
    });
  });
}

async function request(base, path, { method = 'GET', body, cookie } = {}) {
  const res = await fetch(new URL(path, base), {
    method,
    headers: {
      Accept: 'application/json',
      ...(body ? { 'Content-Type': 'application/json' } : {}),
      ...(cookie ? { Cookie: cookie } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_) {
    json = null;
  }
  return { status: res.status, json, text };
}

describe('anchor portal routes auth', () => {
  it('GET /api/anchor-portal/me requires authentication', async () => {
    const app = express();
    app.use(express.json());
    app.use(session({ secret: 'test', resave: false, saveUninitialized: true }));
    app.use(anchorPortalRouter);
    const ctx = await listen(app);
    try {
      const res = await request(ctx.base, '/api/anchor-portal/me');
      assert.equal(res.status, 401);
    } finally {
      await ctx.close();
    }
  });
});

describe('anchor portal service roles', () => {
  it('identifies operator and cleaner roles', () => {
    const { isOperator, isCleaner, isFacilityClient } = require('../services/anchorPortal');
    assert.equal(isOperator({ role: 'admin' }), true);
    assert.equal(isCleaner({ role: 'cleaner' }), true);
    assert.equal(isFacilityClient({ role: 'facility_client' }), true);
  });
});

describe('anchor portal schema module', () => {
  it('exports demo scope sections with kitchen checklist', async () => {
    const { demoScopeSections } = require('../utils/anchorPortalSchema');
    const sections = demoScopeSections();
    const kitchen = sections.find(s => /KITCHEN/i.test(s.title));
    assert.ok(kitchen);
    assert.ok(kitchen.items.some(label => /sink/i.test(label)));
    assert.ok(kitchen.items.some(label => /crumbs/i.test(label)));
  });
});
