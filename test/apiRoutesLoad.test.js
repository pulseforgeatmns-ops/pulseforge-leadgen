'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { describe, it } = require('node:test');

const API_ROUTE_PATH = path.join(__dirname, '../routes/api.js');

function findRoute(router, routePath, method) {
  return router.stack.find((layer) => (
    layer.route
    && layer.route.path === routePath
    && Boolean(layer.route.methods[method])
  ));
}

describe('routes/api.js load smoke', () => {
  it('can be required without throwing', () => {
    let api;
    assert.doesNotThrow(() => {
      api = require('../routes/api');
    });
    assert.equal(typeof api, 'function');
    assert.ok(Array.isArray(api.stack));
    assert.ok(api.stack.length > 0);
  });

  it('protects Paige social content status with the dashboard-read auth stack', () => {
    const api = require('../routes/api');
    const statusRoute = findRoute(api, '/api/paige/social-content/status', 'get');
    const agentStatusRoute = findRoute(api, '/api/agent-status', 'get');
    const approvalsRoute = findRoute(api, '/api/approvals', 'get');

    assert.ok(statusRoute, 'expected GET /api/paige/social-content/status');
    assert.ok(agentStatusRoute, 'expected GET /api/agent-status comparison route');
    assert.ok(approvalsRoute, 'expected GET /api/approvals comparison route');

    const statusStack = statusRoute.route.stack.map((layer) => layer.handle.name);
    const agentStatusStack = agentStatusRoute.route.stack.map((layer) => layer.handle.name);

    assert.equal(statusRoute.route.stack.length, agentStatusRoute.route.stack.length);
    assert.ok(statusRoute.route.stack.length >= 3, 'auth + role + handler');
    assert.equal(statusStack[0], 'requireAuth');
    assert.deepEqual(statusStack, agentStatusStack);

    const source = fs.readFileSync(API_ROUTE_PATH, 'utf8');
    assert.match(
      source,
      /router\.get\('\/api\/paige\/social-content\/status',\s*requireDashboardRead,/
    );
    assert.doesNotMatch(
      source,
      /router\.get\('\/api\/paige\/social-content\/status',\s*requireAuth,/
    );
  });
});
