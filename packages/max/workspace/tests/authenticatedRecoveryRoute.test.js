'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const http = require('node:http');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createMemoryStore } = require('../../../../services/clientIntelligenceInterview');
const { createMemoryStore: createObjectiveMemoryStore } = require('../../../../services/operatorObjectives');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const amo = require('../../../acquisition-mission');

function createRouteWorkspace(options) {
  return createWorkspaceEngine({
    disableLlm: true,
    acquisitionMissionRuntime: createTestAmoRuntime(),
    clientIntelligenceOpts: { store: createMemoryStore() },
    operatorObjectiveOpts: { store: createObjectiveMemoryStore() },
    ...options,
  });
}

function request(app, tenantId, body) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0, '127.0.0.1', () => {
      const payload = JSON.stringify(body);
      const req = http.request({
        hostname: '127.0.0.1', port: server.address().port,
        path: '/api/v1/max/workspace/ask', method: 'POST',
        headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload), 'x-tenant-id': tenantId },
      }, res => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => server.close(() => resolve({ status: res.statusCode, body: JSON.parse(data) })));
      });
      req.on('error', error => server.close(() => reject(error)));
      req.end(payload);
    });
  });
}

function productionRouteApp(workspace) {
  const runtimePath = require.resolve('../../../../utils/maxRuntime');
  const routePath = require.resolve('../../../../routes/maxWorkspace');
  const runtime = require(runtimePath);
  runtime.getMaxRuntime = async () => ({ workspace, askWorkspace: input => workspace.ask(input) });
  delete require.cache[routePath];
  const router = require(routePath);
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    const tenantId = req.get('x-tenant-id');
    req.session = { user: { id: 1, role: 'admin', client_id: Number(tenantId) }, active_client_id: Number(tenantId) };
    next();
  });
  app.use(router);
  return app;
}

test('production ask route recovers only for the authenticated tenant and restores no approval authority', async () => {
  const workspace = createRouteWorkspace({
    conversationContinuity: {
      async resume(tenantId) {
        assert.equal(tenantId, '10');
        return { context: { version: 1, candidates: ['mission-1'], focusId: 'mission-1', evidenceRefs: [], requiresEvidenceRefresh: true } };
      },
      remember() {},
    },
    conversationReferenceLoader: async ({ tenantId, missionId }) => ({ found: true, mission: { id: missionId, tenantId } }),
  });
  const app = productionRouteApp(workspace);
  amo.clearExecutionRouterAudit();
  const recovered = await request(app, '10', { sessionId: 'durable-session', question: 'Approve it.' });
  assert.equal(recovered.status, 200);
  assert.equal(workspace.sessions.get('durable-session').context.requiresEvidenceRefresh, true);
  assert.deepEqual(amo.listExecutionRouterAudit(), []);

  const foreign = await request(app, '11', { sessionId: 'durable-session', question: 'Show the mission.' });
  assert.equal(foreign.status, 403);
  assert.equal(foreign.body.error, 'tenant_mismatch');
});

test('production ask route cannot turn failed recovery into trusted context', async () => {
  const workspace = createRouteWorkspace({
    conversationContinuity: {
      async resume() { return { context: { version: 1, candidates: ['gone'], evidenceRefs: [], requiresEvidenceRefresh: true } }; },
      remember() {},
    },
    conversationReferenceLoader: async () => ({ found: false, mission: null }),
  });
  const response = await request(productionRouteApp(workspace), '10', { sessionId: 'untrusted-session', question: 'Approve it.' });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, 'workspace_ask_failed');
  assert.equal(workspace.sessions.get('untrusted-session'), null);
  assert.deepEqual(amo.listExecutionRouterAudit(), []);
});
