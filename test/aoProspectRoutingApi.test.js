'use strict';

const assert = require('node:assert/strict');
const express = require('express');
const test = require('node:test');

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

test('AO API scopes prospect, task, debrief, and inspection access to the authenticated owner', async () => {
  const observed = {};
  const restores = [
    stub('../utils/aoFieldSchema', { ensureAoFieldSchema: async () => {} }),
    stub('../utils/aoProspectRoutingSchema', { ensureAoProspectRoutingSchema: async () => {} }),
    stub('../services/aoProspectRoutingService', { routeProspect: () => ({ recommended_motion: 'AO_LED' }) }),
    stub('../services/aoProspectTaskService', {
      formatAoTask: value => value,
      fetchProspectBundle: async (prospectId, clientId) => {
        observed.bundleClientId = clientId;
        if (prospectId === 'foreign') return null;
        return { prospect: { id: prospectId, assigned_ao_id: 102 }, company: null, touchpoints: [] };
      },
      fetchAvailableAos: async () => [],
      routeAndPersistProspect: async () => null,
      generateWeeklyAoTasks: async () => ({ created_count: 0, tasks: [] }),
      listOpenTasks: async args => { observed.listOwner = args.aoOwnerId; return []; },
      getTaskById: async (_id, options) => { observed.taskOwner = options.aoOwnerId; return null; },
    }),
    stub('../services/aoAdvisoryDebriefService', {
      submitDebrief: async args => { observed.submitOwner = args.aoOwnerId; return {}; },
      listDebriefs: async args => { observed.debriefOwner = args.aoOwnerId; return []; },
    }),
    stub('../services/aoMissionInspection', {
      prospectsToWorkToday: async args => { observed.workOwner = args.aoOwnerId; return []; },
      explainAssignment: async (_id, _client, _db, owner) => { observed.assignmentOwner = owner; return null; },
      answerInspectionQuestion: async (_question, args) => { observed.askOwner = args.aoOwnerId; return { intent: 'overview' }; },
    }),
  ];

  let running;
  try {
    delete require.cache[require.resolve('../routes/aoProspectRouting')];
    const router = require('../routes/aoProspectRouting');
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.session = { user: { id: 101, role: 'ao', client_id: 10, active: true } };
      next();
    });
    app.use(router);
    running = await listen(app);

    let response = await fetch(`${running.base}/api/v1/ao/routing/evaluate`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ prospect_id: 'foreign' }),
    });
    assert.equal(response.status, 404);
    assert.equal(observed.bundleClientId, 10, 'AO tenant comes from the authenticated user');

    response = await fetch(`${running.base}/api/v1/ao/routing/evaluate`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ prospect_id: 'other-ao' }),
    });
    assert.equal(response.status, 403);

    response = await fetch(`${running.base}/api/v1/ao/tasks/task-1`);
    assert.equal(response.status, 404);
    assert.equal(observed.taskOwner, 101);

    response = await fetch(`${running.base}/api/v1/ao/debriefs`);
    assert.equal(response.status, 200);
    assert.equal(observed.debriefOwner, 101);

    response = await fetch(`${running.base}/api/v1/ao/inspection/assignment/prospect-1`);
    assert.equal(response.status, 404);
    assert.equal(observed.assignmentOwner, 101);

    response = await fetch(`${running.base}/api/v1/ao/inspection/ask`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ question: 'show weak debriefs' }),
    });
    assert.equal(response.status, 200);
    assert.equal(observed.askOwner, 101);
  } finally {
    if (running) await new Promise(resolve => running.server.close(resolve));
    delete require.cache[require.resolve('../routes/aoProspectRouting')];
    for (const restore of restores.reverse()) restore();
  }
});
