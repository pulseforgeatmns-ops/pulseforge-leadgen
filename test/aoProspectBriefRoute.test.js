'use strict';

const assert = require('node:assert/strict');
const express = require('express');
const test = require('node:test');
const { aoResultHttpStatus } = require('../utils/aoHttpResult');

test('aoResultHttpStatus treats only numeric HTTP codes as errors', () => {
  assert.equal(aoResultHttpStatus({ error: 'missing', status: 404 }), 404);
  assert.equal(aoResultHttpStatus({ error: 'bad', status: 'active' }), null);
  assert.equal(aoResultHttpStatus({ conversationStatus: 'active', ok: true }), null);
  assert.equal(aoResultHttpStatus({ status: 'active', brief: 'hello' }), null);
});

test('prospect-brief returns HTTP 200 when conversation is active (not res.status("active"))', async () => {
  const observed = { statusCalls: [] };
  const restores = [
    stubModule('../utils/aoFieldSchema', { ensureAoFieldSchema: async () => {} }),
    stubModule('../services/aoFieldService', {
      getAoProfile: async () => ({ id: 101, name: 'Tony', client_id: 10 }),
    }),
    stubModule('../services/aoMaxConversation', {
      handleProspectBriefAction: async () => ({
        ok: true,
        session_id: 'sess-1',
        mode: 'conversation',
        completed: false,
        conversationStatus: 'active',
        accountStatus: 'ready_to_call',
        intent: 'prospect_brief',
        reply: 'Curtin Law Office — follow up on prior interest.',
        brief: 'Curtin Law Office — follow up on prior interest.',
        brief_sections: {
          account_name: 'Curtin Law Office',
          suggested_next_move: 'follow_up',
        },
        action: 'prospect_brief',
        prospect_id: 'p-curtin',
      }),
    }),
  ];

  const originalStatus = express.response.status;
  express.response.status = function patchedStatus(code) {
    observed.statusCalls.push(code);
    return originalStatus.call(this, code);
  };

  let running;
  try {
    delete require.cache[require.resolve('../routes/ao')];
    const router = require('../routes/ao');
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.user = { id: 101, role: 'ao', client_id: 10, name: 'Tony' };
      req.session = { user: req.user };
      next();
    });
    app.use('/ao', router);
    running = await listen(app);

    const response = await fetch(`${running.base}/ao/api/max/prospect-brief`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        prospect_id: 'p-curtin',
        source: 'crm_card_brief_button',
      }),
    });

    assert.equal(response.status, 200);
    assert.deepEqual(observed.statusCalls, []);
    const body = await response.json();
    assert.equal(body.conversationStatus, 'active');
    assert.equal(body.accountStatus, 'ready_to_call');
    assert.match(body.brief, /Curtin Law Office/);
    assert.equal(body.status, undefined);
  } finally {
    express.response.status = originalStatus;
    if (running) await new Promise(resolve => running.server.close(resolve));
    delete require.cache[require.resolve('../routes/ao')];
    for (const restore of restores.reverse()) restore();
  }
});

function stubModule(modulePath, exports) {
  const resolved = require.resolve(modulePath);
  const previous = require.cache[resolved];
  require.cache[resolved] = { id: resolved, filename: resolved, loaded: true, exports };
  return () => {
    delete require.cache[resolved];
    if (previous) require.cache[resolved] = previous;
  };
}

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise(resolve => server.on('listening', () => resolve({
    server,
    base: `http://127.0.0.1:${server.address().port}`,
  })));
}
