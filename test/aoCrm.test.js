'use strict';

const assert = require('node:assert/strict');
const express = require('express');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  AO_CRM_OUTCOMES,
  OUTCOME_TO_STATUS,
  deriveDefaultStatus,
  isValidCrmOutcome,
} = require('../utils/aoCrmTypes');

test('CRM outcome mapping covers Tony-style call flows', () => {
  assert.equal(isValidCrmOutcome('left_voicemail'), true);
  assert.equal(OUTCOME_TO_STATUS.left_voicemail, 'call_attempted');
  assert.equal(OUTCOME_TO_STATUS.booked_walkthrough, 'walkthrough_booked');
  assert.equal(AO_CRM_OUTCOMES.length >= 10, true);
});

test('deriveDefaultStatus keeps assigned accounts visible', () => {
  assert.equal(
    deriveDefaultStatus({ assigned_ao_id: 5, last_debrief_status: null }),
    'ready_to_call'
  );
  assert.equal(
    deriveDefaultStatus({ last_debrief_status: 'left_voicemail' }),
    'call_attempted'
  );
});

test('ao routes expose SPEC-AO-CRM-001 endpoints', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'routes', 'ao.js'), 'utf8');
  assert.match(src, /\/api\/crm\/dashboard/);
  assert.match(src, /\/api\/crm\/accounts\/:prospectId\/outcome/);
  assert.match(src, /\/api\/crm\/manager\/accounts/);
  assert.match(src, /\/crm/);
});

test('CRM pages reference durable account views', () => {
  const crm = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-crm.html'), 'utf8');
  assert.match(crm, /My Accounts/);
  assert.match(crm, /today_queue/);
  assert.match(crm, /\/ao\/api\/crm\/accounts/);
  const mgr = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-crm-manager.html'), 'utf8');
  assert.match(mgr, /manager\/accounts/);
});

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

test('CRM dashboard API scopes to AO owner', async () => {
  const observed = {};
  const restores = [
    stub('../utils/aoFieldSchema', { ensureAoFieldSchema: async () => {} }),
    stub('../utils/aoCrmSchema', { ensureAoCrmSchema: async () => {} }),
    stub('../services/aoFieldService', {
      getAoProfile: async id => ({ id, name: 'Tony' }),
    }),
    stub('../services/aoCrmService', {
      getAoCrmDashboard: async args => {
        observed.aoUserId = args.aoUserId;
        return {
          summary: { my_accounts: 2, today_queue: 1 },
          sections: { my_accounts: [{ prospect_id: 'p1', company_name: 'Manchester Family Dentistry' }] },
          outcomes: AO_CRM_OUTCOMES,
        };
      },
    }),
  ];

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

    const response = await fetch(`${running.base}/ao/api/crm/dashboard`);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(observed.aoUserId, 101);
    assert.equal(body.sections.my_accounts[0].company_name, 'Manchester Family Dentistry');
  } finally {
    if (running) await new Promise(resolve => running.server.close(resolve));
    delete require.cache[require.resolve('../routes/ao')];
    for (const restore of restores.reverse()) restore();
  }
});
