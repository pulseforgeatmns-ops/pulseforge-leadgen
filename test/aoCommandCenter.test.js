'use strict';

const assert = require('node:assert/strict');
const express = require('express');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  computePriorityScore,
  rankProspectRows,
  isOverdue,
  isSameDay,
} = require('../utils/aoCommandCenterRanking');
const { isValidOutcomeType, AO_OUTCOME_TYPES } = require('../utils/aoProspectUpdateTypes');
const { parseArgs } = require('../scripts/reviewAoDailyCommandCenter');

test('priority ranking prefers overdue follow-ups', () => {
  const dateStr = '2026-09-24';
  const overdue = {
    company_name: 'Overdue Co',
    next_action_due_at: '2026-09-23T12:00:00Z',
    ao_fit_score: 40,
    conversation_status: null,
    ao_assignment_category: null,
    assigned_at: '2026-09-01',
    last_touch_at: '2026-09-20',
  };
  const highFit = {
    company_name: 'High Fit Co',
    next_action_due_at: '2026-10-01T12:00:00Z',
    ao_fit_score: 90,
    conversation_status: null,
    ao_assignment_category: 'HIGH_VALUE_ICP',
    assigned_at: '2026-09-01',
    last_touch_at: '2026-09-20',
  };
  const activeConv = {
    company_name: 'Active Conv Co',
    next_action_due_at: '2026-10-01T12:00:00Z',
    ao_fit_score: 50,
    conversation_status: 'active',
    ao_assignment_category: null,
    assigned_at: '2026-09-01',
    last_touch_at: '2026-09-20',
  };

  const ranked = rankProspectRows([highFit, activeConv, overdue], { dateStr });
  assert.equal(ranked[0].company_name, 'Overdue Co');
  assert.ok(ranked[0].priority_score > ranked[1].priority_score);
});

test('follow-up due helpers distinguish overdue, today, and future', () => {
  const dateStr = '2026-09-24';
  assert.equal(isOverdue('2026-09-23T12:00:00Z', dateStr), true);
  assert.equal(isSameDay('2026-09-24T15:00:00Z', dateStr), true);
  assert.equal(isOverdue('2026-09-25T12:00:00Z', dateStr), false);
  assert.equal(isSameDay('2026-09-25T12:00:00Z', dateStr), false);
});

test('outcome type enum matches spec', () => {
  assert.equal(AO_OUTCOME_TYPES.length, 11);
  assert.equal(isValidOutcomeType('booked_assessment'), true);
  assert.equal(isValidOutcomeType('invalid'), false);
});

test('ao routes expose SPEC-AO-005 command center endpoints', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'routes', 'ao.js'), 'utf8');
  assert.match(src, /\/api\/command-center/);
  assert.match(src, /\/api\/prospects\/:prospectId\/log-update/);
  assert.match(src, /\/api\/max\/conversations\/continue/);
  assert.match(src, /\/command-center/);
  assert.match(src, /\/field/);
});

test('command center page exposes core AO actions', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-command-center.html'), 'utf8');
  assert.match(src, /Today's Command Center/);
  assert.match(src, /command_center_brief_button/);
  assert.match(src, /\/api\/max\/flag-routing/);
  assert.match(src, /log-update/);
  assert.match(src, /conversations\/continue/);
});

test('review script parses tenant ao and date flags', () => {
  assert.deepEqual(parseArgs(['--tenant', '10', '--ao', 'tony', '--date', 'today']), {
    tenantId: '10',
    aoName: 'tony',
    date: 'today',
    json: false,
  });
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

test('command center API scopes to authenticated AO owner', async () => {
  const observed = {};
  const restores = [
    stub('../utils/aoFieldSchema', { ensureAoFieldSchema: async () => {} }),
    stub('../services/aoFieldService', {
      getAoProfile: async id => ({ id, name: 'Tony', client_id: 10 }),
    }),
    stub('../services/aoCommandCenterService', {
      getCommandCenter: async args => {
        observed.aoUserId = args.aoUserId;
        observed.clientId = args.clientId;
        return {
          date: '2026-09-24',
          tenant_id: '10',
          ao_user: { id: '101', name: 'Tony' },
          summary: {
            priority_accounts: 1,
            followups_due: 1,
            conversations_to_continue: 0,
            routing_flags_open: 0,
            updates_needed: 0,
          },
          sections: {
            priority_accounts: [{ prospect_id: 'p1', company_name: 'Test Co' }],
            followups_due: [],
            conversations_to_continue: [],
            recently_assigned: [],
            routing_issues: [],
            updates_needed: [],
          },
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

    const response = await fetch(`${running.base}/ao/api/command-center`);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(observed.aoUserId, 101);
    assert.equal(observed.clientId, 10);
    assert.equal(body.summary.priority_accounts, 1);
    assert.equal(body.sections.priority_accounts[0].company_name, 'Test Co');
  } finally {
    if (running) await new Promise(resolve => running.server.close(resolve));
    delete require.cache[require.resolve('../routes/ao')];
    for (const restore of restores.reverse()) restore();
  }
});

test('log update API rejects invalid outcome and delegates to service', async () => {
  const observed = {};
  const restores = [
    stub('../utils/aoFieldSchema', { ensureAoFieldSchema: async () => {} }),
    stub('../services/aoProspectUpdateService', {
      logProspectUpdate: async args => {
        observed.args = args;
        if (args.outcomeType === 'invalid') return { error: 'Valid outcome_type required', status: 400 };
        return { ok: true, update: { outcome_type: args.outcomeType } };
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

    let response = await fetch(`${running.base}/ao/api/prospects/p1/log-update`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ outcome_type: 'invalid' }),
    });
    assert.equal(response.status, 400);

    response = await fetch(`${running.base}/ao/api/prospects/p1/log-update`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        outcome_type: 'left_voicemail',
        notes: 'Left voicemail.',
        next_action_due_at: '2026-09-25T14:00:00-04:00',
      }),
    });
    assert.equal(response.status, 200);
    assert.equal(observed.args.prospectId, 'p1');
    assert.equal(observed.args.outcomeType, 'left_voicemail');
  } finally {
    if (running) await new Promise(resolve => running.server.close(resolve));
    delete require.cache[require.resolve('../routes/ao')];
    for (const restore of restores.reverse()) restore();
  }
});

test('log update service maps outcome to prospect fields', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoProspectUpdateService.js'), 'utf8');
  assert.match(src, /ao_prospect_updates/);
  assert.match(src, /AO_PROSPECT_UPDATE_LOGGED/);
  assert.match(src, /last_debrief_status/);
  assert.match(src, /next_action_owner/);
});

test('continue conversation helper is exported from aoMaxConversation', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoMaxConversation.js'), 'utf8');
  assert.match(src, /continueConversationForProspect/);
  assert.match(src, /AO_COMMAND_CENTER_CONTINUE_CLICKED/);
});
