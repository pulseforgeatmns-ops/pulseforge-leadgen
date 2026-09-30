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
  assert.match(src, /\/api\/crm\/accounts\/:prospectId\/followup\/draft/);
  assert.match(src, /\/api\/crm\/accounts\/:prospectId\/followup\/save/);
  assert.match(src, /\/api\/crm\/manager\/accounts/);
  assert.match(src, /\/api\/tasks\/:id\/crm-context/);
  assert.match(src, /\/crm/);
});

test('field queue cannot mark tasks done without CRM outcome', async () => {
  const aoField = require('../services/aoFieldService');
  const result = await aoField.updateTask('00000000-0000-4000-8000-000000000001', 1, { status: 'done' });
  assert.equal(result.code, 'CRM_OUTCOME_REQUIRED');
  assert.equal(result.status, 409);
});

test('CRM pages reference durable account views', () => {
  const crm = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-crm.html'), 'utf8');
  assert.match(crm, /My Accounts/);
  assert.match(crm, /today_queue/);
  assert.match(crm, /\/ao\/api\/crm\/accounts/);
  assert.match(crm, /Draft Follow-Up/);
  assert.match(crm, /followup\/draft/);
  const mgr = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-crm-manager.html'), 'utf8');
  assert.match(mgr, /manager\/accounts/);
});

test('AO CRM Brief Me modal keeps long briefs within the mobile viewport', () => {
  const crm = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-crm.html'), 'utf8');
  assert.match(crm, /max-height:\s*calc\(100dvh - 32px\)/);
  assert.match(crm, /modal-brief-scroll/);
  assert.match(crm, /body\.modal-open/);
  assert.match(crm, /openModalBackdrop/);
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

const ALLOWED_AGENT_LOG_STATUSES = new Set([
  'success', 'failed', 'skipped', 'pending', 'completed', 'posted', 'in_progress',
]);

test('logAoAuditEvent status is in agent_log_status_check allow-list', async () => {
  const captured = [];
  const restoreDb = stub('../db', {
    query: async (sql, params) => {
      captured.push({ sql, params });
      return { rows: [] };
    },
  });
  try {
    delete require.cache[require.resolve('../utils/aoAuditEvents')];
    const { logAoAuditEvent } = require('../utils/aoAuditEvents');
    await logAoAuditEvent({
      event: 'AO_CRM_DASHBOARD_VIEWED',
      clientId: 10,
      aoUserId: 101,
      payload: { account_count: 3 },
    });
    assert.equal(captured.length, 1);
    assert.match(captured[0].sql, /INSERT INTO agent_log/);
    const statusMatch = captured[0].sql.match(/,\s*'([^']+)',\s*NOW\(\)/);
    assert.ok(statusMatch, 'expected status literal before NOW()');
    assert.ok(ALLOWED_AGENT_LOG_STATUSES.has(statusMatch[1]), statusMatch[1]);
    assert.equal(captured[0].params[0], 'AO_CRM_DASHBOARD_VIEWED');
  } finally {
    delete require.cache[require.resolve('../utils/aoAuditEvents')];
    restoreDb();
  }
});

test('AO CRM audit helper does not insert invalid agent_log.status ok', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'utils', 'aoAuditEvents.js'), 'utf8');
  assert.match(src, /VALUES \(\s*'ao'[^)]+'success'/s);
  assert.doesNotMatch(src, /,\s*'ok',\s*NOW\(\)/);
});

test('AO CRM shell routes serve HTML when authenticated', async () => {
  const restores = [
    stub('../utils/aoFieldSchema', { ensureAoFieldSchema: async () => {} }),
    stub('../services/aoFieldService', {
      getAoProfile: async id => ({ id, name: 'Tony', client_id: 10 }),
    }),
  ];

  let running;
  try {
    delete require.cache[require.resolve('../routes/ao')];
    const router = require('../routes/ao');
    const app = express();
    app.use((req, _res, next) => {
      req.session = { user: req.headers['x-test-user'] === 'manager'
        ? { id: 1, role: 'manager', client_id: 10, name: 'Jake' }
        : { id: 101, role: 'ao', client_id: 10, name: 'Tony' },
      };
      next();
    });
    app.use('/ao', router);
    running = await listen(app);

    for (const [route, asManager] of [
      ['/ao/crm', false],
      ['/ao/field', false],
      ['/ao/crm/manager', true],
    ]) {
      const response = await fetch(`${running.base}${route}`, {
        headers: asManager ? { 'x-test-user': 'manager' } : {},
      });
      assert.equal(response.status, 200, route);
      const body = await response.text();
      assert.match(body, /<!DOCTYPE html>|<html/i, route);
    }
  } finally {
    if (running) await new Promise(resolve => running.server.close(resolve));
    delete require.cache[require.resolve('../routes/ao')];
    for (const restore of restores.reverse()) restore();
  }
});
