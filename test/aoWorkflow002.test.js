'use strict';

const assert = require('node:assert/strict');
const express = require('express');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  buildProspectBriefSections,
  renderCrmBriefText,
} = require('../utils/aoProspectBrief');
const { recommendJakeActionForFlag } = require('../utils/aoAccountFlagTypes');

test('CRM Max brief includes required AO-facing sections', () => {
  const sections = buildProspectBriefSections({
    prospect: {
      name: 'Jane Doe',
      job_title: 'Office Manager',
      ao_why_account_matters: 'Single-tenant law office in the Manchester pilot.',
      recommended_angle: 'Backup vendor when turnover hits.',
      ao_next_action: 'call',
      vertical: 'law_firm',
    },
    company: { name: 'Beacon Law', location: 'Manchester NH' },
    touchpoints: [],
    task: { suggested_opener: 'Quick question on who owns cleaning decisions.' },
  });
  const text = renderCrmBriefText(sections);
  assert.match(text, /Where this stands/);
  assert.match(text, /Why this is the next move/);
  assert.match(text, /Known context/);
  assert.match(text, /Next action/);
  assert.match(text, /Short talk track/);
  assert.match(text, /What to listen for/);
  assert.equal(sections.account_name, 'Beacon Law');
});

test('flag reasons map to operator recommended actions', () => {
  const action = recommendJakeActionForFlag('walkthrough_or_proposal', 'Beacon Law');
  assert.match(action, /Beacon Law/);
  assert.match(action, /walkthrough|proposal/i);
});

test('ao routes expose SPEC-AO-WORKFLOW-002 endpoints', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'routes', 'ao.js'), 'utf8');
  assert.match(src, /\/api\/crm\/accounts\/:prospectId\/flag-for-jake/);
  assert.match(src, /\/api\/max\/prospect-brief/);
});

test('AO CRM UI wires Brief me and Flag for Jake', () => {
  const crm = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-crm.html'), 'utf8');
  assert.match(crm, /Brief me/);
  assert.match(crm, /Flag for Jake/);
  assert.match(crm, /flag-for-jake/);
  assert.match(crm, /prospect-brief/);
  const field = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-dashboard.html'), 'utf8');
  assert.match(field, /Open CRM/);
  assert.match(field, /href="\/ao\/crm"/);
});

test('shell nav exposes Open CRM link', () => {
  const nav = fs.readFileSync(path.join(__dirname, '..', 'public', 'shared', 'aoShellNav.js'), 'utf8');
  assert.match(nav, /Open CRM/);
  assert.match(nav, /\/ao\/crm/);
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

test('flag-for-jake API validates reason', async () => {
  const restores = [
    stub('../utils/aoFieldSchema', { ensureAoFieldSchema: async () => {} }),
    stub('../utils/aoCrmSchema', { ensureAoCrmSchema: async () => {} }),
    stub('../services/aoAccountFlagService', {
      AO_ACCOUNT_FLAG_REASONS: [{ value: 'other', label: 'Something else' }],
      createAccountFlag: async () => ({ ok: true }),
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

    const bad = await fetch(`${running.base}/ao/api/crm/accounts/p1/flag-for-jake`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ note: 'help' }),
    });
    assert.equal(bad.status, 400);

    const good = await fetch(`${running.base}/ao/api/crm/accounts/p1/flag-for-jake`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ reason: 'other', note: 'Need pricing guidance' }),
    });
    assert.equal(good.status, 200);
  } finally {
    if (running) await new Promise(resolve => running.server.close(resolve));
    delete require.cache[require.resolve('../routes/ao')];
    for (const restore of restores.reverse()) restore();
  }
});
