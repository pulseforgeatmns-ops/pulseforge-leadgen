'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  crmLinkageStatus,
  parseAddressParts,
  formatAddress,
} = require('../services/aoCrmLinkRecoveryService');

test('PEC-AO-CRM-LINK-001 routes are registered', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'routes', 'ao.js'), 'utf8');
  assert.match(src, /\/api\/crm\/accounts\/search/);
  assert.match(src, /\/api\/leads\/:leadId\/crm-linkage/);
  assert.match(src, /\/api\/leads\/:leadId\/crm-link/);
  assert.match(src, /\/api\/leads\/:leadId\/crm-account/);
  assert.match(src, /\/api\/leads\/:leadId\/prospect/);
});

test('Field dashboard exposes CRM link recovery modal actions', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-dashboard.html'), 'utf8');
  assert.match(html, /crmLinkRecoveryModal/);
  assert.match(html, /Prospect not linked to CRM/);
  assert.match(html, /Link Existing Account/);
  assert.match(html, /Create CRM Account/);
  assert.match(html, /Edit Prospect/);
  assert.doesNotMatch(html, /ask Jake to link the prospect/);
});

test('crm linkage status derives from crm_prospect_id', () => {
  assert.equal(crmLinkageStatus({ crm_prospect_id: 'abc' }), 'linked');
  assert.equal(crmLinkageStatus({ crm_prospect_id: null }), 'unlinked');
});

test('address parsing supports street, city, state, postal editing', () => {
  const parts = parseAddressParts('100 Main St, Manchester, NH 03101');
  assert.equal(parts.street, '100 Main St');
  assert.equal(parts.city, 'Manchester');
  assert.equal(parts.state, 'NH');
  assert.equal(parts.postal, '03101');
  assert.equal(
    formatAddress(parts),
    '100 Main St, Manchester, NH 03101'
  );
});

test('getTaskCrmContext does not auto-link CRM rows from business name', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoFieldService.js'), 'utf8');
  assert.doesNotMatch(src, /resolveTaskCrmProspectId\(row, row\.client_id\)/);
  assert.doesNotMatch(src, /findCrmLinkForBusiness\(clientId, taskRow\.business_name\)/);
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

test('link existing account API delegates to recovery service', async () => {
  const observed = {};
  const restores = [
    stub('../services/aoCrmLinkRecoveryService', {
      linkLeadToExistingAccount: async args => {
        observed.args = args;
        return { ok: true, crm_prospect_id: 'p-99', crm_linkage_status: 'linked' };
      },
    }),
  ];

  let running;
  try {
    delete require.cache[require.resolve('../routes/ao')];
    const express = require('express');
    const router = require('../routes/ao');
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.user = { id: 7, role: 'ao', client_id: 10, name: 'Tony' };
      req.session = { user: req.user, active_client_id: 10 };
      next();
    });
    app.use('/ao', router);
    running = await listen(app);

    const response = await fetch(`${running.base}/ao/api/leads/lead-1/crm-link`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prospect_id: 'p-99', task_id: 'task-1' }),
    });
    assert.equal(response.status, 200);
    assert.equal(observed.args.leadId, 'lead-1');
    assert.equal(observed.args.prospectId, 'p-99');
    assert.equal(observed.args.clientId, 10);
  } finally {
    if (running) await new Promise(resolve => running.server.close(resolve));
    delete require.cache[require.resolve('../routes/ao')];
    for (const restore of restores.reverse()) restore();
  }
});

test('create CRM account duplicate guard returns candidates', async () => {
  const restores = [
    stub('../services/aoCrmLinkRecoveryService', {
      createCrmAccountForLead: async () => ({
        error: 'Likely existing CRM matches found — link an existing account or confirm create',
        status: 409,
        code: 'DUPLICATE_CANDIDATES',
        duplicate_candidates: [{ prospect_id: 'p1', company_name: 'Acme LLC' }],
      }),
    }),
  ];

  let running;
  try {
    delete require.cache[require.resolve('../routes/ao')];
    const express = require('express');
    const router = require('../routes/ao');
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.user = { id: 7, role: 'ao', client_id: 10, name: 'Tony' };
      req.session = { user: req.user, active_client_id: 10 };
      next();
    });
    app.use('/ao', router);
    running = await listen(app);

    const response = await fetch(`${running.base}/ao/api/leads/lead-1/crm-account`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ task_id: 'task-1' }),
    });
    assert.equal(response.status, 409);
    const body = await response.json();
    assert.equal(body.code, 'DUPLICATE_CANDIDATES');
    assert.equal(body.duplicate_candidates[0].company_name, 'Acme LLC');
  } finally {
    if (running) await new Promise(resolve => running.server.close(resolve));
    delete require.cache[require.resolve('../routes/ao')];
    for (const restore of restores.reverse()) restore();
  }
});
