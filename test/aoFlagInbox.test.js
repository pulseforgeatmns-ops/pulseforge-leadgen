'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');
const {
  buildIdempotencyKey,
  createAccountFlag,
} = require('../services/aoAccountFlagService');
const { isSelfFlag } = require('../utils/aoFlagAssignee');
const { resetForTests, snapshot } = require('../utils/aoFlagMetrics');
const {
  resolveMaxBriefingClientId,
  withMaxBriefingClientId,
  PAGE_DEFAULT_CLIENT_ID,
} = require('../lib/maxAoBriefingTenantApi');

test('idempotency key is stable for the same escalation identity', () => {
  const a = buildIdempotencyKey({
    clientId: 10,
    sourceType: 'crm_account',
    sourceId: 'p1',
    createdByUserId: 5,
    reason: 'other',
    note: 'Need help',
  });
  const b = buildIdempotencyKey({
    clientId: 10,
    sourceType: 'crm_account',
    sourceId: 'p1',
    createdByUserId: 5,
    reason: 'other',
    note: 'Need help',
  });
  const c = buildIdempotencyKey({
    clientId: 10,
    sourceType: 'crm_account',
    sourceId: 'p1',
    createdByUserId: 5,
    reason: 'other',
    note: 'Different issue',
  });
  assert.equal(a, b);
  assert.notEqual(a, c);
});

test('self-flag detection skips operator self-notification', () => {
  assert.equal(isSelfFlag({
    createdByUserId: 1,
    assigneeUserId: 1,
    creatorRole: 'admin',
  }), true);
  assert.equal(isSelfFlag({
    createdByUserId: 2,
    assigneeUserId: 1,
    creatorRole: 'ao',
  }), false);
});

test('max briefing routes expose AO flag inbox APIs', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'routes', 'maxAoBriefing.js'), 'utf8');
  assert.match(src, /\/api\/v1\/max\/ao-flags/);
  assert.match(src, /ao-flags\/unread-count/);
  assert.match(src, /ao-flag-notifications/);
});

test('max AO briefing UI includes Needs Jake flags inbox', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'max-ao-briefing.html'), 'utf8');
  const js = fs.readFileSync(path.join(__dirname, '..', 'public', 'max-ao-briefing', 'max-ao-briefing.js'), 'utf8');
  assert.match(html, /Needs Jake/);
  assert.match(html, /mabFlags/);
  assert.match(html, /tenant-api\.js/);
  assert.match(js, /ao-flags/);
  assert.match(js, /withMaxBriefingClientId/);
  assert.match(js, /resolveMaxBriefingClientId/);
});

test('AO-FLAG-INBOX-003 — page client_id=10 wins over session active_client_id=1', () => {
  const clients = [
    { id: 1, name: 'Pulseforge' },
    { id: 10, name: 'Anchor Cleaning' },
  ];
  const resolved = resolveMaxBriefingClientId({
    urlClientId: '10',
    selectValue: '1',
    clients,
    pageDefault: PAGE_DEFAULT_CLIENT_ID,
  });
  assert.equal(resolved, 10);
});

test('AO-FLAG-INBOX-003 — flag list and unread URLs carry page tenant client_id', () => {
  const listUrl = withMaxBriefingClientId('/api/v1/max/ao-flags?status=open', 10);
  const unreadUrl = withMaxBriefingClientId('/api/v1/max/ao-flags/unread-count', 10);
  const patchUrl = withMaxBriefingClientId('/api/v1/max/ao-flags/abc-123', 10);
  assert.equal(listUrl, '/api/v1/max/ao-flags?status=open&client_id=10');
  assert.equal(unreadUrl, '/api/v1/max/ao-flags/unread-count?client_id=10');
  assert.equal(patchUrl, '/api/v1/max/ao-flags/abc-123?client_id=10');
});

test('AO-FLAG-INBOX-003 — withMaxBriefingClientId replaces stale client_id query param', () => {
  const url = withMaxBriefingClientId('/api/v1/max/ao-flags?status=open&client_id=1', 10);
  assert.equal(url, '/api/v1/max/ao-flags?status=open&client_id=10');
});

function makeMockDb(state) {
  return {
    query: async (sql, params) => {
      state.queries.push({ sql, params });
      if (/FROM agent_log/.test(sql) && /AO_FLAG_INBOX_001_BACKFILL_DONE/.test(sql)) {
        return { rows: state.backfillDone ? [{ '?column?': 1 }] : [] };
      }
      if (/INSERT INTO agent_log/.test(sql) && /AO_FLAG_INBOX_001_BACKFILL_DONE/.test(sql)) {
        state.backfillDone = true;
        return { rows: [] };
      }
      if (/FROM users/.test(sql)) {
        return { rows: state.assignee ? [state.assignee] : [] };
      }
      if (/FROM prospects p/.test(sql) && /company_name/.test(sql)) {
        return { rows: state.account ? [state.account] : [] };
      }
      if (/SELECT \* FROM ao_account_flags[\s\S]*idempotency_key/.test(sql)) {
        return { rows: state.duplicate ? [state.duplicateRow] : [] };
      }
      if (/INSERT INTO ao_account_flags/.test(sql)) {
        state.flag = {
          id: 'flag-1',
          client_id: params[0],
          account_id: params[1],
          ao_id: params[2],
          reason: params[3],
          note: params[4],
          status: 'open',
          source_type: 'crm_account',
          source_id: params[1],
          conversation_id: params[5],
          created_by_user_id: params[2],
          created_by_role: params[6],
          assigned_to_user_id: params[7],
          unread: true,
          idempotency_key: params[8],
          source_context: {},
          created_at: new Date().toISOString(),
        };
        return { rows: [state.flag] };
      }
      if (/UPDATE prospects SET/.test(sql)) return { rows: [] };
      if (/INSERT INTO ao_prospect_activity/.test(sql)) return { rows: [] };
      if (/INSERT INTO agent_log/.test(sql)) return { rows: [] };
      if (/VALUES \('ao'/.test(sql)) return { rows: [] };
      if (/SELECT name FROM users WHERE id/.test(sql)) {
        return { rows: [{ name: 'Tony' }] };
      }
      if (/INSERT INTO ao_flag_notifications/.test(sql)) {
        if (state.failNotifications) throw new Error('notification down');
        state.notificationCreated = true;
        return { rows: [{ id: 'n1' }] };
      }
      if (/SELECT COUNT/.test(sql)) return { rows: [{ count: 0, missing_linkage: 0, missing_reason: 0 }] };
      if (/UPDATE ao_account_flags/.test(sql) && /assigned_to_user_id/.test(sql)) {
        return { rowCount: 0 };
      }
      if (/FROM ao_prospect_activity/.test(sql)) return { rows: [] };
      if (/UPDATE ao_account_flags/.test(sql)) return { rows: [] };
      return { rows: [] };
    },
  };
}

test('F1 Tony flags — durable flag, assignee, notification', async () => {
  resetForTests();
  const prospectId = '11111111-1111-4111-8111-111111111111';
  const state = {
    queries: [],
    assignee: { id: 1, name: 'Jake', email: 'jake@test.com', role: 'admin' },
    account: { id: prospectId, company_name: 'Exeter Phillips' },
    failNotifications: false,
  };
  const db = makeMockDb(state);
  const result = await createAccountFlag({
    clientId: 10,
    aoUserId: 99,
    prospectId,
    reason: 'decision_maker_block',
    note: 'Decision-maker question',
    creatorRole: 'ao',
    db,
  });
  assert.equal(result.ok, true);
  assert.equal(result.flag.business_name, 'Exeter Phillips');
  assert.equal(state.notificationCreated, true);
  assert.ok(Object.keys(snapshot()).some(k => k.startsWith('ao_flag_created_count')));
});

test('F2 Jake self-flag — no notification', async () => {
  resetForTests();
  const prospectId = '11111111-1111-4111-8111-111111111111';
  const state = {
    queries: [],
    assignee: { id: 1, name: 'Jake', email: 'jake@test.com', role: 'admin' },
    account: { id: prospectId, company_name: 'Exeter Phillips' },
  };
  const db = makeMockDb(state);
  const result = await createAccountFlag({
    clientId: 10,
    aoUserId: 1,
    prospectId,
    reason: 'other',
    note: 'Self review',
    creatorRole: 'admin',
    db,
  });
  assert.equal(result.ok, true);
  assert.equal(result.notification_skipped, true);
  assert.equal(state.notificationCreated, undefined);
});

test('F6 duplicate retry returns existing flag', async () => {
  resetForTests();
  const state = {
    queries: [],
    assignee: { id: 1, role: 'admin' },
    account: { id: '22222222-2222-4222-8222-222222222222', company_name: 'Exeter Phillips' },
    duplicate: true,
    duplicateRow: {
      id: 'existing',
      client_id: 10,
      account_id: '22222222-2222-4222-8222-222222222222',
      ao_id: 99,
      reason: 'other',
      note: 'x',
      status: 'open',
      source_type: 'crm_account',
      source_id: 'p1',
      unread: true,
      created_at: new Date().toISOString(),
    },
  };
  const db = makeMockDb(state);
  const result = await createAccountFlag({
    clientId: 10,
    aoUserId: 99,
    prospectId: '22222222-2222-4222-8222-222222222222',
    reason: 'other',
    note: 'x',
    creatorRole: 'ao',
    db,
  });
  assert.equal(result.duplicate, true);
  assert.equal(result.flag.id, 'existing');
});

test('F10 notification failure keeps durable flag', async () => {
  resetForTests();
  const state = {
    queries: [],
    assignee: { id: 1, role: 'admin' },
    account: { id: '33333333-3333-4333-8333-333333333333', company_name: 'Granite State Daycare' },
    failNotifications: true,
  };
  const db = makeMockDb(state);
  const result = await createAccountFlag({
    clientId: 10,
    aoUserId: 99,
    prospectId: '33333333-3333-4333-8333-333333333333',
    reason: 'other',
    note: 'Needs next step',
    creatorRole: 'ao',
    db,
  });
  assert.equal(result.ok, true);
  assert.equal(result.flag.company_name, 'Granite State Daycare');
  assert.ok(snapshot()['ao_flag_notification_failure_count']);
});

test('F13 briefing digest prioritizes open AO flags', () => {
  const { buildDailyDigestText } = require('../services/aoBriefingService');
  const text = buildDailyDigestText({
    today: { visits_today: 0 },
    leads: [],
    escalations: [],
    warmOpportunities: [],
    recommendations: { jake: [], mike: [] },
    campaign: { visited: 0, target_total: 0, walkthrough_requests: 0, remaining_route_queue: 0 },
    aoAccountFlags: [{
      ao_name: 'Tony',
      business_name: 'Exeter Phillips',
      reason: 'Decision-maker question',
    }],
  });
  assert.match(text.text, /AO flag need your attention/);
  assert.match(text.text, /Tony — Exeter Phillips/);
});

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise(resolve => server.on('listening', () => resolve({
    server,
    base: `http://127.0.0.1:${server.address().port}`,
  })));
}

test('AO-FLAG-INBOX-003 — unread-count and list honor query client_id when session tenant differs', async () => {
  const restores = [];
  const listCalls = [];
  const unreadCalls = [];
  restores.push((() => {
    const mod = require('../services/aoAccountFlagService');
    const origList = mod.listFlagsForAssignee;
    const origUnread = mod.countUnreadFlags;
    mod.listFlagsForAssignee = async (opts) => {
      listCalls.push(opts);
      return [{ id: 'f1', client_id: opts.clientId, status: 'open' }];
    };
    mod.countUnreadFlags = async (opts) => {
      unreadCalls.push(opts);
      return 5;
    };
    return () => {
      mod.listFlagsForAssignee = origList;
      mod.countUnreadFlags = origUnread;
    };
  })());
  restores.push((() => {
    const mod = require('../utils/aoFlagAssignee');
    const original = mod.resolveFlagAssigneeUserId;
    mod.resolveFlagAssigneeUserId = async () => ({ id: 1 });
    return () => { mod.resolveFlagAssigneeUserId = original; };
  })());

  let running;
  const fieldSchema = require('../utils/aoFieldSchema');
  const crmSchema = require('../utils/aoCrmSchema');
  const prevField = fieldSchema.ensureAoFieldSchema;
  const prevCrm = crmSchema.ensureAoCrmSchema;
  try {
    fieldSchema.ensureAoFieldSchema = async () => {};
    crmSchema.ensureAoCrmSchema = async () => {};

    delete require.cache[require.resolve('../routes/maxAoBriefing')];
    const router = require('../routes/maxAoBriefing');
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.user = { id: 1, role: 'admin' };
      req.session = { user: req.user, active_client_id: 1 };
      next();
    });
    app.use(router);
    running = await listen(app);

    const listRes = await fetch(`${running.base}/api/v1/max/ao-flags?status=open&client_id=10`);
    const listBody = await listRes.json();
    assert.equal(listRes.status, 200);
    assert.equal(listCalls.length, 1);
    assert.equal(listCalls[0].clientId, 10);
    assert.equal(listBody.flags.length, 1);

    const unreadRes = await fetch(`${running.base}/api/v1/max/ao-flags/unread-count?client_id=10`);
    const unreadBody = await unreadRes.json();
    assert.equal(unreadRes.status, 200);
    assert.equal(unreadCalls.length, 1);
    assert.equal(unreadCalls[0].clientId, 10);
    assert.equal(unreadBody.unread_count, 5);

    const wrongTenant = await fetch(`${running.base}/api/v1/max/ao-flags?status=open&client_id=1`);
    await wrongTenant.json();
    assert.equal(listCalls[1].clientId, 1);
  } finally {
    if (running) await new Promise(r => running.server.close(r));
    delete require.cache[require.resolve('../routes/maxAoBriefing')];
    fieldSchema.ensureAoFieldSchema = prevField;
    crmSchema.ensureAoCrmSchema = prevCrm;
    for (const r of restores.reverse()) r();
  }
});

test('F8 tenant isolation on flag inbox list uses assignee + client scope', async () => {
  const restores = [];
  const listCalls = [];
  restores.push((() => {
    const mod = require('../services/aoAccountFlagService');
    const original = mod.listFlagsForAssignee;
    mod.listFlagsForAssignee = async (opts) => {
      listCalls.push(opts);
      return [{ id: 'f1', client_id: opts.clientId }];
    };
    return () => { mod.listFlagsForAssignee = original; };
  })());
  restores.push((() => {
    const mod = require('../utils/aoFlagAssignee');
    const original = mod.resolveFlagAssigneeUserId;
    mod.resolveFlagAssigneeUserId = async () => ({ id: 1 });
    return () => { mod.resolveFlagAssigneeUserId = original; };
  })());

  let running;
  const fieldSchema = require('../utils/aoFieldSchema');
  const crmSchema = require('../utils/aoCrmSchema');
  const prevField = fieldSchema.ensureAoFieldSchema;
  const prevCrm = crmSchema.ensureAoCrmSchema;
  try {
    fieldSchema.ensureAoFieldSchema = async () => {};
    crmSchema.ensureAoCrmSchema = async () => {};

    delete require.cache[require.resolve('../routes/maxAoBriefing')];
    const router = require('../routes/maxAoBriefing');
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.user = { id: 1, role: 'admin' };
      req.session = { user: req.user, active_client_id: 10 };
      next();
    });
    app.use(router);
    running = await listen(app);
    await fetch(`${running.base}/api/v1/max/ao-flags?client_id=10`);
    assert.equal(listCalls[0].clientId, 10);
  } finally {
    if (running) await new Promise(r => running.server.close(r));
    delete require.cache[require.resolve('../routes/maxAoBriefing')];
    fieldSchema.ensureAoFieldSchema = prevField;
    crmSchema.ensureAoCrmSchema = prevCrm;
    for (const r of restores.reverse()) r();
  }
});
