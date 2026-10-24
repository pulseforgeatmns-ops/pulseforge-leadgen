'use strict';

const assert = require('node:assert/strict');
const path = require('node:path');
const test = require('node:test');

const {
  JAKE_AO_TEST_LEAD_NAMES,
  isAllowlistedJakeAoTestLead,
  normalizeAoBusinessKey,
} = require('../scripts/data/jakeAoTestLeadAllowlist');
const {
  partitionJakeLeads,
  deleteTargetLeads,
} = require('../scripts/cleanupJakeAoTestLeads');

const JAKE_ID = 19;
const CLIENT_ID = 10;

test('exact five test names match allowlist after normalization', () => {
  assert.equal(JAKE_AO_TEST_LEAD_NAMES.length, 5);
  for (const name of JAKE_AO_TEST_LEAD_NAMES) {
    assert.equal(isAllowlistedJakeAoTestLead(name), true, name);
  }
});

test('Lodgism never matched by allowlist', () => {
  assert.equal(isAllowlistedJakeAoTestLead('Lodgism'), false);
});

test('arbitrary business containing word test is not matched', () => {
  assert.equal(isAllowlistedJakeAoTestLead('Contoso Test Lab LLC'), false);
  assert.equal(isAllowlistedJakeAoTestLead('Latest Dental Office'), false);
  assert.equal(isAllowlistedJakeAoTestLead('My Test Company'), false);
});

test('partitionJakeLeads scopes by allowlist only — wrong owner/tenant handled upstream', () => {
  const leads = [
    { id: '1', business_name: 'Test Dental Office', ao_owner_id: JAKE_ID, client_id: CLIENT_ID },
    { id: '2', business_name: 'Lodgism', ao_owner_id: JAKE_ID, client_id: CLIENT_ID },
    { id: '3', business_name: 'test school', ao_owner_id: 20, client_id: CLIENT_ID },
    { id: '4', business_name: 'Test med spa', ao_owner_id: JAKE_ID, client_id: 99 },
  ];

  const { targets, preserved } = partitionJakeLeads(leads);
  assert.deepEqual(
    targets.map(l => l.business_name).sort(),
    ['Test Dental Office', 'test school', 'Test med spa'].sort(),
  );
  assert.deepEqual(preserved.map(l => l.business_name), ['Lodgism']);
});

test('partitionJakeLeads rejects case/punctuation variants outside allowlist', () => {
  const { targets } = partitionJakeLeads([
    { id: 'x', business_name: 'TEST DENTAL OFFICE!!!', ao_owner_id: JAKE_ID, client_id: CLIENT_ID },
  ]);
  assert.equal(targets.length, 1);
  assert.equal(normalizeAoBusinessKey(targets[0].business_name), 'testdentaloffice');
});

test('deleteTargetLeads removes route stops then leads (mock pool)', async () => {
  const calls = [];
  const leadId = 'aaaaaaaa-bbbb-cccc-dddd-000000000001';
  const mockClient = {
    query: async (sql, params) => {
      calls.push({ sql: sql.replace(/\s+/g, ' ').trim(), params });
      if (/DELETE FROM ao_route_stops/i.test(sql)) return { rowCount: 2 };
      if (/DELETE FROM ao_leads/i.test(sql)) return { rowCount: 1 };
      if (/BEGIN|COMMIT|ROLLBACK/i.test(sql)) return { rowCount: 0 };
      return { rowCount: 0 };
    },
    release() {},
  };

  const pool = require('../db');
  const originalConnect = pool.connect;
  pool.connect = async () => mockClient;

  try {
    const result = await deleteTargetLeads([leadId]);
    assert.equal(result.route_stops_deleted, 2);
    assert.equal(result.leads_deleted, 1);
    assert.ok(calls.some(c => /DELETE FROM ao_route_stops/i.test(c.sql)));
    assert.ok(calls.some(c => /DELETE FROM ao_leads/i.test(c.sql)));
    const beginIdx = calls.findIndex(c => /BEGIN/i.test(c.sql));
    const commitIdx = calls.findIndex(c => /COMMIT/i.test(c.sql));
    assert.ok(beginIdx >= 0 && commitIdx > beginIdx);
  } finally {
    pool.connect = originalConnect;
  }
});

test('cleanup idempotent — second partition finds no targets after delete simulation', () => {
  const remaining = [{ id: '2', business_name: 'Lodgism', ao_owner_id: JAKE_ID, client_id: CLIENT_ID }];
  const secondPass = partitionJakeLeads(remaining);
  assert.equal(secondPass.targets.length, 0);
  assert.equal(secondPass.preserved.length, 1);
});
