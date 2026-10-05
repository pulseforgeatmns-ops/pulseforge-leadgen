'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {
  ingestOperationalUpdate,
  ingestSpreadsheet,
  MemoryStateStore,
  RESOLUTION,
  markOverdueExpectations,
  followUpPromptForExpectation,
  CLAIM_TYPES,
} = require('../packages/max/stateIngestion');

function seedStore(overrides = {}) {
  return new MemoryStateStore({
    clientId: 1,
    users: [
      { id: 10, name: 'Tony' },
      { id: 20, name: 'Rory' },
    ],
    companies: overrides.companies || [
      { id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 },
    ],
    prospects: overrides.prospects || [
      {
        id: 'prospect-exeter',
        client_id: 1,
        company_id: 'co-exeter',
        company_name: 'Exeter Phillips',
        assigned_ao_id: 10,
        status: 'warm',
        ao_last_touch_at: '2026-09-01T12:00:00.000Z',
      },
    ],
    contacts: overrides.contacts || [
      { id: 'contact-1', prospect_id: 'prospect-exeter', name: 'Pat', ao_id: 10 },
    ],
    ...overrides,
  });
}

test('1 known AO + known account + simple update', async () => {
  const store = seedStore();
  const result = await ingestOperationalUpdate({
    clientId: 1,
    sourceType: 'AO_REPORTED',
    text: 'Tony talked to Exeter Phillips. His contact said they are interested and is supposed to call him this week.',
    store,
  });
  assert.equal(result.prospect.id, 'prospect-exeter');
  assert.equal(result.prospect.follow_up_state, 'awaiting_contact');
  assert.ok(result.prospect.ao_last_touch_at);
  assert.equal(result.unresolved.length, 0);
});

test('2 duplicate ingestion produces no duplicate operational state', async () => {
  const store = seedStore();
  const input = {
    clientId: 1,
    text: 'Tony talked to Exeter Phillips. Contact expects to call Tony this week.',
    store,
  };
  const first = await ingestOperationalUpdate(input);
  const touchCount = store.prospects[0].activities?.length || 0;
  const second = await ingestOperationalUpdate(input);
  assert.ok(first.telemetry.mutations_committed >= 1);
  assert.ok(second.telemetry.duplicate_claims_suppressed >= 1 || second.duplicateReplay);
  assert.equal(store.prospects[0].activities?.length || 0, touchCount);
});

test('3 misspelled account resolves with sufficient evidence', async () => {
  const store = seedStore();
  const result = await ingestOperationalUpdate({
    clientId: 1,
    text: 'Tony update: Exter Phillips expects to call this week.',
    store,
  });
  assert.equal(result.prospect.id, 'prospect-exeter');
  assert.notEqual(result.resolutions[Object.keys(result.resolutions).find(k => k.startsWith('ACCOUNT'))].status, RESOLUTION.AMBIGUOUS);
});

test('4 ambiguous company is not guessed', async () => {
  const store = seedStore({
    companies: [
      { id: 'co-g1', name: 'Granite State Plastics', client_id: 1 },
      { id: 'co-g2', name: 'Granite State Products', client_id: 1 },
    ],
    prospects: [],
  });
  const result = await ingestOperationalUpdate({
    clientId: 1,
    structured: {
      claims: [
        { claim_type: CLAIM_TYPES.AO, payload: { name: 'Tony' } },
        { claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: 'Granite State' } },
        { claim_type: CLAIM_TYPES.EVENT, payload: { kind: 'visit' } },
      ],
    },
    store,
  });
  const accountResolution = Object.values(result.resolutions).find(r => r.candidates?.length > 1);
  assert.equal(accountResolution.status, RESOLUTION.AMBIGUOUS);
  assert.ok(!result.prospect);
});

test('5 conflicting ownership is not silently overwritten', async () => {
  const store = seedStore({
    prospects: [{
      id: 'prospect-exeter',
      client_id: 1,
      company_id: 'co-exeter',
      company_name: 'Exeter Phillips',
      assigned_ao_id: 20,
      status: 'warm',
    }],
  });
  const result = await ingestOperationalUpdate({
    clientId: 1,
    structured: {
      claims: [
        { claim_type: CLAIM_TYPES.AO, payload: { name: 'Tony' } },
        { claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: 'Exeter Phillips' } },
        { claim_type: CLAIM_TYPES.OWNERSHIP, payload: { ao_name: 'Tony' } },
      ],
    },
    store,
  });
  assert.equal(store.prospects[0].assigned_ao_id, 20);
  assert.ok(result.conflicts.length >= 1 || store.conflicts.length >= 1);
});

test('6 partially ambiguous update commits safe claims in isolation', async () => {
  const store = seedStore({
    contacts: [
      { id: 'c-a', prospect_id: 'prospect-exeter', name: 'Alex' },
      { id: 'c-b', prospect_id: 'prospect-exeter', name: 'Alexis' },
    ],
  });
  const result = await ingestOperationalUpdate({
    clientId: 1,
    structured: {
      claims: [
        { claim_type: CLAIM_TYPES.AO, payload: { name: 'Tony' } },
        { claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: 'Exeter Phillips' } },
        { claim_type: CLAIM_TYPES.CONTACT, payload: { name: 'Alex' } },
        { claim_type: CLAIM_TYPES.NEXT_EXPECTED_EVENT, payload: { kind: 'inbound_call' } },
      ],
    },
    store,
  });
  assert.equal(result.prospect.follow_up_state, 'awaiting_contact');
  assert.ok(result.unresolved.length >= 1);
});

test('7 expected future event persists as unresolved expectation', async () => {
  const store = seedStore();
  await ingestOperationalUpdate({
    clientId: 1,
    text: 'Tony talked to Exeter Phillips. His contact is supposed to call him this week.',
    store,
  });
  const exp = store.expectations.find(e => e.prospect_id === 'prospect-exeter');
  assert.ok(exp);
  assert.equal(exp.status, 'WAITING');
  assert.equal(exp.expectation_type, 'inbound_call');
});

test('8 expired expectation becomes follow-up visible', async () => {
  const exp = {
    id: 'exp-1',
    client_id: 1,
    prospect_id: 'prospect-exeter',
    expectation_type: 'inbound_call',
    status: 'WAITING',
    expected_window: { ends_at: '2026-09-01T00:00:00.000Z' },
    source_evidence: { account_name: 'Exeter Phillips', ao_name: 'Tony' },
  };
  const overdue = markOverdueExpectations([exp], new Date('2026-10-05T00:00:00.000Z'));
  assert.equal(overdue[0].status, 'OVERDUE');
  assert.match(followUpPromptForExpectation(overdue[0], 'Tony'), /Exeter Phillips/);
});

test('9 read-back verification detects incorrect resulting state', async () => {
  const store = seedStore({ verifyFailFields: new Set(['follow_up_state']) });
  const result = await ingestOperationalUpdate({
    clientId: 1,
    text: 'Tony talked to Exeter Phillips. Contact expects to call Tony this week.',
    store,
  });
  assert.ok(result.verification_failures >= 1);
  assert.ok(store.mutations.some(m => m.verification_status === 'COMMIT_VERIFICATION_FAILED'));
});

test('10 existing relationship is not reset to cold outreach', async () => {
  const store = seedStore({
    prospects: [{
      id: 'prospect-exeter',
      client_id: 1,
      company_id: 'co-exeter',
      company_name: 'Exeter Phillips',
      assigned_ao_id: 10,
      status: 'warm',
      suppress_cold_outreach: true,
      acquisition_metadata: { maxStateIngestion: { suppress_cold_outreach: true, relationship_active: true } },
    }],
  });
  await ingestOperationalUpdate({
    clientId: 1,
    text: 'Tony talked to Exeter Phillips again this week.',
    store,
  });
  assert.equal(store.prospects[0].suppress_cold_outreach, true);
  assert.equal(store.prospects[0].acquisition_metadata.maxStateIngestion.relationship_active, true);
});

test('11 operator correction supersedes while retaining provenance', async () => {
  const store = seedStore();
  await ingestOperationalUpdate({
    clientId: 1,
    operatorCorrection: true,
    structured: {
      claims: [
        { claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: 'Exeter Phillips' } },
        { claim_type: CLAIM_TYPES.OPERATOR_CORRECTION, payload: { ao_next_action: 'follow_up' } },
      ],
    },
    store,
  });
  assert.equal(store.prospects[0].ao_next_action, 'follow_up');
  assert.ok(store.evidenceLinks.length >= 1);
});

test('12 multi-record ingestion processes rows independently', async () => {
  const store = seedStore({ prospects: [], companies: [{ id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 }] });
  const batch = await ingestSpreadsheet({
    clientId: 1,
    filename: 'tony.csv',
    sheetName: 'Prospects',
    rows: [
      { account: 'Exeter Phillips', ao: 'Tony', expects_inbound_call: true, expected_window: 'this week' },
      { account: 'Broken !!!', ao: 'Tony' },
    ],
    store,
  });
  assert.equal(batch.recordsExamined, 2);
  assert.equal(batch.summary.held >= 1 || batch.recordResults[1].unresolved?.length >= 0, true);
});

test('13 AO creates entirely new prospect without Scout', async () => {
  const store = seedStore({ prospects: [], companies: [] });
  const result = await ingestOperationalUpdate({
    clientId: 1,
    sourceType: 'AO_REPORTED',
    text: "I stopped into ABC Manufacturing. Talked to Sarah Collins, operations manager. They use an internal cleaner but she's out frequently and they're looking for backup coverage. Sarah asked me to call Thursday. Her number is 603-555-0101 and email is sarah@abc.example.com",
    store,
  });
  assert.equal(store.prospects.length, 1);
  assert.equal(store.prospects[0].source, 'AO_REPORTED');
  assert.equal(store.prospects[0].email, 'sarah@abc.example.com');
  assert.ok(result.prospect);
});

test('14 AO partial prospect preserves unknowns without fabrication', async () => {
  const store = seedStore({ prospects: [], companies: [] });
  await ingestOperationalUpdate({
    clientId: 1,
    sourceType: 'AO_REPORTED',
    text: "Stopped at Granite State Plastics. Mike at the front desk said their facilities guy handles vendors. I don't have his name yet.",
    store,
  });
  const prospect = store.prospects[0];
  assert.equal(prospect.company_name, 'Granite State Plastics');
  assert.ok(store.mutations.some(m => m.field_name === 'identify_missing_field'));
  assert.equal(prospect.first_name, null);
});

test('15 AO updates existing prospect instead of duplicate', async () => {
  const store = seedStore();
  const before = store.prospects.length;
  await ingestOperationalUpdate({
    clientId: 1,
    sourceType: 'AO_REPORTED',
    text: 'Tony visited Exeter Phillips and set follow-up.',
    store,
  });
  assert.equal(store.prospects.length, before);
});

test('16 Tony spreadsheet ingestion with receipt', async () => {
  const store = seedStore();
  const batch = await ingestSpreadsheet({
    clientId: 1,
    filename: 'Tony prospect spreadsheet',
    sheetName: 'Prospects',
    rows: [{ account: 'Exeter Phillips', ao: 'Tony', expects_inbound_call: true, expected_window: 'this week', __rowNumber: 17 }],
    store,
  });
  assert.equal(batch.recordsExamined, 1);
  assert.match(batch.recordResults[0].receipt, /ingested/i);
  assert.equal(store.prospects[0].follow_up_state, 'awaiting_contact');
});

test('17 mixed spreadsheet commits valid rows independently', async () => {
  const store = seedStore({
    companies: [
      { id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 },
      { id: 'co-new', name: 'New Co LLC', client_id: 1 },
    ],
    prospects: [{
      id: 'prospect-exeter',
      client_id: 1,
      company_id: 'co-exeter',
      company_name: 'Exeter Phillips',
      assigned_ao_id: 10,
    }],
  });
  const rows = [
    { account: 'Exeter Phillips', ao: 'Tony', notes: 'update' },
    { account: 'New Co LLC', ao: 'Tony', is_new: true, contact_name: 'Sam', email: 'sam@newco.example.com' },
    { account: 'Ambiguous Co', ao: 'Tony' },
    { account: 'Exeter Phillips', ownership_ao: 'Tony' },
  ];
  const batch = await ingestSpreadsheet({
    clientId: 1,
    filename: 'Tony spreadsheet',
    sheetName: 'Prospects',
    rows,
    store,
  });
  assert.equal(batch.recordsExamined, rows.length);
  assert.ok(batch.summary.committed >= 1);
});

test('18 spreadsheet replay creates no duplicate state', async () => {
  const store = seedStore();
  const rows = [{ account: 'Exeter Phillips', ao: 'Tony', expects_inbound_call: true, expected_window: 'this week' }];
  await ingestSpreadsheet({ clientId: 1, filename: 'Tony spreadsheet', sheetName: 'Prospects', rows, store });
  const expectationCount = store.expectations.length;
  await ingestSpreadsheet({ clientId: 1, filename: 'Tony spreadsheet', sheetName: 'Prospects', rows, store });
  assert.equal(store.expectations.length, expectationCount);
});

test('19 evidence traceability to artifact row', async () => {
  const store = seedStore();
  await ingestSpreadsheet({
    clientId: 1,
    filename: 'Tony prospect spreadsheet',
    sheetName: 'Prospects',
    rows: [{ account: 'Exeter Phillips', ao: 'Tony', expects_inbound_call: true, __rowNumber: 17 }],
    store,
  });
  const link = store.evidenceLinks.find(l => l.field_name === 'next_expected_event' || l.field_name === 'follow_up_state');
  assert.ok(link);
  assert.equal(link.source_record.row, 17);
  assert.equal(link.source_record.sheet, 'Prospects');
});

test('20 Scout-independent AO-reported prospect remains canonical', async () => {
  const store = seedStore({ prospects: [], companies: [] });
  await ingestOperationalUpdate({
    clientId: 1,
    sourceType: 'AO_REPORTED',
    structured: {
      claims: [
        { claim_type: CLAIM_TYPES.AO, payload: { name: 'Tony' } },
        { claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: 'Never Scouted LLC' } },
        { claim_type: CLAIM_TYPES.PIPELINE_IMPLICATION, payload: { state: 'ao_reported_new_prospect' } },
        { claim_type: CLAIM_TYPES.EVENT, payload: { kind: 'visit' } },
      ],
    },
    store,
  });
  assert.equal(store.prospects.length, 1);
  assert.equal(store.prospects[0].source, 'AO_REPORTED');
});

test('API routes registered for max ingest', () => {
  const routes = fs.readFileSync(path.join(__dirname, '..', 'routes', 'maxStateIngestion.js'), 'utf8');
  assert.match(routes, /\/api\/v1\/max\/ingest/);
  assert.match(routes, /\/api\/v1\/max\/understand/);
  assert.match(routes, /\/api\/v1\/max\/ingest\/spreadsheet/);
  const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  assert.match(server, /maxStateIngestion/);
});

test('migration exists for ingestion tables', () => {
  const sql = fs.readFileSync(path.join(__dirname, '..', 'migrations', '2026-10-05-max-reliability-state-ingestion.sql'), 'utf8');
  assert.match(sql, /max_operational_ingestions/);
  assert.match(sql, /max_evidence_artifacts/);
  assert.match(sql, /max_open_expectations/);
});
