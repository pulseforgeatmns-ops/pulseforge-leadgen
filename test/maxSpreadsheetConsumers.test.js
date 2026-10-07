'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { activityReadModel, sourceActivityDate, fetchSpreadsheetCrmEvidence } = require('../utils/spreadsheetCrmEvidence');
const { formatProspectBrief } = require('../utils/aoProspectBrief');

test('read model separates date-only historical event from recording timestamp without rewriting source notes', () => {
  const notes = '9/23:  Called\n  Left message.  ';
  const row = { notes, created_at: '2026-10-07T15:01:02Z', metadata: { occurredOn: '2026-09-23' } };
  const value = activityReadModel(row);
  assert.equal(value.occurredOn, '2026-09-23');
  assert.equal(value.occurredAt, '2026-09-23T00:00:00.000Z');
  assert.equal(value.recordedAt, '2026-10-07T15:01:02.000Z');
  assert.equal(value.notes, notes);
  assert.equal(value.created_at, row.created_at);
  assert.equal(sourceActivityDate({ metadata: { occurredOn: '2026-02-30' } }), null);
  assert.equal(sourceActivityDate({ metadata: { occurredOn: '10/2' } }), null);
});

test('actual brief text uses source date with separate recorded-at audit', () => {
  const result = formatProspectBrief({ prospect: { first_name: 'Lori' }, company: { name: 'Example' }, touchpoints: [], activity: [{ activity_type: 'call', notes: 'Left a message.', metadata: { occurredOn: '2026-09-23' }, created_at: '2026-10-07T15:01:02Z' }], task: null, aoName: 'AO' });
  assert.match(result, /2026-09-23 \(call\) \[recorded 2026-10-07T15:01:02.000Z\]: Left a message\./);
  assert.doesNotMatch(result, /2026-10-07 \(call\)/);
});

test('AO CRM actual detail renderer shows source dates, existing and approved contacts, relationships and escaped raw notes', async () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/ao-crm.html'), 'utf8');
  const helpers = html.slice(html.indexOf('function fmtDate('), html.indexOf('function renderTabs('));
  const detailFunction = html.slice(html.indexOf('async function showDetail('), html.indexOf('async function loadDashboard('));
  const modal = { innerHTML: '' }, button = {};
  const context = vm.createContext({ document: { getElementById: id => id === 'modal' ? modal : button }, openModalBackdrop() {}, closeModal() {}, api: async () => ({ summary: { company_name: 'Account' }, sales_state: {}, contact: { name: 'Existing Contact' }, contacts: [{ name: 'Approved <Contact>', source: 'approved_spreadsheet' }], providerRelationships: [{ providerName: 'Known Provider', verified: true }], history: [{ activity_type: 'call', occurredOn: '2026-09-23', recordedAt: '2026-10-07T15:01:02Z', notes: 'First line\n<img src=x onerror=alert(1)>', created_at: '2026-10-07T15:01:02Z' }] }) });
  vm.runInContext(helpers + detailFunction, context);
  await context.showDetail('00000000-0000-4000-8000-000000000001');
  assert.match(modal.innerHTML, /2026-09-23/);
  assert.match(modal.innerHTML, /Source date; recorded/);
  assert.match(modal.innerHTML, /Existing Contact/);
  assert.match(modal.innerHTML, /Approved &lt;Contact&gt;/);
  assert.match(modal.innerHTML, /Known Provider/);
  assert.match(modal.innerHTML, /First line\n&lt;img src=x onerror=alert\(1\)&gt;/);
  assert.doesNotMatch(modal.innerHTML, /<img/);
});

test('optional evidence tables may be absent, but read errors do not silently become empty CRM evidence', async () => {
  const empty = await fetchSpreadsheetCrmEvidence({ db: { query: async () => ({ rows: [{ contacts: null, relationships: null, activities: null }] }) }, clientId: 1, prospectId: 'p1', includeActivities: true });
  assert.deepEqual(empty, { contacts: [], relationships: [], activities: [] });
  await assert.rejects(fetchSpreadsheetCrmEvidence({ db: { query: async () => { throw new Error('read unavailable'); } }, clientId: 1, prospectId: 'p1' }), /read unavailable/);
});
