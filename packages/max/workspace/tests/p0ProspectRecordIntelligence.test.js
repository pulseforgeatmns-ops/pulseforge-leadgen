'use strict';
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { loadRecords, composeRecords, evaluate, LIMIT } = require('../ProspectRecordIntelligence');
const { classifyOperationalRead } = require('../OperationalReadIntent');
const { operatingStructured, loadOperatingEvidence, composeOperatingEvidenceAnswer } = require('../OperatingEvidenceRetrieval');
const { PresentationEngine } = require('../PresentationEngine');
const id = n => `00000000-0000-0000-0000-${String(n).padStart(12, '0')}`;
const fixture = (n, client = 21) => ({ id: id(n), client_id: client, first_name: `Synthetic ${n}`,
  last_name: 'Prospect', source: 'operator_import', vertical: 'example', icp_score: 80,
  status: n === 1 ? 'hot' : n === 2 ? 'warm' : 'cold', is_hot: n === 1,
  setter_status: 'new', service_area_match: 'Recorded area', email: `synthetic${n}@example.invalid`,
  notes: 'Recorded fit evidence; verify current need.', do_not_contact: false,
  ao_outreach_review_required: false, acquisition_metadata: {
    category: 'borrower', reviewState: n <= 6 ? 'contact_now' : n === 19 ? 'hold' : 'qualify_first',
    sourceUrls: ['https://example.invalid/record'] } });
const fixtures = () => Array.from({ length: 19 }, (_, i) => fixture(i + 1));
const records = rows => ({ status: rows.length ? 'found' : 'empty', complete: true, rows });
const bundle = rows => ({ clientId: 21, tenantId: '21', prospects: { records: records(rows) } });
const list = { wantsRecordList: true };
const rank = { wantsRanking: true, topCount: 5 };
function poolFor(rows, failPage = 0) {
  const calls = [];
  return { calls, async query(sql, values) {
    calls.push({ sql, values });
    assert.match(sql, /^SELECT/); assert.match(sql, /p.client_id = \$1/); assert.match(sql, /ORDER BY p.id ASC LIMIT \$3/);
    if (calls.length === failPage) throw new Error('synthetic failure');
    return { rows: rows.filter(r => r.client_id === values[0] && (!values[1] || r.id > values[1]))
      .sort((a,b) => a.id.localeCompare(b.id)).slice(0, values[2]).map(r => ({ ...r, record: r })) };
  } };
}
test('19 fixture records retrieved over two bounded tenant-scoped SELECT pages', async () => {
  const pool = poolFor([...fixtures(), fixture(100, 22)]);
  const r = await loadRecords(pool, 21);
  assert.equal(r.rows.length, 19); assert.equal(pool.calls.length, 2); assert.equal(r.complete, true);
  assert.equal(composeRecords({ ...bundle([]), prospects: { records: r } }, list).items.length, 19);
  assert.ok(pool.calls.every(c => c.values[0] === 21 && c.values[2] <= 10));
});
test('contact_now returns exactly six records and preserves gates', () => {
  const read = classifyOperationalRead('Which prospects are marked contact_now, and what should happen next?');
  assert.equal(read.isRead, true); assert.equal(read.reviewState, 'contact_now');
  assert.equal(composeRecords(bundle(fixtures()), read).items.length, 6);
});
test('eligibility, qualification and priority are independent; review-derived score is not value', () => {
  const rows = fixtures(); rows[0].icp_score = 99;
  rows[1].status = 'hot'; rows[1].is_hot = true; rows[0].status = 'cold'; rows[0].is_hot = false;
  const r = composeRecords(bundle(rows), rank);
  assert.equal(r.items[0].recordId, id(2)); assert.equal(r.items[0].priority.position, 1);
  assert.equal(evaluate(fixture(19)).eligible, false);
  for (const reviewState of ['reject', 'rejected', 'hold', 'qualify_first']) {
    const row = fixture(1); row.acquisition_metadata.reviewState = reviewState;
    assert.equal(evaluate(row).eligible, false);
  }
  for (const patch of [{ do_not_contact: true }, { ao_outreach_review_required: true },
    { icp_score: 69 }, { service_area_match: null }, { status: 'disqualified' },
    { status: 'rejected' }, { setter_status: 'hold' }, { setter_status: 'closed' }]) {
    assert.equal(evaluate({ ...fixture(1), ...patch }).eligible, false);
  }
});
test('complete evidence ranks eligible records, includes all boundary ties and traceable calculations', () => {
  const r = composeRecords(bundle(fixtures()), rank);
  assert.equal(r.items.length, 6); assert.ok(r.items.every(i => i.recordRef && i.priority?.epistemic === 'calculated'));
  assert.ok(r.items.every(i => ![id(19), id(7)].includes(i.recordId)));
});
test('incomplete evidence returns unranked candidates with specific missing fields', () => {
  const rows = fixtures(); rows[0].source = null; rows[0].first_name = null; rows[0].last_name = null;
  rows[0].acquisition_metadata.category = null; rows[0].notes = null;
  const r = composeRecords(bundle(rows), rank);
  assert.ok(r.items.every(i => i.priority === null)); assert.match(r.prose, /Unranked candidates/);
  assert.match(r.prose, /source attribution/); assert.match(r.prose, /source: not recorded/);
});
test('duplicate IDs suppressed; distinct same-name records retained; ordering deterministic', () => {
  const rows = fixtures(); rows[1].first_name = rows[0].first_name;
  const a = composeRecords(bundle([...rows, rows[0]]), list);
  const b = composeRecords(bundle([...rows].reverse()), list);
  assert.equal(a.items.length, 19); assert.deepEqual(a.items, b.items);
});
test('empty, failed, unavailable and second-page failures never claim verified records', async () => {
  for (const status of ['empty', 'failed', 'unavailable']) {
    const b = bundle([]); b.prospects.records.status = status;
    const r = composeRecords(b, list); assert.equal(r.items.length, 0); assert.equal(r.sourceStatuses.prospects, status);
  }
  const r = await loadRecords(poolFor(fixtures(), 2), 21);
  assert.equal(r.status, 'failed'); assert.deepEqual(r.rows, []);
  assert.equal((await loadRecords(null, 21)).status, 'unavailable');
});
test('query bound is explicit and incomplete retrieval cannot produce full-set ranking', async () => {
  const r = await loadRecords(poolFor(Array.from({length: LIMIT + 1}, (_,i) => fixture(i+1))), 21);
  assert.equal(r.rows.length, LIMIT); assert.equal(r.complete, false); assert.ok(r.nextCursor);
  const b = bundle([]); b.prospects.records = r;
  const c = composeRecords(b, rank); assert.match(c.prose, /Additional records may exist/);
  assert.ok(c.items.every(i => i.priority === null));
});
test('citation detail, stable reference, limitations and calculation survive response presenter without model execution', async () => {
  const composed = composeRecords(bundle(fixtures()), rank);
  const s = operatingStructured(composed.prose, { items: composed.items, recordIntelligence: true });
  const presenter = new PresentationEngine({ anthropic: { messages: { create: () => { throw new Error('must not call model'); } } } });
  const p = await presenter.present(s);
  assert.equal(p.prose, composed.prose);
  for (const ev of p.structured.supportingEvidence) {
    assert.ok(ev.detail.length > 20); assert.ok(ev.recordRef); assert.equal(ev.priority.epistemic, 'calculated');
  }
});
test('default operating loader reuses pool and leaves aggregate-count requests unchanged', async () => {
  const fake = poolFor(fixtures());
  const nr = async () => ({ available: false });
  const pool = { async query(sql, values) {
    if (/to_jsonb\(p\) -/.test(sql)) return fake.query(sql, values);
    return { rows: /COUNT\(\*\)::int AS total/.test(sql) ? [{ total: 19 }] : [] };
  } };
  const input = { tenantId: '21', context: { tenantId: '21', clientId: 21 }, pool,
    operatingEvidenceOpts: { loadCampaignAo: nr, loadScout: nr, loadMissions: nr, loadObjectives: nr,
      loadActivity: nr, loadOutcomes: nr, loadOperatorAttested: nr, loadCapability: nr } };
  const count = await loadOperatingEvidence(input); assert.equal(fake.calls.length, 0);
  assert.equal(count.prospects.counts.total, 19);
  const question = 'List my actual prospect records with names and record IDs.';
  const read = classifyOperationalRead(question);
  const loaded = await loadOperatingEvidence({ ...input, operationalRead: read });
  assert.equal(loaded.prospects.records.rows.length, 19);
  const answer = composeOperatingEvidenceAnswer(question, loaded, { operationalRead: read });
  assert.equal(answer.items.length, 19);
});

test('continuation reads are tenant-scoped subsets and cannot claim a full-set ranking', async () => {
  const read = classifyOperationalRead(`List my prospects after record ID ${id(10)}`);
  assert.equal(read.cursor, id(10));
  const result = await loadRecords(poolFor(fixtures()), 21, { after: read.cursor });
  assert.equal(result.rows.length, 9);
  const b = bundle([]); b.prospects.records = result;
  const answer = composeRecords(b, rank);
  assert.match(answer.prose, /subset/); assert.ok(answer.items.every(i => i.priority === null));
});
test('query boundary rejects foreign results and invalid tenant; conflicting authorization fails closed', async () => {
  const pool = { query: async () => ({ rows: [{ ...fixture(1, 22), record: fixture(1, 22) }] }) };
  assert.equal((await loadRecords(pool, 21)).status, 'failed');
  assert.equal((await loadRecords(pool, null)).status, 'unavailable');
  const result = await loadOperatingEvidence({ tenantId: '21', clientId: 22,
    session: { context: { tenantId: '21', clientId: 21 } }, operationalRead: list,
    loadProspects: () => { throw new Error('must not be reached'); } });
  assert.equal(result.failClosed, true); assert.deepEqual(result.items, []);
});
test('same-ID conflicts never establish a priority', () => {
  const rows = fixtures();
  const result = composeRecords(bundle([...rows, { ...rows[0], status: 'cold' }]), rank);
  assert.ok(result.items.every(i => i.priority === null));
});
test('record citation renders supporting detail and escapes record content in the actual evidence UI function', () => {
  const fs = require('node:fs');
  const vm = require('node:vm');
  const source = fs.readFileSync(require('node:path').join(__dirname, '../../../../public/command-deck/command-deck.js'), 'utf8');
  const fn = source.slice(source.indexOf('  function evidenceGroup('), source.indexOf('  function renderMetadataStrip('));
  const context = { investigation: null, escapeHtml: value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') };
  vm.createContext(context); vm.runInContext(fn, context);
  const output = context.evidenceGroup('Supporting evidence', [{ id: 'prospect:21:1',
    recordRef: 'prospect:21:1', summary: 'prospects table', detail: 'Example <script> [prospect:21:1]; source: not recorded' }]);
  assert.match(output, /Example &lt;script&gt;/); assert.match(output, /source: not recorded/);
  assert.doesNotMatch(output, /<script>/);
});

test('requested rank counts remain explicit rather than silently capped at 100', () => {
  assert.equal(classifyOperationalRead('Identify my top 500 prospects.').topCount, 500);
  assert.equal(classifyOperationalRead('Identify my top 0 prospects.').topCount, 1);
  assert.equal(classifyOperationalRead('Identify my five highest-priority opportunities.').isRead, true);
});
