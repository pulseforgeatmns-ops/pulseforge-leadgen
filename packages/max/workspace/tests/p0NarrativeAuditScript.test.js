'use strict';
// Tests for scripts/investigationNarrativeAudit.js: tenant scoping, read-only
// behaviour and output redaction. Hermetic: fake connections only.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const audit = require('../../../../scripts/investigationNarrativeAudit');
const { narrativeFixture } = require('./fixtures/narrativeInvestigationProspects');

const ids = n => `${String(n).repeat(8).slice(0, 8)}-aaaa-4aaa-8aaa-aaaaaaaaaaaa`;
const withIds = rows => rows.map((r, i) => ({ ...r, id: ids(i + 1) }));
const CLEAN_INPUTS = [
  { name: 'Zane Example', fit: 'Active flipper requesting $35k for rehab on a purchased SFR. Proposed first lien on the subject property. Exit via DSCR refinance after rehab. Targeting close in ~3 weeks.', risks: 'Request exceeds the $5k–$20k working range.' },
  { name: 'Brook Example', fit: 'Under contract on a duplex; needs $15,000 equity gap. Offers first-position lien. Exit: sale after 4-month rehab.', risks: 'Contract not verified.' },
  { name: 'Cam Example', fit: 'Requesting $12k; proposed first lien; exit via refinance.', risks: 'Title unverified.' },
  { name: 'Dale Example', fit: 'Requesting $20k. Exit via sale.', risks: 'Unknown timeline.' },
  { name: 'Eli Example', fit: 'Under contract on a townhouse.', risks: 'Amount unknown.' },
  { name: 'Fay Example', fit: 'Public investor profile.', risks: 'No active deal recorded.' },
];
const cleanRows = () => withIds(narrativeFixture(21, CLEAN_INPUTS)).map((r, i) => ({
  ...r, email: `person${i}@private.example`, phone: '(603) 555-01' + String(10 + i) }));

// Fake keyset-paginated prospects table + statement log.
function fakeDb(rows, { log = [] } = {}) {
  const sorted = [...rows].sort((a, b) => String(a.id).localeCompare(String(b.id)));
  return { log, async query(sql, params = []) {
    const text = typeof sql === 'string' ? sql : sql.text;
    log.push({ text, params });
    if (/FROM prospects p/.test(text)) {
      const [clientId, cursor, limit] = params;
      const page = sorted.filter(r => (cursor == null || r.id > cursor)).slice(0, limit)
        .map(r => ({ id: r.id, client_id: r.client_id, record: { ...r, acquisition_metadata: undefined },
          acquisition_metadata: r.acquisition_metadata, ao_outreach_review_required: true }));
      // Model the SQL tenant filter honestly unless the test injects a leak.
      return { rows: rows.leak ? page : page.filter(r => r.client_id === clientId) };
    }
    return { rows: [] };
  } };
}
const blueprintDeps = (services = 'Bridge loans. Financing range: $5,000–$20,000.', calls = []) => ({
  clientIntelligence: { async loadApprovedClientIntelligence(input) {
    calls.push(input);
    return { summary: { clientId: 21, approved: true, semanticAuthority: 'CANONICAL', canonicalSnapshotId: 'snap-1',
      services, idealCustomers: 'UNRELATED_BLUEPRINT_SECRET operators', businessName: 'UNRELATED_NAME' } };
  } },
});

test('tenant scoping: every prospect read is bound to tenant 21 and only the review-only contact_now cohort is audited', async () => {
  const rows = cleanRows();
  const extra = [
    { ...rows[0], id: ids(7), acquisition_metadata: { ...rows[0].acquisition_metadata, reviewState: 'hold' } },
    { ...rows[0], id: ids(8), source: 'scout' },
  ];
  const db = fakeDb([...rows, ...extra, { ...rows[0], id: ids(9), client_id: 22, notes: 'FOREIGN_SECRET requesting $99k' }]);
  const calls = [];
  const report = await audit.runAudit(db, blueprintDeps(undefined, calls));
  const prospectReads = db.log.filter(l => /FROM prospects p/.test(l.text));
  assert.ok(prospectReads.length > 0);
  assert.ok(prospectReads.every(l => l.params[0] === 21 && /p\.client_id = \$1/.test(l.text)));
  assert.equal(report.cohort.found, 6);
  assert.deepEqual(report.records.map(r => r.id).sort(), rows.map(r => r.id.slice(0, 8)).sort());
  assert.equal(calls.length, 1); assert.equal(calls[0].clientId, 21); assert.equal(calls[0].tenantId, '21');
  assert.equal(calls[0].cieOpts.pool, db);
  assert.deepEqual(Object.keys(calls[0].cieService), ['getApprovedClientBlueprint']);
  assert.doesNotMatch(JSON.stringify(report) + audit.formatReport(report), /FOREIGN_SECRET|\$99k|99999999/);
  assert.equal(audit.TENANT_ID, 21);
});

test('a foreign-tenant row leaking from the database fails closed without reporting any record', async () => {
  const rows = [...cleanRows(), { ...cleanRows()[0], id: ids(9), client_id: 22, notes: 'FOREIGN_SECRET' }];
  rows.leak = true;
  const report = await audit.runAudit(fakeDb(rows), blueprintDeps());
  assert.equal(report.conclusion, 'FAIL'); assert.equal(report.records.length, 0);
  assert.match(report.reasons.join(' '), /prospect retrieval failed/);
  assert.doesNotMatch(audit.formatReport(report), /FOREIGN_SECRET/);
});

test('the Blueprint read is bound to the guarded connection and exposes only the approved-Blueprint call', async () => {
  const seen = [];
  const cie = { getApprovedClientBlueprint: async (id, opts) => { seen.push({ id, pool: opts.pool }); throw Object.assign(new Error('No approved blueprint for client'), { code: 'not_found' }); } };
  const db = fakeDb(cleanRows());
  const report = await audit.runAudit(db, { cie });
  assert.deepEqual(seen, [{ id: 21, pool: db }]);
  assert.equal(report.blueprint.approvedBlueprint, false); assert.equal(report.blueprint.loadError, 'no_approved_blueprint');
  assert.equal(report.blueprint.parserRange, null);
});

test('read-only: READ ONLY transaction verified, guarded statements, always rolled back and released', async () => {
  const statements = []; let released = 0;
  const client = { async query(sql, params) {
    statements.push(sql);
    if (sql === 'SHOW transaction_read_only') return { rows: [{ transaction_read_only: 'on' }] };
    return fakeDb(cleanRows()).query(sql, params);
  }, release() { released++; } };
  const out = []; const code = await audit.main({ pool: { connect: async () => client }, out: s => out.push(s), deps: blueprintDeps(), argv: [] });
  assert.equal(code, 0, out.join('\n'));
  assert.equal(statements[0], 'BEGIN TRANSACTION READ ONLY'); assert.equal(statements[1], 'SHOW transaction_read_only');
  assert.equal(statements.at(-1), 'ROLLBACK'); assert.equal(released, 1);
  assert.ok(!statements.some(s => /^\s*(insert|update|delete|create|alter|drop|truncate)\b/i.test(s)));
  assert.match(out[0], /READ ONLY transaction, rolled back/);
});

test('read-only: the guarded connection rejects writes, including through connect()', async () => {
  const boundary = { reads: 0, writesBlocked: 0 }; const executed = [];
  const conn = audit.guardedConnection({ query: async sql => { executed.push(sql); return { rows: [] }; } }, boundary);
  for (const sql of ['UPDATE prospects SET notes = $1', 'INSERT INTO touchpoints VALUES (1)', 'DELETE FROM prospects', 'SELECT 1 FOR UPDATE']) {
    await assert.rejects(conn.query(sql), /read_only_violation/);
  }
  const inner = await conn.connect();
  await assert.rejects(inner.query('UPDATE prospects SET status = $1'), /read_only_violation/);
  await inner.query('SELECT 1'); inner.release();
  assert.deepEqual(executed, ['SELECT 1']); assert.equal(boundary.writesBlocked, 5);
});

test('read-only: refuses to read when the transaction is not read-only, and reports denied access without secrets', async () => {
  const statements = [];
  const notReadOnly = { async query(sql) { statements.push(sql); return sql === 'SHOW transaction_read_only' ? { rows: [{ transaction_read_only: 'off' }] } : { rows: [] }; }, release() {} };
  const out = [];
  assert.equal(await audit.main({ pool: { connect: async () => notReadOnly }, out: s => out.push(s), argv: [] }), 2);
  assert.ok(!statements.some(s => /prospects/.test(s))); assert.equal(statements.at(-1), 'ROLLBACK');
  assert.match(out[0], /ACCESS UNAVAILABLE: READ_ONLY_REQUIRED/);

  const denied = Object.assign(new Error('password authentication failed: postgres://user:hunter2@db.internal:5432/app'), { code: '28P01' });
  const out2 = [];
  assert.equal(await audit.main({ pool: { connect: async () => { throw denied; } }, out: s => out2.push(s), argv: [] }), 2);
  assert.match(out2[0], /ACCESS UNAVAILABLE: database connection refused \(28P01\)/);
  assert.doesNotMatch(out2.join(' '), /hunter2|postgres:\/\/|db\.internal/);

  const old = process.env.DATABASE_URL; delete process.env.DATABASE_URL;
  try { const out3 = []; assert.equal(await audit.main({ out: s => out3.push(s), argv: [] }), 2); assert.match(out3[0], /DATABASE_URL is not set/); }
  finally { if (old !== undefined) process.env.DATABASE_URL = old; }
});

test('redaction: output carries short IDs, states and minimal phrases only — no names, contacts, URLs, notes or unrelated Blueprint content', async () => {
  const rows = cleanRows();
  rows[4].notes = rows[4].notes.replace('Under contract on a townhouse.', 'Deal stage: Eli Example townhouse under contract, call (603) 555-0199 or eli@private.example');
  const report = await audit.runAudit(fakeDb(rows), blueprintDeps());
  for (const output of [audit.formatReport(report), JSON.stringify(report)]) {
    assert.doesNotMatch(output, /Zane|Brook|Eli\b|Example|private\.example|555-01|example\.invalid|https?:/);
    assert.doesNotMatch(output, /Active flipper|Public investor profile|Request exceeds|Contract not verified/);
    assert.doesNotMatch(output, /UNRELATED_BLUEPRINT_SECRET|UNRELATED_NAME/);
    assert.doesNotMatch(output, /aaaa-4aaa/); // full UUIDs never printed
  }
  const eli = report.records.find(r => r.id === rows[4].id.slice(0, 8));
  assert.equal(eli.categories.stage.state, 'recorded_unverified');
  assert.match(eli.categories.stage.phrase, /\[name\] \[name\] townhouse under contract, call \[phone\] or \[email\]/);
  assert.equal(audit.redact('x'.repeat(200)).length, 80);
});

test('clean cohort: six-record matrix, recognized approved range, ranked, PASS', async () => {
  const report = await audit.runAudit(fakeDb(cleanRows()), blueprintDeps());
  assert.equal(report.conclusion, 'PASS', report.reasons.join('; '));
  assert.deepEqual(report.records.map(r => r.coverage), [5, 4, 3, 2, 1, 0]);
  assert.equal(report.records[0].categories.amount.phrase, 'requesting $35k');
  assert.equal(report.records[0].categories.amount.state, 'recorded_unverified');
  assert.deepEqual(report.blueprint.parserRange, { min: 5000, max: 20000 });
  assert.deepEqual(report.blueprint.financingRangeCandidates.map(c => c.text), ['Bridge loans. Financing range: $5,000–$20,000.']);
  assert.equal(report.ranking.ranked, true);
  const text = audit.formatReport(report);
  assert.match(text, /Coverage matrix/); assert.match(text, /Financing range recognized by current parser: \$5,000–\$20,000/);
  assert.match(text, /Conclusion: PASS/);
});

test('review findings: possible missed claims, non-comparable details and wrong cohort size produce FAIL with reasons', async () => {
  const rows = withIds(narrativeFixture());
  const report = await audit.runAudit(fakeDb(rows), blueprintDeps('Bridge loans for rehab, typically $5k–$20k.'));
  assert.equal(report.conclusion, 'FAIL');
  const dale = report.records.find(r => r.id === rows[3].id.slice(0, 8));
  assert.ok(dale.findings.some(f => f.type === 'possible_missed_claim' && f.field === 'amount')); // "$60k" budget, intentionally not a request
  const eli = report.records.find(r => r.id === rows[4].id.slice(0, 8));
  assert.equal(eli.categories.amount.state, 'non_comparable'); assert.equal(eli.categories.amount.reason, 'multiple_values');
  assert.equal(report.blueprint.parserRange, null);
  assert.equal(report.blueprint.financingRangeCandidates[0].text, 'Bridge loans for rehab, typically $5k–$20k.');
  assert.match(audit.formatReport(report), /Financing range recognized by current parser: none/);

  const five = await audit.runAudit(fakeDb(cleanRows().slice(0, 5)), blueprintDeps());
  assert.match(five.reasons.join(' '), /cohort size 5, expected 6/); assert.equal(five.conclusion, 'FAIL');
});

test('real Blueprint normalization path: an approved Blueprint range is recognized and only financing facts are reported', async () => {
  const realCie = require('../../../../services/clientIntelligenceInterview');
  const store = realCie.createMemoryStore();
  let interview = await realCie.startClientInterview({ clientId: 21, forceNew: true }, { store });
  for (const answer of ['Lending to investors.', 'Bridge loans. Financing range: $5,000–$20,000.', 'Real-estate operators.',
    'Unsupported borrowers.', 'Example County.', 'Clear terms.', 'Professional.', 'Qualified pipeline.', 'Funded loans.']) {
    interview = await realCie.postInterviewMessage(interview.interviewId, answer, { store });
  }
  await realCie.approveBlueprint(interview.blueprint.id, { store });
  const pools = [];
  const db = fakeDb(cleanRows());
  const cie = { getApprovedClientBlueprint: (id, opts) => { pools.push(opts.pool); return realCie.getApprovedClientBlueprint(id, { ...opts, store }); } };
  const report = await audit.runAudit(db, { cie });
  assert.deepEqual(pools, [db]);
  assert.equal(report.blueprint.approvedBlueprint, true);
  assert.deepEqual(report.blueprint.parserRange, { min: 5000, max: 20000 });
  assert.ok(report.blueprint.financingRangeCandidates.every(c => /financ|loan|lend|\$/i.test(c.text)));
  assert.doesNotMatch(JSON.stringify(report.blueprint), /Real-estate operators|Example County|Professional/);
});
