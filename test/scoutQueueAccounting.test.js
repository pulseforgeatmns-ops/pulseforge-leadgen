'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { ELIGIBILITY_SQL, createQueueAccounting } = require('../services/scoutQueueAccounting');
const { admissionReceiptDiagnostic } = require('../services/outboundControlDiagnostics');
const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
const now = new Date('2026-10-17T12:00:00Z');
const HOUR = 3600000;
const row = (attempts, age) => ({ vertical: 'painting', enrichment_attempts: attempts,
  last_attempt_at: age == null ? null : new Date(+now - age) });
test('database evaluates exact selector boundaries without JavaScript timestamp conversion', () => {
  assert.match(ELIGIBILITY_SQL, /u.vertical = ANY\(\$5::text\[\]\)/);
  assert.match(ELIGIBILITY_SQL, /COALESCE\(u.enrichment_attempts, 0\) < 3/);
  assert.match(ELIGIBILITY_SQL, /COALESCE\(u.enrichment_attempts, 0\) < 6/);
  assert.match(ELIGIBILITY_SQL, /INTERVAL '24 hours'/);
  assert.match(ELIGIBILITY_SQL, /INTERVAL '7 days'/);
  assert.match(ELIGIBILITY_SQL, /COALESCE\(u.last_attempt_at, NOW\(\) - INTERVAL '100 years'\)/);
  assert.match(ELIGIBILITY_SQL, /INTERVAL '1 hour'/);
  assert.equal((ELIGIBILITY_SQL.match(/IS NOT TRUE/g) || []).length, 4);
});

test('UUID/multirow duplicates count distinct matched rows, not candidate hits or full stock', () => {
  const a = createQueueAccounting();
  const one = { ...row(6, null), id: 'a1b2c3d4-1111-4222-8333-123456789abc', client_id: 13, eligibility_bucket: 'attempt_limit_hold' };
  const two = { ...row(2, HOUR), id: 'b2c3d4e5-2222-4333-8444-23456789abcd', client_id: 13, eligibility_bucket: 'eligible_now' };
  const options = { tenantId: '13', verticals: ['painting'] };
  a.observe([one, two, { ...two, id: 'other-tenant', client_id: 10 }], options);
  a.observe([{ ...one, id: one.id.toUpperCase() }], options);
  a.observe([], options);
  const result = a.snapshot();
  assert.equal(result.duplicateCandidateMatches, 3);
  assert.equal(result.unobservedCandidateMatches, 1);
  assert.equal(result.distinctMatchedRows, 2);
  assert.equal(result.firstFailureCounts.attempt_limit_hold, 1);
  assert.equal(result.firstFailureCounts.eligible_now, 1);
  assert.equal(Object.values(result.firstFailureCounts).reduce((a,b) => a+b, 0), 2);
  assert.ok(!JSON.stringify(result).includes(one.id));
  a.observe([{ ...one, eligibility_bucket: 'eligible_now' }], options);
  assert.equal(a.snapshot().firstFailureCounts.unknown, 1);
});

test('real admission preserves fit and net-new while rediscovery reports duplicate eligibility', async () => {
  let inserted = false;
  const queries = [];
  const pool = { query: async (sql, params) => {
    queries.push(sql);
    if (/FROM prospects p/.test(sql)) return { rows: [] };
    if (/INSERT INTO scout_unenriched/.test(sql)) {
      assert.equal(params[7], 13);
      const count = inserted ? 0 : 1; inserted = true;
      return { rowCount: count, rows: count ? [{ id: 'a1b2c3d4-1111-4222-8333-123456789abc' }] : [] };
    }
    assert.match(sql, /WHERE u.client_id=\$1/);
    assert.ok(sql.includes(ELIGIBILITY_SQL));
    assert.ok(params[4].includes('painting'));
    assert.match(sql, /COALESCE\(u.enrichment_attempts, 0\) = 0/);
    assert.equal(params[0], 13);
    return { rows: [{ ...row(6, null), id: 'a1b2c3d4-1111-4222-8333-123456789abc', client_id: 13, eligibility_bucket: 'attempt_limit_hold' }] };
  } };
  const company = { name: 'Fixture Painting', website: 'https://fixture.example', placeTypes: ['painter'],
    description: 'Founder-owned painting contractor', location: 'Denver, Colorado' };
  const run = () => persistDiscoveredCompanies(pool,
    { tenantId: '13', clientId: 13, candidateOwnership: async () => null },
    { tenantId: '13', companies: [company, company], scoutContext: { scope: { segment: 'small_business_owners', region: 'United States' } } });
  const first = await run();
  assert.equal(first.admission.fit, 2);
  assert.equal(first.admission.admittedToEnrichment, 1);
  assert.equal(first.admission.alreadyQueued, 1);
  const repeated = await run();
  assert.equal(repeated.admission.fit, 2);
  assert.equal(repeated.admission.admittedToEnrichment, 0);
  assert.equal(repeated.admission.alreadyQueued, 2);
  assert.equal(repeated.admission.queueMatches.distinctMatchedRows, 1);
  assert.equal(repeated.admission.queueMatches.firstFailureCounts.attempt_limit_hold, 1);
  assert.ok(queries.every(sql => !/UPDATE|DELETE/.test(sql)));
  const diagnostic = admissionReceiptDiagnostic('13', null, repeated.admission, true);
  assert.equal(diagnostic.netNewAdmitted, 0);
  assert.equal(diagnostic.existingQueueCandidateMatches, 2);
  assert.ok(!JSON.stringify(diagnostic).includes('fixture.example'));
});

test('unknown and malicious queue telemetry cannot become successful inventory or leak data', () => {
  const unknown = admissionReceiptDiagnostic('13', null, {}, true).queueMatches;
  assert.equal(unknown.available, false);
  assert.equal(unknown.distinctMatchedRows, null);
  const d = admissionReceiptDiagnostic('13', null, { queueMatches: {
    scope: 'private@example.test', duplicateCandidateMatches: '2', distinctMatchedRows: -1,
    firstFailureCounts: { eligible_now: 'secret', unknown: 2, 'private@example.test': 10 },
  } }, true).queueMatches;
  assert.equal(d.distinctMatchedRows, null);
  assert.equal(d.firstFailureCounts.eligible_now, null);
  assert.equal(d.firstFailureCounts.unknown, 2);
  assert.ok(!JSON.stringify(d).includes('private'));
});

test('failed duplicate observation preserves original admission without inventing eligibility', async () => {
  const result = await persistDiscoveredCompanies({ query: async sql => {
    if (/FROM prospects p/.test(sql)) return { rows: [] };
    if (/INSERT INTO scout_unenriched/.test(sql)) return { rowCount: 0, rows: [] };
    throw new Error('read unavailable with private details');
  } }, { tenantId: '13', clientId: 13, candidateOwnership: async () => null }, {
    tenantId: '13', companies: [{ name: 'Fixture Painting', website: 'https://fixture.example',
      placeTypes: ['painter'], description: 'Painting contractor', location: 'Denver, Colorado' }],
    scoutContext: { scope: { segment: 'small_business_owners', region: 'United States' } },
  });
  assert.equal(result.admission.fit, 1);
  assert.equal(result.admission.admittedToEnrichment, 0);
  assert.equal(result.admission.alreadyQueued, 1);
  assert.equal(result.admission.queueMatches.unobservedCandidateMatches, 1);
  assert.equal(result.admission.queueMatches.firstFailureCounts.eligible_now, 0);
  assert.ok(!JSON.stringify(admissionReceiptDiagnostic('13', null, result.admission, true)).includes('private'));
});
