'use strict';
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const { providerReceiptDiagnostic } = require('../services/outboundControlDiagnostics');
const empty = () => ({ providerId: 'google_places', status: 'empty', execution: { executed: false, errors: [], queries: [] } });
const successful = () => ({ providerId: 'google_places', status: 'completed', execution: { executed: true,
  totals: { queries: 1, results: 9 }, queries: [{ googleStatus: 'OK', httpStatus: 200 }], errors: [] } });
function receipt(extra = [], errors = [], status = 'completed') {
  return providerReceiptDiagnostic('10', 'moc_10_1234567890abcdef', { kind: 'investigated', result: {
    status, errors, payload: { providerReceipts: [successful(), ...Array.from({ length: 80 }, empty), ...extra] },
  } }, { researchOnly: true });
}

test('late unavailable adapters survive the detail sample without an unexplained blocker', () => {
  const r = receipt([{ providerId: 'website', status: 'failed', errors: [{ code: 'adapter_unavailable' }] }]);
  assert.equal(r.providers.length, 20); assert.equal(r.reportsTruncated, true);
  assert.equal(r.discovery.providerIssuesComplete, true);
  assert.equal(r.discovery.blocker, 'none');
  assert.equal(r.discovery.nonBlockingProviderIssue, 'provider_unavailable');
  assert.equal(r.providerIssueSummary.reportsTotal, 82);
  assert.equal(r.providerIssueSummary.reportsScanned, 82);
  assert.ok(r.providerIssueSummary.issues.some(x => x.provider === 'website' && x.code === 'adapter_unavailable'));
});

for (const [code, blocker] of [['REQUEST_DENIED', 'provider_denied'], ['OVER_QUERY_LIMIT', 'provider_quota'],
  ['provider_error', 'provider_failed'], ['private@example.test?token=secret', 'provider_failed']]) {
  test(`a late ${blocker} cannot be concealed by optional-adapter errors`, () => {
    const r = receipt([{ providerId: 'website', status: 'failed', errors: [{ code: 'adapter_unavailable' }] },
      { providerId: 'google_places', status: 'failed', errors: [{ code, message: 'private exception' }] }]);
    assert.equal(r.discovery.blocker, blocker);
    assert.equal(r.discovery.nonBlockingProviderIssue, null);
    assert.equal(r.discovery.providerIssuesComplete, true);
    assert.ok(!JSON.stringify(r).includes('private'));
  });
}

test('late query status, failed reports with no cause, and errors beyond legacy error limits stay visible', () => {
  assert.equal(receipt([{ ...empty(), execution: { queries: [{ googleStatus: 'REQUEST_DENIED' }] } }]).discovery.blocker, 'provider_denied');
  assert.equal(receipt([{ providerId: 'website', status: 'failed' }]).discovery.blocker, 'provider_failed');
  const errors = [...Array(130).fill({ code: 'adapter_unavailable' }), { code: 'provider_error' }];
  assert.equal(receipt([], errors).discovery.blocker, 'provider_failed');
  assert.equal(receipt([{ providerId: 'website', status: 'failed', errors }]).discovery.blocker, 'provider_failed');
});

test('a bounded partial pass remains a coverage limit after observing all optional-adapter errors', () => {
  const r = receipt([{ providerId: 'website', status: 'failed', errors: [{ code: 'adapter_unavailable' }] }],
    [{ code: 'insufficient_coverage' }], 'partial');
  assert.equal(r.discovery.providerIssuesComplete, true);
  assert.equal(r.discovery.blocker, 'insufficient_coverage');
  assert.equal(r.discovery.nonBlockingProviderIssue, null);
});

test('oversized and malformed receipts explicitly refuse diagnostic completeness', () => {
  const oversized = receipt(Array.from({ length: 1100 }, empty));
  assert.equal(oversized.discovery.providerIssuesComplete, false);
  assert.equal(oversized.discovery.blocker, 'diagnostic_incomplete');
  assert.ok(oversized.providerIssueSummary.omittedReports > 0);
  assert.ok(oversized.providerIssueSummary.reportsScanned <= 1000);
  const malformed = receipt([null]);
  assert.equal(malformed.discovery.providerIssuesComplete, false);
  assert.notEqual(malformed.discovery.blocker, 'none');
  assert.equal(receipt([{ ...empty(), execution: 'malformed' }]).discovery.providerIssuesComplete, false);
  const tooManyErrors = receipt([], Array(11000).fill({ code: 'adapter_unavailable' }));
  assert.equal(tooManyErrors.discovery.providerIssuesComplete, false);
  assert.notEqual(tooManyErrors.discovery.blocker, 'none');
});

test('query and grouped issue bounds stay explicit without leaking arbitrary fields', () => {
  const rows = [];
  for (const providerId of ['google_places', 'website', 'news', 'linkedin', 'facebook'])
    for (const code of ['provider_error', 'provider_failed', 'provider_unavailable', 'adapter_unavailable',
      'REQUEST_DENIED', 'OVER_QUERY_LIMIT', 'INVALID_REQUEST', 'UNKNOWN_ERROR', 'UNKNOWN_STATUS',
      'RESOURCE_EXHAUSTED', 'insufficient_coverage', 'invalid_target', 'enrichment_unavailable', 'discovery_provider_failed'])
      rows.push({ providerId, status: 'failed', errors: [{ code }] });
  const r = receipt(rows);
  assert.equal(r.providerIssueSummary.issues.length, 64);
  assert.equal(r.providerIssueSummary.issuesTruncated, true);
  assert.equal(r.discovery.providerIssuesComplete, false);
  assert.equal(r.discovery.blocker, 'provider_denied', 'known failures remain visible even when grouped detail is bounded');
  const queries = Array(11000).fill({ googleStatus: 'OK', httpStatus: 200 });
  const q = receipt([{ ...empty(), execution: { queries } }]);
  assert.equal(q.discovery.blocker, 'diagnostic_incomplete');
  assert.equal(q.discovery.providerIssuesComplete, false);
});

test('summaries sanitize unknown identities, codes and causes and leave the source untouched', () => {
  const secret = 'private@example.test https://private.test/?token=secret';
  const row = { providerId: secret, status: 'failed', errors: [{ code: secret, googleStatus: secret, causeCode: secret, message: secret }] };
  const before = structuredClone(row), r = receipt([row]);
  assert.deepEqual(row, before);
  assert.equal(r.providerIssueSummary.unknownErrorCount, 1);
  assert.equal(r.discovery.blocker, 'provider_failed');
  assert.ok(!JSON.stringify(r).includes('private'));
  const ordinary = providerReceiptDiagnostic('13', 'moc_13_1234567890abcdef', { status: 'completed',
    payload: { providerReceipts: [successful(), ...Array.from({ length: 80 }, empty), row] } });
  assert.equal(ordinary.providerIssueSummary, undefined, 'ordinary diagnostics retain their existing projection contract');
});
