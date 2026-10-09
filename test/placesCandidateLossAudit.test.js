'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  buildClient10PlacesWorkloads,
  auditPlacesWorkload,
  runPlacesCandidateLossAudit,
  classifyCandidate,
  REJECTION_BUCKETS,
} = require('../packages/scout/audit/placesCandidateLossAudit');

function mockFetch(responses) {
  const queue = [...responses];
  return async () => {
    const next = queue.shift();
    if (!next) throw new Error('unexpected_fetch');
    return {
      ok: next.status >= 200 && next.status < 300,
      status: next.status,
      json: async () => next.body,
    };
  };
}

describe('AUDIT-057 placesCandidateLossAudit', () => {
  it('builds 36 client_id=10 workloads (6 cities × 6 verticals)', () => {
    const workloads = buildClient10PlacesWorkloads();
    assert.equal(workloads.length, 36);
    assert.ok(workloads.every((row) => / NH$/.test(row.city)));
    assert.ok(workloads.some((row) => /property management company Bedford NH/.test(row.query)));
  });

  it('classifies missing website as the primary Scout loss bucket', () => {
    const seen = new Set();
    const verdict = classifyCandidate(
      { place_id: 'p1', name: 'Acme Law', types: ['lawyer'] },
      { place_id: 'p1', name: 'Acme Law', types: ['lawyer'], formatted_phone_number: '(603) 555-0100' },
      seen,
      { enforceServiceArea: false, enforceB2B: false }
    );
    assert.equal(verdict.accepted, false);
    assert.equal(verdict.bucket, REJECTION_BUCKETS.MISSING_WEBSITE);
  });

  it('tracks detail fetch, website, and duplicate loss per workload', async () => {
    const fetchImpl = mockFetch([
      {
        status: 200,
        body: {
          status: 'OK',
          results: [
            { place_id: 'a', name: 'Alpha PM' },
            { place_id: 'b', name: 'Beta PM' },
          ],
        },
      },
      {
        status: 200,
        body: {
          status: 'OK',
          result: {
            place_id: 'a',
            name: 'Alpha PM',
            website: 'https://alpha-pm.com',
            formatted_phone_number: '(603) 555-0101',
            types: ['real_estate_agency'],
            address_components: [
              { long_name: 'Bedford', types: ['locality'] },
              { short_name: 'NH', types: ['administrative_area_level_1'] },
            ],
          },
        },
      },
      { status: 200, body: { status: 'OK', result: null } },
    ]);

    const row = await auditPlacesWorkload(
      { query: 'property management company Bedford NH' },
      {
        apiKey: 'test-key',
        fetchImpl,
        detailDelayMs: 0,
        enforceB2B: false,
        enforceServiceArea: false,
      }
    );

    assert.equal(row.textSearchHits, 2);
    assert.equal(row.detailFetchSuccess, 1);
    assert.equal(row.hasWebsite, 1);
    assert.equal(row.accepted, 1);
    assert.equal(row.rejected, 1);
    assert.match(row.rejectionReasonSummary, /detail_fetch_failed/);
  });

  it('aggregates funnel totals across workloads', async () => {
    const fetchImpl = mockFetch([
      {
        status: 200,
        body: { status: 'OK', results: [{ place_id: 'x', name: 'Solo PM' }] },
      },
      {
        status: 200,
        body: {
          status: 'OK',
          result: {
            place_id: 'x',
            name: 'Solo PM',
            website: 'https://solo-pm.com',
            types: ['point_of_interest'],
            address_components: [{ long_name: 'Hooksett', types: ['locality'] }],
          },
        },
      },
      {
        status: 200,
        body: { status: 'ZERO_RESULTS', results: [] },
      },
    ]);

    const report = await runPlacesCandidateLossAudit(
      [
        { query: 'property management company Hooksett NH' },
        { query: 'commercial office Auburn NH' },
      ],
      {
        apiKey: 'test-key',
        fetchImpl,
        detailDelayMs: 0,
        enforceB2B: false,
        enforceServiceArea: false,
      }
    );

    assert.equal(report.totals.textSearchHits, 1);
    assert.equal(report.summary.returned, 1);
    assert.equal(report.summary.accepted, 1);
    assert.equal(report.summary.dropped, 0);
  });
});
