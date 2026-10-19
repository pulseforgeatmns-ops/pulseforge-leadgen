'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildStrGreaterManchesterPlan,
  probePlacesProviderQuery,
  diagnoseStrGreaterManchesterPlacesWorkload,
} = require('../services/scoutPlacesWorkloadDiagnostic');

describe('scoutPlacesWorkloadDiagnostic', () => {
  it('builds 36 public_business_data workloads for Greater Manchester STR', () => {
    const { plan } = buildStrGreaterManchesterPlan();
    assert.equal(plan.totals.cities, 6);
    assert.equal(plan.totals.concepts, 6);
    assert.equal(plan.totals.searches, 36);
    assert.ok(plan.workloads.every((w) => w.source === 'public_business_data'));
    assert.ok(plan.workloads.some((w) => w.concept === 'STR' && /Manchester/i.test(w.city)));
  });

  it('probePlacesProviderQuery reports website-filter drops separately from text search hits', async () => {
    const report = await probePlacesProviderQuery({
      apiKey: 'test-key',
      industry: 'STR',
      location: 'Manchester NH',
      fetchImpl: async (url) => {
        if (String(url).includes('textsearch')) {
          return {
            ok: true,
            status: 200,
            async text() {
              return JSON.stringify({
                status: 'OK',
                results: [
                  { place_id: 'p1', name: 'No Site STR' },
                  { place_id: 'p2', name: 'Has Site STR' },
                ],
              });
            },
          };
        }
        if (String(url).includes('place_id=p1')) {
          return {
            ok: true,
            status: 200,
            async text() {
              return JSON.stringify({ result: { name: 'No Site STR', place_id: 'p1' } });
            },
          };
        }
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({
              result: {
                name: 'Has Site STR',
                place_id: 'p2',
                website: 'https://example-str.com',
              },
            });
          },
        };
      },
    });

    assert.equal(report.googleStatus, 'OK');
    assert.equal(report.textSearchResultCount, 2);
    assert.equal(report.detailsFetchedCount, 2);
    assert.equal(report.droppedNoWebsiteCount, 1);
    assert.equal(report.withWebsiteCount, 1);
    assert.equal(report.queryText, 'STR Manchester NH');
  });

  it('diagnoseStrGreaterManchesterPlacesWorkload marks ok when all workloads return ZERO_RESULTS', async () => {
    const report = await diagnoseStrGreaterManchesterPlacesWorkload({
      apiKey: 'test-key',
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({ status: 'ZERO_RESULTS', results: [] });
        },
      }),
    });

    assert.equal(report.workloadsPlanned, 36);
    assert.equal(report.summary.executed, 36);
    assert.equal(report.summary.apiErrors, 0);
    assert.equal(report.summary.zeroTextSearch, 36);
    assert.equal(report.summary.finalCandidates, 0);
    assert.equal(report.ok, true);
    assert.match(report.conclusion, /genuinely empty market/i);
  });
});
