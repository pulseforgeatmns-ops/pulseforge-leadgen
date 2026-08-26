'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const {
  PLACES_FEATURES,
  PLACES_ENDPOINTS,
  buildPlacesCostReport,
  formatPlacesCostReportMarkdown,
  ensurePlacesAttributionSchema,
  recordPlacesRequest,
  withPlacesContext,
} = require('../utils/placesCostAttribution');
const { legacyTextSearch, legacyPlaceDetails } = require('../utils/placesApi');

function mockDb() {
  const rows = [];
  return {
    rows,
    async query(sql, params) {
      if (/CREATE TABLE/i.test(sql)) return { rows: [] };
      if (/INSERT INTO places_api_requests/i.test(sql)) {
        rows.push({
          id: rows.length + 1,
          caller: params[0],
          feature: params[1],
          mission_id: params[2],
          tenant_id: params[3],
          execution_id: params[4],
          trigger_mode: params[5],
          endpoint: params[6],
          is_autocomplete: params[7],
          is_nearby_search: params[8],
          is_find_place: params[9],
          cost_class: params[10],
          http_status: params[11],
          google_status: params[12],
          latency_ms: params[13],
          created_at: new Date().toISOString(),
        });
        return { rows: [] };
      }
      if (/SELECT \* FROM places_api_requests/i.test(sql)) {
        return { rows: [...rows] };
      }
      return { rows: [] };
    },
  };
}

describe('AUDIT-063 places cost attribution', () => {
  it('buildPlacesCostReport produces caller, endpoint, and ratio tables', () => {
    const rows = [];
    for (let i = 0; i < 100; i += 1) {
      rows.push({ feature: PLACES_FEATURES.LEADGEN, endpoint: PLACES_ENDPOINTS.TEXT_SEARCH });
      rows.push({ feature: PLACES_FEATURES.LEADGEN, endpoint: PLACES_ENDPOINTS.PLACE_DETAILS });
    }
    for (let i = 0; i < 10; i += 1) {
      rows.push({ feature: PLACES_FEATURES.DISCOVERY, endpoint: PLACES_ENDPOINTS.TEXT_SEARCH });
    }
    for (let i = 0; i < 50; i += 1) {
      rows.push({ feature: PLACES_FEATURES.CANDIDATE_REFRESH, endpoint: PLACES_ENDPOINTS.SEARCH_TEXT_V1 });
      rows.push({ feature: PLACES_FEATURES.CANDIDATE_REFRESH, endpoint: PLACES_ENDPOINTS.PLACE_DETAILS_V1 });
    }

    const report = buildPlacesCostReport(rows);
    assert.equal(report.total, 310);

    const leadgen = report.callerBreakdown.find((row) => row.caller === 'leadgen.js');
    assert.ok(leadgen);
    assert.equal(leadgen.calls, 200);

    const discovery = report.callerBreakdown.find((row) => row.caller === 'Scout Discovery');
    assert.ok(discovery);
    assert.equal(discovery.calls, 10);

    const textSearch = report.endpointBreakdown.find((row) => row.endpoint === 'Text Search');
    assert.ok(textSearch);
    assert.equal(textSearch.calls, 160);

    const leadgenRatio = report.featureRatioBreakdown.find((row) => row.feature === 'leadgen.js');
    assert.ok(leadgenRatio);
    assert.equal(leadgenRatio.detailsTextRatio, '100:100');
  });

  it('formatPlacesCostReportMarkdown includes table headers', () => {
    const md = formatPlacesCostReportMarkdown(buildPlacesCostReport([]));
    assert.match(md, /By caller/);
    assert.match(md, /By endpoint/);
    assert.match(md, /Details\/Text ratio/);
  });

  it('recordPlacesRequest merges AsyncLocalStorage context', async () => {
    const db = mockDb();
    await ensurePlacesAttributionSchema(db);

    await withPlacesContext(
      {
        caller: 'leadgen.js',
        feature: PLACES_FEATURES.LEADGEN,
        tenantId: 1,
        missionId: 'mission-abc',
        executionId: 'exec-123',
        triggerMode: 'cron',
      },
      async () => {
        await recordPlacesRequest(
          { endpoint: PLACES_ENDPOINTS.TEXT_SEARCH },
          { db }
        );
      }
    );

    assert.equal(db.rows.length, 1);
    assert.equal(db.rows[0].caller, 'leadgen.js');
    assert.equal(db.rows[0].feature, PLACES_FEATURES.LEADGEN);
    assert.equal(db.rows[0].tenant_id, 1);
    assert.equal(db.rows[0].mission_id, 'mission-abc');
    assert.equal(db.rows[0].execution_id, 'exec-123');
    assert.equal(db.rows[0].trigger_mode, 'cron');
    assert.equal(db.rows[0].endpoint, PLACES_ENDPOINTS.TEXT_SEARCH);
  });

  it('legacyTextSearch records attribution via fetch mock', async () => {
    const db = mockDb();
    await ensurePlacesAttributionSchema(db);

    const fetchImpl = async () => ({
      status: 200,
      ok: true,
      async json() {
        return { status: 'OK', results: [{ place_id: 'abc', name: 'Test Co' }] };
      },
    });

    await legacyTextSearch(
      {
        query: 'law firm Manchester NH',
        apiKey: 'test-key',
        fetchImpl,
        record: {
          caller: 'PlacesProvider',
          feature: PLACES_FEATURES.DISCOVERY,
        },
      },
      { db }
    );

    assert.equal(db.rows.length, 1);
    assert.equal(db.rows[0].endpoint, PLACES_ENDPOINTS.TEXT_SEARCH);
    assert.equal(db.rows[0].google_status, 'OK');
  });

  it('legacyPlaceDetails records place_details endpoint', async () => {
    const db = mockDb();
    await ensurePlacesAttributionSchema(db);

    const fetchImpl = async () => ({
      status: 200,
      ok: true,
      async json() {
        return { status: 'OK', result: { name: 'Test Co', place_id: 'abc' } };
      },
    });

    await legacyPlaceDetails(
      {
        placeId: 'abc',
        fields: 'name,place_id',
        apiKey: 'test-key',
        fetchImpl,
        record: {
          caller: 'PlacesProvider',
          feature: PLACES_FEATURES.DISCOVERY,
        },
      },
      { db }
    );

    assert.equal(db.rows.length, 1);
    assert.equal(db.rows[0].endpoint, PLACES_ENDPOINTS.PLACE_DETAILS);
  });
});
