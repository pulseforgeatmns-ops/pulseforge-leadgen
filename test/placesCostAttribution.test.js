'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  PLACES_FEATURES,
  PLACES_ENDPOINTS,
  buildPlacesCostReport,
  formatPlacesCostReportMarkdown,
  ensurePlacesAttributionSchema,
  recordPlacesRequest,
  withPlacesContext,
  normalizePlacesQuery,
  buildPlacesContextFromDiscovery,
} = require('../utils/placesCostAttribution');
const { legacyTextSearch, legacyPlaceDetails } = require('../utils/placesApi');
const { clearPlacesQueryCache } = require('../utils/placesQueryCache');

function mockDb() {
  const rows = [];
  return {
    rows,
    async query(sql, params) {
      if (/CREATE TABLE|ALTER TABLE|CREATE INDEX/i.test(sql)) return { rows: [] };
      if (/INSERT INTO places_api_requests/i.test(sql)) {
        rows.push({
          id: rows.length + 1,
          caller: params[0],
          feature: params[1],
          mission_id: params[2],
          mission_stage: params[3],
          tenant_id: params[4],
          execution_id: params[5],
          operator_id: params[6],
          trigger_mode: params[7],
          endpoint: params[8],
          hypothesis_id: params[16],
          hypothesis_label: params[17],
          evidence_requirement: params[18],
          investigation_task: params[19],
          provider_id: params[20],
          cache_hit: params[21],
          cache_miss: params[22],
          original_query: params[26],
          normalized_query: params[27],
          businesses_returned: params[28],
          businesses_accepted: params[29],
          candidates_created: params[30],
          qualified_candidates: params[31],
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
  it('normalizePlacesQuery canonicalizes operator queries', () => {
    assert.equal(
      normalizePlacesQuery('Commercial Property Management Manchester NH'),
      'property management manchester'
    );
  });

  it('buildPlacesContextFromDiscovery captures mission and cognitive fields', () => {
    const ctx = buildPlacesContextFromDiscovery({
      searchDefinition: {
        tenantId: '5',
        missionId: 'amo-123',
        missionStage: 'discovery',
        operatorId: 'jacob',
        evidenceRequest: { evidenceType: 'identity', investigationTaskId: 'task:identity' },
      },
      mission: {
        hypotheses: [{ id: 'hyp-str', text: 'Company manages multiple STR properties' }],
      },
      assignment: { providerId: 'google_maps', evidenceType: 'identity' },
    });

    assert.equal(ctx.missionId, 'amo-123');
    assert.equal(ctx.missionStage, 'discovery');
    assert.equal(ctx.operatorId, 'jacob');
    assert.equal(ctx.hypothesisLabel, 'Company manages multiple STR properties');
    assert.equal(ctx.evidenceRequirement, 'identity');
    assert.equal(ctx.investigationTask, 'Collect business identities');
    assert.equal(ctx.providerId, 'google_maps');
  });

  it('buildPlacesCostReport includes cache, cognitive, and efficiency sections', () => {
    const rows = [
      {
        feature: PLACES_FEATURES.DISCOVERY,
        endpoint: PLACES_ENDPOINTS.TEXT_SEARCH,
        mission_id: 'amo-1',
        mission_stage: 'discovery',
        hypothesis_label: 'Company manages multiple STR properties',
        evidence_requirement: 'identity',
        investigation_task: 'Collect business identities',
        normalized_query: 'property management manchester',
        cache_hit: false,
        cache_miss: true,
        businesses_returned: 20,
        businesses_accepted: 8,
        candidates_created: 8,
        qualified_candidates: 3,
      },
      {
        feature: PLACES_FEATURES.DISCOVERY,
        endpoint: PLACES_ENDPOINTS.TEXT_SEARCH,
        normalized_query: 'property management manchester',
        cache_hit: true,
        cache_miss: false,
        businesses_returned: 20,
      },
      {
        feature: PLACES_FEATURES.LEADGEN,
        endpoint: PLACES_ENDPOINTS.PLACE_DETAILS,
        cache_hit: false,
        cache_miss: true,
        businesses_returned: 1,
        businesses_accepted: 1,
        candidates_created: 1,
      },
    ];

    const report = buildPlacesCostReport(rows);
    assert.equal(report.total, 3);
    assert.equal(report.apiCalls, 2);
    assert.equal(report.cache.hits, 1);
    assert.equal(report.cache.misses, 2);
    assert.equal(report.duplicateNormalizedQueries.length, 1);
    assert.equal(report.efficiency.businessesReturned, 41);
    assert.equal(report.efficiency.candidatesCreated, 9);
    assert.equal(report.cognitive.hypothesisBreakdown[0].hypothesis, 'Company manages multiple STR properties');
    assert.ok(report.estimatedSpendUsd > 0);
    assert.ok(report.efficiency.costPerCandidateUsd > 0);
  });

  it('formatPlacesCostReportMarkdown includes extended daily sections', () => {
    const md = formatPlacesCostReportMarkdown(buildPlacesCostReport([]), {
      headerLines: ['Report day (UTC): **2026-08-25**'],
    });
    assert.match(md, /Spend by feature/);
    assert.match(md, /Cache/);
    assert.match(md, /Cognitive breakdown/);
    assert.match(md, /Efficiency funnel/);
    assert.match(md, /Cost per qualified candidate/);
  });

  it('recordPlacesRequest merges mission and cognitive AsyncLocalStorage context', async () => {
    const db = mockDb();
    await ensurePlacesAttributionSchema(db);

    await withPlacesContext(
      {
        caller: 'HypothesisDrivenDiscoveryEngine',
        feature: PLACES_FEATURES.DISCOVERY,
        tenantId: 1,
        missionId: 'amo-abc',
        missionStage: 'discovery',
        executionId: 'exec-123',
        operatorId: 'jacob',
        triggerMode: 'operator',
        hypothesisId: 'hyp-str',
        hypothesisLabel: 'Company manages multiple STR properties',
        evidenceRequirement: 'identity',
        investigationTask: 'Collect business identities',
        providerId: 'google_places',
      },
      async () => {
        await recordPlacesRequest(
          {
            endpoint: PLACES_ENDPOINTS.TEXT_SEARCH,
            originalQuery: 'Commercial Property Management Manchester NH',
            cacheHit: false,
            cacheMiss: true,
            cacheStrategy: 'memory_ttl',
          },
          { db }
        );
      }
    );

    assert.equal(db.rows.length, 1);
    assert.equal(db.rows[0].mission_id, 'amo-abc');
    assert.equal(db.rows[0].mission_stage, 'discovery');
    assert.equal(db.rows[0].operator_id, 'jacob');
    assert.equal(db.rows[0].hypothesis_label, 'Company manages multiple STR properties');
    assert.equal(db.rows[0].evidence_requirement, 'identity');
    assert.equal(db.rows[0].normalized_query, 'property management manchester');
  });

  it('legacyTextSearch uses cache on second identical normalized query', async () => {
    clearPlacesQueryCache();
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
        query: 'Commercial Property Management Manchester NH',
        apiKey: 'test-key',
        fetchImpl,
        record: { caller: 'PlacesProvider', feature: PLACES_FEATURES.DISCOVERY },
      },
      { db }
    );
    await legacyTextSearch(
      {
        query: 'commercial property management manchester nh',
        apiKey: 'test-key',
        fetchImpl,
        record: { caller: 'PlacesProvider', feature: PLACES_FEATURES.DISCOVERY },
      },
      { db }
    );

    assert.equal(db.rows.length, 2);
    assert.equal(db.rows[0].cache_miss, true);
    assert.equal(db.rows[1].cache_hit, true);
  });

  it('legacyPlaceDetails records place_details endpoint', async () => {
    const db = mockDb();
    await ensurePlacesAttributionSchema(db);

    const fetchImpl = async () => ({
      status: 200,
      ok: true,
      async json() {
        return { status: 'OK', result: { name: 'Test Co', place_id: 'abc', website: 'https://test.co' } };
      },
    });

    const traced = await legacyPlaceDetails(
      {
        placeId: 'abc',
        fields: 'name,place_id,website',
        apiKey: 'test-key',
        fetchImpl,
        deferRecord: true,
        record: { caller: 'PlacesProvider', feature: PLACES_FEATURES.DISCOVERY },
      },
      { db }
    );
    await traced.commitRecord({ businessesAccepted: 1, candidatesCreated: 1 });

    assert.equal(db.rows.length, 1);
    assert.equal(db.rows[0].endpoint, PLACES_ENDPOINTS.PLACE_DETAILS);
    assert.equal(db.rows[0].businesses_accepted, 1);
  });
});
