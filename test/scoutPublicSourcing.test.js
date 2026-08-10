'use strict';

/**
 * SPEC-077 — Scout public-source prospect sourcing execution.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  SCOUT_HANDOFF_STATUSES,
  SCOUT_HANDOFF_UI_STATUS,
  buildScoutHandoff,
  handBriefToScout,
  handBriefToScoutAsync,
  executeScoutWorkRequest,
  isScoutSourcingExecutionWired,
  approveScoutResults,
} = require('../services/scoutHandoff');
const {
  isScoutPublicSourcingAvailable,
  buildSearchQueries,
  mapPublicHitToScoutCandidate,
  sourceScoutCandidatesFromPublicSources,
  diagnosePlacesConfig,
  TARGET_COUNT_MIN,
  TARGET_COUNT_MAX,
} = require('../services/scoutPublicSourcing');
const {
  createMemoryScoutWorkRequestStore,
} = require('../services/scoutWorkRequestStore');

function sampleHits(count = 18) {
  const rows = [];
  for (let i = 1; i <= count; i += 1) {
    rows.push({
      companyName: `Manchester Property Co ${i}`,
      website: `https://example.com/pm-${i}`,
      address: `Manchester NH — suite ${i}`,
      location: 'Manchester NH',
      placeId: `place_${i}`,
      placeTypes: ['real_estate_agency'],
      source: 'fixture_public',
      phone: i % 3 === 0 ? null : `603-555-${String(1000 + i).slice(-4)}`,
    });
  }
  return rows;
}

describe('scoutPublicSourcing (SPEC-077)', () => {
  it('is unavailable without Places key or injected search', () => {
    assert.equal(
      isScoutPublicSourcingAvailable({
        scoutPublicSourcingSupported: false,
      }),
      false
    );
    assert.equal(
      isScoutPublicSourcingAvailable({
        apiKey: '',
        fetchImpl: null,
      }),
      false
    );
  });

  it('is available when publicSearchFn or searchProvider is injected', () => {
    assert.equal(
      isScoutPublicSourcingAvailable({
        publicSearchFn: async () => [],
      }),
      true
    );
    assert.equal(
      isScoutPublicSourcingAvailable({
        searchProvider: {
          available: () => true,
          search: async () => [],
        },
      }),
      true
    );
  });

  it('builds market/segment search queries from the work request', () => {
    const queries = buildSearchQueries({
      targetSegment: 'Property managers',
      targetSubtype: 'multi-family',
      marketBounds: 'Greater Manchester NH',
      inclusionCriteria: ['Local portfolios'],
    });
    assert.ok(queries.length >= 1);
    assert.ok(queries.some((q) => /Property managers/i.test(q)));
    assert.ok(queries.some((q) => /Manchester/i.test(q)));
  });

  it('maps public hits to Scout candidate fields with source URL', () => {
    const row = mapPublicHitToScoutCandidate(
      {
        companyName: 'Granite PM',
        website: 'https://granitepm.example',
        address: 'Manchester NH',
        placeTypes: ['real_estate_agency'],
      },
      {
        targetSegment: 'Property managers',
        targetSubtype: 'multi-family',
        marketBounds: 'Manchester NH',
      },
      0
    );
    assert.ok(row);
    assert.equal(row.companyName, 'Granite PM');
    assert.ok(row.sourceUrl);
    assert.ok(row.fitRationale);
    assert.ok(row.risks);
    assert.ok(row.suggestedContactRole);
    assert.ok(row.confidence);
    assert.equal(row.placeholder, false);
    assert.equal(row.reviewOnly, true);
  });

  it('uses maps listing as source URL when website is missing', () => {
    const row = mapPublicHitToScoutCandidate(
      {
        companyName: 'No Website LLC',
        address: 'Bedford NH',
        placeId: 'ChIJtest',
      },
      { targetSegment: 'Property managers', marketBounds: 'Bedford NH' },
      0
    );
    assert.ok(row);
    assert.match(row.sourceUrl, /google\.com\/maps/);
    assert.match(row.risks, /No company website/i);
  });

  it('drops hits without company name', () => {
    const row = mapPublicHitToScoutCandidate(
      { website: 'https://x.example' },
      { targetSegment: 'x' },
      0
    );
    assert.equal(row, null);
  });

  it('sources 15–25 evidenced candidates from public search fn', async () => {
    const workRequest = {
      workRequestId: 'wr-test-1',
      handoffId: 'ho-test-1',
      targetSegment: 'Property managers',
      targetSubtype: 'multi-family',
      marketBounds: 'Manchester NH',
      inclusionCriteria: ['Local portfolios'],
      exclusionCriteria: ['National chains'],
      targetCountMin: TARGET_COUNT_MIN,
      targetCountMax: TARGET_COUNT_MAX,
    };
    const result = await sourceScoutCandidatesFromPublicSources({
      workRequest,
      opts: {
        publicSearchFn: async () => sampleHits(22),
      },
    });
    assert.equal(result.ok, true);
    assert.ok(result.candidates.length >= TARGET_COUNT_MIN);
    assert.ok(result.candidates.length <= TARGET_COUNT_MAX);
    assert.equal(result.crmWritesMade, false);
    assert.equal(result.outreachCopyGenerated, false);
    assert.equal(result.accountChangesMade, false);
    for (const row of result.candidates) {
      assert.ok(row.companyName);
      assert.ok(row.sourceUrl);
      assert.ok(row.location);
      assert.ok(row.fitRationale);
      assert.ok(row.risks);
      assert.ok(row.suggestedContactRole);
      assert.ok(row.confidence);
      assert.equal(row.placeholder, false);
    }
  });

  it('fails explicitly with no placeholders when search returns empty', async () => {
    const result = await sourceScoutCandidatesFromPublicSources({
      workRequest: {
        targetSegment: 'Property managers',
        marketBounds: 'Manchester NH',
      },
      opts: {
        publicSearchFn: async () => [],
      },
    });
    assert.equal(result.ok, false);
    assert.equal(result.candidates.length, 0);
    assert.equal(result.error, 'no_usable_candidates');
    assert.equal(result.retryable, true);
    assert.doesNotMatch(JSON.stringify(result), /placeholder company/i);
  });

  it('surfaces operator setup guidance for Google Places REQUEST_DENIED', async () => {
    const result = await sourceScoutCandidatesFromPublicSources({
      workRequest: {
        targetSegment: 'Property managers',
        marketBounds: 'Manchester NH',
      },
      opts: {
        apiKey: 'test-key-present',
        fetchImpl: async () => ({
          ok: true,
          async json() {
            return {
              status: 'REQUEST_DENIED',
              error_message:
                'This API project is not authorized to use this API.',
              results: [],
            };
          },
        }),
      },
    });
    assert.equal(result.ok, false);
    assert.equal(result.error, 'google_places_status_REQUEST_DENIED');
    assert.equal(result.retryable, true);
    assert.ok(result.operatorMessage);
    assert.match(result.operatorMessage, /REQUEST_DENIED/);
    assert.match(result.operatorMessage, /GOOGLE_PLACES_KEY/);
    assert.match(result.operatorMessage, /Places API/i);
    assert.match(result.operatorMessage, /billing/i);
    assert.match(result.operatorMessage, /restrictions/i);
    assert.match(result.operatorMessage, /Fallback:/i);
    assert.match(result.operatorMessage, /not wired/i);
    assert.match(result.operatorMessage, /retryable/i);
    assert.ok(Array.isArray(result.setupNeeded));
    assert.ok(result.setupNeeded.length >= 4);
    assert.equal(result.fallback.available, false);
    assert.equal(result.candidates.length, 0);
  });

  it('surfaces missing-key setup guidance when Places is unavailable', async () => {
    const result = await sourceScoutCandidatesFromPublicSources({
      workRequest: {
        targetSegment: 'Property managers',
        marketBounds: 'Manchester NH',
      },
      opts: {
        apiKey: '',
        fetchImpl: async () => {
          throw new Error('should_not_call_places_without_key');
        },
      },
    });
    assert.equal(result.ok, false);
    assert.ok(
      result.error === 'public_sourcing_unavailable' ||
        result.error === 'google_places_key_missing'
    );
    assert.equal(result.retryable, true);
    assert.match(result.operatorMessage, /GOOGLE_PLACES_KEY/);
    assert.match(result.operatorMessage, /Railway/);
    assert.equal(result.fallback.available, false);
  });

  it('diagnosePlacesConfig lists actionable Railway / GCP setup steps', () => {
    const d = diagnosePlacesConfig({
      status: 'REQUEST_DENIED',
      errorMessage: 'API key not valid',
      keyPresent: true,
    });
    assert.equal(d.code, 'google_places_status_REQUEST_DENIED');
    assert.equal(d.retryable, true);
    assert.match(d.operatorMessage, /API key not valid/);
    assert.ok(d.setupNeeded.some((s) => /GOOGLE_PLACES_KEY/.test(s)));
    assert.ok(d.setupNeeded.some((s) => /Places API/.test(s)));
    assert.ok(d.setupNeeded.some((s) => /billing/i.test(s)));
    assert.ok(d.setupNeeded.some((s) => /restrictions/i.test(s)));
    assert.ok(d.setupNeeded.some((s) => /retry/i.test(s)));
  });
});

describe('scout work request execution (SPEC-077)', () => {
  let store;

  beforeEach(() => {
    store = createMemoryScoutWorkRequestStore();
  });

  it('wires sourcing when publicSearchFn is available', () => {
    assert.equal(
      isScoutSourcingExecutionWired({
        publicSearchFn: async () => sampleHits(16),
      }),
      true
    );
  });

  it('handBriefToScout queues for async public sourcing when wired via publicSearchFn', () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'Validate walkthrough demand',
      targetSegment: 'Property managers',
      marketBounds: 'Manchester NH',
    });
    const queued = handBriefToScout(draft, {
      publicSearchFn: async () => sampleHits(16),
      workRequestStore: store,
    });
    assert.equal(queued.ok, true);
    assert.equal(queued.shouldExecuteScoutSourcing, true);
    assert.equal(queued.intent, 'scout_handoff_queued');
    assert.equal(queued.handoff.status, SCOUT_HANDOFF_STATUSES.QUEUED);
    assert.equal(queued.workRequest.status, SCOUT_HANDOFF_STATUSES.QUEUED);
    assert.ok(store.getByWorkRequestId(queued.workRequest.workRequestId));
  });

  it('executeScoutWorkRequest reads by workRequestId and returns review-only batch', async () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'Validate walkthrough demand',
      targetSegment: 'Property managers',
      targetSubtype: 'multi-family',
      marketBounds: 'Manchester NH',
      inclusionCriteria: ['Local portfolios'],
      exclusionCriteria: ['National chains'],
    });
    const queued = handBriefToScout(draft, {
      publicSearchFn: async () => sampleHits(18),
      workRequestStore: store,
    });

    const result = await executeScoutWorkRequest({
      workRequestId: queued.workRequest.workRequestId,
      publicSearchFn: async () => sampleHits(18),
      workRequestStore: store,
    });

    assert.equal(result.ok, true);
    assert.equal(result.intent, 'scout_handoff_completed');
    assert.equal(result.handoff.status, SCOUT_HANDOFF_STATUSES.COMPLETED);
    assert.equal(
      result.handoff.uiStatus,
      SCOUT_HANDOFF_UI_STATUS.SCOUT_RESULTS_READY
    );
    assert.ok(result.candidateBatch);
    assert.equal(result.candidateBatch.reviewOnly, true);
    assert.equal(result.candidateBatch.resultsApproved, false);
    assert.equal(result.candidateBatch.crmWritesMade, false);
    assert.equal(result.candidateBatch.outreachCopyGenerated, false);
    assert.ok(result.candidateBatch.candidates.length >= TARGET_COUNT_MIN);
    assert.ok(result.candidateBatch.candidates.length <= TARGET_COUNT_MAX);

    const stored = store.getByWorkRequestId(queued.workRequest.workRequestId);
    assert.equal(stored.status, SCOUT_HANDOFF_STATUSES.COMPLETED);
    assert.ok(stored.candidateBatch);
    assert.equal(stored.crmWritesMade, false);

    const byHandoff = store.getByHandoffId(draft.handoffId);
    assert.ok(byHandoff);
    assert.equal(byHandoff.workRequestId, queued.workRequest.workRequestId);
  });

  it('handBriefToScoutAsync runs end-to-end public sourcing', async () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'Validate walkthrough demand',
      targetSegment: 'Property managers',
      marketBounds: 'Manchester NH',
    });
    const result = await handBriefToScoutAsync(draft, {
      publicSearchFn: async () => sampleHits(20),
      workRequestStore: store,
    });
    assert.equal(result.ok, true);
    assert.equal(result.scoutRan, true);
    assert.equal(result.intent, 'scout_handoff_completed');
    assert.ok(result.candidateBatch.candidates.every((c) => c.sourceUrl));
  });

  it('executeScoutWorkRequest by handoffId works', async () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'x',
      targetSegment: 'Property managers',
      marketBounds: 'Manchester NH',
    });
    handBriefToScout(draft, {
      publicSearchFn: async () => sampleHits(16),
      workRequestStore: store,
    });
    const result = await executeScoutWorkRequest({
      handoffId: draft.handoffId,
      publicSearchFn: async () => sampleHits(16),
      workRequestStore: store,
    });
    assert.equal(result.ok, true);
    assert.equal(result.handoff.handoffId, draft.handoffId);
  });

  it('preserves work request on sourcing failure — no placeholders', async () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'x',
      targetSegment: 'Property managers',
      marketBounds: 'Manchester NH',
    });
    const queued = handBriefToScout(draft, {
      publicSearchFn: async () => [],
      workRequestStore: store,
    });
    const result = await executeScoutWorkRequest({
      workRequestId: queued.workRequest.workRequestId,
      publicSearchFn: async () => [],
      workRequestStore: store,
    });
    assert.equal(result.ok, false);
    assert.equal(result.intent, 'scout_sourcing_failed');
    assert.equal(result.handoff.status, SCOUT_HANDOFF_STATUSES.FAILED);
    assert.equal(result.candidateBatch, null);
    assert.match(result.message, /preserved/i);
    assert.match(result.message, /No .*placeholder/i);
    assert.doesNotMatch(result.message, /fabricated company/i);

    const stored = store.getByWorkRequestId(queued.workRequest.workRequestId);
    assert.ok(stored);
    assert.equal(stored.status, SCOUT_HANDOFF_STATUSES.FAILED);
    assert.equal(stored.workRequestId, queued.workRequest.workRequestId);
  });

  it('REQUEST_DENIED surfaces Places setup message and remains retryable', async () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'x',
      targetSegment: 'Property managers',
      marketBounds: 'Manchester NH',
    });
    const deniedFetch = async () => ({
      ok: true,
      async json() {
        return {
          status: 'REQUEST_DENIED',
          error_message: 'This API project is not authorized to use this API.',
          results: [],
        };
      },
    });
    const queued = handBriefToScout(draft, {
      apiKey: 'present-but-denied',
      fetchImpl: deniedFetch,
      workRequestStore: store,
    });
    const failed = await executeScoutWorkRequest({
      workRequestId: queued.workRequest.workRequestId,
      apiKey: 'present-but-denied',
      fetchImpl: deniedFetch,
      workRequestStore: store,
    });
    assert.equal(failed.ok, false);
    assert.equal(failed.retryable, true);
    assert.equal(failed.workRequest.failureReason, 'google_places_status_REQUEST_DENIED');
    assert.match(failed.message, /REQUEST_DENIED/);
    assert.match(failed.message, /GOOGLE_PLACES_KEY/);
    assert.match(failed.message, /Places API/i);
    assert.match(failed.message, /billing/i);
    assert.match(failed.message, /retryable/i);
    assert.ok(failed.setupNeeded.length >= 4);
    assert.equal(failed.fallback.available, false);

    // Same work request can be retried after config is fixed.
    const recovered = await executeScoutWorkRequest({
      workRequestId: queued.workRequest.workRequestId,
      publicSearchFn: async () => sampleHits(16),
      workRequestStore: store,
    });
    assert.equal(recovered.ok, true);
    assert.equal(recovered.intent, 'scout_handoff_completed');
    assert.equal(recovered.handoff.status, SCOUT_HANDOFF_STATUSES.COMPLETED);
    assert.ok(recovered.candidateBatch.candidates.length >= TARGET_COUNT_MIN);
    const stored = store.getByWorkRequestId(queued.workRequest.workRequestId);
    assert.equal(stored.status, SCOUT_HANDOFF_STATUSES.COMPLETED);
    assert.equal(stored.workRequestId, queued.workRequest.workRequestId);
  });

  it('approveScoutResults still gates downstream without CRM/outreach', async () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'x',
      targetSegment: 'Property managers',
      marketBounds: 'Manchester NH',
    });
    const ran = await handBriefToScoutAsync(draft, {
      publicSearchFn: async () => sampleHits(16),
      workRequestStore: store,
    });
    const approved = approveScoutResults(ran.handoff);
    assert.equal(approved.ok, true);
    assert.equal(approved.handoff.resultsApproved, true);
    assert.equal(approved.handoff.crmWritesMade, false);
    assert.equal(approved.handoff.outreachCopyGenerated, false);
    assert.equal(approved.handoff.accountChangesMade, false);
  });

  it('missing workRequestId fails without inventing a request', async () => {
    const result = await executeScoutWorkRequest({
      workRequestId: 'does-not-exist',
      workRequestStore: store,
      publicSearchFn: async () => sampleHits(16),
    });
    assert.equal(result.ok, false);
    assert.match(result.message, /No Scout work request found/i);
  });
});
