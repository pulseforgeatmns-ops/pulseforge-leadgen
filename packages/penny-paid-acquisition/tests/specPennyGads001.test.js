'use strict';

/**
 * SPEC-PENNY-GADS-001 — Google Ads readiness, evidence correctness, attribution.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  readGoogleAdsEvidence,
  assessGoogleAdsReadiness,
  resolveGoogleAdsApiVersion,
  buildGoogleAdsReadinessInspection,
  READINESS_STATE,
  AVAILABILITY,
  UNAVAILABLE_REASON,
  PLATFORM,
  loadFirstPartyAttributionEvidence,
  normalizeEvidenceWindowInput,
  windowBounds,
  deriveCampaignLeadEconomics,
  resolveAdAccountsForClient,
} = require('../index');
const {
  buildPaidAcquisitionRecommendationPayload,
  VIABILITY,
} = require('../../max/workspace/PennyPaidAcquisitionExecutor');
const { buildAttributionRecord, parseQueryAttribution } = require('../../../lib/walkthroughAttribution');
const { validateWalkthroughPayload } = require('../../../lib/walkthroughValidate');
const { captureWalkthroughLead, buildWalkthroughActionPayload } = require('../../../lib/walkthroughCapture');
const { createWalkthroughCaptureMockPool } = require('../../../test/helpers/walkthroughCaptureMockPool');
const pool = require('../../../db');
const { ACQUISITION_APPROACHES } = require('../../acquisition-mission');

const CLIENT_10 = 10;
const CLIENT_1 = 1;
const FIXED_NOW = new Date('2026-09-14T15:00:00.000Z');

const CUSTOMER_FIXTURE = {
  results: [{ customer: { id: '9876543210', currencyCode: 'USD', timeZone: 'America/New_York' } }],
};

function campaignFixture(overrides = {}) {
  return {
    results: [{
      campaign: {
        id: '2002',
        name: 'Paused Historical',
        status: 'PAUSED',
        advertisingChannelType: 'SEARCH',
        ...(overrides.campaign || {}),
      },
      campaignBudget: { amountMicros: '10000000' },
      metrics: {
        impressions: '400',
        clicks: '12',
        ctr: 0.03,
        averageCpc: '2000000',
        conversions: 1,
        conversionsValue: 99.5,
        costMicros: '24000000',
        ...(overrides.metrics || {}),
      },
    }],
  };
}

function mockGoogleHttpSequence(fixtures = {}) {
  const pages = fixtures.campaignPages || [campaignFixture()];
  let campaignPageIndex = 0;
  return {
    post: async (url, body) => {
      if (url.includes('oauth2.googleapis.com/token')) {
        return { data: { access_token: 'access-token-test' } };
      }
      const query = body?.query || '';
      if (/FROM customer/i.test(query)) return { data: CUSTOMER_FIXTURE };
      if (/FROM campaign/i.test(query)) {
        const page = pages[campaignPageIndex] || { results: [] };
        campaignPageIndex += 1;
        if (campaignPageIndex < pages.length) {
          return { data: { ...page, nextPageToken: `page-${campaignPageIndex}` } };
        }
        return { data: page };
      }
      if (/FROM ad_group_criterion/i.test(query)) return { data: { results: [] } };
      return { data: { results: [] } };
    },
  };
}

describe('SPEC-PENNY-GADS-001 — Google Ads API config', () => {
  beforeEach(() => {
    delete process.env.GOOGLE_ADS_API_VERSION;
    process.env.GOOGLE_ADS_DEVELOPER_TOKEN = 'dev-token';
    process.env.GOOGLE_ADS_CLIENT_ID = 'client-id';
    process.env.GOOGLE_ADS_CLIENT_SECRET = 'client-secret';
  });

  it('defaults to supported v25 instead of sunset v18', () => {
    const resolved = resolveGoogleAdsApiVersion();
    assert.equal(resolved.version, 'v25');
    assert.equal(resolved.supported, true);
    assert.notEqual(resolved.version, 'v18');
  });

  it('returns API_VERSION_UNSUPPORTED readiness for unknown API versions', async () => {
    process.env.GOOGLE_ADS_API_VERSION = 'v18';
    const evidence = await readGoogleAdsEvidence({
      account: { account_id: '123', refresh_token: 'refresh' },
      http: mockGoogleHttpSequence(),
    });
    assert.equal(evidence.availability, AVAILABILITY.UNAVAILABLE);
    assert.equal(evidence.readiness.accountStatus, READINESS_STATE.API_VERSION_UNSUPPORTED);
  });
});

describe('SPEC-PENNY-GADS-001 — evidence correctness', () => {
  beforeEach(() => {
    process.env.GOOGLE_ADS_DEVELOPER_TOKEN = 'dev-token';
    process.env.GOOGLE_ADS_CLIENT_ID = 'client-id';
    process.env.GOOGLE_ADS_CLIENT_SECRET = 'client-secret';
    delete process.env.GOOGLE_ADS_API_VERSION;
  });

  it('keeps conversion value in currency units (not micros)', async () => {
    const evidence = await readGoogleAdsEvidence({
      account: { id: 'acc', account_id: '987-654-3210', refresh_token: 'refresh', client_id: 10 },
      http: mockGoogleHttpSequence(),
    });
    assert.equal(evidence.campaigns[0].platformConversionValue, 99.5);
  });

  it('includes paused campaigns with spend in the observation window', async () => {
    const evidence = await readGoogleAdsEvidence({
      account: { account_id: '987-654-3210', refresh_token: 'refresh' },
      http: mockGoogleHttpSequence({ campaignPages: [campaignFixture()] }),
      window: { start: '2026-09-01', end: '2026-09-14', days: 14, label: 'CUSTOM_14_DAYS' },
    });
    assert.equal(evidence.campaigns[0].status, 'PAUSED');
    assert.equal(evidence.campaigns[0].spend, 24);
  });

  it('paginates campaign search until nextPageToken is exhausted', async () => {
    const page1 = campaignFixture({ campaign: { id: '1', name: 'A', status: 'ENABLED' } });
    const page2 = campaignFixture({ campaign: { id: '2', name: 'B', status: 'ENABLED' } });
    const http = mockGoogleHttpSequence({ campaignPages: [page1, page2] });
    const evidence = await readGoogleAdsEvidence({
      account: { account_id: '987-654-3210', refresh_token: 'refresh' },
      http,
    });
    assert.equal(evidence.campaigns.length, 2);
  });

  it('rejects nonpositive paid test budgets in Penny viability', () => {
    const payload = buildPaidAcquisitionRecommendationPayload({
      specialistInput: {
        acquisitionApproach: { selectedApproach: ACQUISITION_APPROACHES.PAID },
        availableBudget: { amount: 0, currency: 'USD' },
        conversionReadiness: { ready: true },
        measurementReadiness: { ready: true },
        candidatePaidChannels: [{ name: 'Yelp' }],
        platformEvidence: [],
      },
    });
    assert.equal(payload.viability, VIABILITY.BLOCKED);
    assert.ok(payload.blockers.some((row) => row.kind === 'nonpositive_paid_test_budget'));
  });
});

describe('SPEC-PENNY-GADS-001 — first-party observation window contract', () => {
  const fixtureRow = {
    id: 42,
    created_at: new Date('2026-09-10T12:00:00.000Z'),
    payload: {
      attribution: {
        raw: { gclid: 'gclid-test', campaign_id: '1001' },
        normalized: { lead_source: 'google_ads', attribution_status: 'deterministic' },
        provenance: { sourceKind: 'FIRST_PARTY_ATTRIBUTION' },
      },
    },
  };

  it('accepts observationWindow and window input shapes equivalently', async () => {
    const window = { start: '2026-09-01', end: '2026-09-14', days: 14 };
    const queryRows = async () => [fixtureRow];

    const viaWindow = await loadFirstPartyAttributionEvidence({
      clientId: CLIENT_10,
      window,
      queryRows,
    });
    const viaObservationWindow = await loadFirstPartyAttributionEvidence({
      clientId: CLIENT_10,
      observationWindow: window,
      queryRows,
    });

    assert.equal(viaWindow.observedCount, 1);
    assert.equal(viaObservationWindow.observedCount, 1);
  });

  it('uses exclusive end-date bounds without including the following day', () => {
    const { startAt, endAt } = windowBounds({ start: '2026-09-01', end: '2026-09-14' });
    assert.equal(startAt.toISOString(), '2026-09-01T00:00:00.000Z');
    assert.equal(endAt.toISOString(), '2026-09-15T00:00:00.000Z');
    const included = new Date('2026-09-14T23:59:59.000Z');
    const excluded = new Date('2026-09-15T00:00:00.000Z');
    assert.ok(included >= startAt && included < endAt);
    assert.ok(!(excluded >= startAt && excluded < endAt));
  });

  it('normalizeEvidenceWindowInput prefers observationWindow when both are supplied', () => {
    const normalized = normalizeEvidenceWindowInput({
      window: { start: '2026-01-01', end: '2026-01-07' },
      observationWindow: { start: '2026-09-01', end: '2026-09-14' },
    });
    assert.equal(normalized.window.start, '2026-09-01');
  });
});

describe('SPEC-PENNY-GADS-001 — Google click-id attribution acceptance', () => {
  it('parses gclid/gbraid/wbraid from landing query params', () => {
    const parsed = parseQueryAttribution('?gclid=abc&gclid=ignored&gbraid=gb1&wbraid=wb1&utm_source=google&utm_medium=cpc&utm_campaign=anchor-law');
    assert.equal(parsed.gclid, 'abc');
    assert.equal(parsed.gbraid, 'gb1');
    assert.equal(parsed.wbraid, 'wb1');
    assert.equal(parsed.utm_campaign, 'anchor-law');
  });

  it('stores Google Ads attribution on walkthrough capture and joins to first-party evidence', async () => {
    const landing = parseQueryAttribution('?gclid=gclid-acceptance&gbraid=gbraid-acceptance&utm_source=google&utm_medium=cpc&utm_campaign=1001&campaign_id=1001&ad_group_id=2001&ad_id=3001');
    const record = buildAttributionRecord({
      ...landing,
      landing_page_url: 'https://goanchorcleaning.com/?gclid=gclid-acceptance',
      referrer: 'https://www.google.com/',
    }, { observedAt: FIXED_NOW });
    assert.equal(record.normalized.lead_source, 'google_ads');
    assert.equal(record.normalized.attribution_status, 'deterministic');

    const validated = validateWalkthroughPayload({
      name: 'Alex Owner',
      business_name: 'Riverside Law',
      phone: '(603) 555-0142',
      email: 'alex@riverside.example',
      city: 'Manchester',
      space_type: 'law_office',
      attribution: record.raw,
    });
    assert.equal(validated.ok, true);

    const originalQuery = pool.query;
    const mock = createWalkthroughCaptureMockPool();
    pool.query = mock.query.bind(mock);
    try {
      await captureWalkthroughLead(validated.values, record);
      const payload = mock.state.agentActions[0].payload;
      assert.equal(payload.attribution.raw.gclid, 'gclid-acceptance');
      assert.equal(payload.attribution.raw.gbraid, 'gbraid-acceptance');

      const rowsByClient = {
        [CLIENT_10]: [{
          id: mock.state.agentActions[0].id,
          created_at: FIXED_NOW,
          payload,
        }],
      };
      const queryRows = async ({ clientId }) => rowsByClient[clientId] || [];

      const fp = await loadFirstPartyAttributionEvidence({
        clientId: CLIENT_10,
        window: { start: '2026-09-01', end: '2026-09-14', days: 14 },
        queryRows,
      });
      assert.equal(fp.observedCount, 1);
      assert.equal(fp.evidence[0].leadSource, 'google_ads');
      assert.equal(fp.evidence[0].campaignId, '1001');

      const wrongTenant = await loadFirstPartyAttributionEvidence({
        clientId: CLIENT_1,
        window: { start: '2026-09-01', end: '2026-09-14', days: 14 },
        queryRows,
      });
      assert.equal(wrongTenant.observedCount, 0);

      const economics = deriveCampaignLeadEconomics({
        platformEvidence: [{
          platform: PLATFORM.GOOGLE_ADS,
          availability: AVAILABILITY.AVAILABLE,
          observationWindow: { start: '2026-09-01', end: '2026-09-14' },
          campaigns: [{
            externalCampaignId: '1001',
            spend: 50,
            platformConversions: 1,
          }],
          aggregates: { spend: 50, platformConversions: 1 },
        }],
        firstPartyAttributionEvidence: fp.evidence,
        firstPartyAttributionRetrieval: {
          availability: AVAILABILITY.AVAILABLE,
          observedCount: 1,
          observationWindow: { start: '2026-09-01', end: '2026-09-14' },
        },
        observationWindow: { start: '2026-09-01', end: '2026-09-14' },
      });
      assert.equal(economics.campaignLeadEconomics[0].firstPartyAttributedLeadCount, 1);
      assert.equal(economics.campaignLeadEconomics[0].attribution.deterministicLeadCount, 1);
      assert.equal(economics.campaignLeadEconomics[0].externalCampaignId, '1001');
    } finally {
      pool.query = originalQuery;
    }
  });

  it('buildWalkthroughActionPayload preserves attribution identifiers end-to-end', () => {
    const record = buildAttributionRecord({
      gclid: 'g1',
      utm_source: 'google',
      utm_medium: 'cpc',
      campaign_id: '1001',
      landing_page_url: 'https://goanchorcleaning.com/',
    });
    const payload = buildWalkthroughActionPayload(
      { name: 'A', business_name: 'B', phone: '1', email: 'a@b.com', city: 'Manchester', space_type: 'law_office', space_type_label: 'Law office' },
      record,
      { prospectId: 1, linkStatus: 'LINKED', isNew: false },
      FIXED_NOW.toISOString()
    );
    assert.equal(payload.attribution.raw.gclid, 'g1');
    assert.equal(payload.attribution.raw.landing_page_url, 'https://goanchorcleaning.com/');
  });
});

describe('SPEC-PENNY-GADS-001 — tenant-scoped account binding', () => {
  it('scopes Google account resolution to requested tenant only', async () => {
    const accounts = await resolveAdAccountsForClient({
      clientId: 10,
      platform: PLATFORM.GOOGLE_ADS,
      queryAccounts: ({ clientId }) => (clientId === 10
        ? [{ id: 'a10', client_id: 10, platform: 'google_ads', account_id: '111', refresh_token: 'r10', is_active: true }]
        : [{ id: 'a1', client_id: 1, platform: 'google_ads', account_id: '222', refresh_token: 'r1', is_active: true }]),
    });
    assert.equal(accounts[0].account_id, '111');
  });

  it('warns but does not block readiness when developer token is absent (post-sunset OAuth access)', async () => {
    delete process.env.GOOGLE_ADS_DEVELOPER_TOKEN;
    process.env.GOOGLE_ADS_CLIENT_ID = 'client-id';
    process.env.GOOGLE_ADS_CLIENT_SECRET = 'client-secret';
    const readiness = await assessGoogleAdsReadiness({
      clientId: 10,
      resolveAccounts: () => [{ id: 'a10', client_id: 10, platform: 'google_ads', account_id: '111', refresh_token: 'r10' }],
      http: mockGoogleHttpSequence(),
    });
    assert.equal(readiness.credentialStatus, READINESS_STATE.READY);
    assert.equal(readiness.accountStatus, READINESS_STATE.READY);
    assert.ok(readiness.warnings.some((row) => /GOOGLE_ADS_DEVELOPER_TOKEN/i.test(row)));
  });

  it('returns structured readiness when OAuth client credentials are missing', async () => {
    delete process.env.GOOGLE_ADS_DEVELOPER_TOKEN;
    delete process.env.GOOGLE_ADS_CLIENT_ID;
    delete process.env.GOOGLE_ADS_CLIENT_SECRET;
    const readiness = await assessGoogleAdsReadiness({
      clientId: 10,
      resolveAccounts: () => [{ id: 'a10', client_id: 10, platform: 'google_ads', account_id: '111', refresh_token: 'r10' }],
    });
    assert.equal(readiness.credentialStatus, READINESS_STATE.MISSING_CREDENTIALS);
    assert.ok(readiness.blockers.length);
  });

  it('omits developer-token header when env is unset but still sends OAuth bearer token', async () => {
    delete process.env.GOOGLE_ADS_DEVELOPER_TOKEN;
    process.env.GOOGLE_ADS_CLIENT_ID = 'client-id';
    process.env.GOOGLE_ADS_CLIENT_SECRET = 'client-secret';
    let searchHeaders = null;
    const http = {
      post: async (url, body, config) => {
        if (url.includes('oauth2.googleapis.com/token')) {
          return { data: { access_token: 'access-token-test' } };
        }
        searchHeaders = config?.headers || null;
        return mockGoogleHttpSequence().post(url, body);
      },
    };
    await readGoogleAdsEvidence({
      account: { account_id: '987-654-3210', refresh_token: 'refresh', client_id: 10 },
      http,
    });
    assert.ok(searchHeaders);
    assert.equal(searchHeaders['developer-token'], undefined);
    assert.match(searchHeaders.Authorization, /Bearer access-token-test/);
  });

  it('buildGoogleAdsReadinessInspection exposes operator-facing fields', () => {
    const row = buildGoogleAdsReadinessInspection({
      tenantId: 10,
      accountStatus: READINESS_STATE.READY,
      credentialStatus: READINESS_STATE.READY,
      customerId: '111-222-3333',
      apiVersion: 'v25',
      campaignEvidenceStatus: 'OK',
      conversionEvidenceStatus: 'OK',
      attributionCaptureStatus: 'CONFIGURED',
      nextRequiredAction: '',
    });
    assert.equal(row.platform, PLATFORM.GOOGLE_ADS);
    assert.equal(row.apiVersion, 'v25');
  });
});
