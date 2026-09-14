'use strict';

/**
 * SPEC-252 — Canonical Penny live platform evidence bridge.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../acquisition-mission');
const {
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  ACQUISITION_APPROACHES,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  createExecutionRequest,
  routeExecutionRequest,
  createAcquisitionMissionEngine,
  buildExecutionInput,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  collectPaidPlatformEvidence,
  mergePlatformEvidence,
  resolveAdAccountsForClient,
  readGoogleAdsEvidence,
  AVAILABILITY,
  UNAVAILABLE_REASON,
  PLATFORM,
  stripSecrets,
} = require('../index');
const {
  resolvePlatformEvidenceForPenny,
  runPennyForAmoMission,
} = require('../../max/workspace/PennyPaidAcquisitionExecutor');

const GOOGLE_FIXTURE = {
  results: [{
    campaign: {
      id: '1001',
      name: 'Anchor Commercial Search',
      status: 'ENABLED',
      advertisingChannelType: 'SEARCH',
    },
    campaignBudget: { amountMicros: '25000000' },
    metrics: {
      impressions: '1200',
      clicks: '48',
      ctr: 0.04,
      averageCpc: '2470000',
      conversions: 2,
      costPerConversion: '59280000',
      costMicros: '118560000',
    },
  }],
};

const KEYWORD_FIXTURE = {
  results: [{
    adGroupCriterion: {
      keyword: { text: 'commercial cleaning manchester nh' },
      qualityInfo: { qualityScore: 7 },
    },
    campaign: { name: 'Anchor Commercial Search' },
    adGroup: { name: 'Core Terms' },
  }],
};

function mockGoogleHttp() {
  let call = 0;
  return {
    post: async (url, body) => {
      if (url.includes('oauth2.googleapis.com/token')) {
        return { data: { access_token: 'test-access-token' } };
      }
      call += 1;
      if (call === 1) return { data: GOOGLE_FIXTURE };
      return { data: KEYWORD_FIXTURE };
    },
  };
}

function accountsForClient(clientId) {
  const accounts = {
    1: [{
      id: 'acc-client-1',
      client_id: 1,
      platform: 'google_ads',
      account_id: '123-456-7890',
      refresh_token: 'refresh-client-1',
      is_active: true,
      company_name: 'Pulseforge',
    }],
    10: [{
      id: 'acc-client-10',
      client_id: 10,
      platform: 'google_ads',
      account_id: '987-654-3210',
      refresh_token: 'refresh-client-10',
      is_active: true,
      company_name: 'Anchor Cleaning',
    }],
  };
  return accounts[clientId] || [];
}

describe('SPEC-252 — Paid platform evidence collector', () => {
  beforeEach(() => {
    process.env.GOOGLE_ADS_DEVELOPER_TOKEN = 'dev-token';
    process.env.GOOGLE_ADS_CLIENT_ID = 'client-id';
    process.env.GOOGLE_ADS_CLIENT_SECRET = 'client-secret';
  });

  it('normalizes successful Google read without secrets', async () => {
    const evidence = await readGoogleAdsEvidence({
      account: {
        id: 'acc-1',
        account_id: '123-456-7890',
        refresh_token: 'refresh-token',
      },
      http: mockGoogleHttp(),
    });

    assert.equal(evidence.availability, AVAILABILITY.AVAILABLE);
    assert.equal(evidence.platform, PLATFORM.GOOGLE_ADS);
    assert.equal(evidence.campaigns[0].externalCampaignId, '1001');
    assert.equal(evidence.campaigns[0].platformConversions, 2);
    assert.equal(evidence.campaigns[0].spend, 118.56);
    assert.ok(evidence.platformMetricsAreEvidenceOnly);
    assert.ok(evidence.provenance.readOnly);
    assert.doesNotMatch(JSON.stringify(evidence), /refresh-token|access_token/i);
  });

  it('returns unavailable when credentials are missing', async () => {
    const evidence = await readGoogleAdsEvidence({
      account: { account_id: '123', refresh_token: null },
    });
    assert.equal(evidence.availability, AVAILABILITY.UNAVAILABLE);
    assert.equal(evidence.reason, UNAVAILABLE_REASON.MISSING_CREDENTIALS);
  });

  it('returns error evidence on API failure', async () => {
    const evidence = await readGoogleAdsEvidence({
      account: { account_id: '123', refresh_token: 'refresh' },
      http: {
        post: async () => {
          const err = new Error('boom');
          err.response = { data: { error: { message: 'API quota exceeded' } } };
          throw err;
        },
      },
    });
    assert.equal(evidence.availability, AVAILABILITY.ERROR);
    assert.match(evidence.error, /quota/i);
  });

  it('scopes account resolution to the requested client only', async () => {
    const rows = await resolveAdAccountsForClient({
      clientId: 10,
      queryAccounts: ({ clientId }) => accountsForClient(clientId),
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].client_id, 10);
    assert.equal(rows[0].account_id, '987-654-3210');
  });

  it('does not return client 1 accounts when client 10 is requested', async () => {
    const resolveAccounts = ({ clientId }) => accountsForClient(clientId);
    const client10 = await collectPaidPlatformEvidence({
      clientId: 10,
      channels: ['Google Search'],
      resolveAccounts,
      http: mockGoogleHttp(),
    });
    const client1 = await collectPaidPlatformEvidence({
      clientId: 1,
      channels: ['Google Search'],
      resolveAccounts,
      http: mockGoogleHttp(),
    });

    assert.equal(client10[0].account.externalAccountId, '987-654-3210');
    assert.equal(client1[0].account.externalAccountId, '123-456-7890');
    assert.notEqual(client10[0].account.externalAccountId, client1[0].account.externalAccountId);
  });

  it('returns explicit unavailable when no linked account exists', async () => {
    const evidence = await collectPaidPlatformEvidence({
      clientId: 99,
      channels: ['Google Search'],
      resolveAccounts: () => [],
    });
    assert.equal(evidence[0].availability, AVAILABILITY.UNAVAILABLE);
    assert.equal(evidence[0].reason, UNAVAILABLE_REASON.NO_LINKED_ACCOUNT);
  });

  it('reports ChatGPT Ads and Yelp as unavailable adapters, not zero spend', async () => {
    const evidence = await collectPaidPlatformEvidence({
      clientId: 10,
      channels: ['ChatGPT Ads', 'Yelp'],
      resolveAccounts: () => [],
    });
    assert.equal(evidence.length, 2);
    for (const row of evidence) {
      assert.equal(row.availability, AVAILABILITY.UNAVAILABLE);
      assert.equal(row.reason, UNAVAILABLE_REASON.PLATFORM_ADAPTER_NOT_IMPLEMENTED);
      assert.equal(row.campaigns.length, 0);
      assert.notEqual(row.aggregates?.spend, 0);
    }
  });

  it('keeps observed API evidence primary over operator-supplied metrics', () => {
    const merged = mergePlatformEvidence([
      {
        channel: 'Google Search',
        metrics: { impressions: 5, clicks: 1, conversions: 99 },
        source: 'operator_manual',
      },
    ], [{
      platform: PLATFORM.GOOGLE_ADS,
      channel: 'Google Search',
      availability: AVAILABILITY.AVAILABLE,
      aggregates: { impressions: 1200, clicks: 48, platformConversions: 2 },
      provenance: { sourceKind: 'PLATFORM_API', readOnly: true },
    }]);

    assert.equal(merged.length, 1);
    assert.equal(merged[0].aggregates.impressions, 1200);
    assert.equal(merged[0].supplementalEvidence.length, 1);
    assert.equal(merged[0].supplementalEvidence[0].metrics.conversions, 99);
  });

  it('stripSecrets removes token-like fields recursively', () => {
    const cleaned = stripSecrets({
      account_id: '123',
      refresh_token: 'secret',
      nested: { access_token: 'secret2', spend: 10 },
    });
    assert.equal(cleaned.account_id, '123');
    assert.equal(cleaned.refresh_token, undefined);
    assert.equal(cleaned.nested.access_token, undefined);
    assert.equal(cleaned.nested.spend, 10);
  });
});

describe('SPEC-252 — Canonical Penny integration', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: 'Acquire recurring commercial cleaning customers in Manchester NH.',
      targetSegment: 'Law Firms',
    });
    process.env.GOOGLE_ADS_DEVELOPER_TOKEN = 'dev-token';
    process.env.GOOGLE_ADS_CLIENT_ID = 'client-id';
    process.env.GOOGLE_ADS_CLIENT_SECRET = 'client-secret';
  });

  async function throughPrioritization() {
    await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    await advanceDiscoveryAfterApproval({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      question: 'Approved discovery.',
      allowFixtureFallback: true,
    });
    await advancePrioritizationAfterApproval({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      question: 'Approved prioritization.',
    });
  }

  it('collects live platform evidence before Penny reasoning', async () => {
    await throughPrioritization();
    const evidence = await resolvePlatformEvidenceForPenny(
      engine.get(mission.id, '10'),
      {
        tenantId: '10',
        candidatePaidChannels: ['Google Search', 'ChatGPT Ads'],
        resolveAccounts: ({ clientId }) => accountsForClient(clientId),
        http: mockGoogleHttp(),
      }
    );

    assert.ok(evidence.some((row) => row.platform === PLATFORM.GOOGLE_ADS && row.availability === AVAILABILITY.AVAILABLE));
    assert.ok(evidence.some((row) => row.channel === 'ChatGPT Ads' && row.reason === UNAVAILABLE_REASON.PLATFORM_ADAPTER_NOT_IMPLEMENTED));
  });

  it('persists observed platform evidence on Penny contribution for client 10', async () => {
    await throughPrioritization();
    const approachRequest = createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      missionId: mission.id,
      operatorId: 'operator-test',
      intent: EXECUTION_INTENTS.DECIDE_ACQUISITION_APPROACH,
      payload: { approach: ACQUISITION_APPROACHES.PAID, question: 'Use paid.' },
    });
    await routeExecutionRequest(approachRequest, {
      engine,
      tenantId: '10',
      operatorId: 'operator-test',
      allowFixtureFallback: true,
    });

    const legacyPath = require.resolve('../../../pennyAgent');
    let legacyInvoked = false;
    require.cache[legacyPath] = {
      id: legacyPath,
      filename: legacyPath,
      loaded: true,
      exports: {
        run: async () => {
          legacyInvoked = true;
          throw new Error('legacy pennyAgent.js must not be invoked');
        },
      },
    };

    const assessRequest = createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      missionId: mission.id,
      operatorId: 'operator-test',
      intent: EXECUTION_INTENTS.ASSESS_PAID_ACQUISITION,
      payload: {
        question: 'Assess paid acquisition.',
        availableBudget: { amount: 600, currency: 'USD' },
        conversionReadiness: { ready: true, source: 'landing_page_readiness' },
        measurementReadiness: { ready: true, source: 'tracking_readiness' },
        candidatePaidChannels: ['Google Search', 'ChatGPT Ads', 'Yelp'],
      },
    });

    const routed = await routeExecutionRequest(assessRequest, {
      engine,
      tenantId: '10',
      operatorId: 'operator-test',
      allowFixtureFallback: true,
      resolveAccounts: ({ clientId }) => accountsForClient(clientId),
      http: mockGoogleHttp(),
    });

    assert.equal(legacyInvoked, false);
    const recommendation = routed.snapshot.paidAcquisitionRecommendation;
    assert.ok(recommendation.platformEvidence.length >= 3);
    const google = recommendation.platformEvidence.find((row) => row.platform === PLATFORM.GOOGLE_ADS);
    assert.equal(google.availability, AVAILABILITY.AVAILABLE);
    assert.equal(google.account.externalAccountId, '987-654-3210');
    assert.ok(recommendation.evidence.some((row) => /Live Google Search platform observations/i.test(row.label)));
    assert.ok(recommendation.platformMetricsAreEvidenceOnly);
  });

  it('feeds normalized platform evidence into pennyInput for canonical reasoning', async () => {
    await throughPrioritization();
    const current = engine.get(mission.id, '10');
    const platformEvidence = await resolvePlatformEvidenceForPenny(current, {
      tenantId: '10',
      resolveAccounts: ({ clientId }) => accountsForClient(clientId),
      http: mockGoogleHttp(),
    });
    const input = buildExecutionInput({
      mission: current,
      specialist: SPECIALISTS.PENNY,
      contributions: engine.inspect(mission.id, { tenantId: '10' }).contributions,
      platformEvidence,
      acquisitionApproach: { selectedApproach: ACQUISITION_APPROACHES.PAID },
    });

    assert.equal(input.specialistInput.platformEvidence[0].platform, PLATFORM.GOOGLE_ADS);
    assert.equal(input.specialistInput.platformEvidence[0].campaigns[0].platformConversions, 2);
    assert.doesNotMatch(JSON.stringify(input), /refresh-token|access_token/i);
  });

  it('runPennyForAmoMission does not invoke legacy pennyAgent.run', async () => {
    await throughPrioritization();
    engine.contribute(mission.id, {
      specialist: SPECIALISTS.MAX,
      kind: CONTRIBUTION_KINDS.ACQUISITION_APPROACH,
      payload: {
        acquisitionApproach: {
          selectedApproach: ACQUISITION_APPROACHES.PAID,
          rationale: 'Test paid path.',
        },
      },
    }, { tenantId: '10' });

    const legacyPath = require.resolve('../../../pennyAgent');
    require.cache[legacyPath] = {
      id: legacyPath,
      filename: legacyPath,
      loaded: true,
      exports: {
        run: async () => {
          throw new Error('legacy pennyAgent.js must not be invoked');
        },
      },
    };

    const result = await runPennyForAmoMission(engine.get(mission.id, '10'), {
      engine,
      tenantId: '10',
      contributions: engine.inspect(mission.id, { tenantId: '10' }).contributions,
      transactionId: 'txn-test',
      candidatePaidChannels: ['Google Search'],
      resolveAccounts: ({ clientId }) => accountsForClient(clientId),
      http: mockGoogleHttp(),
      acquisitionApproach: { selectedApproach: ACQUISITION_APPROACHES.PAID },
      availableBudget: { amount: 500, currency: 'USD' },
      conversionReadiness: { ready: true },
      measurementReadiness: { ready: true },
    });

    assert.equal(result.spec, 'SPEC-132');
    const platformEvidence = result.contributions.paidAcquisitionRecommendation?.platformEvidence
      || result.contributions.platformEvidence
      || [];
    assert.ok(platformEvidence.some((row) => row.platform === PLATFORM.GOOGLE_ADS));
  });
});
