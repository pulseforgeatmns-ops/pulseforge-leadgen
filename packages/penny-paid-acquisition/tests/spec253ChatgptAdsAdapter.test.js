'use strict';

/**
 * SPEC-253 — Canonical Penny ChatGPT Ads read adapter.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

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
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  collectPaidPlatformEvidence,
  resolveAdAccountsForClient,
  readChatGptAdsEvidence,
  assessChatGptAdsProductionReadiness,
  readGoogleAdsEvidence,
  AVAILABILITY,
  UNAVAILABLE_REASON,
  PRODUCTION_READINESS,
  PLATFORM,
} = require('../index');
const {
  READ_ONLY_OPERATIONS,
  FORBIDDEN_OPENAI_ADS_MUTATIONS,
  OPENAI_ADS_BASE_URL,
  DELIVERY_INSIGHT_FIELDS,
  deliveryInsightsTimeRange,
  resolveOpenAiAdsReadOperation,
  assertOpenAiAdsMutationRejected,
  isForbiddenMutation,
} = require('../adapters/chatgptAds');
const { readYelpAdsEvidence } = require('../adapters/stubPlatform');
const { runPennyForAmoMission } = require('../../max/workspace/PennyPaidAcquisitionExecutor');

const ANCHOR_API_KEY = 'sk-anchor-chatgpt-ads-key-TESTONLY';
const OTHER_API_KEY = 'sk-client1-chatgpt-ads-key-TESTONLY';

const ANCHOR_CAMPAIGN = {
  id: 'cmpn_anchor_1',
  name: 'Anchor ChatGPT Commercial',
  status: 'active',
  budget: { lifetime_spend_limit_micros: 250000000 },
};

const ANCHOR_INSIGHT = {
  campaign_id: 'cmpn_anchor_1',
  campaign_name: 'Anchor ChatGPT Commercial',
  campaign_status: 'active',
  impressions: 2400,
  clicks: 72,
  spend: 96.48,
  ctr: 0.03,
  cpc: 1.34,
  cpm: 40.2,
};

const ANCHOR_CONVERSION = {
  entity_id: 'cmpn_anchor_1',
  conversions: 4,
  click_through_conversions: 4,
  view_through_conversions: 1,
  order_created_attributed_sales: 12.5,
};

function chatgptAccountsForClient(clientId) {
  if (Number(clientId) === 10) {
    return [{
      id: 'acc-chatgpt-10',
      client_id: 10,
      platform: 'chatgpt_ads',
      account_id: 'adacct_anchor',
      access_token: ANCHOR_API_KEY,
      is_active: true,
      company_name: 'Anchor Cleaning',
    }];
  }
  if (Number(clientId) === 1) {
    return [{
      id: 'acc-chatgpt-1',
      client_id: 1,
      platform: 'chatgpt_ads',
      account_id: 'adacct_pulseforge',
      access_token: OTHER_API_KEY,
      is_active: true,
      company_name: 'Pulseforge',
    }];
  }
  return [];
}

function mockChatGptHttp(overrides = {}) {
  const calls = [];
  return {
    calls,
    get: async (url, config) => {
      calls.push({ method: 'GET', url, headers: config?.headers || {}, params: config?.params || {} });
      if (overrides.get) return overrides.get(url, config, calls);
      if (url.endsWith('/ad_account')) {
        return {
          data: overrides.adAccount || {
            id: 'adacct_anchor',
            name: 'Anchor Cleaning',
            timezone: 'America/New_York',
            currency_code: 'USD',
            status: 'active',
          },
        };
      }
      if (url.endsWith('/campaigns') && !url.includes('/insights')) {
        return {
          data: {
            object: 'list',
            data: overrides.campaigns || [ANCHOR_CAMPAIGN],
            has_more: false,
          },
        };
      }
      if (url.endsWith('/ad_account/insights') || /\/campaigns\/[^/]+\/insights$/.test(url)) {
        return { data: { object: 'list', data: overrides.insights || [ANCHOR_INSIGHT] } };
      }
      throw new Error(`unexpected GET ${url}`);
    },
    post: async (url, body, config) => {
      calls.push({
        method: 'POST',
        url,
        body,
        headers: config?.headers || {},
      });
      if (overrides.post) return overrides.post(url, body, config, calls);
      if (url.endsWith('/conversions/insights')) {
        return { data: { object: 'list', data: overrides.conversions || [ANCHOR_CONVERSION] } };
      }
      throw new Error(`unexpected POST ${url}`);
    },
  };
}

const GOOGLE_FIXTURE = {
  results: [{
    campaign: { id: '1001', name: 'Anchor Commercial Search', status: 'ENABLED' },
    campaignBudget: { amountMicros: '25000000' },
    metrics: {
      impressions: '1200',
      clicks: '48',
      ctr: 0.04,
      averageCpc: '2470000',
      conversions: 2,
      costMicros: '118560000',
    },
  }],
};

function mockGoogleHttp() {
  let call = 0;
  return {
    post: async (url) => {
      if (url.includes('oauth2.googleapis.com/token')) {
        return { data: { access_token: 'test-access-token' } };
      }
      call += 1;
      if (call === 1) return { data: GOOGLE_FIXTURE };
      return { data: { results: [] } };
    },
  };
}

describe('SPEC-253 — ChatGPT Ads tenant binding and readiness', () => {
  it('resolves a valid tenant-linked ChatGPT Ads account for client 10', async () => {
    const rows = await resolveAdAccountsForClient({
      clientId: 10,
      queryAccounts: ({ clientId }) => chatgptAccountsForClient(clientId),
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].client_id, 10);
    assert.equal(rows[0].account_id, 'adacct_anchor');
    assert.equal(rows[0].platform, 'chatgpt_ads');
  });

  it('does not give client 10 another client\'s ChatGPT Ads account or key', async () => {
    const resolveAccounts = ({ clientId }) => chatgptAccountsForClient(clientId);
    const client10 = await collectPaidPlatformEvidence({
      clientId: 10,
      channels: ['ChatGPT Ads'],
      resolveAccounts,
      http: mockChatGptHttp(),
    });
    const client1 = await collectPaidPlatformEvidence({
      clientId: 1,
      channels: ['ChatGPT Ads'],
      resolveAccounts,
      http: mockChatGptHttp({
        adAccount: { id: 'adacct_pulseforge', currency_code: 'USD', timezone: 'UTC' },
        campaigns: [],
        insights: [],
        conversions: [],
      }),
    });

    assert.equal(client10[0].account.externalAccountId, 'adacct_anchor');
    assert.equal(client1[0].account.externalAccountId, 'adacct_pulseforge');
    assert.doesNotMatch(JSON.stringify(client10), new RegExp(OTHER_API_KEY));
    assert.doesNotMatch(JSON.stringify(client1), new RegExp(ANCHOR_API_KEY));
  });

  it('has no client 1 fallback when the requested tenant has no ChatGPT Ads account', async () => {
    const evidence = await collectPaidPlatformEvidence({
      clientId: 99,
      channels: ['ChatGPT Ads'],
      resolveAccounts: ({ clientId }) => chatgptAccountsForClient(clientId),
    });
    assert.equal(evidence[0].availability, AVAILABILITY.UNAVAILABLE);
    assert.equal(evidence[0].reason, UNAVAILABLE_REASON.CHATGPT_ADS_ACCOUNT_NOT_LINKED);
    assert.equal(evidence[0].productionReadiness, PRODUCTION_READINESS.BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL);
    assert.equal(evidence[0].account, null);
  });

  it('fails closed when the ChatGPT Ads account is missing', async () => {
    const evidence = await readChatGptAdsEvidence({});
    assert.equal(evidence.availability, AVAILABILITY.UNAVAILABLE);
    assert.equal(evidence.reason, UNAVAILABLE_REASON.CHATGPT_ADS_ACCOUNT_NOT_LINKED);
    assert.equal(evidence.productionReadiness, PRODUCTION_READINESS.BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL);
  });

  it('fails closed when the Ads Manager key is missing', async () => {
    const evidence = await collectPaidPlatformEvidence({
      clientId: 10,
      channels: ['ChatGPT Ads'],
      resolveAccounts: () => [{
        id: 'acc-chatgpt-10',
        client_id: 10,
        platform: 'chatgpt_ads',
        account_id: 'adacct_anchor',
        access_token: null,
        is_active: true,
      }],
    });
    assert.equal(evidence[0].availability, AVAILABILITY.UNAVAILABLE);
    assert.equal(evidence[0].reason, UNAVAILABLE_REASON.MISSING_CREDENTIALS);
    assert.equal(evidence[0].productionReadiness, PRODUCTION_READINESS.BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL);
  });

  it('reports production readiness READY only when a tenant-linked credential exists', async () => {
    const blocked = await assessChatGptAdsProductionReadiness({
      clientId: 10,
      resolveAccounts: () => [],
    });
    const ready = await assessChatGptAdsProductionReadiness({
      clientId: 10,
      resolveAccounts: ({ clientId }) => chatgptAccountsForClient(clientId),
    });
    assert.equal(blocked.status, PRODUCTION_READINESS.BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL);
    assert.equal(blocked.reason, UNAVAILABLE_REASON.CHATGPT_ADS_ACCOUNT_NOT_LINKED);
    assert.ok(Array.isArray(blocked.operatorSetup));
    assert.equal(ready.status, PRODUCTION_READINESS.READY);
    assert.equal(ready.linkedAccountId, 'adacct_anchor');
    assert.doesNotMatch(JSON.stringify(ready), new RegExp(ANCHOR_API_KEY));
  });
});

describe('SPEC-253 — account insights request shape', () => {
  it('uses singular time_range instead of time_ranges[] on GET /ad_account/insights', async () => {
    const http = mockChatGptHttp();
    await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http,
    });
    const insightsCall = http.calls.find((call) => call.url.endsWith('/ad_account/insights'));
    assert.ok(insightsCall);
    assert.equal(insightsCall.method, 'GET');
    assert.ok(insightsCall.params.time_range);
    assert.equal(insightsCall.params['time_ranges[]'], undefined);
    assert.equal(insightsCall.params.time_ranges, undefined);
  });

  it('requests a supported relative_interval time_range for the observation window', async () => {
    const http = mockChatGptHttp();
    await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      window: { start: '2026-09-01', end: '2026-09-08', days: 7, label: 'LAST_7_DAYS' },
      http,
    });
    const insightsCall = http.calls.find((call) => call.url.endsWith('/ad_account/insights'));
    assert.deepEqual(insightsCall.params.time_range, deliveryInsightsTimeRange({ days: 7 }));
    assert.deepEqual(insightsCall.params.time_range, {
      type: 'relative_interval',
      unit: 'day',
      start_ago: 7,
      end_ago: 0,
    });
  });

  it('requests delivery-only fields and excludes conversion-only metrics', async () => {
    const http = mockChatGptHttp();
    await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http,
    });
    const insightsCall = http.calls.find((call) => call.url.endsWith('/ad_account/insights'));
    assert.deepEqual(insightsCall.params['fields[]'], DELIVERY_INSIGHT_FIELDS);
    assert.doesNotMatch(JSON.stringify(insightsCall.params['fields[]']), /conversions/);
    assert.doesNotMatch(JSON.stringify(insightsCall.params['fields[]']), /order_created_attributed_sales/);
  });

  it('keeps conversion insights on POST /conversions/insights', async () => {
    const http = mockChatGptHttp();
    await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http,
    });
    const conversionCall = http.calls.find(
      (call) => call.method === 'POST' && call.url.endsWith('/conversions/insights')
    );
    assert.ok(conversionCall);
    assert.deepEqual(conversionCall.body.entity_ids, ['cmpn_anchor_1']);
  });

  it('normalizes delivery metrics from account insights without delivery conversion fields', async () => {
    const http = mockChatGptHttp({
      insights: [{
        campaign_id: 'cmpn_anchor_1',
        campaign_name: 'Anchor ChatGPT Commercial',
        campaign_status: 'active',
        impressions: 1000,
        clicks: 25,
        spend: 40,
        ctr: 0.025,
        cpc: 1.6,
        cpm: 40,
      }],
      conversions: [],
    });
    const evidence = await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http,
    });
    const campaign = evidence.campaigns[0];
    assert.equal(campaign.impressions, 1000);
    assert.equal(campaign.clicks, 25);
    assert.equal(campaign.spend, 40);
    assert.equal(campaign.averageCpc, 1.6);
    assert.equal(campaign.platformConversions, null);
    assert.equal(campaign.clickThroughConversions, null);
    assert.equal(campaign.viewThroughConversions, null);
  });
});

describe('SPEC-253 — ChatGPT Ads identity, metrics, and provenance', () => {
  it('verifies GET /ad_account identity before accepting performance data', async () => {
    const http = mockChatGptHttp();
    const evidence = await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http,
    });
    assert.equal(evidence.availability, AVAILABILITY.AVAILABLE);
    assert.equal(evidence.account.externalAccountId, 'adacct_anchor');
    assert.equal(evidence.account.currency, 'USD');
    assert.equal(evidence.account.timezone, 'America/New_York');
    assert.equal(http.calls[0].url, `${OPENAI_ADS_BASE_URL}/ad_account`);
    assert.equal(http.calls[0].method, 'GET');
  });

  it('fails closed when GET /ad_account identity does not match the linked account', async () => {
    const http = mockChatGptHttp({
      adAccount: { id: 'adacct_other', currency_code: 'USD', timezone: 'UTC' },
    });
    const evidence = await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http,
    });
    assert.equal(evidence.availability, AVAILABILITY.UNAVAILABLE);
    assert.equal(evidence.reason, UNAVAILABLE_REASON.ACCOUNT_IDENTITY_MISMATCH);
    assert.equal(http.calls.length, 1);
    assert.equal(http.calls[0].url, `${OPENAI_ADS_BASE_URL}/ad_account`);
  });

  it('normalizes campaign, spend, impressions, clicks, CTR, and CPC', async () => {
    const evidence = await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http: mockChatGptHttp(),
    });
    const campaign = evidence.campaigns[0];
    assert.equal(campaign.externalCampaignId, 'cmpn_anchor_1');
    assert.equal(campaign.name, 'Anchor ChatGPT Commercial');
    assert.equal(campaign.status, 'active');
    assert.equal(campaign.spend, 96.48);
    assert.equal(campaign.impressions, 2400);
    assert.equal(campaign.clicks, 72);
    assert.equal(campaign.ctr, 0.03);
    assert.equal(campaign.averageCpc, 1.34);
    assert.equal(campaign.cpm, 40.2);
    assert.equal(campaign.budget.amount, 250);
    assert.equal(campaign.budget.period, 'LIFETIME');
  });

  it('keeps OpenAI conversions as platformConversions and does not invent business outcomes', async () => {
    const evidence = await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http: mockChatGptHttp(),
    });
    const campaign = evidence.campaigns[0];
    assert.equal(campaign.platformConversions, 4);
    assert.equal(campaign.clickThroughConversions, 4);
    assert.equal(campaign.viewThroughConversions, 1);
    assert.equal(campaign.platformConversionValue, 12.5);
    const blob = JSON.stringify(evidence);
    assert.doesNotMatch(blob, /qualifiedLead|walkthrough|proposal|recurringClient|"revenue"/);
    assert.doesNotMatch(blob, /lead_created/);
  });

  it('sets PLATFORM_API provenance and readOnly=true', async () => {
    const evidence = await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http: mockChatGptHttp(),
    });
    assert.equal(evidence.provenance.sourceKind, 'PLATFORM_API');
    assert.equal(evidence.provenance.source, PLATFORM.CHATGPT_ADS);
    assert.equal(evidence.provenance.readOnly, true);
    assert.ok(evidence.provenance.observedAt);
    assert.equal(evidence.platformMetricsAreEvidenceOnly, true);
    assert.equal(evidence.platform, PLATFORM.CHATGPT_ADS);
  });

  it('omits the API key from normalized evidence', async () => {
    const evidence = await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http: mockChatGptHttp(),
    });
    const blob = JSON.stringify(evidence);
    assert.doesNotMatch(blob, new RegExp(ANCHOR_API_KEY));
    assert.doesNotMatch(blob, /access_token|api[_-]?key|Authorization/i);
  });

  it('omits the API key from error output when the vendor echoes it', async () => {
    const http = mockChatGptHttp({
      get: async () => {
        const err = new Error(`boom ${ANCHOR_API_KEY}`);
        err.response = { data: { error: { message: `Invalid API key ${ANCHOR_API_KEY}` } } };
        throw err;
      },
    });
    const evidence = await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http,
    });
    assert.equal(evidence.availability, AVAILABILITY.ERROR);
    assert.doesNotMatch(evidence.error || '', new RegExp(ANCHOR_API_KEY));
    assert.doesNotMatch(JSON.stringify(evidence), new RegExp(ANCHOR_API_KEY));
  });
});

describe('SPEC-253 — read-only OpenAI Ads security boundary', () => {
  it('rejects unsupported and mutating methods at the adapter boundary', () => {
    assert.throws(
      () => resolveOpenAiAdsReadOperation({ method: 'POST', path: '/campaigns' }),
      (err) => err.code === 'OPENAI_ADS_ARBITRARY_REQUEST_REJECTED'
    );
    assert.throws(
      () => resolveOpenAiAdsReadOperation('POST_CAMPAIGNS'),
      (err) => err.code === 'OPENAI_ADS_OPERATION_NOT_PERMITTED'
    );
    assert.throws(
      () => resolveOpenAiAdsReadOperation('GET', { path: '/ad_account' }),
      (err) => err.code === 'OPENAI_ADS_OPERATION_NOT_PERMITTED'
    );
    assert.throws(
      () => assertOpenAiAdsMutationRejected({
        method: 'POST',
        path: '/campaigns',
        prohibition: 'campaign_creation',
      }),
      (err) => err.code === 'OPENAI_ADS_MUTATION_REJECTED'
    );
  });

  it('does not expose a reachable create/update/delete Ads API path', () => {
    const allowlisted = new Set(
      Object.values(READ_ONLY_OPERATIONS).map((row) => `${row.method} ${row.path}`)
    );
    for (const row of FORBIDDEN_OPENAI_ADS_MUTATIONS) {
      const concretePath = row.path.replace('{id}', 'cmpn_anchor_1');
      assert.equal(isForbiddenMutation(row.method, concretePath), true);
      assert.equal(allowlisted.has(`${row.method} ${row.path}`), false);
    }

    const source = fs.readFileSync(
      path.join(__dirname, '../adapters/chatgptAds.js'),
      'utf8'
    );
    assert.match(source, /READ_ONLY_OPERATIONS/);
    assert.doesNotMatch(source, /operation:\s*'POST_CAMPAIGNS'/);
    assert.doesNotMatch(source, /executeReadOnlyOperation\(\{[\s\S]*path:\s*['"]\/campaigns['"]/);
  });

  it('issues only allowlisted OpenAI Ads calls on a successful read', async () => {
    const http = mockChatGptHttp();
    await readChatGptAdsEvidence({
      account: chatgptAccountsForClient(10)[0],
      http,
    });
    const allowed = new Set([
      `${OPENAI_ADS_BASE_URL}/ad_account`,
      `${OPENAI_ADS_BASE_URL}/campaigns`,
      `${OPENAI_ADS_BASE_URL}/ad_account/insights`,
      `${OPENAI_ADS_BASE_URL}/conversions/insights`,
    ]);
    assert.ok(http.calls.length >= 3);
    for (const call of http.calls) {
      assert.ok(allowed.has(call.url), `unexpected OpenAI Ads call ${call.method} ${call.url}`);
      if (call.method === 'POST') {
        assert.equal(call.url, `${OPENAI_ADS_BASE_URL}/conversions/insights`);
      }
    }
  });
});

describe('SPEC-253 — collector wiring and canonical Penny', () => {
  it('routes chatgpt_ads through the live adapter instead of stubPlatform', async () => {
    const evidence = await collectPaidPlatformEvidence({
      clientId: 10,
      channels: ['ChatGPT Ads'],
      resolveAccounts: ({ clientId }) => chatgptAccountsForClient(clientId),
      http: mockChatGptHttp(),
    });
    assert.equal(evidence[0].availability, AVAILABILITY.AVAILABLE);
    assert.equal(evidence[0].reason, undefined);
    assert.notEqual(evidence[0].reason, UNAVAILABLE_REASON.PLATFORM_ADAPTER_NOT_IMPLEMENTED);
    assert.equal(evidence[0].spec, 'SPEC-253');
  });

  it('keeps Yelp as an explicit UNAVAILABLE stub', async () => {
    const stub = readYelpAdsEvidence();
    const collected = await collectPaidPlatformEvidence({
      clientId: 10,
      channels: ['Yelp'],
      resolveAccounts: ({ clientId }) => chatgptAccountsForClient(clientId),
    });
    assert.equal(stub.reason, UNAVAILABLE_REASON.PLATFORM_ADAPTER_NOT_IMPLEMENTED);
    assert.equal(collected[0].platform, PLATFORM.YELP);
    assert.equal(collected[0].availability, AVAILABILITY.UNAVAILABLE);
    assert.equal(collected[0].reason, UNAVAILABLE_REASON.PLATFORM_ADAPTER_NOT_IMPLEMENTED);
  });

  it('does not change Google Ads adapter behavior', async () => {
    process.env.GOOGLE_ADS_DEVELOPER_TOKEN = 'dev-token';
    process.env.GOOGLE_ADS_CLIENT_ID = 'client-id';
    process.env.GOOGLE_ADS_CLIENT_SECRET = 'client-secret';
    const evidence = await readGoogleAdsEvidence({
      account: { id: 'acc-1', account_id: '123-456-7890', refresh_token: 'refresh-token' },
      http: mockGoogleHttp(),
    });
    assert.equal(evidence.availability, AVAILABILITY.AVAILABLE);
    assert.equal(evidence.platform, PLATFORM.GOOGLE_ADS);
    assert.equal(evidence.campaigns[0].spend, 118.56);
  });

  it('feeds ChatGPT Ads evidence into canonical Penny', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire recurring commercial cleaning customers in Manchester NH.',
      targetSegment: 'Law Firms',
    });
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

    const result = await runPennyForAmoMission(engine.get(mission.id, '10'), {
      engine,
      tenantId: '10',
      contributions: engine.inspect(mission.id, { tenantId: '10' }).contributions,
      transactionId: 'txn-chatgpt-ads',
      candidatePaidChannels: ['ChatGPT Ads'],
      resolveAccounts: ({ clientId }) => chatgptAccountsForClient(clientId),
      http: mockChatGptHttp(),
      acquisitionApproach: { selectedApproach: ACQUISITION_APPROACHES.PAID },
      availableBudget: { amount: 500, currency: 'USD' },
      conversionReadiness: { ready: true },
      measurementReadiness: { ready: true },
    });

    const platformEvidence = result.contributions.paidAcquisitionRecommendation?.platformEvidence
      || result.contributions.platformEvidence
      || [];
    const chatgpt = platformEvidence.find((row) => row.platform === PLATFORM.CHATGPT_ADS);
    assert.ok(chatgpt);
    assert.equal(chatgpt.availability, AVAILABILITY.AVAILABLE);
    assert.equal(chatgpt.campaigns[0].platformConversions, 4);
    assert.equal(chatgpt.provenance.sourceKind, 'PLATFORM_API');
    assert.doesNotMatch(JSON.stringify(result), new RegExp(ANCHOR_API_KEY));
  });

  it('persists ChatGPT Ads evidence on an ASSESS_PAID_ACQUISITION contribution', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire recurring commercial cleaning customers in Manchester NH.',
      targetSegment: 'Law Firms',
    });
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
    await routeExecutionRequest(createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      missionId: mission.id,
      operatorId: 'operator-test',
      intent: EXECUTION_INTENTS.DECIDE_ACQUISITION_APPROACH,
      payload: { approach: ACQUISITION_APPROACHES.PAID, question: 'Use paid.' },
    }), {
      engine,
      tenantId: '10',
      operatorId: 'operator-test',
      allowFixtureFallback: true,
    });

    const routed = await routeExecutionRequest(createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      missionId: mission.id,
      operatorId: 'operator-test',
      intent: EXECUTION_INTENTS.ASSESS_PAID_ACQUISITION,
      payload: {
        question: 'Assess ChatGPT Ads.',
        availableBudget: { amount: 600, currency: 'USD' },
        conversionReadiness: { ready: true, source: 'landing_page_readiness' },
        measurementReadiness: { ready: true, source: 'tracking_readiness' },
        candidatePaidChannels: ['ChatGPT Ads', 'Yelp'],
      },
    }), {
      engine,
      tenantId: '10',
      operatorId: 'operator-test',
      allowFixtureFallback: true,
      resolveAccounts: ({ clientId }) => chatgptAccountsForClient(clientId),
      http: mockChatGptHttp(),
    });

    const recommendation = routed.snapshot.paidAcquisitionRecommendation;
    const chatgpt = recommendation.platformEvidence.find((row) => row.platform === PLATFORM.CHATGPT_ADS);
    const yelp = recommendation.platformEvidence.find((row) => row.platform === PLATFORM.YELP);
    assert.equal(chatgpt.availability, AVAILABILITY.AVAILABLE);
    assert.equal(chatgpt.aggregates.clicks, 72);
    assert.equal(yelp.reason, UNAVAILABLE_REASON.PLATFORM_ADAPTER_NOT_IMPLEMENTED);
    assert.ok(recommendation.evidence.some((row) => /Live ChatGPT Ads platform observations/i.test(row.label)));
  });
});
