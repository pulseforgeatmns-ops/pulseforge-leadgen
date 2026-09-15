'use strict';

/**
 * SPEC-253 — Read-only OpenAI / ChatGPT Ads Advertiser API adapter.
 *
 * OpenAI Ads Manager keys are not platform-scoped read-only.
 * PulseForge enforces the read-only boundary internally via an explicit
 * operation allowlist. Callers cannot pass arbitrary paths or methods.
 */

const axios = require('axios');
const {
  AVAILABILITY,
  UNAVAILABLE_REASON,
  PRODUCTION_READINESS,
  PLATFORM,
  CHANNEL_BY_PLATFORM,
  platformProvenance,
  unavailableEvidence,
} = require('../types');
const { resolveAdAccountsForClient, accountUnavailableReason } = require('../accountResolution');

const SPEC = 'SPEC-253';
const OPENAI_ADS_BASE_URL = 'https://api.ads.openai.com/v1';
const DEFAULT_WINDOW_DAYS = 7;
const RESOURCE_ID_RE = /^[A-Za-z0-9_-]+$/;

const READ_ONLY_OPERATIONS = Object.freeze({
  GET_AD_ACCOUNT: Object.freeze({ method: 'GET', path: '/ad_account' }),
  GET_CAMPAIGNS: Object.freeze({ method: 'GET', path: '/campaigns' }),
  GET_AD_GROUPS: Object.freeze({ method: 'GET', path: '/ad_groups' }),
  GET_ADS: Object.freeze({ method: 'GET', path: '/ads' }),
  GET_AD_ACCOUNT_INSIGHTS: Object.freeze({ method: 'GET', path: '/ad_account/insights' }),
  GET_CAMPAIGN_INSIGHTS: Object.freeze({ method: 'GET', path: '/campaigns/{id}/insights' }),
  GET_AD_GROUP_INSIGHTS: Object.freeze({ method: 'GET', path: '/ad_groups/{id}/insights' }),
  GET_AD_INSIGHTS: Object.freeze({ method: 'GET', path: '/ads/{id}/insights' }),
  POST_CONVERSION_INSIGHTS: Object.freeze({ method: 'POST', path: '/conversions/insights' }),
});

const FORBIDDEN_OPENAI_ADS_MUTATIONS = Object.freeze([
  Object.freeze({ method: 'POST', path: '/campaigns', prohibition: 'campaign_creation' }),
  Object.freeze({ method: 'POST', path: '/campaigns/{id}', prohibition: 'campaign_update' }),
  Object.freeze({ method: 'POST', path: '/campaigns/{id}/activate', prohibition: 'campaign_resume' }),
  Object.freeze({ method: 'POST', path: '/campaigns/{id}/pause', prohibition: 'campaign_pause' }),
  Object.freeze({ method: 'POST', path: '/campaigns/{id}/archive', prohibition: 'campaign_archive' }),
  Object.freeze({ method: 'POST', path: '/ad_groups', prohibition: 'ad_group_mutation' }),
  Object.freeze({ method: 'POST', path: '/ad_groups/{id}', prohibition: 'ad_group_mutation' }),
  Object.freeze({ method: 'POST', path: '/ad_groups/{id}/activate', prohibition: 'ad_group_mutation' }),
  Object.freeze({ method: 'POST', path: '/ad_groups/{id}/pause', prohibition: 'ad_group_mutation' }),
  Object.freeze({ method: 'POST', path: '/ads', prohibition: 'ad_mutation' }),
  Object.freeze({ method: 'POST', path: '/ads/{id}', prohibition: 'ad_mutation' }),
  Object.freeze({ method: 'POST', path: '/ads/{id}/activate', prohibition: 'ad_mutation' }),
  Object.freeze({ method: 'POST', path: '/ads/{id}/pause', prohibition: 'ad_mutation' }),
  Object.freeze({ method: 'POST', path: '/upload', prohibition: 'upload_mutation' }),
  Object.freeze({ method: 'POST', path: '/custom_audiences', prohibition: 'audience_mutation' }),
  Object.freeze({ method: 'POST', path: '/ad_account/brand', prohibition: 'account_mutation' }),
  Object.freeze({ method: 'POST', path: '/ad_account/spend_limit_windows', prohibition: 'billing_mutation' }),
  Object.freeze({ method: 'POST', path: '/ad_account/daily_spend_limit', prohibition: 'budget_change' }),
  Object.freeze({ method: 'POST', path: '/bulk', prohibition: 'bulk_mutation' }),
  Object.freeze({ method: 'DELETE', path: '*', prohibition: 'delete' }),
  Object.freeze({ method: 'PUT', path: '*', prohibition: 'update' }),
  Object.freeze({ method: 'PATCH', path: '*', prohibition: 'update' }),
]);

const CHATGPT_ADS_OPERATOR_SETUP = Object.freeze([
  'Open Anchor\'s OpenAI Ads Manager account.',
  'Confirm operator has Admin access.',
  'Open Settings → API Keys.',
  'Create an Advertiser API key.',
  'Store the key in ad_accounts.access_token for platform=chatgpt_ads (Railway/server-side only).',
  'Link the account to the canonical client (client_id=10 for Anchor) with the OpenAI ad account id in ad_accounts.account_id.',
  'Smoke-test GET /ad_account through the read-only adapter.',
  'Confirm the returned account identity matches the linked Anchor account.',
]);

const INSIGHT_FIELDS = Object.freeze([
  'campaign.id',
  'campaign.name',
  'campaign.status',
  'impressions',
  'clicks',
  'spend',
  'ctr',
  'cpc',
  'cpm',
  'conversions',
  'order_created_attributed_sales',
]);

function observationWindowFromDays(days = DEFAULT_WINDOW_DAYS) {
  const end = new Date();
  const start = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  return {
    start: start.toISOString().slice(0, 10),
    end: end.toISOString().slice(0, 10),
    days,
    label: `LAST_${days}_DAYS`,
  };
}

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function redactSecret(text, secret) {
  const raw = text == null ? '' : String(text);
  if (!secret) return raw;
  return raw.split(String(secret)).join('[REDACTED]');
}

function boundaryError(code, extra = {}) {
  const err = new Error(code);
  err.code = code;
  Object.assign(err, extra);
  return err;
}

/**
 * Resolve an allowlisted read operation. Rejects arbitrary method/path objects
 * from Penny/runtime callers.
 *
 * @param {string} operationKey
 * @param {{ id?: string }} [pathParams]
 * @returns {{ method: string, path: string, operation: string }}
 */
function resolveOpenAiAdsReadOperation(operationKey, pathParams = {}) {
  if (operationKey && typeof operationKey === 'object') {
    throw boundaryError('OPENAI_ADS_ARBITRARY_REQUEST_REJECTED', {
      attemptedMethod: operationKey.method || null,
      attemptedPath: operationKey.path || null,
    });
  }

  const op = READ_ONLY_OPERATIONS[operationKey];
  if (!op) {
    throw boundaryError('OPENAI_ADS_OPERATION_NOT_PERMITTED', {
      operation: operationKey || null,
    });
  }

  let path = op.path;
  if (path.includes('{id}')) {
    const id = asText(pathParams.id);
    if (!id || !RESOURCE_ID_RE.test(id)) {
      throw boundaryError('OPENAI_ADS_INVALID_RESOURCE_ID');
    }
    path = path.replace('{id}', id);
  }

  return { method: op.method, path, operation: operationKey };
}

function assertOpenAiAdsMutationRejected(attempt = {}) {
  throw boundaryError('OPENAI_ADS_MUTATION_REJECTED', {
    method: attempt.method || null,
    path: attempt.path || null,
    prohibition: attempt.prohibition || 'unsupported_mutation',
  });
}

function isForbiddenMutation(method, path) {
  const normalizedMethod = asText(method).toUpperCase();
  const normalizedPath = asText(path);
  if (!normalizedMethod || ['PUT', 'PATCH', 'DELETE'].includes(normalizedMethod)) {
    return true;
  }
  return FORBIDDEN_OPENAI_ADS_MUTATIONS.some((row) => {
    if (row.method !== normalizedMethod) return false;
    if (row.path === '*') return true;
    const pattern = `^${row.path.replace(/\{id\}/g, '[A-Za-z0-9_-]+')}$`;
    return new RegExp(pattern).test(normalizedPath);
  });
}

function serializeParams(params) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params || {})) {
    if (value == null) continue;
    if (Array.isArray(value)) {
      for (const item of value) search.append(key, String(item));
    } else {
      search.append(key, typeof value === 'object' ? JSON.stringify(value) : String(value));
    }
  }
  return search.toString();
}

function sanitizeThrownError(err, apiKey) {
  const raw = err.response?.data?.error?.message
    || err.response?.data?.message
    || err.message
    || 'OpenAI Ads API error';
  const safe = new Error(redactSecret(raw, apiKey));
  safe.code = err.code || 'OPENAI_ADS_API_ERROR';
  return safe;
}

async function executeReadOnlyOperation({
  operation,
  pathParams,
  query,
  body,
  apiKey,
  http,
}) {
  if (!asText(apiKey)) {
    throw boundaryError('MISSING_CREDENTIALS');
  }
  const resolved = resolveOpenAiAdsReadOperation(operation, pathParams);
  if (isForbiddenMutation(resolved.method, resolved.path)) {
    assertOpenAiAdsMutationRejected(resolved);
  }

  const headers = {
    Authorization: `Bearer ${apiKey}`,
    Accept: 'application/json',
  };
  if (resolved.method === 'POST') {
    headers['Content-Type'] = 'application/json';
  }

  const url = `${OPENAI_ADS_BASE_URL}${resolved.path}`;
  try {
    if (resolved.method === 'GET') {
      return await http.get(url, {
        headers,
        params: query,
        paramsSerializer: serializeParams,
      });
    }
    return await http.post(url, body || {}, { headers });
  } catch (err) {
    throw sanitizeThrownError(err, apiKey);
  }
}

function microsToCurrency(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.round((n / 1_000_000) * 100) / 100;
}

function asNumber(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function identitiesMatch(expected, actual) {
  const left = asText(expected);
  const right = asText(actual);
  return Boolean(left) && Boolean(right) && left === right;
}

function remoteAccountId(remote) {
  return asText(remote?.id || remote?.account_id || remote?.ad_account_id);
}

function budgetFromCampaign(campaign, currency) {
  const budget = campaign?.budget || {};
  if (budget.lifetime_spend_limit_micros != null) {
    const amount = microsToCurrency(budget.lifetime_spend_limit_micros);
    return amount != null ? { amount, period: 'LIFETIME', currency: currency || 'USD' } : null;
  }
  if (budget.daily_spend_limit_micros != null) {
    const amount = microsToCurrency(budget.daily_spend_limit_micros);
    return amount != null ? { amount, period: 'DAILY', currency: currency || 'USD' } : null;
  }
  return null;
}

function isRelevantCampaign(campaign) {
  const status = asText(campaign?.status).toLowerCase();
  return status === 'active' || status === 'enabled' || status === 'paused';
}

function aggregateInsightRows(rows) {
  const byCampaign = new Map();
  for (const row of rows || []) {
    const id = asText(row.campaign_id || row['campaign.id'] || row.entity_id);
    if (!id) continue;
    const current = byCampaign.get(id) || {
      externalCampaignId: id,
      name: row.campaign_name || row['campaign.name'] || null,
      status: row.campaign_status || row['campaign.status'] || null,
      spend: 0,
      impressions: 0,
      clicks: 0,
      platformConversions: 0,
      platformConversionValue: 0,
      cpcSum: 0,
      cpcWeight: 0,
      cpmSum: 0,
      cpmWeight: 0,
      seen: false,
    };
    current.seen = true;
    current.name = current.name || row.campaign_name || row['campaign.name'] || null;
    current.status = current.status || row.campaign_status || row['campaign.status'] || null;
    current.spend += asNumber(row.spend) || 0;
    current.impressions += asNumber(row.impressions) || 0;
    current.clicks += asNumber(row.clicks) || 0;
    current.platformConversions += asNumber(row.conversions) || 0;
    current.platformConversionValue += asNumber(row.order_created_attributed_sales) || 0;
    const cpc = asNumber(row.cpc);
    const clicks = asNumber(row.clicks) || 0;
    if (cpc != null && clicks > 0) {
      current.cpcSum += cpc * clicks;
      current.cpcWeight += clicks;
    }
    const cpm = asNumber(row.cpm);
    const impressions = asNumber(row.impressions) || 0;
    if (cpm != null && impressions > 0) {
      current.cpmSum += cpm * impressions;
      current.cpmWeight += impressions;
    }
    byCampaign.set(id, current);
  }
  return byCampaign;
}

function normalizeCampaignEvidence(campaign, insight, conversion, currency) {
  const impressions = insight?.impressions ?? 0;
  const clicks = insight?.clicks ?? 0;
  const spend = insight?.spend ?? 0;
  const ctr = impressions > 0 ? clicks / impressions : null;
  const averageCpc = insight?.cpcWeight
    ? insight.cpcSum / insight.cpcWeight
    : (clicks > 0 ? spend / clicks : null);
  const cpm = insight?.cpmWeight
    ? insight.cpmSum / insight.cpmWeight
    : (impressions > 0 ? (spend / impressions) * 1000 : null);
  const platformConversions = conversion?.conversions != null
    ? asNumber(conversion.conversions)
    : (insight?.platformConversions ?? null);
  const clickThroughConversions = conversion?.click_through_conversions != null
    ? asNumber(conversion.click_through_conversions)
    : platformConversions;
  const viewThroughConversions = conversion?.view_through_conversions != null
    ? asNumber(conversion.view_through_conversions)
    : null;

  return {
    externalCampaignId: asText(campaign.id || insight?.externalCampaignId) || null,
    name: campaign.name || insight?.name || 'Unknown',
    status: campaign.status || insight?.status || 'UNKNOWN',
    spend,
    impressions,
    clicks,
    ctr,
    averageCpc,
    cpm,
    platformConversions,
    platformConversionValue: insight?.platformConversionValue || null,
    clickThroughConversions,
    viewThroughConversions,
    budget: budgetFromCampaign(campaign, currency),
  };
}

function chatgptUnavailable(reason, extra = {}) {
  return unavailableEvidence(PLATFORM.CHATGPT_ADS, reason, {
    spec: SPEC,
    productionReadiness: extra.productionReadiness
      || (reason === UNAVAILABLE_REASON.API_ERROR
        ? undefined
        : PRODUCTION_READINESS.BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL),
    ...extra,
  });
}

function chatgptError(message, extra = {}) {
  return {
    ...chatgptUnavailable(UNAVAILABLE_REASON.API_ERROR, {
      productionReadiness: undefined,
      ...extra,
    }),
    availability: AVAILABILITY.ERROR,
    error: message,
  };
}

async function listCampaigns({ apiKey, http }) {
  const campaigns = [];
  let after = null;
  for (let page = 0; page < 5; page += 1) {
    const query = { limit: 100, order: 'desc' };
    if (after) query.after = after;
    const res = await executeReadOnlyOperation({
      operation: 'GET_CAMPAIGNS',
      query,
      apiKey,
      http,
    });
    const rows = res.data?.data || [];
    campaigns.push(...rows);
    if (!res.data?.has_more || !res.data?.last_id) break;
    after = res.data.last_id;
  }
  return campaigns.filter(isRelevantCampaign);
}

async function readAccountInsights({ apiKey, http, window }) {
  const res = await executeReadOnlyOperation({
    operation: 'GET_AD_ACCOUNT_INSIGHTS',
    query: {
      aggregation_level: 'campaign',
      time_granularity: 'none',
      'time_ranges[]': JSON.stringify({
        type: 'date_range',
        since: window.start,
        until: window.end,
      }),
      'fields[]': INSIGHT_FIELDS.slice(),
      'includes[]': 'zero_impression_items',
      limit: 200,
    },
    apiKey,
    http,
  });
  return aggregateInsightRows(res.data?.data || []);
}

async function readConversionInsights({ apiKey, http, window, campaignIds }) {
  if (!campaignIds.length) return new Map();
  try {
    const res = await executeReadOnlyOperation({
      operation: 'POST_CONVERSION_INSIGHTS',
      body: {
        aggregation_level: 'campaign',
        time_granularity: 'none',
        time_ranges: [
          JSON.stringify({
            type: 'date_range',
            since: window.start,
            until: window.end,
          }),
        ],
        entity_ids: campaignIds,
      },
      apiKey,
      http,
    });
    const byId = new Map();
    for (const row of res.data?.data || []) {
      const id = asText(row.entity_id || row.campaign_id);
      if (id) byId.set(id, row);
    }
    return byId;
  } catch (_err) {
    return new Map();
  }
}

/**
 * Production readiness for a tenant-linked ChatGPT Ads account.
 * Never reports READY from mocked success unless a tenant-linked credential exists.
 */
async function assessChatGptAdsProductionReadiness(input = {}) {
  const clientId = Number(input.clientId != null ? input.clientId : input.tenantId);
  if (!Number.isInteger(clientId) || clientId <= 0) {
    return {
      status: PRODUCTION_READINESS.BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL,
      reason: UNAVAILABLE_REASON.CHATGPT_ADS_ACCOUNT_NOT_LINKED,
      clientId: clientId || null,
      operatorSetup: CHATGPT_ADS_OPERATOR_SETUP,
    };
  }

  const accounts = await resolveAdAccountsForClient({
    clientId,
    platform: PLATFORM.CHATGPT_ADS,
    pool: input.pool,
    queryAccounts: input.resolveAccounts,
  });
  const account = (accounts || []).find((row) => {
    const platform = asText(row.platform).toLowerCase();
    return platform === PLATFORM.CHATGPT_ADS || platform === 'chatgpt' || platform === 'openai_ads';
  }) || accounts[0] || null;

  if (!account) {
    return {
      status: PRODUCTION_READINESS.BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL,
      reason: UNAVAILABLE_REASON.CHATGPT_ADS_ACCOUNT_NOT_LINKED,
      clientId,
      operatorSetup: CHATGPT_ADS_OPERATOR_SETUP,
    };
  }

  const missing = accountUnavailableReason(account, PLATFORM.CHATGPT_ADS);
  if (missing) {
    return {
      status: PRODUCTION_READINESS.BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL,
      reason: missing,
      clientId,
      linkedAccountId: account.account_id || null,
      operatorSetup: CHATGPT_ADS_OPERATOR_SETUP,
    };
  }

  return {
    status: PRODUCTION_READINESS.READY,
    reason: null,
    clientId,
    linkedAccountId: account.account_id || null,
  };
}

/**
 * @param {object} input
 * @param {object} input.account
 * @param {object} [input.window]
 * @param {object} [input.http]
 * @param {number} [input.clientId]
 */
async function readChatGptAdsEvidence(input = {}) {
  const account = input.account;
  const window = input.window || observationWindowFromDays(input.windowDays || DEFAULT_WINDOW_DAYS);
  const http = input.http || axios;
  const apiKey = account?.access_token;

  if (!account) {
    return chatgptUnavailable(UNAVAILABLE_REASON.CHATGPT_ADS_ACCOUNT_NOT_LINKED, {
      clientId: input.clientId || null,
    });
  }
  if (!apiKey) {
    return chatgptUnavailable(UNAVAILABLE_REASON.MISSING_CREDENTIALS, {
      clientId: input.clientId || account.client_id || null,
    });
  }

  try {
    const accountRes = await executeReadOnlyOperation({
      operation: 'GET_AD_ACCOUNT',
      apiKey,
      http,
    });
    const remote = accountRes.data || {};
    const remoteId = remoteAccountId(remote);
    if (!identitiesMatch(account.account_id, remoteId)) {
      return chatgptUnavailable(UNAVAILABLE_REASON.ACCOUNT_IDENTITY_MISMATCH, {
        clientId: input.clientId || account.client_id || null,
        productionReadiness: PRODUCTION_READINESS.BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL,
        details: {
          expectedExternalAccountId: asText(account.account_id) || null,
          observedExternalAccountId: remoteId || null,
        },
      });
    }

    const campaigns = await listCampaigns({ apiKey, http });
    const insightsById = await readAccountInsights({ apiKey, http, window });
    const conversionById = await readConversionInsights({
      apiKey,
      http,
      window,
      campaignIds: campaigns.map((row) => asText(row.id)).filter(Boolean),
    });

    const currency = asText(remote.currency_code || remote.currency) || 'USD';
    const timezone = asText(remote.timezone) || null;
    const normalized = campaigns.map((campaign) => normalizeCampaignEvidence(
      campaign,
      insightsById.get(asText(campaign.id)),
      conversionById.get(asText(campaign.id)),
      currency
    ));

    return {
      spec: SPEC,
      platform: PLATFORM.CHATGPT_ADS,
      channel: CHANNEL_BY_PLATFORM[PLATFORM.CHATGPT_ADS],
      availability: AVAILABILITY.AVAILABLE,
      productionReadiness: PRODUCTION_READINESS.READY,
      account: {
        externalAccountId: remoteId,
        linkedAccountId: account.id || null,
        currency,
        timezone,
      },
      observationWindow: window,
      campaigns: normalized,
      keywords: [],
      aggregates: {
        spend: normalized.reduce((sum, row) => sum + (row.spend || 0), 0),
        impressions: normalized.reduce((sum, row) => sum + (row.impressions || 0), 0),
        clicks: normalized.reduce((sum, row) => sum + (row.clicks || 0), 0),
        platformConversions: normalized.reduce(
          (sum, row) => sum + (row.platformConversions || 0),
          0
        ),
      },
      platformMetricsAreEvidenceOnly: true,
      provenance: platformProvenance(PLATFORM.CHATGPT_ADS, {
        accountLinkedId: account.id || null,
      }),
    };
  } catch (err) {
    return chatgptError(redactSecret(err.message || 'OpenAI Ads API error', apiKey), {
      clientId: input.clientId || account.client_id || null,
    });
  }
}

module.exports = {
  SPEC,
  OPENAI_ADS_BASE_URL,
  DEFAULT_WINDOW_DAYS,
  READ_ONLY_OPERATIONS,
  FORBIDDEN_OPENAI_ADS_MUTATIONS,
  CHATGPT_ADS_OPERATOR_SETUP,
  observationWindowFromDays,
  resolveOpenAiAdsReadOperation,
  assertOpenAiAdsMutationRejected,
  isForbiddenMutation,
  readChatGptAdsEvidence,
  assessChatGptAdsProductionReadiness,
};
