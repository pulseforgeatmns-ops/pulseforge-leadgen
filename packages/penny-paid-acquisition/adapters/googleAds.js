'use strict';

/**
 * SPEC-252 — Read-only Google Ads evidence adapter.
 */

const axios = require('axios');
const {
  AVAILABILITY,
  UNAVAILABLE_REASON,
  PLATFORM,
  CHANNEL_BY_PLATFORM,
  platformProvenance,
  unavailableEvidence,
} = require('../types');

const GOOGLE_ADS_VERSION = 'v18';
const DEFAULT_WINDOW_DAYS = 7;

function requiredGoogleEnv() {
  return ['GOOGLE_ADS_DEVELOPER_TOKEN', 'GOOGLE_ADS_CLIENT_ID', 'GOOGLE_ADS_CLIENT_SECRET']
    .filter((key) => !process.env[key]);
}

async function googleAdsToken(refreshToken, http = axios) {
  const res = await http.post('https://oauth2.googleapis.com/token', {
    client_id: process.env.GOOGLE_ADS_CLIENT_ID,
    client_secret: process.env.GOOGLE_ADS_CLIENT_SECRET,
    refresh_token: refreshToken,
    grant_type: 'refresh_token',
  });
  return res.data.access_token;
}

function googleAdsHeaders(token) {
  const headers = {
    Authorization: `Bearer ${token}`,
    'developer-token': process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
    'Content-Type': 'application/json',
  };
  if (process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID) {
    headers['login-customer-id'] = process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID;
  }
  return headers;
}

async function gaqlSearch(customerId, query, token, http = axios) {
  const res = await http.post(
    `https://googleads.googleapis.com/${GOOGLE_ADS_VERSION}/customers/${customerId}/googleAds:search`,
    { query },
    { headers: googleAdsHeaders(token) }
  );
  return res.data.results || [];
}

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

function microsToCurrency(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.round((n / 1_000_000) * 100) / 100;
}

function normalizeCampaignRow(row) {
  const metrics = row.metrics || {};
  const campaign = row.campaign || {};
  const budgetMicros = row.campaignBudget?.amountMicros;
  const spend = microsToCurrency(metrics.costMicros);
  const dailyBudget = microsToCurrency(budgetMicros);
  const impressions = Number(metrics.impressions || 0);
  const clicks = Number(metrics.clicks || 0);
  const ctrRaw = metrics.ctr != null ? Number(metrics.ctr) : null;
  const averageCpc = microsToCurrency(metrics.averageCpc);

  return {
    externalCampaignId: campaign.id != null ? String(campaign.id) : null,
    name: campaign.name || 'Unknown',
    status: campaign.status || 'UNKNOWN',
    advertisingChannelType: campaign.advertisingChannelType || null,
    spend,
    impressions,
    clicks,
    ctr: ctrRaw,
    averageCpc,
    platformConversions: metrics.conversions != null ? Number(metrics.conversions) : null,
    platformConversionValue: metrics.conversionsValue != null
      ? microsToCurrency(metrics.conversionsValue)
      : null,
    costPerPlatformConversion: microsToCurrency(metrics.costPerConversion),
    budget: dailyBudget != null
      ? { amount: dailyBudget, period: 'DAILY', currency: 'USD' }
      : null,
    spendPace: dailyBudget && spend != null && dailyBudget > 0
      ? {
        observedSpend: spend,
        referenceBudget: dailyBudget * (observationWindowFromDays().days || DEFAULT_WINDOW_DAYS),
        period: 'OBSERVATION_WINDOW',
      }
      : null,
  };
}

function normalizeKeywordRow(row) {
  const keyword = row.adGroupCriterion?.keyword || {};
  const quality = row.adGroupCriterion?.qualityInfo || {};
  return {
    text: keyword.text || '',
    qualityScore: quality.qualityScore != null ? Number(quality.qualityScore) : null,
    campaignName: row.campaign?.name || '',
    adGroupName: row.adGroup?.name || '',
  };
}

/**
 * @param {object} input
 * @param {object} input.account
 * @param {object} [input.window]
 * @param {object} [input.http]
 */
async function readGoogleAdsEvidence(input = {}) {
  const account = input.account;
  const window = input.window || observationWindowFromDays(input.windowDays || DEFAULT_WINDOW_DAYS);
  const http = input.http || axios;

  const missingEnv = requiredGoogleEnv();
  if (missingEnv.length) {
    return unavailableEvidence(PLATFORM.GOOGLE_ADS, UNAVAILABLE_REASON.MISSING_ENV_CREDENTIALS, {
      details: { missingEnvKeys: missingEnv },
    });
  }
  if (!account?.refresh_token) {
    return unavailableEvidence(PLATFORM.GOOGLE_ADS, UNAVAILABLE_REASON.MISSING_CREDENTIALS);
  }

  try {
    const token = await googleAdsToken(account.refresh_token, http);
    const customerId = String(account.account_id || '').replace(/-/g, '');
    const dateClause = window.label === 'LAST_7_DAYS'
      ? 'segments.date DURING LAST_7_DAYS'
      : `segments.date BETWEEN '${window.start}' AND '${window.end}'`;

    const [campaignRows, keywordRows] = await Promise.all([
      gaqlSearch(customerId, `
        SELECT
          campaign.id, campaign.name, campaign.status,
          campaign.advertising_channel_type,
          campaign_budget.amount_micros,
          metrics.impressions, metrics.clicks, metrics.ctr,
          metrics.average_cpc, metrics.conversions, metrics.conversions_value,
          metrics.cost_per_conversion, metrics.cost_micros
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND ${dateClause}
      `, token, http),
      gaqlSearch(customerId, `
        SELECT
          ad_group_criterion.keyword.text,
          ad_group_criterion.quality_info.quality_score,
          campaign.name, ad_group.name
        FROM ad_group_criterion
        WHERE ad_group_criterion.type = 'KEYWORD'
          AND ad_group_criterion.status = 'ENABLED'
          AND campaign.status = 'ENABLED'
        LIMIT 100
      `, token, http),
    ]);

    const campaigns = campaignRows.map(normalizeCampaignRow);
    const keywords = keywordRows.map(normalizeKeywordRow).filter((row) => row.text);

    return {
      spec: 'SPEC-252',
      platform: PLATFORM.GOOGLE_ADS,
      channel: CHANNEL_BY_PLATFORM[PLATFORM.GOOGLE_ADS],
      availability: AVAILABILITY.AVAILABLE,
      account: {
        externalAccountId: account.account_id,
        linkedAccountId: account.id || null,
        currency: 'USD',
        timezone: null,
      },
      observationWindow: window,
      campaigns,
      keywords,
      aggregates: {
        spend: campaigns.reduce((sum, row) => sum + (row.spend || 0), 0),
        impressions: campaigns.reduce((sum, row) => sum + (row.impressions || 0), 0),
        clicks: campaigns.reduce((sum, row) => sum + (row.clicks || 0), 0),
        platformConversions: campaigns.reduce(
          (sum, row) => sum + (row.platformConversions || 0),
          0
        ),
      },
      platformMetricsAreEvidenceOnly: true,
      provenance: platformProvenance(PLATFORM.GOOGLE_ADS, {
        accountLinkedId: account.id || null,
      }),
    };
  } catch (err) {
    const message = err.response?.data?.error?.message || err.message || 'Google Ads API error';
    return {
      ...unavailableEvidence(PLATFORM.GOOGLE_ADS, UNAVAILABLE_REASON.API_ERROR),
      availability: AVAILABILITY.ERROR,
      error: message,
    };
  }
}

module.exports = {
  GOOGLE_ADS_VERSION,
  DEFAULT_WINDOW_DAYS,
  observationWindowFromDays,
  readGoogleAdsEvidence,
  gaqlSearch,
  googleAdsToken,
};
