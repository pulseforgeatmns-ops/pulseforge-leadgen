'use strict';

/**
 * SPEC-252 — Read-only Google Ads evidence adapter.
 * SPEC-PENNY-GADS-001 — Current API version, readiness, pagination, evidence correctness.
 */

const axios = require('axios');
const {
  AVAILABILITY,
  UNAVAILABLE_REASON,
  READINESS_STATE,
  PLATFORM,
  CHANNEL_BY_PLATFORM,
  platformProvenance,
  unavailableEvidence,
} = require('../types');
const { resolveAdAccountsForClient, accountUnavailableReason } = require('../accountResolution');

const SPEC = 'SPEC-PENNY-GADS-001';
const DEFAULT_WINDOW_DAYS = 7;
const SUPPORTED_API_VERSIONS = Object.freeze(['v24', 'v25']);
const DEFAULT_API_VERSION = 'v25';

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function resolveGoogleAdsApiVersion() {
  const configured = asText(process.env.GOOGLE_ADS_API_VERSION || process.env.GOOGLE_ADS_VERSION);
  const version = configured || DEFAULT_API_VERSION;
  if (!/^v\d+$/.test(version)) {
    return { version: DEFAULT_API_VERSION, supported: false, configured: version || null };
  }
  return {
    version,
    supported: SUPPORTED_API_VERSIONS.includes(version),
    configured: version,
  };
}

function requiredGoogleOAuthEnv() {
  return ['GOOGLE_ADS_CLIENT_ID', 'GOOGLE_ADS_CLIENT_SECRET']
    .filter((key) => !asText(process.env[key]));
}

/** Required env keys for live Google Ads reads (OAuth client + tenant refresh token). */
function requiredGoogleAdsEnv() {
  return requiredGoogleOAuthEnv();
}

/** Post–Sept 2026: developer token is optional; OAuth project access replaces it. */
function googleAdsDeveloperTokenWarnings() {
  if (asText(process.env.GOOGLE_ADS_DEVELOPER_TOKEN)) return [];
  return [
    'GOOGLE_ADS_DEVELOPER_TOKEN is unset; proceeding with OAuth-only Google Ads API access (developer-token header omitted).',
  ];
}

function mergeReadinessWarnings(readiness, extraWarnings = []) {
  if (!extraWarnings.length) return readiness;
  return {
    ...readiness,
    warnings: [...array(readiness.warnings), ...extraWarnings],
  };
}

function normalizeCustomerId(raw) {
  return asText(raw).replace(/-/g, '');
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

function googleAdsHeaders(token, account = null) {
  const headers = {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
  };
  const developerToken = asText(process.env.GOOGLE_ADS_DEVELOPER_TOKEN);
  if (developerToken) {
    headers['developer-token'] = developerToken;
  }
  const loginCustomerId = asText(account?.manager_customer_id || account?.login_customer_id)
    || asText(process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID);
  if (loginCustomerId) {
    headers['login-customer-id'] = normalizeCustomerId(loginCustomerId);
  }
  return headers;
}

function mapGoogleAdsApiError(err) {
  const status = err.response?.status;
  const message = asText(err.response?.data?.error?.message || err.message);
  const lower = message.toLowerCase();
  if (/unsupported|sunset|deprecated.*version|invalid.*version/i.test(lower)) {
    return { readiness: READINESS_STATE.API_VERSION_UNSUPPORTED, reason: UNAVAILABLE_REASON.API_ERROR, message };
  }
  if (status === 401 || /invalid_grant|unauthorized|invalid credentials/i.test(lower)) {
    return { readiness: READINESS_STATE.AUTH_FAILED, reason: UNAVAILABLE_REASON.MISSING_CREDENTIALS, message };
  }
  if (status === 403 || /permission|authorization|developer token/i.test(lower)) {
    return { readiness: READINESS_STATE.PERMISSION_DENIED, reason: UNAVAILABLE_REASON.API_ERROR, message };
  }
  if (status === 404 || /not found|no customer/i.test(lower)) {
    return { readiness: READINESS_STATE.ACCOUNT_NOT_FOUND, reason: UNAVAILABLE_REASON.API_ERROR, message };
  }
  return { readiness: READINESS_STATE.UNAVAILABLE, reason: UNAVAILABLE_REASON.API_ERROR, message };
}

async function gaqlSearchPage(customerId, query, token, http, apiVersion, account, pageToken = null) {
  const body = { query };
  if (pageToken) body.pageToken = pageToken;
  const res = await http.post(
    `https://googleads.googleapis.com/${apiVersion}/customers/${customerId}/googleAds:search`,
    body,
    { headers: googleAdsHeaders(token, account) }
  );
  return {
    results: res.data.results || [],
    nextPageToken: res.data.nextPageToken || null,
  };
}

async function gaqlSearch(customerId, query, token, http = axios, options = {}) {
  const apiVersion = options.apiVersion || resolveGoogleAdsApiVersion().version;
  const account = options.account || null;
  const page = await gaqlSearchPage(customerId, query, token, http, apiVersion, account);
  return page.results;
}

async function gaqlSearchAll(customerId, query, token, http = axios, options = {}) {
  const apiVersion = options.apiVersion || resolveGoogleAdsApiVersion().version;
  const account = options.account || null;
  const rows = [];
  let pageToken = null;
  do {
    const page = await gaqlSearchPage(customerId, query, token, http, apiVersion, account, pageToken);
    rows.push(...page.results);
    pageToken = page.nextPageToken;
  } while (pageToken);
  return rows;
}

function resolveObservationWindow(input = {}, windowDays = DEFAULT_WINDOW_DAYS) {
  const explicit = input.window || (input.start && input.end ? input : null);
  if (explicit && explicit.start && explicit.end) {
    const days = explicit.days || input.windowDays || windowDays;
    return {
      start: asText(explicit.start),
      end: asText(explicit.end),
      days,
      label: explicit.label || `CUSTOM_${days}_DAYS`,
      includesToday: explicit.includesToday !== false,
      timezone: explicit.timezone || 'UTC',
    };
  }
  return observationWindowFromDays(
    input.windowDays != null ? input.windowDays : windowDays,
    { now: input.now, includesToday: input.includesToday }
  );
}

function observationWindowFromDays(days = DEFAULT_WINDOW_DAYS, options = {}) {
  const now = options.now instanceof Date ? options.now : new Date();
  const end = new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate()
  ));
  const start = new Date(end.getTime() - (Math.max(1, days) - 1) * 24 * 60 * 60 * 1000);
  return {
    start: start.toISOString().slice(0, 10),
    end: end.toISOString().slice(0, 10),
    days,
    label: days === 7 ? 'LAST_7_DAYS' : `LAST_${days}_DAYS`,
    includesToday: options.includesToday !== false,
    timezone: 'UTC',
  };
}

function campaignDateClause(window) {
  if (window.label === 'LAST_7_DAYS' && window.start == null) {
    return 'segments.date DURING LAST_7_DAYS';
  }
  return `segments.date BETWEEN '${window.start}' AND '${window.end}'`;
}

function microsToCurrency(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.round((n / 1_000_000) * 100) / 100;
}

function conversionValueFromMetrics(metrics = {}) {
  if (metrics.conversionsValue == null && metrics.conversions_value == null) return null;
  const raw = metrics.conversionsValue != null ? metrics.conversionsValue : metrics.conversions_value;
  const n = Number(raw);
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
}

function normalizeCampaignRow(row, context = {}) {
  const metrics = row.metrics || {};
  const campaign = row.campaign || {};
  const budgetMicros = row.campaignBudget?.amountMicros ?? row.campaign_budget?.amount_micros;
  const spend = microsToCurrency(metrics.costMicros ?? metrics.cost_micros);
  const dailyBudget = microsToCurrency(budgetMicros);
  const impressions = Number(metrics.impressions || 0);
  const clicks = Number(metrics.clicks || 0);
  const ctrRaw = metrics.ctr != null ? Number(metrics.ctr) : null;
  const averageCpc = microsToCurrency(metrics.averageCpc ?? metrics.average_cpc);
  const currency = context.currency || 'USD';
  const windowDays = context.windowDays || DEFAULT_WINDOW_DAYS;

  return {
    externalCampaignId: campaign.id != null ? String(campaign.id) : null,
    name: campaign.name || 'Unknown',
    status: campaign.status || 'UNKNOWN',
    advertisingChannelType: campaign.advertisingChannelType || campaign.advertising_channel_type || null,
    spend,
    impressions,
    clicks,
    ctr: ctrRaw,
    averageCpc,
    platformConversions: metrics.conversions != null ? Number(metrics.conversions) : null,
    platformConversionValue: conversionValueFromMetrics(metrics),
    conversionReadStatus: metrics.conversions != null ? 'OK' : 'UNAVAILABLE',
    costPerPlatformConversion: microsToCurrency(metrics.costPerConversion ?? metrics.cost_per_conversion),
    budget: dailyBudget != null
      ? { amount: dailyBudget, period: 'DAILY', currency }
      : null,
    spendPace: dailyBudget && spend != null && dailyBudget > 0
      ? {
        observedSpend: spend,
        referenceBudget: dailyBudget * windowDays,
        period: 'OBSERVATION_WINDOW',
      }
      : null,
  };
}

function aggregateCampaignRows(rows, context = {}) {
  const byId = new Map();
  for (const row of rows) {
    const campaign = row.campaign || {};
    const id = campaign.id != null ? String(campaign.id) : null;
    if (!id) continue;
    const metrics = row.metrics || {};
    const existing = byId.get(id);
    if (!existing) {
      byId.set(id, normalizeCampaignRow(row, context));
      continue;
    }
    existing.spend = roundMoney((existing.spend || 0) + microsToCurrency(metrics.costMicros ?? metrics.cost_micros));
    existing.impressions += Number(metrics.impressions || 0);
    existing.clicks += Number(metrics.clicks || 0);
    if (metrics.conversions != null) {
      existing.platformConversions = (existing.platformConversions || 0) + Number(metrics.conversions);
      existing.conversionReadStatus = 'OK';
    }
    const cv = conversionValueFromMetrics(metrics);
    if (cv != null) {
      existing.platformConversionValue = roundMoney((existing.platformConversionValue || 0) + cv);
    }
  }
  return [...byId.values()];
}

function roundMoney(value) {
  return Math.round(value * 100) / 100;
}

function normalizeKeywordRow(row) {
  const keyword = row.adGroupCriterion?.keyword || row.ad_group_criterion?.keyword || {};
  const quality = row.adGroupCriterion?.qualityInfo || row.ad_group_criterion?.quality_info || {};
  return {
    text: keyword.text || '',
    qualityScore: quality.qualityScore != null ? Number(quality.qualityScore)
      : quality.quality_score != null ? Number(quality.quality_score) : null,
    campaignName: row.campaign?.name || '',
    adGroupName: row.adGroup?.name || row.ad_group?.name || '',
  };
}

function classifyCampaignEvidenceStatus(campaigns) {
  if (!campaigns.length) return 'NO_CAMPAIGNS_OR_SPEND';
  if (campaigns.every((row) => (row.spend || 0) <= 0)) return 'NO_SPEND';
  return 'OK';
}

function buildGoogleAdsReadinessInspection(input = {}) {
  return {
    tenantId: input.tenantId ?? input.clientId ?? null,
    platform: PLATFORM.GOOGLE_ADS,
    accountStatus: input.accountStatus || READINESS_STATE.UNAVAILABLE,
    credentialStatus: input.credentialStatus || READINESS_STATE.MISSING_CREDENTIALS,
    customerId: input.customerId || null,
    managerCustomerId: input.managerCustomerId || null,
    apiVersion: input.apiVersion || resolveGoogleAdsApiVersion().version,
    currency: input.currency || null,
    timezone: input.timezone || null,
    campaignEvidenceStatus: input.campaignEvidenceStatus || 'UNAVAILABLE',
    conversionEvidenceStatus: input.conversionEvidenceStatus || 'UNAVAILABLE',
    attributionCaptureStatus: input.attributionCaptureStatus || 'UNKNOWN',
    lastSuccessfulReadAt: input.lastSuccessfulReadAt || null,
    blockers: array(input.blockers),
    warnings: array(input.warnings),
    nextRequiredAction: asText(input.nextRequiredAction),
  };
}

function array(value) {
  return Array.isArray(value) ? value : [];
}

async function readCustomerIdentity(customerId, token, http, apiVersion, account) {
  const rows = await gaqlSearchAll(customerId, `
    SELECT customer.id, customer.currency_code, customer.time_zone, customer.descriptive_name
    FROM customer
    LIMIT 1
  `, token, http, { apiVersion, account });
  const customer = rows[0]?.customer || {};
  return {
    customerId: customer.id != null ? String(customer.id) : customerId,
    currency: asText(customer.currencyCode || customer.currency_code) || 'USD',
    timezone: asText(customer.timeZone || customer.time_zone) || null,
    name: asText(customer.descriptiveName || customer.descriptive_name) || null,
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
  const window = resolveObservationWindow(input, input.windowDays || DEFAULT_WINDOW_DAYS);
  const http = input.http || axios;
  const versionInfo = resolveGoogleAdsApiVersion();

  if (!versionInfo.supported) {
    return {
      ...unavailableEvidence(PLATFORM.GOOGLE_ADS, UNAVAILABLE_REASON.API_ERROR, {
        readiness: buildGoogleAdsReadinessInspection({
          tenantId: account?.client_id,
          accountStatus: READINESS_STATE.API_VERSION_UNSUPPORTED,
          credentialStatus: READINESS_STATE.API_VERSION_UNSUPPORTED,
          apiVersion: versionInfo.configured,
          blockers: [`Google Ads API version ${versionInfo.configured} is not supported.`],
          nextRequiredAction: `Set GOOGLE_ADS_API_VERSION to one of: ${SUPPORTED_API_VERSIONS.join(', ')}`,
        }),
      }),
      availability: AVAILABILITY.UNAVAILABLE,
      error: `Unsupported Google Ads API version: ${versionInfo.configured}`,
    };
  }

  const missingEnv = requiredGoogleAdsEnv();
  if (missingEnv.length) {
    return {
      ...unavailableEvidence(PLATFORM.GOOGLE_ADS, UNAVAILABLE_REASON.MISSING_ENV_CREDENTIALS, {
        details: { missingEnvKeys: missingEnv },
        readiness: buildGoogleAdsReadinessInspection({
          tenantId: account?.client_id,
          accountStatus: READINESS_STATE.MISSING_CREDENTIALS,
          credentialStatus: READINESS_STATE.MISSING_CREDENTIALS,
          apiVersion: versionInfo.version,
          blockers: [`Missing Google Ads environment credentials: ${missingEnv.join(', ')}`],
          nextRequiredAction: 'Configure GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET.',
          warnings: googleAdsDeveloperTokenWarnings(),
        }),
      }),
    };
  }
  if (!account?.refresh_token) {
    return {
      ...unavailableEvidence(PLATFORM.GOOGLE_ADS, UNAVAILABLE_REASON.MISSING_CREDENTIALS, {
        readiness: buildGoogleAdsReadinessInspection({
          tenantId: account?.client_id,
          accountStatus: READINESS_STATE.MISSING_CREDENTIALS,
          credentialStatus: READINESS_STATE.MISSING_CREDENTIALS,
          apiVersion: versionInfo.version,
          blockers: ['Tenant-linked google_ads refresh_token is missing in ad_accounts.'],
          nextRequiredAction: 'Link Anchor Google Ads OAuth refresh token to ad_accounts for client_id=10.',
        }),
      }),
    };
  }

  try {
    const token = await googleAdsToken(account.refresh_token, http);
    const customerId = normalizeCustomerId(account.account_id);
    const dateClause = campaignDateClause(window);
    const identity = await readCustomerIdentity(customerId, token, http, versionInfo.version, account);

    const campaignRows = await gaqlSearchAll(customerId, `
      SELECT
        campaign.id, campaign.name, campaign.status,
        campaign.advertising_channel_type,
        campaign_budget.amount_micros,
        metrics.impressions, metrics.clicks, metrics.ctr,
        metrics.average_cpc, metrics.conversions, metrics.conversions_value,
        metrics.cost_per_conversion, metrics.cost_micros
      FROM campaign
      WHERE campaign.status IN ('ENABLED', 'PAUSED')
        AND ${dateClause}
    `, token, http, { apiVersion: versionInfo.version, account });

    const keywordRows = await gaqlSearchAll(customerId, `
      SELECT
        ad_group_criterion.keyword.text,
        ad_group_criterion.quality_info.quality_score,
        campaign.name, ad_group.name
      FROM ad_group_criterion
      WHERE ad_group_criterion.type = 'KEYWORD'
        AND ad_group_criterion.status = 'ENABLED'
        AND campaign.status IN ('ENABLED', 'PAUSED')
      LIMIT 500
    `, token, http, { apiVersion: versionInfo.version, account });

    const campaigns = aggregateCampaignRows(campaignRows, {
      currency: identity.currency,
      windowDays: window.days || DEFAULT_WINDOW_DAYS,
    });
    const keywords = keywordRows.map(normalizeKeywordRow).filter((row) => row.text);

    const conversionReadFailed = campaigns.some((row) => row.conversionReadStatus !== 'OK')
      && campaigns.some((row) => row.spend > 0);
    const aggregates = {
      spend: roundMoney(campaigns.reduce((sum, row) => sum + (row.spend || 0), 0)),
      impressions: campaigns.reduce((sum, row) => sum + (row.impressions || 0), 0),
      clicks: campaigns.reduce((sum, row) => sum + (row.clicks || 0), 0),
      platformConversions: conversionReadFailed
        ? null
        : campaigns.reduce((sum, row) => sum + (row.platformConversions || 0), 0),
      platformConversionValue: conversionReadFailed
        ? null
        : roundMoney(campaigns.reduce((sum, row) => sum + (row.platformConversionValue || 0), 0)),
    };

    const campaignEvidenceStatus = classifyCampaignEvidenceStatus(campaigns);
    const conversionEvidenceStatus = conversionReadFailed ? 'CONVERSION_READ_FAILED' : 'OK';
    const observedAt = new Date().toISOString();

    return {
      spec: SPEC,
      platform: PLATFORM.GOOGLE_ADS,
      channel: CHANNEL_BY_PLATFORM[PLATFORM.GOOGLE_ADS],
      availability: AVAILABILITY.AVAILABLE,
      account: {
        externalAccountId: account.account_id,
        linkedAccountId: account.id || null,
        customerId: identity.customerId,
        currency: identity.currency,
        timezone: identity.timezone,
      },
      observationWindow: window,
      campaigns,
      keywords,
      aggregates,
      campaignEvidenceStatus,
      conversionEvidenceStatus,
      conversionReadFailed,
      apiVersion: versionInfo.version,
      readiness: buildGoogleAdsReadinessInspection({
        tenantId: account.client_id,
        accountStatus: READINESS_STATE.READY,
        credentialStatus: READINESS_STATE.READY,
        customerId: identity.customerId,
        managerCustomerId: asText(account.manager_customer_id || process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID) || null,
        apiVersion: versionInfo.version,
        currency: identity.currency,
        timezone: identity.timezone,
        campaignEvidenceStatus,
        conversionEvidenceStatus,
        lastSuccessfulReadAt: observedAt,
        blockers: [],
        warnings: [
          ...googleAdsDeveloperTokenWarnings(),
          ...(conversionReadFailed ? ['Platform conversion metrics could not be read for all campaigns with spend.'] : []),
        ],
        nextRequiredAction: '',
      }),
      platformMetricsAreEvidenceOnly: true,
      provenance: platformProvenance(PLATFORM.GOOGLE_ADS, {
        accountLinkedId: account.id || null,
        apiVersion: versionInfo.version,
        observedAt,
      }),
    };
  } catch (err) {
    const mapped = mapGoogleAdsApiError(err);
    return {
      ...unavailableEvidence(PLATFORM.GOOGLE_ADS, mapped.reason, {
        readiness: buildGoogleAdsReadinessInspection({
          tenantId: account?.client_id,
          accountStatus: mapped.readiness,
          credentialStatus: mapped.readiness,
          customerId: account?.account_id || null,
          apiVersion: versionInfo.version,
          blockers: [mapped.message],
          nextRequiredAction: mapped.readiness === READINESS_STATE.AUTH_FAILED
            ? 'Refresh Google Ads OAuth credentials for the tenant-linked ad account.'
            : 'Verify Google Ads customer id, manager login-customer-id, and OAuth project access.',
        }),
      }),
      availability: AVAILABILITY.ERROR,
      error: mapped.message,
    };
  }
}

async function assessGoogleAdsReadiness(input = {}) {
  const clientId = Number(input.clientId != null ? input.clientId : input.tenantId);
  if (!Number.isInteger(clientId) || clientId <= 0) {
    return buildGoogleAdsReadinessInspection({
      tenantId: clientId || null,
      accountStatus: READINESS_STATE.UNAVAILABLE,
      credentialStatus: READINESS_STATE.MISSING_CREDENTIALS,
      blockers: ['Valid tenant clientId is required.'],
      nextRequiredAction: 'Select a tenant before inspecting Google Ads readiness.',
    });
  }

  const versionInfo = resolveGoogleAdsApiVersion();
  if (!versionInfo.supported) {
    return buildGoogleAdsReadinessInspection({
      tenantId: clientId,
      accountStatus: READINESS_STATE.API_VERSION_UNSUPPORTED,
      credentialStatus: READINESS_STATE.API_VERSION_UNSUPPORTED,
      apiVersion: versionInfo.configured,
      blockers: [`Unsupported Google Ads API version ${versionInfo.configured}.`],
      nextRequiredAction: `Set GOOGLE_ADS_API_VERSION to ${DEFAULT_API_VERSION}.`,
    });
  }

  const missingEnv = requiredGoogleAdsEnv();
  if (missingEnv.length) {
    return buildGoogleAdsReadinessInspection({
      tenantId: clientId,
      accountStatus: READINESS_STATE.MISSING_CREDENTIALS,
      credentialStatus: READINESS_STATE.MISSING_CREDENTIALS,
      apiVersion: versionInfo.version,
      blockers: missingEnv.map((key) => `Missing env ${key}`),
      nextRequiredAction: 'Configure GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET.',
      warnings: googleAdsDeveloperTokenWarnings(),
    });
  }

  const devTokenWarnings = googleAdsDeveloperTokenWarnings();

  const accounts = await resolveAdAccountsForClient({
    clientId,
    platform: PLATFORM.GOOGLE_ADS,
    pool: input.pool,
    queryAccounts: input.resolveAccounts,
  });
  const googleAccounts = (accounts || []).filter((row) => asText(row.platform).toLowerCase() === PLATFORM.GOOGLE_ADS);
  if (!googleAccounts.length) {
    return buildGoogleAdsReadinessInspection({
      tenantId: clientId,
      accountStatus: READINESS_STATE.UNAVAILABLE,
      credentialStatus: READINESS_STATE.MISSING_CREDENTIALS,
      apiVersion: versionInfo.version,
      blockers: ['No active tenant-scoped google_ads account is linked.'],
      nextRequiredAction: 'Create an active ad_accounts row (platform=google_ads) for this tenant.',
    });
  }
  if (googleAccounts.length > 1 && !input.selectedAccountId) {
    return buildGoogleAdsReadinessInspection({
      tenantId: clientId,
      accountStatus: READINESS_STATE.UNAVAILABLE,
      credentialStatus: READINESS_STATE.MISSING_CREDENTIALS,
      apiVersion: versionInfo.version,
      blockers: ['Multiple google_ads accounts are linked; explicit account selection is required.'],
      nextRequiredAction: 'Pass selectedAccountId when multiple Google Ads accounts exist for the tenant.',
    });
  }

  const account = googleAccounts.find((row) => row.id === input.selectedAccountId) || googleAccounts[0];
  const missingAccountCreds = accountUnavailableReason(account, PLATFORM.GOOGLE_ADS);
  if (missingAccountCreds) {
    return buildGoogleAdsReadinessInspection({
      tenantId: clientId,
      accountStatus: READINESS_STATE.MISSING_CREDENTIALS,
      credentialStatus: READINESS_STATE.MISSING_CREDENTIALS,
      customerId: account.account_id,
      apiVersion: versionInfo.version,
      blockers: ['Linked google_ads account is missing refresh_token.'],
      nextRequiredAction: 'Store OAuth refresh_token on the tenant ad_accounts row.',
    });
  }

  if (input.skipLiveProbe === true) {
    return buildGoogleAdsReadinessInspection({
      tenantId: clientId,
      accountStatus: READINESS_STATE.READY,
      credentialStatus: READINESS_STATE.READY,
      customerId: account.account_id,
      apiVersion: versionInfo.version,
      campaignEvidenceStatus: 'NOT_PROBED',
      conversionEvidenceStatus: 'NOT_PROBED',
      warnings: devTokenWarnings,
      nextRequiredAction: '',
    });
  }

  const evidence = await readGoogleAdsEvidence({
    account,
    http: input.http,
    window: input.window,
    windowDays: input.windowDays,
  });
  if (evidence.availability !== AVAILABILITY.AVAILABLE) {
    const failed = evidence.readiness || buildGoogleAdsReadinessInspection({
      tenantId: clientId,
      accountStatus: READINESS_STATE.UNAVAILABLE,
      credentialStatus: READINESS_STATE.UNAVAILABLE,
      customerId: account.account_id,
      apiVersion: versionInfo.version,
      blockers: [asText(evidence.error) || evidence.reason],
      nextRequiredAction: 'Fix Google Ads credentials or account access, then re-run readiness inspection.',
    });
    return mergeReadinessWarnings(failed, devTokenWarnings);
  }
  return mergeReadinessWarnings(evidence.readiness, devTokenWarnings);
}

/** @deprecated use resolveGoogleAdsApiVersion().version */
const GOOGLE_ADS_VERSION = DEFAULT_API_VERSION;

module.exports = {
  SPEC,
  GOOGLE_ADS_VERSION,
  DEFAULT_API_VERSION,
  SUPPORTED_API_VERSIONS,
  DEFAULT_WINDOW_DAYS,
  READINESS_STATE,
  resolveGoogleAdsApiVersion,
  requiredGoogleAdsEnv,
  requiredGoogleOAuthEnv,
  googleAdsDeveloperTokenWarnings,
  googleAdsHeaders,
  observationWindowFromDays,
  resolveObservationWindow,
  buildGoogleAdsReadinessInspection,
  assessGoogleAdsReadiness,
  readGoogleAdsEvidence,
  gaqlSearch,
  gaqlSearchAll,
  googleAdsToken,
};
