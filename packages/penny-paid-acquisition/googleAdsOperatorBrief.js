'use strict';

/**
 * SPEC-PENNY-GADS-004 — Read-only Google Ads section for the daily operator brief.
 */

function recommendGoogleAdsNextAction({
  blockers = [],
  campaignCount = 0,
  impressions = 0,
  clicks = 0,
  conversions = 0,
} = {}) {
  if (blockers.length > 0) {
    return 'Fix Google Ads connection before making campaign decisions.';
  }

  if (campaignCount === 0) {
    return 'No active Google Ads campaigns found. Launch or enable a campaign before evaluating performance.';
  }

  if (impressions < 100) {
    return 'Let the campaign gather more impressions before making changes.';
  }

  if (clicks === 0) {
    return 'Review targeting, keywords, and ad copy. The campaign is getting impressions but no clicks.';
  }

  if (conversions === 0 && clicks < 15) {
    return 'Keep monitoring. Not enough click volume yet to judge conversion performance.';
  }

  if (conversions === 0 && clicks >= 15) {
    return 'Review landing page and offer. Clicks are coming in but no conversions are being captured.';
  }

  return 'Continue monitoring. Google Ads is producing conversion signal.';
}

function activeGoogleAdsCampaignCount(evidence) {
  if (evidence != null && Number.isFinite(evidence.campaignCount)) {
    return evidence.campaignCount;
  }
  const campaigns = Array.isArray(evidence?.campaigns) ? evidence.campaigns : [];
  return campaigns.filter((row) => String(row.status || '').toUpperCase() === 'ENABLED').length;
}

function buildGoogleAdsOperatorBrief({ readiness, evidence } = {}) {
  const blockers = readiness?.blockers || [];
  const warnings = readiness?.warnings || [];

  const spend = evidence?.aggregates?.spend ?? 0;
  const impressions = evidence?.aggregates?.impressions ?? 0;
  const clicks = evidence?.aggregates?.clicks ?? 0;
  const rawConversions = evidence?.aggregates?.platformConversions;
  const conversions = rawConversions == null ? 0 : Number(rawConversions) || 0;
  const campaignCount = activeGoogleAdsCampaignCount(evidence);

  const ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
  const costPerConversion = conversions > 0 ? spend / conversions : null;

  const brief = {
    title: 'Paid Acquisition — Google Ads',
    accountStatus: readiness?.accountStatus || 'UNKNOWN',
    spend,
    impressions,
    clicks,
    ctr,
    conversions,
    costPerConversion,
    activeCampaignCount: campaignCount,
    warnings,
    blockers,
    recommendedNextAction: recommendGoogleAdsNextAction({
      blockers,
      campaignCount,
      impressions,
      clicks,
      conversions,
    }),
  };

  brief.text = formatGoogleAdsOperatorBriefText(brief);
  return brief;
}

function formatUsd(amount) {
  const n = Number(amount);
  if (!Number.isFinite(n)) return '$0.00';
  return `$${n.toFixed(2)}`;
}

function formatPercent(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '0.00%';
  return `${n.toFixed(2)}%`;
}

function formatWarningsBlockers(warnings = [], blockers = []) {
  const items = [...blockers, ...warnings].filter(Boolean);
  if (!items.length) return 'none';
  return items.join('; ');
}

function formatGoogleAdsOperatorBriefText(brief) {
  const lines = [
    brief.title,
    '',
    `Status: ${brief.accountStatus}`,
    `Spend: ${formatUsd(brief.spend)}`,
    `Impressions: ${brief.impressions}`,
    `Clicks: ${brief.clicks}`,
    `CTR: ${formatPercent(brief.ctr)}`,
    `Conversions: ${brief.conversions}`,
    `Cost / conversion: ${brief.costPerConversion == null ? '—' : formatUsd(brief.costPerConversion)}`,
    `Active campaigns: ${brief.activeCampaignCount}`,
    `Warnings/blockers: ${formatWarningsBlockers(brief.warnings, brief.blockers)}`,
    '',
    'Recommended next action:',
    brief.recommendedNextAction,
  ];
  return lines.join('\n');
}

/**
 * Loads readiness + evidence for a tenant and returns the operator brief section.
 * Never throws — failures degrade to blockers on the section object.
 */
async function loadGoogleAdsOperatorBriefSection(input = {}) {
  const tenantId = Number(input.tenantId != null ? input.tenantId : input.clientId);
  const pool = input.pool;
  const http = input.http;

  const { PLATFORM } = require('./types');
  const { resolveAdAccountsForClient } = require('./accountResolution');
  const {
    assessGoogleAdsReadiness,
    readGoogleAdsEvidence,
  } = require('./adapters/googleAds');

  try {
    const readiness = await assessGoogleAdsReadiness({
      tenantId,
      clientId: tenantId,
      pool,
      http,
      skipLiveProbe: input.skipLiveProbe,
    });

    let evidence = null;
    if (!input.skipLiveProbe) {
      try {
        const accounts = await resolveAdAccountsForClient({
          clientId: tenantId,
          platform: PLATFORM.GOOGLE_ADS,
          pool,
          queryAccounts: input.resolveAccounts,
        });
        const account = (accounts || []).find((row) => String(row.platform).toLowerCase() === PLATFORM.GOOGLE_ADS)
          || (accounts || [])[0];
        if (account?.refresh_token) {
          evidence = await readGoogleAdsEvidence({
            account,
            http,
            window: input.window,
            windowDays: input.windowDays,
          });
        }
      } catch (err) {
        if (!readiness.blockers?.length) {
          readiness.blockers = [...(readiness.blockers || []), err.message || 'Google Ads evidence read failed'];
        }
      }
    }

    return buildGoogleAdsOperatorBrief({ readiness, evidence });
  } catch (err) {
    return buildGoogleAdsOperatorBrief({
      readiness: {
        accountStatus: 'UNAVAILABLE',
        blockers: [err.message || 'Google Ads operator brief unavailable'],
        warnings: [],
      },
      evidence: null,
    });
  }
}

module.exports = {
  recommendGoogleAdsNextAction,
  activeGoogleAdsCampaignCount,
  buildGoogleAdsOperatorBrief,
  formatGoogleAdsOperatorBriefText,
  loadGoogleAdsOperatorBriefSection,
};
