'use strict';

/**
 * SPEC-252 — Read-only Meta Ads evidence adapter (legacy parity, no expansion).
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

const META_API_VERSION = 'v20.0';
const DEFAULT_WINDOW_DAYS = 7;

function observationWindowFromDays(days = DEFAULT_WINDOW_DAYS) {
  const end = new Date();
  const start = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  return {
    start: start.toISOString().slice(0, 10),
    end: end.toISOString().slice(0, 10),
    days,
  };
}

function normalizeAdsetRow(row) {
  const spend = row.spend != null ? Number(row.spend) : null;
  const impressions = row.impressions != null ? Number(row.impressions) : null;
  const clicks = row.clicks != null ? Number(row.clicks) : null;
  const ctr = row.ctr != null ? Number(row.ctr) : null;
  const cpc = row.cpc != null ? Number(row.cpc) : null;
  const frequency = row.frequency != null ? Number(row.frequency) : null;
  const roas = row.purchase_roas?.[0]?.value != null
    ? Number(row.purchase_roas[0].value)
    : null;

  return {
    externalCampaignId: row.campaign_id != null ? String(row.campaign_id) : null,
    name: row.adset_name || row.campaign_name || 'Unknown',
    status: 'ENABLED',
    spend,
    impressions,
    clicks,
    ctr,
    averageCpc: cpc,
    frequency,
    platformReportedRoas: roas,
    platformConversions: null,
    platformConversionValue: null,
    budget: null,
  };
}

/**
 * @param {object} input
 * @param {object} input.account
 * @param {object} [input.window]
 * @param {object} [input.http]
 */
async function readMetaAdsEvidence(input = {}) {
  const account = input.account;
  const window = input.window || observationWindowFromDays(input.windowDays || DEFAULT_WINDOW_DAYS);
  const http = input.http || axios;

  if (!account?.access_token) {
    return unavailableEvidence(PLATFORM.META_ADS, UNAVAILABLE_REASON.MISSING_CREDENTIALS);
  }

  try {
    const res = await http.get(
      `https://graph.facebook.com/${META_API_VERSION}/act_${account.account_id}/insights`,
      {
        params: {
          access_token: account.access_token,
          fields: 'campaign_id,campaign_name,adset_name,impressions,clicks,ctr,spend,cpm,cpc,reach,frequency,purchase_roas',
          time_range: JSON.stringify({ since: window.start, until: window.end }),
          level: 'adset',
          limit: 50,
        },
      }
    );

    const rows = res.data?.data || [];
    const campaigns = rows.map(normalizeAdsetRow);

    return {
      spec: 'SPEC-252',
      platform: PLATFORM.META_ADS,
      channel: CHANNEL_BY_PLATFORM[PLATFORM.META_ADS],
      availability: AVAILABILITY.AVAILABLE,
      account: {
        externalAccountId: account.account_id,
        linkedAccountId: account.id || null,
        currency: 'USD',
        timezone: null,
      },
      observationWindow: window,
      campaigns,
      keywords: [],
      aggregates: {
        spend: campaigns.reduce((sum, row) => sum + (row.spend || 0), 0),
        impressions: campaigns.reduce((sum, row) => sum + (row.impressions || 0), 0),
        clicks: campaigns.reduce((sum, row) => sum + (row.clicks || 0), 0),
      },
      platformMetricsAreEvidenceOnly: true,
      provenance: platformProvenance(PLATFORM.META_ADS, {
        accountLinkedId: account.id || null,
      }),
    };
  } catch (err) {
    const message = err.response?.data?.error?.message || err.message || 'Meta Ads API error';
    return {
      ...unavailableEvidence(PLATFORM.META_ADS, UNAVAILABLE_REASON.API_ERROR),
      availability: AVAILABILITY.ERROR,
      error: message,
    };
  }
}

module.exports = {
  META_API_VERSION,
  readMetaAdsEvidence,
};
