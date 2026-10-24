'use strict';

/**
 * SPEC-PENNY-GADS-004 — Google Ads operator brief formatter + recommendations.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  buildGoogleAdsOperatorBrief,
  recommendGoogleAdsNextAction,
} = require('../googleAdsOperatorBrief');

const READY = { accountStatus: 'READY', blockers: [], warnings: [] };

function briefWithMetrics(metrics, readiness = READY) {
  return buildGoogleAdsOperatorBrief({
    readiness,
    evidence: {
      aggregates: metrics,
      campaigns: metrics.activeCampaigns || [{ status: 'ENABLED' }],
      campaignCount: metrics.campaignCount,
    },
  });
}

describe('SPEC-PENNY-GADS-004 — recommendGoogleAdsNextAction', () => {
  it('1. READY + low impressions → gather more impressions', () => {
    const action = recommendGoogleAdsNextAction({
      blockers: [],
      campaignCount: 1,
      impressions: 55,
      clicks: 3,
      conversions: 0,
    });
    assert.match(action, /gather more impressions/i);
  });

  it('2. READY + impressions + zero clicks → review targeting/copy', () => {
    const action = recommendGoogleAdsNextAction({
      blockers: [],
      campaignCount: 1,
      impressions: 200,
      clicks: 0,
      conversions: 0,
    });
    assert.match(action, /targeting, keywords, and ad copy/i);
  });

  it('3. READY + clicks under 15 + zero conversions → keep monitoring', () => {
    const action = recommendGoogleAdsNextAction({
      blockers: [],
      campaignCount: 1,
      impressions: 500,
      clicks: 10,
      conversions: 0,
    });
    assert.match(action, /Not enough click volume/i);
  });

  it('4. READY + clicks 15+ + zero conversions → review landing page/offer', () => {
    const action = recommendGoogleAdsNextAction({
      blockers: [],
      campaignCount: 1,
      impressions: 500,
      clicks: 15,
      conversions: 0,
    });
    assert.match(action, /landing page and offer/i);
  });

  it('5. READY + conversions > 0 → continue monitoring', () => {
    const action = recommendGoogleAdsNextAction({
      blockers: [],
      campaignCount: 1,
      impressions: 500,
      clicks: 20,
      conversions: 2,
    });
    assert.match(action, /producing conversion signal/i);
  });

  it('6. blocker present → fix Google Ads connection', () => {
    const action = recommendGoogleAdsNextAction({
      blockers: ['Missing refresh_token'],
      campaignCount: 1,
      impressions: 500,
      clicks: 20,
      conversions: 2,
    });
    assert.match(action, /Fix Google Ads connection/i);
  });

  it('7. no active campaigns → launch or enable campaign', () => {
    const action = recommendGoogleAdsNextAction({
      blockers: [],
      campaignCount: 0,
      impressions: 0,
      clicks: 0,
      conversions: 0,
    });
    assert.match(action, /Launch or enable a campaign/i);
  });
});

describe('SPEC-PENNY-GADS-004 — buildGoogleAdsOperatorBrief', () => {
  it('computes CTR and cost per conversion', () => {
    const brief = briefWithMetrics({
      spend: 1.43,
      impressions: 120,
      clicks: 3,
      platformConversions: 2,
      campaignCount: 1,
    });
    assert.equal(brief.title, 'Paid Acquisition — Google Ads');
    assert.equal(brief.accountStatus, 'READY');
    assert.equal(brief.spend, 1.43);
    assert.equal(brief.impressions, 120);
    assert.equal(brief.clicks, 3);
    assert.equal(brief.conversions, 2);
    assert.equal(brief.activeCampaignCount, 1);
    assert.ok(Math.abs(brief.ctr - (3 / 120) * 100) < 0.001);
    assert.ok(Math.abs(brief.costPerConversion - 1.43 / 2) < 0.001);
    assert.match(brief.recommendedNextAction, /conversion signal/i);
    assert.match(brief.text, /Paid Acquisition — Google Ads/);
  });

  it('cost per conversion is null when conversions = 0', () => {
    const brief = briefWithMetrics({
      spend: 5,
      impressions: 200,
      clicks: 4,
      platformConversions: 0,
      campaignCount: 1,
    });
    assert.equal(brief.costPerConversion, null);
  });

  it('CTR is 0 when impressions = 0', () => {
    const brief = briefWithMetrics({
      spend: 0,
      impressions: 0,
      clicks: 0,
      platformConversions: 0,
      campaignCount: 1,
    });
    assert.equal(brief.ctr, 0);
  });
});
