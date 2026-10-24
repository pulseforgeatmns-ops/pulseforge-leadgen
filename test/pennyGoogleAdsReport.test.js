'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { flagsFromGoogleEvidence, googleAdsReportData, buildGoogleAdsDailyReport } = require('../packages/penny-paid-acquisition/googleAdsDailyReport');

function evidence() {
  return {
    account: { currency: 'USD', timezone: 'America/New_York' },
    observationWindow: { start: '2026-10-13', end: '2026-10-19', days: 7 },
    provenance: { observedAt: '2026-10-19T17:13:11Z' },
    campaigns: [
      { name: 'Old PMax', status: 'PAUSED', advertisingChannelType: 'PERFORMANCE_MAX', impressions: 16003, clicks: 257, ctr: 0.016, spend: 99.25, platformConversions: 0, budget: { amount: 32 } },
      { name: 'Residential Search', status: 'ENABLED', advertisingChannelType: 'SEARCH', impressions: 20, clicks: 3, ctr: 0.15, spend: 20.42, platformConversions: 0, budget: { amount: 25 } },
    ],
  };
}

describe('Penny Google Ads evidence reports', () => {
  it('keeps paused PMax history separate from the small active Search sample', () => {
    const data = googleAdsReportData(evidence());
    assert.equal(data.campaigns[0].status, 'PAUSED');
    assert.deepEqual(data.flags, []);
    const report = buildGoogleAdsDailyReport('Anchor Cleaning', data);
    assert.match(report, /Old PMax \[PAUSED; PERFORMANCE_MAX\]/);
    assert.match(report, /Residential Search \[ENABLED; SEARCH\]/);
    assert.match(report, /small traffic sample/);
    assert.match(report, /spend \$99.25/);
    assert.match(report, /through 2026-10-19/);
    assert.match(report, /America\/New_York/);
    assert.doesNotMatch(report, /raise|reallocate|pause it|under-pacing|over budget|landing page immediately/i);
    assert.match(report, /not verified leads, bookings, or revenue/);
  });

  it('flags low CTR only for enabled Search with enough impressions and known CTR', () => {
    const sample = evidence();
    const campaign = sample.campaigns[1];
    Object.assign(campaign, { impressions: 500, clicks: 1, ctr: 0.002 });
    assert.equal(flagsFromGoogleEvidence(sample).length, 1);
    campaign.status = 'PAUSED';
    assert.deepEqual(flagsFromGoogleEvidence(sample), []);
    campaign.status = 'ENABLED';
    campaign.ctr = null;
    assert.deepEqual(flagsFromGoogleEvidence(sample), []);
  });

  it('preserves missing conversions as unavailable rather than reporting zero', () => {
    const sample = evidence();
    sample.campaigns[1].platformConversions = null;
    const report = buildGoogleAdsDailyReport('Anchor Cleaning', googleAdsReportData(sample));
    assert.match(report, /Google-reported conversions unavailable/);
    assert.match(report, /conversion metrics are unavailable, not zero/);
  });

  it('retains known low Quality Scores for active Search without flagging paused campaigns', () => {
    const sample = evidence();
    sample.keywords = [
      { text: 'house cleaning', campaignName: 'Residential Search', qualityScore: 3 },
      { text: 'maid service', campaignName: 'Residential Search', qualityScore: null },
      { text: 'cleaning', campaignName: 'Old PMax', qualityScore: 2 },
    ];
    const flags = flagsFromGoogleEvidence(sample);
    assert.equal(flags.length, 1);
    assert.match(flags[0], /Low Quality Score \(3\/10\)/);
    assert.match(flags[0], /house cleaning/);
  });

  it('calls for conversion verification rather than asserting a cause after more traffic', () => {
    const sample = evidence();
    Object.assign(sample.campaigns[1], { impressions: 300, clicks: 30 });
    const report = buildGoogleAdsDailyReport('Anchor Cleaning', googleAdsReportData(sample));
    assert.match(report, /Check the selected conversion action against actual form submissions/);
    assert.doesNotMatch(report, /small traffic sample/);
  });
});
