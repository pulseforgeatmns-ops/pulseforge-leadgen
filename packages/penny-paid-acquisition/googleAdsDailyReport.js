'use strict';

// Google Ads reports use measured evidence directly. Campaign status and small
// samples must survive the reporting layer; neither implies a budget change.
function flagsFromGoogleEvidence(evidence) {
  const flags = [];
  const activeSearchNames = new Set();
  for (const campaign of evidence.campaigns || []) {
    if (campaign.status !== 'ENABLED' || campaign.advertisingChannelType !== 'SEARCH') continue;
    activeSearchNames.add(campaign.name);
    if (campaign.impressions > 100 && campaign.ctr != null && campaign.ctr < 0.01) {
      flags.push(`"${campaign.name}": Search CTR ${(campaign.ctr * 100).toFixed(2)}% below 1% (${campaign.impressions} impressions). Review search terms and ad relevance before proposing changes.`);
    }
  }
  for (const keyword of evidence.keywords || []) {
    if (activeSearchNames.has(keyword.campaignName) && keyword.qualityScore > 0 && keyword.qualityScore < 5) {
      flags.push(`Low Quality Score (${keyword.qualityScore}/10): "${keyword.text}" in "${keyword.campaignName}". Review relevance before proposing changes.`);
    }
  }
  return flags;
}

function googleAdsReportData(evidence) {
  return {
    observed_at: evidence.provenance?.observedAt || evidence.readiness?.lastSuccessfulReadAt || null,
    observation_window: evidence.observationWindow || null,
    currency: evidence.account?.currency || null,
    account_timezone: evidence.account?.timezone || null,
    campaigns: (evidence.campaigns || []).map((campaign) => ({
      name: campaign.name,
      status: campaign.status || 'UNKNOWN',
      type: campaign.advertisingChannelType || 'UNKNOWN',
      impressions: campaign.impressions,
      clicks: campaign.clicks,
      ctr: campaign.ctr == null ? null : campaign.ctr * 100,
      avg_cpc: campaign.averageCpc,
      conversions: campaign.platformConversions,
      spend: campaign.spend,
      daily_budget: campaign.budget?.amount ?? null,
    })),
    flags: flagsFromGoogleEvidence(evidence),
  };
}

function amount(value, currency) {
  if (value == null || !Number.isFinite(Number(value))) return 'unavailable';
  const formatted = Number(value).toFixed(2);
  return currency === 'USD' ? `$${formatted}` : `${formatted} ${currency || '(currency unavailable)'}`;
}

function metric(value) {
  return value == null || !Number.isFinite(Number(value)) ? 'unavailable' : String(value);
}

function campaignLine(campaign, currency) {
  const ctr = campaign.ctr == null ? 'unavailable' : `${Number(campaign.ctr).toFixed(2)}%`;
  return `- ${campaign.name} [${campaign.status}; ${campaign.type}]: ${metric(campaign.impressions)} impressions, ${metric(campaign.clicks)} clicks, CTR ${ctr}, spend ${amount(campaign.spend, currency)}, Google-reported conversions ${metric(campaign.conversions)}. Current average daily budget: ${amount(campaign.daily_budget, currency)}.`;
}

function buildGoogleAdsDailyReport(companyName, data) {
  const campaigns = data.campaigns || [];
  const active = campaigns.filter((campaign) => campaign.status === 'ENABLED');
  const historical = campaigns.filter((campaign) => campaign.status !== 'ENABLED');
  const lines = [
    `# ${companyName} — Google Ads Evidence Report`,
    'Prepared by Penny, Pulseforge · Read and recommend',
    '',
    '**CONNECTION AND REPORTING WINDOW**',
    'Live Google Ads evidence read succeeded.',
  ];
  if (data.observed_at) lines.push(`Read at: ${data.observed_at}`);
  if (data.observation_window?.start && data.observation_window?.end) {
    lines.push(`Date filter: ${data.observation_window.start} through ${data.observation_window.end}.`);
  }
  if (data.account_timezone) lines.push(`Google Ads account time zone: ${data.account_timezone}.`);
  lines.push('', '**CURRENTLY ENABLED CAMPAIGNS**');
  lines.push(...(active.length ? active.map((campaign) => campaignLine(campaign, data.currency)) : ['- No currently enabled campaigns returned.']));
  if (historical.length) {
    lines.push('', '**PAUSED OR UNKNOWN STATUS — HISTORICAL WINDOW DATA**');
    lines.push(...historical.map((campaign) => campaignLine(campaign, data.currency)));
    lines.push('Current status is separate from date-window totals. Historical traffic does not mean a paused campaign is still serving.');
  }
  lines.push('', '**NEEDS ATTENTION**');
  if (!active.length) lines.push('- Verify campaign status in Google Ads before making delivery decisions.');
  for (const campaign of active) {
    if (campaign.impressions == null || campaign.clicks == null) {
      lines.push(`- ${campaign.name}: traffic metrics are unavailable; verify the data before judging performance.`);
    } else if (campaign.impressions < 100 || campaign.clicks < 15) {
      lines.push(`- ${campaign.name}: small traffic sample. Keep monitoring at the approved budget; these totals alone do not establish a delivery, tracking, or landing-page failure. Campaign age and time enabled are not available in this report.`);
    } else if (campaign.conversions === 0) {
      lines.push(`- ${campaign.name}: Google reported zero conversions in this window. Check the selected conversion action against actual form submissions before attributing the result to tracking, traffic quality, or the landing page.`);
    }
    if (campaign.conversions == null) {
      lines.push(`- ${campaign.name}: conversion metrics are unavailable, not zero.`);
    }
  }
  lines.push(...(data.flags || []).map((flag) => `- ${flag}`));
  lines.push(
    '- Google-reported conversions are platform signals, not verified leads, bookings, or revenue.',
    '- An average daily budget is not a required daily spend or a seven-day cap. Lower spend alone does not justify a budget increase. Budget history and daily delivery were not inspected.',
    '',
    '**CLIENT SUMMARY**',
    `The connection is working. This read returned ${active.length} currently enabled campaign(s) and ${historical.length} paused or unknown-status campaign(s). Review each campaign separately and allow a new or low-volume campaign to gather evidence. Any proposed campaign or budget change requires operator approval.`,
  );
  return lines.join('\n');
}

module.exports = { flagsFromGoogleEvidence, googleAdsReportData, buildGoogleAdsDailyReport };
