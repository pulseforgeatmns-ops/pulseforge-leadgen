'use strict';
const { tenant } = require('./common');
const DISCLAIMER = 'Aggregate correlation is not causation. Device, page and campaign attribution are not established by this digest. Cross-source date labels may match while reporting timezone alignment remains unverified.';
const numeric = value => typeof value === 'number' && Number.isFinite(value);
function observed(label, value, unit = '') { return { label, value: value == null ? 'Unavailable' : `${value}${unit}` }; }
function matchedWindow(value, report) { return value?.start === report.window_start && value?.end === report.window_end; }
function summarizeCrossEvidence({ clientId, report, googleAds, firstParty } = {}) {
  const cid = tenant(clientId);
  // Callers pass tenant-bound observations, not browser payloads. Window mismatch stays unknown.
  const ads = googleAds?.client_id === cid && googleAds?.live?.available === true
    && matchedWindow(googleAds.live.window,report) ? googleAds.live : null;
  const leads = firstParty?.client_id === cid && firstParty.availability === 'AVAILABLE'
    && matchedWindow(firstParty.observationWindow,report) ? firstParty : null;
  const campaigns = ads?.campaigns || [];
  const sumKnown = key => campaigns.length && campaigns.every(c => numeric(c[key])) ? campaigns.reduce((n,c) => n+c[key],0) : null;
  return {
    window_start: report.window_start, window_end: report.window_end,
    google_ads: { available: !!ads, clicks: ads ? sumKnown('clicks') : null,
      spend: ads ? sumKnown('spend') : null, platform_conversions: ads ? sumKnown('conversions') : null,
      currency: ads?.currency || null, observed_at: ads?.observedAt || null,
      source: 'existing_google_ads_readonly_adapter', reason: ads ? null : 'unavailable_or_window_mismatch' },
    first_party: { available: !!leads, attributed_walkthrough_leads: leads ? leads.observedCount : null,
      source: 'tenant_scoped_agent_actions_walkthrough_attribution',
      observed_at: leads?.provenance?.observedAt || null,
      truncated: leads ? leads.observedCount >= 500 : false,
      reason: leads ? null : 'unavailable_or_window_mismatch' },
    limitation: DISCLAIMER,
  };
}
function buildRecommendations({ report, history = [], crossEvidence = {} }) {
  const m = report.metrics, d = report.deltas_pct || {};
  const previous = history.filter(r => r.id !== report.id && r.window_end < report.window_start)
    .sort((a,b) => b.window_end.localeCompare(a.window_end))[0];
  const adjacent = previous && new Date(`${report.window_start}T00:00:00Z`) - new Date(`${previous.window_end}T00:00:00Z`) === 86400000;
  const baseEvidence = [
    { label: 'Reporting window', value: `${report.window_start} through ${report.window_end}` },
    observed('Sessions',m.sessions),
    { label: 'Source', value: `${report.synthetic ? 'Synthetic test fixture; not live evidence. ' : ''}Gmail message ${report.provenance.gmail_message_id}; parser ${report.provenance.parser_version}` },
  ];
  const confidence = numeric(m.sessions) && m.sessions >= 100 ? 'moderate' : 'low';
  const result = [];
  const add = (key,title,summary,evidence,validation_steps) => result.push({ key,title,summary,
    confidence, evidence: [...baseEvidence,...evidence], validation_steps,
    expected_outcome: 'A measured, reversible test plan for a separate implementation review.',
    limitation: DISCLAIMER, approval_scope: 'Recommendation review only. No website changes or deployments are authorized.' });
  if (numeric(m.scroll_depth) && (m.scroll_depth < 20 || (numeric(d.scroll_depth) && d.scroll_depth <= -20))) {
    add('above_fold','Investigate above-the-fold engagement',
      'Low scroll depth warrants reviewing the first screen and CTA visibility. Mobile friction is a hypothesis, not an observed device-specific finding.',
      [observed('Scroll depth',m.scroll_depth,'%'), observed('Reported weekly scroll change',d.scroll_depth,'%'),
        observed('Previous observed scroll depth',adjacent ? previous.metrics.scroll_depth : null,'%'),
        observed('Session duration',m.session_duration,' seconds')],
      ['Inspect Clarity recordings and heatmaps segmented by device and landing page; confirm whether mobile sessions are affected.',
        'Check first-screen loading, offer clarity and CTA visibility before proposing changes.',
        'Propose a controlled test with scroll and qualified lead conversion guardrails. Obtain separate implementation and deployment approval.']);
  }
  const frictions = [['js_errors','JavaScript errors'],['rage_clicks','Rage clicks'],['dead_clicks','Dead clicks'],['quick_backs','Quick backs']]
    .filter(([k]) => numeric(m[k]) && m[k] > 0);
  if (frictions.length) {
    add('interaction_friction','Validate reported interaction friction',
      'Recorded friction may identify nonresponsive controls or navigation confusion. Aggregate percentages do not identify the responsible element.',
      frictions.map(([key,label]) => observed(label,m[key],'%')),
      ['Review affected recordings and reproduce the interaction on the reported device/browser.',
        'Confirm an actionable control or reproducible error before proposing a fix.',
        'Compare the same friction rate and lead-quality guardrails after a separately approved change.']);
  }
  const sessionsUp = numeric(d.sessions) && d.sessions > 0;
  const engagementDown = (numeric(d.scroll_depth) && d.scroll_depth < 0) || (numeric(d.session_duration) && d.session_duration < 0);
  if ((sessionsUp && engagementDown) || crossEvidence.google_ads?.available || crossEvidence.first_party?.available) {
    add('traffic_quality','Compare traffic quality and conversion evidence',
      'Compare acquisition mix with engagement over the same reporting window. Do not attribute an engagement change to paid traffic without segment-level evidence.',
      [observed('Reported session change',d.sessions,'%'),observed('Pages per session',m.pages_per_session),
        observed('Google Ads clicks',crossEvidence.google_ads?.clicks),
        observed('Google Ads platform conversions',crossEvidence.google_ads?.platform_conversions),
        observed('First-party attributed walkthrough leads',crossEvidence.first_party?.truncated ? `At least ${crossEvidence.first_party.attributed_walkthrough_leads} (retrieval capped)` : crossEvidence.first_party?.attributed_walkthrough_leads),
        { label: 'Attribution limit', value: 'Platform conversions and first-party leads are different measurements; never add them or imply a shared denominator.' }],
      ['Compare device, landing-page and acquisition-source cohorts for matching dates.',
        'Verify conversion instrumentation and first-party lead quality before judging campaign or website performance.',
        'Propose one controlled experiment; retain a baseline and avoid claiming causation from weekly aggregates.']);
  }
  if (!result.length) add('baseline','Build a comparable engagement baseline',
    'No strong actionable deterioration is established by the available digest. Preserve the baseline and verify measurement coverage before changing the website.',
    [observed('Pages per session',m.pages_per_session),observed('Session duration',m.session_duration,' seconds')],
    ['Collect the next comparable weekly digest and verify report coverage.', 'Review any missing metrics and validate conversion instrumentation before proposing a change.']);
  return result.slice(0,3);
}
module.exports = { buildRecommendations, summarizeCrossEvidence, matchedWindow, DISCLAIMER };
