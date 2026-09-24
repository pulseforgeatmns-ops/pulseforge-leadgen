'use strict';

const axios = require('axios');
const { EVIDENCE_CLASS, buildFinding } = require('../types');

async function fetchPageSpeedMetrics(domain, deps = {}) {
  const apiKey = deps.pagespeedApiKey || process.env.GOOGLE_API_KEY || process.env.GOOGLE_PLACES_KEY;
  const findings = [];
  const raw = { mobile: null, desktop: null, skipped: null };

  if (!apiKey || deps.skipPageSpeed) {
    raw.skipped = deps.skipPageSpeed ? 'skipped_by_caller' : 'no_api_key';
    findings.push(buildFinding({
      id: 'psi_unavailable',
      evidence_class: EVIDENCE_CLASS.UNKNOWN,
      category: 'performance',
      summary: deps.skipPageSpeed
        ? 'PageSpeed Insights skipped by caller'
        : 'PageSpeed Insights unavailable (no API key configured)',
      source: 'pagespeed_insights',
      observed_at: new Date().toISOString(),
      ref: 'pagespeed:unavailable',
    }));
    return { findings, raw };
  }

  const fetchImpl = deps.fetchImpl || axios;
  for (const strategy of ['mobile', 'desktop']) {
    try {
      const url = `https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url=${encodeURIComponent(`https://${domain}`)}&strategy=${strategy}&category=performance&category=accessibility&key=${apiKey}`;
      const res = await fetchImpl.get(url, { timeout: deps.timeoutMs || 45000 });
      const lighthouse = res.data?.lighthouseResult;
      raw[strategy] = lighthouse?.categories || null;
      const audits = lighthouse?.audits || {};
      const perfScore = lighthouse?.categories?.performance?.score;
      if (perfScore != null) {
        findings.push(buildFinding({
          id: `psi_perf_${strategy}`,
          evidence_class: EVIDENCE_CLASS.MEASURED,
          category: 'performance',
          summary: `${strategy} performance score ${Math.round(perfScore * 100)}/100 (PageSpeed Insights)`,
          measurement: { strategy, performance_score: Math.round(perfScore * 100) },
          source: 'pagespeed_insights',
          observed_at: new Date().toISOString(),
          ref: `pagespeed:performance:${strategy}`,
        }));
      }
      for (const [metric, auditKey] of [
        ['lcp', 'largest-contentful-paint'],
        ['cls', 'cumulative-layout-shift'],
        ['inp', 'interaction-to-next-paint'],
        ['fcp', 'first-contentful-paint'],
      ]) {
        const audit = audits[auditKey];
        if (audit?.numericValue != null) {
          findings.push(buildFinding({
            id: `psi_${metric}_${strategy}`,
            evidence_class: EVIDENCE_CLASS.MEASURED,
            category: 'performance',
            summary: `${strategy} ${metric.toUpperCase()} measured ${audit.displayValue || audit.numericValue}`,
            measurement: {
              strategy,
              metric,
              numeric_value: audit.numericValue,
              display_value: audit.displayValue || null,
            },
            source: 'pagespeed_insights',
            observed_at: new Date().toISOString(),
            ref: `pagespeed:${metric}:${strategy}`,
          }));
        }
      }
      const a11yScore = lighthouse?.categories?.accessibility?.score;
      if (a11yScore != null) {
        findings.push(buildFinding({
          id: `psi_a11y_${strategy}`,
          evidence_class: EVIDENCE_CLASS.MEASURED,
          category: 'accessibility',
          summary: `${strategy} accessibility score ${Math.round(a11yScore * 100)}/100 (PageSpeed Insights)`,
          measurement: { strategy, accessibility_score: Math.round(a11yScore * 100) },
          source: 'pagespeed_insights',
          observed_at: new Date().toISOString(),
          ref: `pagespeed:accessibility:${strategy}`,
        }));
      }
    } catch (err) {
      raw[`${strategy}_error`] = String(err.message || err).slice(0, 200);
    }
  }

  return { findings, raw };
}

module.exports = {
  fetchPageSpeedMetrics,
};
