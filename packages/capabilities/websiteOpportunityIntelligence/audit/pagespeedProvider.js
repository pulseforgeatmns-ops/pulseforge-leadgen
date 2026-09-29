'use strict';

const axios = require('axios');
const { EVIDENCE_CLASS, buildFinding } = require('../types');

function initPsiTelemetry() {
  return {
    psi_attempted: 0,
    psi_success: 0,
    psi_failed: 0,
    psi_unknown: 0,
  };
}

async function fetchPageSpeedMetrics(domain, deps = {}) {
  const apiKey = deps.pagespeedApiKey || process.env.GOOGLE_API_KEY || process.env.GOOGLE_PLACES_KEY;
  const findings = [];
  const raw = { mobile: null, desktop: null, skipped: null };
  const telemetry = initPsiTelemetry();

  if (!apiKey || deps.skipPageSpeed) {
    raw.skipped = deps.skipPageSpeed ? 'skipped_by_caller' : 'no_api_key';
    telemetry.psi_unknown = 1;
    findings.push(buildFinding({
      id: 'psi_unavailable',
      evidence_class: EVIDENCE_CLASS.UNKNOWN,
      category: 'performance',
      summary: deps.skipPageSpeed
        ? 'PageSpeed Insights skipped by caller'
        : 'PageSpeed Insights unavailable (no API key configured)',
      detail: raw.skipped,
      source: 'pagespeed_insights',
      observed_at: new Date().toISOString(),
      ref: 'pagespeed:unavailable',
    }));
    return { findings, raw, telemetry };
  }

  const fetchImpl = deps.fetchImpl || axios;
  for (const strategy of ['mobile', 'desktop']) {
    telemetry.psi_attempted += 1;
    try {
      const url = `https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url=${encodeURIComponent(`https://${domain}`)}&strategy=${strategy}&category=performance&category=accessibility&key=${apiKey}`;
      const res = await fetchImpl.get(url, { timeout: deps.timeoutMs || 45000 });
      const lighthouse = res.data?.lighthouseResult;
      if (!lighthouse?.categories) {
        telemetry.psi_failed += 1;
        raw[`${strategy}_error`] = 'empty_lighthouse_result';
        findings.push(buildFinding({
          id: `psi_failed_${strategy}`,
          evidence_class: EVIDENCE_CLASS.UNKNOWN,
          category: 'performance',
          summary: `PageSpeed Insights returned unusable ${strategy} result`,
          detail: 'empty_lighthouse_result',
          source: 'pagespeed_insights',
          observed_at: new Date().toISOString(),
          ref: `pagespeed:failed:${strategy}`,
        }));
        continue;
      }

      raw[strategy] = lighthouse.categories;
      const audits = lighthouse.audits || {};
      const perfScore = lighthouse.categories?.performance?.score;
      let strategySuccess = false;

      if (perfScore != null) {
        strategySuccess = true;
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
          strategySuccess = true;
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
      const a11yScore = lighthouse.categories?.accessibility?.score;
      if (a11yScore != null) {
        strategySuccess = true;
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

      if (strategySuccess) {
        telemetry.psi_success += 1;
      } else {
        telemetry.psi_failed += 1;
        findings.push(buildFinding({
          id: `psi_failed_${strategy}`,
          evidence_class: EVIDENCE_CLASS.UNKNOWN,
          category: 'performance',
          summary: `PageSpeed Insights ${strategy} run returned no usable metrics`,
          detail: 'no_usable_metrics',
          source: 'pagespeed_insights',
          observed_at: new Date().toISOString(),
          ref: `pagespeed:failed:${strategy}`,
        }));
      }
    } catch (err) {
      telemetry.psi_failed += 1;
      const message = String(err.message || err).slice(0, 200);
      raw[`${strategy}_error`] = message;
      findings.push(buildFinding({
        id: `psi_failed_${strategy}`,
        evidence_class: EVIDENCE_CLASS.UNKNOWN,
        category: 'performance',
        summary: `PageSpeed Insights ${strategy} request failed`,
        detail: message,
        source: 'pagespeed_insights',
        observed_at: new Date().toISOString(),
        ref: `pagespeed:failed:${strategy}`,
      }));
    }
  }

  if (telemetry.psi_success === 0 && telemetry.psi_failed > 0 && telemetry.psi_unknown === 0) {
    telemetry.psi_unknown = telemetry.psi_failed;
  }

  return { findings, raw, telemetry };
}

module.exports = {
  fetchPageSpeedMetrics,
  initPsiTelemetry,
};
