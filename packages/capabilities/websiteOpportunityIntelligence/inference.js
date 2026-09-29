'use strict';

const { EVIDENCE_CLASS, buildFinding } = require('./types');

function normalizeSummaryText(text) {
  return String(text || '').toLowerCase().replace(/\s+/g, ' ').trim();
}

function isDuplicateOfSource(summary, sourceFindings = []) {
  const norm = normalizeSummaryText(summary);
  if (!norm) return true;
  return sourceFindings.some((f) => {
    const sourceNorm = normalizeSummaryText(f.summary);
    if (!sourceNorm) return false;
    return norm === sourceNorm || norm.includes(sourceNorm) || sourceNorm.includes(norm);
  });
}

function pushInference(inferred, sourceFindings, partial) {
  if (isDuplicateOfSource(partial.summary, sourceFindings)) return;
  inferred.push(buildFinding({
    ...partial,
    evidence_class: EVIDENCE_CLASS.INFERRED,
    source: partial.source || 'inference_engine',
    observed_at: partial.observed_at || new Date().toISOString(),
    derived_from: partial.derived_from || [],
  }));
}

/**
 * Build bounded INFERRED findings from MEASURED/OBSERVED source evidence.
 * Never copies source fact text verbatim.
 */
function buildInferredFindings(findings = []) {
  const measured = findings.filter((f) => f.evidence_class === EVIDENCE_CLASS.MEASURED);
  const observed = findings.filter((f) => f.evidence_class === EVIDENCE_CLASS.OBSERVED);
  const source = [...measured, ...observed];
  const inferred = [];
  const observedAt = new Date().toISOString();

  const fetchMs = measured.find((f) => f.ref === 'performance:fetch_ms' || f.id === 'perf_fetch_time')
    ?.measurement?.fetch_ms;
  if (fetchMs != null && fetchMs >= 3000) {
    pushInference(inferred, source, {
      id: 'inf_slow_fetch_investigation',
      category: 'performance',
      summary: 'The observed homepage response time warrants additional performance investigation before recommending acquisition.',
      detail: `Derived from fetch_ms=${fetchMs}`,
      derived_from: ['performance:fetch_ms'],
      observed_at: observedAt,
    });
  }

  const mobilePerf = measured.find((f) => f.ref === 'pagespeed:performance:mobile');
  const mobileScore = mobilePerf?.measurement?.performance_score;
  if (mobileScore != null && mobileScore < 50) {
    pushInference(inferred, source, {
      id: 'inf_mobile_perf_remediation',
      category: 'performance',
      summary: 'Measured mobile performance suggests remediation may affect lead capture before a full redesign is justified.',
      detail: `Derived from mobile performance_score=${mobileScore}`,
      derived_from: ['pagespeed:performance:mobile'],
      observed_at: observedAt,
    });
  }

  const sitemapMissing = measured.find((f) => f.id === 'seo_sitemap_missing');
  const robotsMissing = measured.find((f) => f.id === 'seo_robots_missing');
  if (sitemapMissing || robotsMissing) {
    pushInference(inferred, source, {
      id: 'inf_seo_crawlability',
      category: 'technical_health',
      summary: 'Missing crawl/discovery files may limit search visibility; targeted SEO fixes may suffice without redesign.',
      detail: 'Derived from robots/sitemap HTTP checks',
      derived_from: [sitemapMissing?.ref, robotsMissing?.ref].filter(Boolean),
      observed_at: observedAt,
    });
  }

  const noConvPath = observed.find((f) => f.id === 'conv_no_obvious_path');
  const noContactNav = observed.find((f) => f.id === 'dom_no_contact_nav');
  if (noConvPath || noContactNav) {
    pushInference(inferred, source, {
      id: 'inf_conversion_path_gap',
      category: 'conversion_structure',
      summary: 'Conversion-path gaps may reduce inbound inquiries; CTA and contact visibility improvements should be evaluated first.',
      detail: 'Derived from conversion structure observations',
      derived_from: [noConvPath?.ref, noContactNav?.ref].filter(Boolean),
      observed_at: observedAt,
    });
  }

  const altMissing = measured.find((f) => f.id === 'a11y_missing_alt');
  if (altMissing?.measurement?.missing_alt_count >= 3) {
    pushInference(inferred, source, {
      id: 'inf_accessibility_remediation',
      category: 'accessibility',
      summary: 'Multiple accessibility gaps on the homepage may warrant remediation; severity should be confirmed before broader redesign.',
      detail: `Derived from missing_alt_count=${altMissing.measurement.missing_alt_count}`,
      derived_from: ['a11y:img_alt'],
      observed_at: observedAt,
    });
  }

  const httpError = measured.find((f) => f.id === 'tech_http_error');
  if (httpError) {
    pushInference(inferred, source, {
      id: 'inf_http_error_reliability',
      category: 'technical_health',
      summary: 'Homepage availability errors suggest reliability issues that should be resolved before evaluating redesign scope.',
      detail: `Derived from status_code=${httpError.measurement?.status_code}`,
      derived_from: ['technical:http_status'],
      observed_at: observedAt,
    });
  }

  const healthySignals = !fetchMs || fetchMs < 2000;
  const hasMajorPerfIssue = mobileScore != null && mobileScore < 50;
  const materialDeficiencies = [sitemapMissing, robotsMissing, noConvPath, httpError, hasMajorPerfIssue]
    .filter(Boolean).length;
  if (healthySignals && materialDeficiencies === 0 && source.length >= 3) {
    pushInference(inferred, source, {
      id: 'inf_healthy_site',
      category: 'diagnosis',
      summary: 'Current deterministic evidence does not support a broad redesign case; monitor or targeted fixes only.',
      detail: 'Derived from absence of material measured/observed deficiencies',
      derived_from: source.slice(0, 5).map((f) => f.ref).filter(Boolean),
      observed_at: observedAt,
    });
  }

  if (materialDeficiencies >= 3) {
    pushInference(inferred, source, {
      id: 'inf_redesign_consideration',
      category: 'diagnosis',
      summary: 'Multiple material website deficiencies together support broader redesign consideration, subject to business economics.',
      detail: `Derived from ${materialDeficiencies} material deficiency signal(s)`,
      derived_from: source.filter((f) =>
        ['performance', 'technical_health', 'conversion_structure'].includes(f.category)
      ).slice(0, 6).map((f) => f.ref).filter(Boolean),
      observed_at: observedAt,
    });
  }

  return inferred;
}

function assertInferredIntegrity(sourceFindings, inferredFindings) {
  for (const inf of inferredFindings) {
    if (inf.evidence_class !== EVIDENCE_CLASS.INFERRED) {
      throw new Error(`Expected INFERRED class on inference finding: ${inf.id}`);
    }
    if (isDuplicateOfSource(inf.summary, sourceFindings)) {
      throw new Error(`INFERRED duplicates source evidence unchanged: ${inf.summary}`);
    }
    if (!Array.isArray(inf.derived_from) || inf.derived_from.length === 0) {
      throw new Error(`INFERRED finding missing derived_from: ${inf.id}`);
    }
  }
  return inferredFindings;
}

module.exports = {
  normalizeSummaryText,
  isDuplicateOfSource,
  buildInferredFindings,
  assertInferredIntegrity,
};
