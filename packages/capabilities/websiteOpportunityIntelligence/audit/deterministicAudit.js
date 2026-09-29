'use strict';

const axios = require('axios');
const { EVIDENCE_CLASS, buildFinding } = require('../types');
const { fetchPageSpeedMetrics } = require('./pagespeedProvider');
const { observeDomStructure } = require('./domObserver');

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname.replace(/^www\./i, '').toLowerCase();
  } catch {
    return raw.replace(/^https?:\/\//i, '').replace(/^www\./i, '').split(/[/?#]/)[0].toLowerCase() || null;
  }
}

function buildUrl(domain, path = '/') {
  const d = normalizeDomain(domain);
  if (!d) return null;
  return `https://${d}${path.startsWith('/') ? path : `/${path}`}`;
}

async function fetchHtml(url, deps = {}) {
  const fetchImpl = deps.fetchImpl || axios;
  const started = Date.now();
  const res = await fetchImpl.get(url, {
    timeout: deps.timeoutMs || 15000,
    maxRedirects: 5,
    validateStatus: () => true,
    headers: { 'User-Agent': 'PulseForge-WebsiteAudit/1.0 (read-only research)' },
  });
  return {
    status: res.status,
    headers: res.headers || {},
    html: typeof res.data === 'string' ? res.data : '',
    loadMs: Date.now() - started,
  };
}

function parseMetaAndTechnical(html, url, fetchResult) {
  const findings = [];
  const observedAt = new Date().toISOString();

  if (url.startsWith('https://')) {
    findings.push(buildFinding({
      id: 'tech_https',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'technical_health',
      summary: 'Site served over HTTPS',
      source: 'http_fetch',
      observed_at: observedAt,
      ref: 'technical:https',
    }));
  } else {
    findings.push(buildFinding({
      id: 'tech_no_https',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'technical_health',
      summary: 'Homepage URL is not HTTPS',
      source: 'http_fetch',
      observed_at: observedAt,
      ref: 'technical:no_https',
    }));
  }

  if (fetchResult.status >= 400) {
    findings.push(buildFinding({
      id: 'tech_http_error',
      evidence_class: EVIDENCE_CLASS.MEASURED,
      category: 'technical_health',
      summary: `Homepage HTTP status ${fetchResult.status}`,
      measurement: { status_code: fetchResult.status },
      source: 'http_fetch',
      observed_at: observedAt,
      ref: 'technical:http_status',
    }));
  } else {
    findings.push(buildFinding({
      id: 'tech_http_ok',
      evidence_class: EVIDENCE_CLASS.MEASURED,
      category: 'technical_health',
      summary: `Homepage HTTP status ${fetchResult.status}`,
      measurement: { status_code: fetchResult.status },
      source: 'http_fetch',
      observed_at: observedAt,
      ref: 'technical:http_status',
    }));
  }

  const titleMatch = html.match(/<title[^>]*>([^<]*)<\/title>/i);
  if (!titleMatch || !titleMatch[1].trim()) {
    findings.push(buildFinding({
      id: 'seo_missing_title',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'technical_health',
      summary: 'Missing or empty document title',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'seo:title',
    }));
  }

  const viewport = /<meta[^>]+name=["']viewport["']/i.test(html);
  if (!viewport) {
    findings.push(buildFinding({
      id: 'mobile_no_viewport',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'technical_health',
      summary: 'Missing viewport meta tag',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'mobile:viewport',
    }));
  }

  const desc = html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["']/i);
  if (!desc || !desc[1].trim()) {
    findings.push(buildFinding({
      id: 'seo_missing_description',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'technical_health',
      summary: 'Missing meta description',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'seo:description',
    }));
  }

  const imgs = [...html.matchAll(/<img\b[^>]*>/gi)];
  let missingAlt = 0;
  for (const tag of imgs) {
    if (!/\balt\s*=\s*["'][^"']+["']/i.test(tag[0])) missingAlt++;
  }
  if (missingAlt > 0) {
    findings.push(buildFinding({
      id: 'a11y_missing_alt',
      evidence_class: EVIDENCE_CLASS.MEASURED,
      category: 'accessibility',
      summary: `${missingAlt} image(s) missing non-empty alt text on homepage`,
      measurement: { missing_alt_count: missingAlt, total_images: imgs.length },
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'a11y:img_alt',
    }));
  }

  if (!/<html[^>]*\blang\s*=/i.test(html)) {
    findings.push(buildFinding({
      id: 'a11y_missing_lang',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'accessibility',
      summary: 'HTML element missing lang attribute',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'a11y:lang',
    }));
  }

  findings.push(buildFinding({
    id: 'perf_fetch_time',
    evidence_class: EVIDENCE_CLASS.MEASURED,
    category: 'performance',
    summary: `Homepage fetch completed in ${fetchResult.loadMs} ms during audit`,
    measurement: { fetch_ms: fetchResult.loadMs },
    source: 'http_fetch',
    observed_at: observedAt,
    ref: 'performance:fetch_ms',
  }));

  return findings;
}

function parseConversionStructure(html) {
  const findings = [];
  const observedAt = new Date().toISOString();
  const lower = html.toLowerCase();

  const hasTel = /href\s*=\s*["']tel:/i.test(html);
  const hasMailto = /href\s*=\s*["']mailto:/i.test(html);
  const hasForm = /<form\b/i.test(html);
  const hasContactLink = /href\s*=\s*["'][^"']*contact/i.test(html);
  const hasBook = /book(?:ing)?|schedule|appointment/i.test(lower);

  if (hasTel) {
    findings.push(buildFinding({
      id: 'conv_phone_link',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'conversion_structure',
      summary: 'Phone link present on homepage',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'conversion:phone',
    }));
  }
  if (hasMailto) {
    findings.push(buildFinding({
      id: 'conv_email_link',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'conversion_structure',
      summary: 'Email mailto link present on homepage',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'conversion:email',
    }));
  }
  if (hasForm) {
    findings.push(buildFinding({
      id: 'conv_form',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'conversion_structure',
      summary: 'Form element present on homepage',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'conversion:form',
    }));
  }
  if (hasContactLink) {
    findings.push(buildFinding({
      id: 'conv_contact_nav',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'conversion_structure',
      summary: 'Contact link present in homepage markup',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'conversion:contact_link',
    }));
  }
  if (hasBook) {
    findings.push(buildFinding({
      id: 'conv_booking_language',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'conversion_structure',
      summary: 'Booking/scheduling language present on homepage',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'conversion:booking',
    }));
  }

  if (!hasTel && !hasMailto && !hasForm && !hasContactLink) {
    findings.push(buildFinding({
      id: 'conv_no_obvious_path',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'conversion_structure',
      summary: 'No obvious phone, email, form, or contact link detected on homepage',
      source: 'html_parse',
      observed_at: observedAt,
      ref: 'conversion:none_detected',
    }));
  }

  return findings;
}

async function headCheck(url, deps = {}) {
  const fetchImpl = deps.fetchImpl || axios;
  try {
    const res = await fetchImpl.head(url, {
      timeout: deps.timeoutMs || 10000,
      validateStatus: () => true,
      headers: { 'User-Agent': 'PulseForge-WebsiteAudit/1.0 (read-only research)' },
    });
    return res.status;
  } catch {
    return null;
  }
}

async function checkSitemapRobots(domain, deps = {}) {
  const findings = [];
  const observedAt = new Date().toISOString();
  for (const [kind, path] of [['robots', '/robots.txt'], ['sitemap', '/sitemap.xml']]) {
    const url = buildUrl(domain, path);
    const status = await headCheck(url, deps);
    if (status === 200) {
      findings.push(buildFinding({
        id: `seo_${kind}_present`,
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'technical_health',
        summary: `${kind}.txt/xml returned HTTP 200`,
        measurement: { status_code: status, path },
        source: 'http_head',
        observed_at: observedAt,
        ref: `seo:${kind}`,
      }));
    } else if (status != null) {
      findings.push(buildFinding({
        id: `seo_${kind}_missing`,
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'technical_health',
        summary: `${path} returned HTTP ${status}`,
        measurement: { status_code: status, path },
        source: 'http_head',
        observed_at: observedAt,
        ref: `seo:${kind}`,
      }));
    }
  }
  return findings;
}

/**
 * Deterministic website audit — read-only, no form submissions.
 * @param {string} domain
 * @param {object} [deps]
 */
async function runDeterministicAudit(domain, deps = {}) {
  const normalized = normalizeDomain(domain);
  if (!normalized) {
    return {
      domain: domain || '',
      audited_at: new Date().toISOString(),
      findings: [],
      technical_evidence: { error: 'invalid_domain' },
      error: 'invalid_domain',
    };
  }

  if (deps.fixtureAudit) {
    return deps.fixtureAudit(normalized, deps);
  }

  const url = buildUrl(normalized);
  const findings = [];
  let html = '';
  let fetchResult = { status: 0, loadMs: 0, headers: {} };

  try {
    fetchResult = await fetchHtml(url, deps);
    html = fetchResult.html || '';
    findings.push(...parseMetaAndTechnical(html, url, fetchResult));
    findings.push(...parseConversionStructure(html));
    findings.push(...await checkSitemapRobots(normalized, deps));
  } catch (err) {
    findings.push(buildFinding({
      id: 'fetch_failed',
      evidence_class: EVIDENCE_CLASS.UNKNOWN,
      category: 'technical_health',
      summary: 'Homepage fetch failed during audit',
      detail: String(err.message || err).slice(0, 200),
      source: 'http_fetch',
      observed_at: new Date().toISOString(),
      ref: 'technical:fetch_failed',
    }));
  }

  const pagespeed = await fetchPageSpeedMetrics(normalized, deps);
  if (pagespeed.findings.length) findings.push(...pagespeed.findings);
  const psiTelemetry = pagespeed.telemetry || {
    psi_attempted: 0,
    psi_success: 0,
    psi_failed: 0,
    psi_unknown: 0,
  };

  if (deps.usePuppeteer !== false && html) {
    const domFindings = await observeDomStructure(url, deps);
    findings.push(...domFindings);
  }

  const byCategory = {};
  for (const f of findings) {
    if (!byCategory[f.category]) byCategory[f.category] = [];
    byCategory[f.category].push(f);
  }

  return {
    domain: normalized,
    audited_at: new Date().toISOString(),
    findings,
    technical_evidence: {
      performance: byCategory.performance || [],
      accessibility: byCategory.accessibility || [],
      technical_health: byCategory.technical_health || [],
      conversion_structure: byCategory.conversion_structure || [],
      pagespeed: pagespeed.raw || null,
      psi_telemetry: psiTelemetry,
    },
    psi_telemetry: psiTelemetry,
  };
}

module.exports = {
  normalizeDomain,
  buildUrl,
  runDeterministicAudit,
  parseMetaAndTechnical,
  parseConversionStructure,
  fetchHtml,
};
