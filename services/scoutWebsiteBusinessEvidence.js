'use strict';
const axios = require('axios');
const { crawlWebsite, normalizeDomain, resolveEnrichmentDomain } = require('../utils/websiteEnrichmentCrawl');
const { isLocationInMissionGeography, resolveMissionAllowedCities } = require('../utils/missionGeography');
const { evaluateReplenishmentAdmission, isFounderLedSmallBusinessMission } = require('../utils/replenishmentVertical');
const { isSmallBusinessOwnerSegment } = require('../utils/canonicalBusinessTaxonomy');
const BUSINESS_EVIDENCE_VERSION = 'scout-website-business-evidence-v2';

// Only known transient failures may authorize a later bounded retry. Provider
// prose, URLs and exception messages never become durable outcome reasons.
function responseFailure(response, requestedUrl, domain) {
  if (normalizeDomain(response?.url || requestedUrl) !== domain) return { reason: 'offsite_redirect', retryable: false };
  const status = response?.status;
  if ([408, 425, 429].includes(status) || (Number.isInteger(status) && status >= 500 && status <= 599)) {
    return { reason: 'transient_http_failure', retryable: true };
  }
  if ([401, 403].includes(status)) return { reason: 'http_access_denied', retryable: false };
  if ([404, 410].includes(status)) return { reason: 'http_not_found', retryable: false };
  if (response?.ok === false || (status != null && status >= 400)) return { reason: 'http_failure', retryable: false };
  if (!/html|text/i.test(String(response?.text ?? '').slice(0, 300))) return { reason: 'invalid_page_content', retryable: false };
  return null;
}
function exceptionFailure(error) {
  if (error?.name === 'AbortError' || ['ETIMEDOUT', 'ECONNABORTED'].includes(error?.code)) {
    return { reason: 'fetch_timeout', retryable: true };
  }
  if (['ECONNRESET', 'ECONNREFUSED', 'EAI_AGAIN', 'ENETUNREACH', 'EHOSTUNREACH'].includes(error?.code)) {
    return { reason: 'transient_network_failure', retryable: true };
  }
  return { reason: 'unknown_fetch_failure', retryable: false };
}

// Observe official-site business facts only for local, otherwise unresolved
// candidates. Search wording is never accepted as business or location evidence.
// reportOutcome:true returns { outcome, result, reason, crawl }, with retryable
// only for fetch_failed. It never schedules retries. A caller must bound them;
// no_evidence, ineligible and retryable:false must await a changed dependency.
async function acquireBusinessEvidence(candidate, context, { fetchPage, reportOutcome = false } = {}) {
  let crawl = { attempts: 0, successfulPages: 0, partial: false };
  const finish = (outcome, result = null, reason = 'no_supported_business_evidence', extra = {}) =>
    reportOutcome === true ? { outcome, result, reason, crawl, ...extra } : result;
  const domain = resolveEnrichmentDomain({ domain: candidate.domain, website: candidate.website });
  const allowedCities = resolveMissionAllowedCities(context);
  if (!domain) return finish('ineligible', null, 'no_official_domain');
  if (!allowedCities.length) return finish('ineligible', null, 'missing_allowed_cities');
  if (!isLocationInMissionGeography({
    location: candidate.location || candidate.address, city: candidate.city, allowedCities,
  })) return finish('ineligible', null, 'outside_mission_geography');
  const fetcher = fetchPage || (async url => {
    const response = await axios.get(url, { timeout: 5000, maxContentLength: 1500000,
      maxRedirects: 3, headers: { 'User-Agent': 'Mozilla/5.0' }, validateStatus: () => true });
    return { ok: response.status === 200, status: response.status, text: response.data,
      url: response.request?.res?.responseURL || url };
  });
  const failures = [];
  const observedFetcher = reportOutcome === true ? async url => {
    try {
      const response = await fetcher(url);
      const failure = responseFailure(response, url, domain);
      if (failure) failures.push(failure);
      return response;
    } catch (error) {
      failures.push(exceptionFailure(error));
      throw error;
    }
  } : fetcher;
  const { pages, errors, fetchCounts } = await crawlWebsite(domain, observedFetcher, { maxSuccessfulPages: 3, maxRequests: 6 });
  crawl = { attempts: fetchCounts.size, successfulPages: pages.length, partial: pages.length > 0 && errors.length > 0 };
  // A favorable page cannot hide an unavailable contrary page. Preserve legacy
  // return semantics by applying this stricter completeness check only opt-in.
  // Even a guessed-path 404 holds the attempt; do not claim complete evidence.
  if (reportOutcome === true && errors.length) {
    const failure = failures.find(item => !item.retryable) || failures[0]
      || { reason: 'unknown_fetch_failure', retryable: false };
    return finish('fetch_failed', null, failure.reason, { retryable: failure.retryable });
  }
  const smallBusinessEvidenceEnabled = process.env.SCOUT_DURABLE_CANDIDATES_ENABLED === 'true'
    && ['string', 'number'].includes(typeof context.tenantId) && ['10', '13'].includes(String(context.tenantId));
  if (smallBusinessEvidenceEnabled && (isFounderLedSmallBusinessMission(context)
    || isSmallBusinessOwnerSegment(context.missionSegment || context.segment))) {
    const observations = require('../utils/websiteSmallBusinessEvidence').extractSmallBusinessEvidence(pages, domain, context, candidate);
    if (!observations) return finish('no_evidence');
    const { classificationObservations, ...original } = candidate;
    // Preserve every original observation and flattened descriptive fact. Do not
    // revive flattened query labels after their provenance has been separated.
    const { industry, vertical, segment, category, ...descriptive } = original;
    const prior = classificationObservations?.length ? [...classificationObservations, descriptive] : [original];
    const observed = { ...candidate, description: observations[0].quote,
      classificationObservations: [...prior, ...observations.map(item => ({
        description: item.quote, vertical: item.vertical, discoverySource: 'company_website',
      }))] };
    const admission = evaluateReplenishmentAdmission(observed, context);
    if (!admission.admitted) return finish('no_evidence');
    return finish('evidence', { candidate: observed, admission, evidence: { source: 'company_website',
      source_url: observations[0].source_url, observed_at: new Date().toISOString(),
      quote: observations[0].quote, vertical: admission.vertical, observations } }, 'supported_business_evidence');
  }
  for (const page of pages) {
    if (normalizeDomain(page.url) !== domain) continue;
    const text = String(page.text).replace(/<(script|style|nav|footer)\b[^>]*>[\s\S]*?<\/\1>/gi, ' ')
      .replace(/<[^>]+>/g, ' ').replace(/&nbsp;|&#160;/gi, ' ').replace(/&amp;/gi, '&')
      .replace(/\s+/g, ' ').trim().slice(0, 20000);
    const heading = [...String(page.text).matchAll(/<(?:title|h[12])\b[^>]*>([\s\S]*?)<\/(?:title|h[12])>/gi)]
      .map(match => match[1].replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim())
      .find(value => /^(?:(?:residential|commercial|professional|full.service|vacation|rental)\s+)*(?:property management|vacation rental management|short.term rental management)\b/i.test(value));
    const declaration = text.match(/\b(?:we (?:provide|offer|specialize in)|(?:is|are) (?:an? |the )?(?:full.service |professional )?)(?:residential |commercial |and |\s)*(?:property management|vacation rental management|short.term rental management)\b[^.!?]{0,200}/i);
    const statement = heading || declaration?.[0];
    if (!statement) continue;
    const observed = { ...candidate, description: statement };
    const admission = evaluateReplenishmentAdmission(observed, context);
    if (!admission.admitted) continue;

    return finish('evidence', { candidate: observed, admission, evidence: { source: 'company_website', source_url: page.url,
      observed_at: new Date().toISOString(), quote: statement.slice(0, 400), vertical: admission.vertical } }, 'supported_business_evidence');
  }
  return finish('no_evidence');
}
module.exports = { acquireBusinessEvidence, BUSINESS_EVIDENCE_VERSION };
