'use strict';
const axios = require('axios');
const { crawlWebsite, normalizeDomain, resolveEnrichmentDomain } = require('../utils/websiteEnrichmentCrawl');
const { isLocationInMissionGeography, resolveMissionAllowedCities } = require('../utils/missionGeography');
const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');

// Observe official-site business facts only for local, otherwise unresolved
// candidates. Search wording is never accepted as business or location evidence.
async function acquireBusinessEvidence(candidate, context, { fetchPage } = {}) {
  const domain = resolveEnrichmentDomain({ domain: candidate.domain, website: candidate.website });
  const allowedCities = resolveMissionAllowedCities(context);
  if (!domain || !allowedCities.length || !isLocationInMissionGeography({
    location: candidate.location || candidate.address, city: candidate.city, allowedCities,
  })) return null;
  const fetcher = fetchPage || (async url => {
    const response = await axios.get(url, { timeout: 5000, maxContentLength: 1500000,
      maxRedirects: 3, headers: { 'User-Agent': 'Mozilla/5.0' }, validateStatus: () => true });
    return { ok: response.status === 200, status: response.status, text: response.data,
      url: response.request?.res?.responseURL || url };
  });
  const { pages } = await crawlWebsite(domain, fetcher, { maxSuccessfulPages: 3, maxRequests: 6 });
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

    return { candidate: observed, admission, evidence: { source: 'company_website', source_url: page.url,
      observed_at: new Date().toISOString(), quote: statement.slice(0, 400), vertical: admission.vertical } };
  }
  return null;
}
module.exports = { acquireBusinessEvidence };
