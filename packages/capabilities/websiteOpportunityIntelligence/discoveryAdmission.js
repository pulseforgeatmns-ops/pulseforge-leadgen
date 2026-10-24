'use strict';

const { classifyCompanyUrl, normalizeDomain } = require('../../../utils/canonicalEmailEligibility');

const SEARCH_ENGINE_DOMAINS = new Set([
  'google.com',
  'bing.com',
  'yahoo.com',
  'duckduckgo.com',
  'search.yahoo.com',
]);

const GENERIC_TITLES = new Set([
  'contact',
  'home',
  'about',
  'services',
  'welcome',
  'index',
  'untitled',
  'website',
  'official site',
]);

const EXTRA_DIRECTORY_DOMAINS = new Set([
  'lawinfo.com',
  'lawyers.com',
  'findlaw.com',
  'superpages.com',
  'manta.com',
  'mapquest.com',
  'chamberofcommerce.com',
  'hotfrog.com',
  'citysearch.com',
  'merchantcircle.com',
]);

function isSearchOrMapsDomain(domain) {
  const d = normalizeDomain(domain);
  if (!d) return true;
  if (d === 'maps.google.com' || d.endsWith('.google.com') && /maps/.test(d)) return true;
  if (SEARCH_ENGINE_DOMAINS.has(d)) return true;
  for (const blocked of SEARCH_ENGINE_DOMAINS) {
    if (d.endsWith(`.${blocked}`)) return true;
  }
  return false;
}

function isDirectoryDomain(domain) {
  const d = normalizeDomain(domain);
  if (!d) return true;
  if (EXTRA_DIRECTORY_DOMAINS.has(d)) return true;
  for (const blocked of EXTRA_DIRECTORY_DOMAINS) {
    if (d === blocked || d.endsWith(`.${blocked}`)) return true;
  }
  return classifyCompanyUrl(`https://${d}`) === 'directory';
}

function isGenericCompanyTitle(name) {
  const norm = String(name || '').trim().toLowerCase();
  if (!norm || norm.length < 3) return true;
  if (GENERIC_TITLES.has(norm)) return true;
  if (/^(contact|home|about)\s*[-–|]?\s*$/i.test(norm)) return true;
  return false;
}

function looksOperational(lead = {}) {
  if (lead.operating_status === 'closed') return false;
  if (lead.permanently_closed === true) return false;
  return true;
}

/**
 * Evaluate whether a Scout discovery candidate may enter a website intelligence cohort.
 * @returns {{ admitted: boolean, reason: string|null, domain: string|null }}
 */
function evaluateCohortAdmission(lead = {}, ctx = {}) {
  const company = lead.company || lead.name || lead.business_name || '';
  const url = lead.url || lead.website || lead.website_url || '';
  const domain = normalizeDomain(url || lead.domain);

  if (!company || isGenericCompanyTitle(company)) {
    return { admitted: false, reason: 'non_identifiable_business_name', domain };
  }
  if (!domain) {
    return { admitted: false, reason: 'missing_canonical_domain', domain: null };
  }
  if (isSearchOrMapsDomain(domain)) {
    return { admitted: false, reason: 'search_or_maps_domain', domain };
  }
  if (isDirectoryDomain(domain)) {
    return { admitted: false, reason: 'directory_or_listing_domain', domain };
  }
  const classification = classifyCompanyUrl(url.includes('://') ? url : `https://${domain}`);
  if (classification === 'social_profile' || classification === 'url_shortener') {
    return { admitted: false, reason: `non_canonical_url_class:${classification}`, domain };
  }
  if (!looksOperational(lead)) {
    return { admitted: false, reason: 'business_not_operational', domain };
  }
  if (ctx.seenDomains?.has(domain)) {
    return { admitted: false, reason: 'duplicate_domain', domain };
  }
  if (ctx.seenCompanies?.has(normalizeCompanyKey(company))) {
    return { admitted: false, reason: 'duplicate_business', domain };
  }

  return { admitted: true, reason: null, domain };
}

function normalizeCompanyKey(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Assemble a stratified cohort from per-stratum candidate pools.
 * Preserves Scout discovery order within each stratum.
 */
function assembleStratifiedCohort(stratumPools, { targetSize = 25, minStrata = 3 } = {}) {
  const admitted = [];
  const rejected = [];
  const composition = {};
  const seenDomains = new Set();
  const seenCompanies = new Set();

  const strata = stratumPools.map((pool) => ({
    ...pool,
    candidates: [...(pool.candidates || [])],
  }));

  let round = 0;
  while (admitted.length < targetSize) {
    let picked = false;
    for (const stratum of strata) {
      if (admitted.length >= targetSize) break;
      if (!stratum.candidates.length) continue;
      const lead = stratum.candidates.shift();
      const evalResult = evaluateCohortAdmission(lead, { seenDomains, seenCompanies });
      if (!evalResult.admitted) {
        rejected.push({
          business: lead.company || lead.name,
          domain: evalResult.domain,
          reason: evalResult.reason,
          stratum: stratum.key,
        });
        continue;
      }
      seenDomains.add(evalResult.domain);
      seenCompanies.add(normalizeCompanyKey(lead.company || lead.name));
      const row = {
        ...lead,
        domain: evalResult.domain,
        discovery: {
          ...(lead.discovery || {}),
          stratum: stratum.key,
          vertical: stratum.vertical,
          location: stratum.location,
          admission_reason: 'accepted',
        },
      };
      admitted.push(row);
      composition[stratum.key] = (composition[stratum.key] || 0) + 1;
      picked = true;
    }
    round += 1;
    if (!picked) break;
    if (round > targetSize * 3) break;
  }

  const uniqueStrata = Object.keys(composition).length;
  return {
    admitted,
    rejected,
    composition,
    stratification_met: uniqueStrata >= minStrata,
    vertical_distribution: summarizeByField(admitted, 'vertical'),
    market_distribution: summarizeByField(admitted, 'location'),
  };
}

function summarizeByField(rows, field) {
  const out = {};
  for (const row of rows) {
    const key = row[field] || row.discovery?.[field] || 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

module.exports = {
  evaluateCohortAdmission,
  assembleStratifiedCohort,
  isSearchOrMapsDomain,
  isDirectoryDomain,
  isGenericCompanyTitle,
  normalizeCompanyKey,
};
