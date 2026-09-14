'use strict';

/**
 * Canonical outbound email eligibility contract.
 * Verification alone does not make inferred pattern addresses sendable.
 */

const { invalidOutreachEmailReason } = require('./emailGuard');

const VERIFIED_EMAIL_STATUSES = new Set(['valid', 'verified']);

const CONTAMINATED_EMAIL_DOMAINS = new Set([
  'linkedin.com',
  'facebook.com',
  'fb.com',
  'instagram.com',
  'twitter.com',
  'x.com',
  'tiktok.com',
  'snapchat.com',
  'youtube.com',
  'pinterest.com',
  'reddit.com',
  'yelp.com',
  'avvo.com',
  'findlaw.com',
  'lawyers.com',
  'martindale.com',
  'justia.com',
  'superlawyers.com',
  'zoominfo.com',
  'rocketreach.co',
  'yellowpages.com',
  'bbb.org',
  'alignable.com',
  'nextdoor.com',
  'google.com',
  'bing.com',
  'wikipedia.org',
  'bit.ly',
  't.co',
  'goo.gl',
  'tinyurl.com',
  'ow.ly',
]);

const SOCIAL_PROFILE_DOMAINS = new Set([
  'linkedin.com',
  'facebook.com',
  'fb.com',
  'instagram.com',
  'twitter.com',
  'x.com',
  'tiktok.com',
  'snapchat.com',
  'youtube.com',
  'pinterest.com',
  'reddit.com',
]);

const DIRECTORY_DOMAINS = new Set([
  'yelp.com',
  'avvo.com',
  'findlaw.com',
  'lawyers.com',
  'martindale.com',
  'justia.com',
  'superlawyers.com',
  'zoominfo.com',
  'rocketreach.co',
  'yellowpages.com',
  'bbb.org',
  'alignable.com',
  'nextdoor.com',
]);

const URL_SHORTENER_DOMAINS = new Set([
  'bit.ly',
  't.co',
  'goo.gl',
  'tinyurl.com',
  'ow.ly',
]);

const HOSTED_BUILDER_SUFFIXES = [
  '.webnode.page',
  '.wixsite.com',
  '.squarespace.com',
  '.wordpress.com',
  '.weebly.com',
  '.godaddysites.com',
  '.sites.google.com',
];

const PERSONAL_EMAIL_PROVIDER_DOMAINS = new Set([
  'gmail.com',
  'yahoo.com',
  'hotmail.com',
  'outlook.com',
  'aol.com',
  'icloud.com',
  'live.com',
  'msn.com',
  'comcast.net',
  'verizon.net',
  'att.net',
  'sbcglobal.net',
  'cox.net',
  'charter.net',
  'bellsouth.net',
  'me.com',
  'mac.com',
  'ymail.com',
  'rocketmail.com',
  'proton.me',
  'protonmail.com',
  'tutanota.com',
  'gmx.com',
  'fastmail.com',
]);

const OBSERVED_EMAIL_SOURCE_PREFIXES = [
  'existing_crm',
  'existing_prospect_email',
  'existing_bouncer_verified_email',
  'website_email',
  'hunter',
  'prospeo',
  'scraped',
  'provider_chain',
  'tier0_email_localpart',
];

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/^www\./i, '')
      .toLowerCase();
  } catch {
    const domain = raw
      .replace(/^https?:\/\//i, '')
      .replace(/^www\./i, '')
      .split(/[/?#\s]/)[0]
      .replace(/[.,;:]+$/g, '')
      .toLowerCase();
    return domain || null;
  }
}

function emailDomain(email) {
  return String(email || '').trim().toLowerCase().split('@')[1] || '';
}

function hostMatchesSet(domain, set) {
  if (!domain) return false;
  if (set.has(domain)) return true;
  for (const blocked of set) {
    if (domain === blocked || domain.endsWith(`.${blocked}`)) return true;
  }
  return false;
}

function isContaminatedEmailDomain(domain) {
  return hostMatchesSet(normalizeDomain(domain), CONTAMINATED_EMAIL_DOMAINS);
}

function isPersonalEmailProviderDomain(domain) {
  return PERSONAL_EMAIL_PROVIDER_DOMAINS.has(normalizeDomain(domain));
}

function isHostedBuilderDomain(domain) {
  const normalized = normalizeDomain(domain);
  if (!normalized) return false;
  return HOSTED_BUILDER_SUFFIXES.some((suffix) => normalized.endsWith(suffix));
}

function classifyCompanyUrl(url) {
  const domain = normalizeDomain(url);
  if (!domain) return 'unknown';
  if (hostMatchesSet(domain, SOCIAL_PROFILE_DOMAINS)) return 'social_profile';
  if (hostMatchesSet(domain, DIRECTORY_DOMAINS)) return 'directory';
  if (hostMatchesSet(domain, URL_SHORTENER_DOMAINS)) return 'url_shortener';
  if (isHostedBuilderDomain(domain)) return 'hosted_builder';
  return 'official';
}

/**
 * Prefer an official company domain for enrichment; never return social/directory hosts.
 * @param {object|null} row
 * @returns {string|null}
 */
function resolveOfficialEnrichmentDomain(row) {
  const candidates = [
    { value: row?.website, field: 'website' },
    { value: row?.website_url, field: 'website_url' },
    { value: row?.domain, field: 'domain' },
  ].filter((entry) => entry.value);

  const ranked = [];
  for (const entry of candidates) {
    const domain = normalizeDomain(entry.value);
    if (!domain || isContaminatedEmailDomain(domain)) continue;
    const classification = classifyCompanyUrl(entry.value);
    if (classification === 'social_profile' || classification === 'directory' || classification === 'url_shortener') {
      continue;
    }
    const priority = classification === 'official' ? 0 : 1;
    ranked.push({ domain, priority, field: entry.field });
  }

  ranked.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    return candidates.findIndex((entry) => entry.field === a.field) - candidates.findIndex((entry) => entry.field === b.field);
  });
  return ranked[0]?.domain || null;
}

function isInferredPatternProvenance(source) {
  const normalized = String(source || '').trim().toLowerCase();
  return normalized.startsWith('pattern_');
}

function normalizeProvenanceSource(source) {
  return String(source || '').trim().toLowerCase();
}

function isObservedEmailProvenance(source) {
  const normalized = normalizeProvenanceSource(source);
  if (!normalized) return false;
  if (isInferredPatternProvenance(normalized)) return false;
  return OBSERVED_EMAIL_SOURCE_PREFIXES.some((prefix) => normalized === prefix || normalized.startsWith(`${prefix}+`) || normalized.includes(`+${prefix}`) || normalized.endsWith(`+${prefix}`));
}

function resolveEmailProvenanceSource(row = {}) {
  if (row.email_provenance_source) return normalizeProvenanceSource(row.email_provenance_source);
  if (row.verificationSource) return normalizeProvenanceSource(row.verificationSource);
  const provenanceSource = row.enrichment_provenance?.email?.source;
  if (provenanceSource) return normalizeProvenanceSource(provenanceSource);
  return null;
}

/**
 * True when an observed email on a company page may be kept during website scrape.
 * @param {string} email
 * @param {string|null} enrichmentDomain
 * @returns {boolean}
 */
function isAllowedObservedWebsiteEmail(email, enrichmentDomain) {
  const domain = emailDomain(email);
  const normalizedEnrichmentDomain = normalizeDomain(enrichmentDomain);
  if (!domain || invalidOutreachEmailReason(email)) return false;
  if (isContaminatedEmailDomain(domain)) return false;
  if (normalizedEnrichmentDomain && domain === normalizedEnrichmentDomain) return true;
  return isPersonalEmailProviderDomain(domain);
}

/**
 * Canonical outbound eligibility beyond syntax/verification/DNC.
 * Returns null when eligible, otherwise a machine-readable reason.
 * @param {object|null} row
 * @returns {string|null}
 */
function canonicalOutboundEmailIneligibilityReason(row) {
  if (!row || typeof row !== 'object') return 'missing_row';
  if (row.do_not_contact === true) return 'do_not_contact';

  const email = String(row.email || '').trim();
  if (!email || invalidOutreachEmailReason(email)) return 'invalid_outreach_email';
  if (row.email_verified !== true) return 'email_not_verified';

  const status = String(row.email_status || '').toLowerCase();
  if (!VERIFIED_EMAIL_STATUSES.has(status)) return 'email_status_not_verified';

  if (isContaminatedEmailDomain(emailDomain(email))) return 'contaminated_email_domain';

  const provenance = resolveEmailProvenanceSource(row);
  if (isInferredPatternProvenance(provenance)) return 'inferred_pattern_provenance';
  if (provenance && !isObservedEmailProvenance(provenance)) return 'unobserved_provenance';

  return null;
}

function isCanonicallyOutboundEligible(row) {
  return canonicalOutboundEmailIneligibilityReason(row) === null;
}

function isSendableVerifiedCandidate(candidate = {}) {
  if (!candidate.verified) return false;
  if (isContaminatedEmailDomain(emailDomain(candidate.email))) return false;
  if (isInferredPatternProvenance(candidate.source)) return false;
  return true;
}

module.exports = {
  VERIFIED_EMAIL_STATUSES,
  CONTAMINATED_EMAIL_DOMAINS,
  PERSONAL_EMAIL_PROVIDER_DOMAINS,
  classifyCompanyUrl,
  resolveOfficialEnrichmentDomain,
  isContaminatedEmailDomain,
  isPersonalEmailProviderDomain,
  isHostedBuilderDomain,
  isInferredPatternProvenance,
  isObservedEmailProvenance,
  resolveEmailProvenanceSource,
  isAllowedObservedWebsiteEmail,
  canonicalOutboundEmailIneligibilityReason,
  isCanonicallyOutboundEligible,
  isSendableVerifiedCandidate,
  normalizeDomain,
  emailDomain,
};
