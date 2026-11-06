'use strict';

/**
 * ADR-092 — Identity Before Enrichment.
 *
 * A business identity is established by independent evidence of existence.
 * Enrichment attributes (website, email) shall never determine whether a business exists.
 */

const IDENTITY_SIGNAL_TYPES = Object.freeze([
  'google_business_profile',
  'place_id',
  'name',
  'address',
  'phone',
  'review_history',
  'government_registry',
  'website',
  'social_profile',
]);

/** Signals that can independently establish existence. Website is intentionally excluded. */
const EXISTENCE_SIGNAL_TYPES = Object.freeze([
  'google_business_profile',
  'place_id',
  'name',
  'address',
  'phone',
  'review_history',
  'government_registry',
  'social_profile',
]);

const ENRICHMENT_SIGNAL_TYPES = Object.freeze(['website']);

function normalizeToken(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname.replace(/^www\./i, '')
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

/**
 * Collect all identity and enrichment signals present on a raw lead row.
 * @param {object} raw
 * @returns {object[]}
 */
function collectIdentitySignals(raw = {}) {
  const signals = [];

  if (raw.place_id || raw.placeId) {
    signals.push({
      type: 'place_id',
      value: String(raw.place_id || raw.placeId),
      weight: 1.0,
    });
  }

  const name = String(raw.company || raw.name || '').trim();
  if (name && name.toLowerCase() !== 'unknown') {
    signals.push({ type: 'name', value: name, weight: 0.6 });
  }

  const address = String(raw.address || raw.formatted_address || '').trim();
  if (address) {
    signals.push({ type: 'address', value: address, weight: 0.7 });
  }

  const phone = String(raw.phone || raw.formatted_phone_number || '').trim();
  if (phone) {
    signals.push({ type: 'phone', value: phone, weight: 0.8 });
  }

  if (raw.google_review_count != null || raw.google_rating != null) {
    signals.push({
      type: 'review_history',
      value: {
        count: raw.google_review_count ?? null,
        rating: raw.google_rating ?? null,
      },
      weight: 0.7,
    });
  }

  if (Array.isArray(raw.source) && raw.source.includes('google_places')) {
    signals.push({ type: 'google_business_profile', value: true, weight: 0.9 });
  }

  const registry = raw.registry_id || raw.government_registry;
  if (registry) {
    signals.push({ type: 'government_registry', value: String(registry), weight: 0.95 });
  }

  const website = raw.url || raw.website || raw.website_url;
  if (website) {
    signals.push({
      type: 'website',
      value: String(website),
      weight: 0.5,
      enrichment: true,
    });
  }

  const social = raw.facebook_url || raw.instagram_url || raw.linkedin_url;
  if (social) {
    signals.push({ type: 'social_profile', value: String(social), weight: 0.6 });
  }

  return signals;
}

/**
 * Build a stable dedupe key from identity signals (prefer Place ID).
 * @param {object[]} signals
 * @returns {string|null}
 */
function buildIdentityKey(signals = []) {
  const byType = new Map(signals.map((row) => [row.type, row]));

  const placeId = byType.get('place_id');
  if (placeId) return `place:${placeId.value}`;

  const name = byType.get('name');
  const address = byType.get('address');
  if (name && address) {
    return `nameaddr:${normalizeToken(name.value)}|${normalizeToken(address.value)}`;
  }

  const phone = byType.get('phone');
  if (name && phone) {
    return `namephone:${normalizeToken(name.value)}|${normalizeToken(phone.value)}`;
  }

  const website = byType.get('website');
  if (website) return `domain:${normalizeDomain(website.value)}`;

  if (name) return `name:${normalizeToken(name.value)}`;
  return null;
}

function computeIdentityConfidence(existenceSignals = []) {
  if (!existenceSignals.length) return 0;
  const sum = existenceSignals.reduce((acc, row) => acc + (row.weight || 0.5), 0);
  return Math.min(1, Number((sum / 2).toFixed(3)));
}

/**
 * Establish whether a business identity exists independent of enrichment.
 * @param {object} raw
 * @returns {object}
 */
function establishBusinessIdentity(raw = {}) {
  const signals = collectIdentitySignals(raw);
  const existenceSignals = signals.filter((row) => EXISTENCE_SIGNAL_TYPES.includes(row.type));
  const enrichmentSignals = signals.filter(
    (row) => ENRICHMENT_SIGNAL_TYPES.includes(row.type) || row.enrichment
  );

  const websiteOnly =
    enrichmentSignals.some((row) => row.type === 'website') && existenceSignals.length === 0;

  // ADR-092: at least one non-website existence signal is required.
  const established = existenceSignals.length > 0 && !websiteOnly;

  return {
    established,
    websiteOnly,
    signals,
    existenceSignals,
    enrichmentSignals,
    identityKey: buildIdentityKey(signals),
    confidence: computeIdentityConfidence(existenceSignals),
  };
}

function leadHasEstablishedIdentity(raw = {}) {
  return establishBusinessIdentity(raw).established;
}

/**
 * SQL twin of normalizeDomain() for stored CRM/queue columns that may hold a
 * bare domain, a www. host or a full URL. `column` must be a trusted SQL
 * expression (never user input). Returns NULL for empty values.
 */
function canonicalDomainSql(column) {
  return `NULLIF(split_part(split_part(split_part(split_part(regexp_replace(lower(btrim(COALESCE(${column},''))),`
    + ` '^([a-z]+://)?(www\\.)?', ''), '/', 1), '?', 1), '#', 1), ':', 1), '')`;
}

const US_STATE_OR_ZIP = /^([a-z]{2}|\d{5}(-\d{4})?|[a-z]{2}\s+\d{5}(-\d{4})?|usa?|united states)$/;
function localityTokens(value) {
  return String(value || '').toLowerCase().split(/[,|;]/)
    .map(part => part.replace(/\s+/g, ' ').trim())
    // A street segment (house number) and state/zip/country segments say
    // nothing about which town the business is in.
    .filter(part => part.length >= 3 && !/\d/.test(part.split(' ')[0] || '') && !US_STATE_OR_ZIP.test(part))
    .map(part => part.replace(/\s+([a-z]{2})(\s+\d{5}(-\d{4})?)?$/, '').trim())
    .filter(Boolean);
}

/**
 * True only when two location strings are both present and share no town.
 * Unknown/partial location is never evidence of a different business.
 */
function localitiesConflict(a, b) {
  const left = localityTokens(a), right = localityTokens(b);
  if (!left.length || !right.length) return false;
  return !left.some(x => right.some(y => x === y || x.includes(y) || y.includes(x)));
}

/**
 * ADR-092 identity rule for a name match: two records with the same name are
 * distinct businesses only when BOTH canonical domains are known and differ
 * AND both localities are known and differ. A name alone never merges them;
 * any missing evidence keeps them matched (fail toward "already known").
 */
function sameNameIsDistinctBusiness(known = {}, candidate = {}) {
  const knownDomains = [known.domain, known.website].map(normalizeDomain).filter(Boolean);
  const candidateDomain = normalizeDomain(candidate.domain || candidate.website);
  if (!knownDomains.length || !candidateDomain || knownDomains.includes(candidateDomain)) return false;
  return localitiesConflict(known.location, candidate.location);
}

module.exports = {
  canonicalDomainSql,
  localitiesConflict,
  sameNameIsDistinctBusiness,
  IDENTITY_SIGNAL_TYPES,
  EXISTENCE_SIGNAL_TYPES,
  ENRICHMENT_SIGNAL_TYPES,
  collectIdentitySignals,
  establishBusinessIdentity,
  buildIdentityKey,
  leadHasEstablishedIdentity,
  normalizeDomain,
};
