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

  if (raw.place_id) {
    signals.push({ type: 'place_id', value: String(raw.place_id), weight: 1.0 });
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

module.exports = {
  IDENTITY_SIGNAL_TYPES,
  EXISTENCE_SIGNAL_TYPES,
  ENRICHMENT_SIGNAL_TYPES,
  collectIdentitySignals,
  establishBusinessIdentity,
  buildIdentityKey,
  leadHasEstablishedIdentity,
  normalizeDomain,
};
