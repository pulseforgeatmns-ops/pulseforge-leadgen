'use strict';

/**
 * SPEC-252 — Normalized paid platform evidence types.
 */

const AVAILABILITY = Object.freeze({
  AVAILABLE: 'AVAILABLE',
  UNAVAILABLE: 'UNAVAILABLE',
  ERROR: 'ERROR',
});

const UNAVAILABLE_REASON = Object.freeze({
  PLATFORM_ADAPTER_NOT_IMPLEMENTED: 'PLATFORM_ADAPTER_NOT_IMPLEMENTED',
  NO_LINKED_ACCOUNT: 'NO_LINKED_ACCOUNT',
  MISSING_CREDENTIALS: 'MISSING_CREDENTIALS',
  MISSING_ENV_CREDENTIALS: 'MISSING_ENV_CREDENTIALS',
  API_ERROR: 'API_ERROR',
});

const PLATFORM = Object.freeze({
  GOOGLE_ADS: 'google_ads',
  META_ADS: 'meta_ads',
  CHATGPT_ADS: 'chatgpt_ads',
  YELP: 'yelp',
});

const CHANNEL_BY_PLATFORM = Object.freeze({
  [PLATFORM.GOOGLE_ADS]: 'Google Search',
  [PLATFORM.META_ADS]: 'Meta',
  [PLATFORM.CHATGPT_ADS]: 'ChatGPT Ads',
  [PLATFORM.YELP]: 'Yelp',
});

const PLATFORM_BY_CHANNEL = Object.freeze({
  'Google Search': PLATFORM.GOOGLE_ADS,
  Meta: PLATFORM.META_ADS,
  'ChatGPT Ads': PLATFORM.CHATGPT_ADS,
  Yelp: PLATFORM.YELP,
});

function platformProvenance(platform, extra = {}) {
  return {
    sourceKind: 'PLATFORM_API',
    source: platform,
    observedAt: new Date().toISOString(),
    readOnly: true,
    ...extra,
  };
}

function unavailableEvidence(platform, reason, details = {}) {
  const channel = CHANNEL_BY_PLATFORM[platform] || platform;
  return {
    spec: 'SPEC-252',
    platform,
    channel,
    availability: AVAILABILITY.UNAVAILABLE,
    reason,
    account: null,
    observationWindow: null,
    campaigns: [],
    keywords: [],
    provenance: {
      sourceKind: 'PLATFORM_READ',
      source: platform,
      observedAt: new Date().toISOString(),
      readOnly: true,
    },
    ...details,
  };
}

module.exports = {
  AVAILABILITY,
  UNAVAILABLE_REASON,
  PLATFORM,
  CHANNEL_BY_PLATFORM,
  PLATFORM_BY_CHANNEL,
  platformProvenance,
  unavailableEvidence,
};
