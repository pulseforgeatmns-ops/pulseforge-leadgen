'use strict';

/**
 * Prospect Discovery + Discovery Profile types (SPEC-024).
 */

const DISCOVERY_PROGRESS_STAGES = Object.freeze({
  SEARCHING: 'Searching...',
  FILTERING: 'Filtering...',
  VERIFYING: 'Verifying...',
  RANKING: 'Ranking...',
  COMPLETED: 'Completed',
});

const SIGNAL_WEIGHTS = Object.freeze({
  HIGH: 0.9,
  MEDIUM: 0.55,
  LOW: 0.3,
  NEGATIVE: -0.7,
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildDiscoveryProfile(partial = {}) {
  const version = partial.version != null ? String(partial.version) : '1.0';
  return {
    id: String(partial.id || ''),
    name: String(partial.name || ''),
    description: String(partial.description || ''),
    tenantId: partial.tenantId != null ? String(partial.tenantId) : null,
    clientIds: Array.isArray(partial.clientIds)
      ? partial.clientIds.map((c) => Number(c) || c)
      : [],
    industryTargets: Array.isArray(partial.industryTargets)
      ? partial.industryTargets.map(String)
      : [],
    geography: normalizeGeography(partial.geography),
    targetCount: Number.isFinite(Number(partial.targetCount))
      ? Number(partial.targetCount)
      : 50,
    requiredSignals: Array.isArray(partial.requiredSignals)
      ? partial.requiredSignals.map(String)
      : [],
    preferredSignals: Array.isArray(partial.preferredSignals)
      ? partial.preferredSignals.map(String)
      : [],
    excludedSignals: Array.isArray(partial.excludedSignals)
      ? partial.excludedSignals.map(String)
      : [],
    rankingWeights:
      partial.rankingWeights && typeof partial.rankingWeights === 'object'
        ? { ...partial.rankingWeights }
        : defaultRankingWeights(),
    minimumConfidence: Number.isFinite(Number(partial.minimumConfidence))
      ? Number(partial.minimumConfidence)
      : 0.75,
    deduplicationRules:
      partial.deduplicationRules && typeof partial.deduplicationRules === 'object'
        ? { ...partial.deduplicationRules }
        : { byWebsite: true, byNameAddress: true, respectTenant: true, skipCrmDuplicates: true },
    reviewPolicy:
      partial.reviewPolicy && typeof partial.reviewPolicy === 'object'
        ? { ...partial.reviewPolicy }
        : { returnLowConfidenceForReview: true, autoApprove: false },
    version,
    status: partial.status || 'active',
    parentId: partial.parentId || null,
    createdAt: partial.createdAt || new Date().toISOString(),
    updatedAt: partial.updatedAt || new Date().toISOString(),
  };
}

function defaultRankingWeights() {
  return {
    target_industry: SIGNAL_WEIGHTS.HIGH,
    commercial_office: SIGNAL_WEIGHTS.HIGH,
    professional_services: SIGNAL_WEIGHTS.MEDIUM,
    multi_location: SIGNAL_WEIGHTS.MEDIUM,
    active_website: SIGNAL_WEIGHTS.HIGH,
    verified_address: SIGNAL_WEIGHTS.HIGH,
    hiring_activity: SIGNAL_WEIGHTS.MEDIUM,
    professional_branding: SIGNAL_WEIGHTS.MEDIUM,
    recurring_facility_ops: SIGNAL_WEIGHTS.MEDIUM,
    residential_only: SIGNAL_WEIGHTS.NEGATIVE,
    generic_residential_cleaner: SIGNAL_WEIGHTS.NEGATIVE,
    missing_website: SIGNAL_WEIGHTS.NEGATIVE,
    closed_business: SIGNAL_WEIGHTS.NEGATIVE,
    existing_customer: SIGNAL_WEIGHTS.NEGATIVE,
    existing_prospect: SIGNAL_WEIGHTS.NEGATIVE,
  };
}

function normalizeGeography(geo) {
  if (!geo) {
    return { label: '', cities: [], state: null, radiusMiles: null };
  }
  if (typeof geo === 'string') {
    return { label: geo, cities: [], state: null, radiusMiles: null };
  }
  return {
    label: String(geo.label || ''),
    cities: Array.isArray(geo.cities) ? geo.cities.map(String) : [],
    state: geo.state != null ? String(geo.state) : null,
    radiusMiles:
      geo.radiusMiles != null && Number.isFinite(Number(geo.radiusMiles))
        ? Number(geo.radiusMiles)
        : null,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildProspect(partial = {}) {
  return {
    id: String(partial.id || ''),
    companyName: String(partial.companyName || ''),
    website: partial.website != null ? String(partial.website) : null,
    industry: partial.industry != null ? String(partial.industry) : null,
    address: partial.address != null ? String(partial.address) : null,
    phone: partial.phone != null ? String(partial.phone) : null,
    confidence: Number.isFinite(Number(partial.confidence))
      ? Number(partial.confidence)
      : 0,
    evidence: Array.isArray(partial.evidence) ? partial.evidence : [],
    rankingSignals: Array.isArray(partial.rankingSignals) ? partial.rankingSignals : [],
    discoveryReason: partial.discoveryReason != null ? String(partial.discoveryReason) : '',
    status: partial.status || 'verified',
    placeId: partial.placeId || null,
    source: partial.source || null,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildProspectDiscoveryResult(partial = {}) {
  return {
    prospects: Array.isArray(partial.prospects) ? partial.prospects : [],
    summary: {
      discovered: Number(partial.summary?.discovered) || 0,
      verified: Number(partial.summary?.verified) || 0,
      rejected: Number(partial.summary?.rejected) || 0,
      targetCount: Number(partial.summary?.targetCount) || 0,
      companiesSearched: Number(partial.summary?.companiesSearched) || 0,
    },
    evidence: Array.isArray(partial.evidence) ? partial.evidence : [],
    warnings: Array.isArray(partial.warnings) ? partial.warnings.map(String) : [],
    confidence: Number.isFinite(Number(partial.confidence))
      ? Number(partial.confidence)
      : 0,
    discoveryProfile: partial.discoveryProfile || null,
    reviewPackage: partial.reviewPackage || null,
    rejected: Array.isArray(partial.rejected) ? partial.rejected : [],
    suggestedNextActions: Array.isArray(partial.suggestedNextActions)
      ? partial.suggestedNextActions
      : [],
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildRankingSignal(partial = {}) {
  return {
    signal: String(partial.signal || ''),
    weight: Number.isFinite(Number(partial.weight)) ? Number(partial.weight) : 0,
    matched: partial.matched !== false,
    profileId: partial.profileId != null ? String(partial.profileId) : null,
    profileName: partial.profileName != null ? String(partial.profileName) : null,
    profileVersion: partial.profileVersion != null ? String(partial.profileVersion) : null,
    detail: partial.detail != null ? String(partial.detail) : '',
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildDiscoveryEvidence(partial = {}) {
  return {
    whySelected: String(partial.whySelected || ''),
    sources: Array.isArray(partial.sources) ? partial.sources.map(String) : [],
    confidence: Number.isFinite(Number(partial.confidence))
      ? Number(partial.confidence)
      : 0,
    timestamp: partial.timestamp || new Date().toISOString(),
    discoveryMethod: String(partial.discoveryMethod || 'places'),
    profileId: partial.profileId || null,
    profileVersion: partial.profileVersion || null,
  };
}

module.exports = {
  DISCOVERY_PROGRESS_STAGES,
  SIGNAL_WEIGHTS,
  buildDiscoveryProfile,
  buildProspect,
  buildProspectDiscoveryResult,
  buildRankingSignal,
  buildDiscoveryEvidence,
  defaultRankingWeights,
  normalizeGeography,
};
