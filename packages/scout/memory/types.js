'use strict';

/**
 * SPEC-143 — Scout Acquisition Intelligence Memory types.
 * Scout owns durable acquisition intelligence that compounds across investigations.
 */

const MEMORY_TYPES = Object.freeze({
  MARKET: 'market',
  COMPANY: 'company',
  PERSON: 'person',
  CLAIM: 'claim',
  INVESTIGATION: 'investigation',
});

const MEMORY_STATUS = Object.freeze({
  ACTIVE: 'active',
  CONFLICT: 'conflict',
  STALE: 'stale',
  SUPERSEDED: 'superseded',
});

const STARTING_POINT_BUCKETS = Object.freeze({
  KNOWN: 'known',
  UNKNOWN: 'unknown',
  NEED_TO_VERIFY: 'need_to_verify',
  NEED_TO_DISCOVER: 'need_to_discover',
});

const DEFAULT_FRESHNESS_HALF_LIFE_DAYS = 90;
const DEFAULT_STALE_THRESHOLD_DAYS = 180;
const DEFAULT_MIN_CONFIDENCE = 0.5;

function nowIso() {
  return new Date().toISOString();
}

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function normalizeKey(value) {
  return asText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '');
}

function buildMemoryId(type, tenantId, entityKey) {
  return `${type}:${tenantId}:${entityKey}`;
}

function buildMarketMemory(partial = {}) {
  return {
    id: partial.id || buildMemoryId(MEMORY_TYPES.MARKET, partial.tenantId, partial.entityKey),
    type: MEMORY_TYPES.MARKET,
    tenantId: asText(partial.tenantId),
    entityKey: asText(partial.entityKey),
    label: asText(partial.label) || asText(partial.geography),
    geography: asText(partial.geography),
    segment: asText(partial.segment),
    knownIndustries: Array.isArray(partial.knownIndustries) ? partial.knownIndustries : [],
    marketSize: partial.marketSize != null ? partial.marketSize : null,
    buyingBehavior: partial.buyingBehavior || null,
    seasonality: partial.seasonality || null,
    competition: partial.competition || null,
    coverage: partial.coverage || null,
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    verifiedAt: partial.verifiedAt || nowIso(),
    sourceCount: partial.sourceCount != null ? Number(partial.sourceCount) : 1,
    verificationSources: Array.isArray(partial.verificationSources)
      ? partial.verificationSources
      : [],
    status: partial.status || MEMORY_STATUS.ACTIVE,
    missionId: asText(partial.missionId) || null,
    updatedAt: partial.updatedAt || nowIso(),
  };
}

function buildCompanyMemory(partial = {}) {
  return {
    id: partial.id || buildMemoryId(MEMORY_TYPES.COMPANY, partial.tenantId, partial.entityKey),
    type: MEMORY_TYPES.COMPANY,
    tenantId: asText(partial.tenantId),
    entityKey: asText(partial.entityKey),
    companyId: asText(partial.companyId),
    label: asText(partial.label) || asText(partial.name),
    name: asText(partial.name),
    industry: asText(partial.industry),
    location: asText(partial.location),
    website: asText(partial.website),
    knownOffices: Array.isArray(partial.knownOffices) ? partial.knownOffices : [],
    decisionMakers: Array.isArray(partial.decisionMakers) ? partial.decisionMakers : [],
    cleaningVendors: Array.isArray(partial.cleaningVendors) ? partial.cleaningVendors : [],
    buyingSignals: Array.isArray(partial.buyingSignals) ? partial.buyingSignals : [],
    evidence: Array.isArray(partial.evidence) ? partial.evidence : [],
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    verifiedAt: partial.verifiedAt || nowIso(),
    sourceCount: partial.sourceCount != null ? Number(partial.sourceCount) : 1,
    verificationSources: Array.isArray(partial.verificationSources)
      ? partial.verificationSources
      : [],
    status: partial.status || MEMORY_STATUS.ACTIVE,
    missionId: asText(partial.missionId) || null,
    updatedAt: partial.updatedAt || nowIso(),
  };
}

function buildPersonMemory(partial = {}) {
  return {
    id: partial.id || buildMemoryId(MEMORY_TYPES.PERSON, partial.tenantId, partial.entityKey),
    type: MEMORY_TYPES.PERSON,
    tenantId: asText(partial.tenantId),
    entityKey: asText(partial.entityKey),
    personId: asText(partial.personId),
    companyId: asText(partial.companyId),
    label: asText(partial.label) || asText(partial.name),
    name: asText(partial.name),
    jobTitle: asText(partial.jobTitle),
    preferredChannel: asText(partial.preferredChannel) || null,
    responseHistory: Array.isArray(partial.responseHistory) ? partial.responseHistory : [],
    relationshipHistory: Array.isArray(partial.relationshipHistory)
      ? partial.relationshipHistory
      : [],
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    verifiedAt: partial.verifiedAt || nowIso(),
    sourceCount: partial.sourceCount != null ? Number(partial.sourceCount) : 1,
    verificationSources: Array.isArray(partial.verificationSources)
      ? partial.verificationSources
      : [],
    status: partial.status || MEMORY_STATUS.ACTIVE,
    missionId: asText(partial.missionId) || null,
    updatedAt: partial.updatedAt || nowIso(),
  };
}

function buildClaimMemory(partial = {}) {
  return {
    id: partial.id || buildMemoryId(MEMORY_TYPES.CLAIM, partial.tenantId, partial.entityKey),
    type: MEMORY_TYPES.CLAIM,
    tenantId: asText(partial.tenantId),
    entityKey: asText(partial.entityKey),
    claimId: asText(partial.claimId),
    entityId: asText(partial.entityId),
    entityType: asText(partial.entityType) || 'company',
    text: asText(partial.text),
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    verified: partial.verified === true,
    evidence: Array.isArray(partial.evidence) ? partial.evidence : [],
    supportedBy: Array.isArray(partial.supportedBy) ? partial.supportedBy : [],
    missingEvidence: Array.isArray(partial.missingEvidence) ? partial.missingEvidence : [],
    contradictions: Array.isArray(partial.contradictions) ? partial.contradictions : [],
    verifiedAt: partial.verifiedAt || nowIso(),
    sourceCount: partial.sourceCount != null ? Number(partial.sourceCount) : 1,
    verificationSources: Array.isArray(partial.verificationSources)
      ? partial.verificationSources
      : [],
    status: partial.status || MEMORY_STATUS.ACTIVE,
    missionId: asText(partial.missionId) || null,
    updatedAt: partial.updatedAt || nowIso(),
  };
}

function buildInvestigationMemory(partial = {}) {
  return {
    id:
      partial.id ||
      buildMemoryId(MEMORY_TYPES.INVESTIGATION, partial.tenantId, partial.entityKey),
    type: MEMORY_TYPES.INVESTIGATION,
    tenantId: asText(partial.tenantId),
    entityKey: asText(partial.entityKey),
    marketKey: asText(partial.marketKey),
    geography: asText(partial.geography),
    segment: asText(partial.segment),
    attemptedSteps: Array.isArray(partial.attemptedSteps) ? partial.attemptedSteps : [],
    resolvedGaps: Array.isArray(partial.resolvedGaps) ? partial.resolvedGaps : [],
    remainingGaps: Array.isArray(partial.remainingGaps) ? partial.remainingGaps : [],
    sourceChain: Array.isArray(partial.sourceChain) ? partial.sourceChain : [],
    investigationPlan: partial.investigationPlan || null,
    providerLearning: partial.providerLearning || null,
    overallConfidence:
      partial.overallConfidence != null ? Number(partial.overallConfidence) : 0,
    verifiedAt: partial.verifiedAt || nowIso(),
    sourceCount: partial.sourceCount != null ? Number(partial.sourceCount) : 1,
    status: partial.status || MEMORY_STATUS.ACTIVE,
    missionId: asText(partial.missionId) || null,
    updatedAt: partial.updatedAt || nowIso(),
  };
}

function marketEntityKey(geography, segment) {
  return normalizeKey(`${geography || 'unknown'}:${segment || 'general'}`);
}

function companyEntityKey(company) {
  if (!company) return 'unknown';
  const id = asText(company.id || company.companyId);
  if (id) return normalizeKey(id);
  const name = asText(company.name);
  const website = asText(company.website);
  return normalizeKey(name || website || 'unknown');
}

function personEntityKey(person) {
  if (!person) return 'unknown';
  const id = asText(person.id || person.personId);
  if (id) return normalizeKey(id);
  const name = asText(person.name);
  const companyId = asText(person.companyId);
  return normalizeKey(`${companyId}:${name}`);
}

function claimEntityKey(claim) {
  const entityId = asText(claim.entityId || 'global');
  const text = normalizeKey(claim.text || claim.id || 'claim');
  return `${entityId}:${text}`;
}

module.exports = {
  MEMORY_TYPES,
  MEMORY_STATUS,
  STARTING_POINT_BUCKETS,
  DEFAULT_FRESHNESS_HALF_LIFE_DAYS,
  DEFAULT_STALE_THRESHOLD_DAYS,
  DEFAULT_MIN_CONFIDENCE,
  nowIso,
  asText,
  normalizeKey,
  buildMemoryId,
  buildMarketMemory,
  buildCompanyMemory,
  buildPersonMemory,
  buildClaimMemory,
  buildInvestigationMemory,
  marketEntityKey,
  companyEntityKey,
  personEntityKey,
  claimEntityKey,
};
