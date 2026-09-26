'use strict';

const { normalizeVertical } = require('./normalize');
const { configuredServiceAreas } = require('./serviceArea');
const {
  isLocationInMissionGeography,
  resolveMissionAllowedCities,
} = require('./missionGeography');

const ENRICHABLE_SCOUT_VERTICALS = Object.freeze([
  'property_manager',
  'str_manager',
  'commercial_office',
]);

const STR_EVIDENCE = [
  /\bshort[\s-]?term rental\b/i,
  /\bshort[\s-]?term propert/i,
  /\bvacation rental\b/i,
  /\bvacation homes?\b/i,
  /\bholiday rental\b/i,
  /\bairbnb\b/i,
  /\bvrbo\b/i,
  /\bstr portfolio\b/i,
  /\bstr operator\b/i,
  /\bstr manager\b/i,
  /\bstr_manager\b/i,
  /\bvacation propert(?:y|ies) management\b/i,
  /\brental management\b/i,
  /\bguest stay\b/i,
  /\bco-?host(?:ing)?\b/i,
];

const PROPERTY_MANAGER_EVIDENCE = [
  /\bproperty management\b/i,
  /\bproperty manager\b/i,
  /\bproperty_manager\b/i,
  /\bproperty mgmt\b/i,
  /\bprop(?:erty)?\.?\s*mgmt\b/i,
  /\bresidential property management\b/i,
  /\bcommercial property management\b/i,
  /\brental property management\b/i,
  /\bmultifamily management\b/i,
  /\bapartment management\b/i,
  /\bassoc(?:iation)? management\b/i,
  /\bhoa management\b/i,
  /\bcondo(?:minium)? management\b/i,
  /\bleasing (?:office|agent|management)\b/i,
  /\btenant management\b/i,
  /\breal estate management\b/i,
];

const COMMERCIAL_OFFICE_EVIDENCE = [
  /\bcommercial office\b/i,
  /\bcommercial_office\b/i,
  /\boffice park\b/i,
  /\bbusiness center\b/i,
  /\boffice building management\b/i,
];

const STR_PLACE_TYPES = new Set(['lodging', 'extended stay', 'vacation rental']);
const PM_PLACE_TYPES = new Set(['real estate agency']);

const CONTRADICTORY_BUSINESS_TYPES = [
  { pattern: /\bequipment rental\b/i, label: 'equipment rental' },
  { pattern: /\bwaste haul(?:ing|er)?\b/i, label: 'waste hauling' },
  { pattern: /\bjunk removal\b/i, label: 'junk removal' },
  { pattern: /\btrash haul(?:ing|er)?\b/i, label: 'waste hauling' },
  { pattern: /\bconstruction equipment\b/i, label: 'equipment rental' },
  { pattern: /\bdumpster rental\b/i, label: 'waste hauling' },
  { pattern: /\brestoration company\b/i, label: 'restoration/remediation' },
  { pattern: /\bwater damage restoration\b/i, label: 'restoration/remediation' },
  { pattern: /\bhvac\b/i, label: 'HVAC contractor' },
  { pattern: /\broofing\b/i, label: 'roofing contractor' },
  { pattern: /\blandscap(?:e|ing)\b/i, label: 'landscaping contractor' },
  { pattern: /\bplumb(?:er|ing)\b/i, label: 'plumbing contractor' },
  { pattern: /\belectric(?:al|ian)\b/i, label: 'electrical contractor' },
];

const MISSION_VERTICALS = Object.freeze({
  short_term_rental: ['str_manager', 'property_manager'],
  short_term_rental_operators: ['str_manager', 'property_manager'],
  property_management: ['property_manager', 'str_manager'],
  property_manager: ['property_manager', 'str_manager'],
  commercial_office: ['commercial_office'],
});

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function asClassifyText(value) {
  return asText(value).replace(/[_-]+/g, ' ');
}

function signalText(candidate = {}) {
  return (Array.isArray(candidate.signals) ? candidate.signals : [])
    .map(item => asText(item?.label || item?.text || item?.type))
    .filter(Boolean)
    .join(' ');
}

function placeTypeText(candidate = {}) {
  const types = candidate.placeTypes || candidate.types || candidate.place_types || [];
  return (Array.isArray(types) ? types : [])
    .map(item => asText(item).replace(/_/g, ' '))
    .filter(Boolean)
    .join(' ');
}

function isSearchDerivedField(value, context = {}) {
  const normalized = normalizeVertical(value);
  if (!normalized) return false;

  const concept = normalizeVertical(context.discoveryConcept || context.discoveryQuery || '');
  if (concept && normalized === concept) return true;

  const discoveryCity = normalizeVertical(context.discoveryCity || '');
  if (discoveryCity && normalized.includes(discoveryCity)) return true;

  if (/_nh$/.test(normalized) || /_(manchester|bedford|hooksett|goffstown|londonderry|auburn)_nh/.test(normalized)) {
    return true;
  }

  const tokens = normalized.split('_').filter(Boolean);
  const geoTokens = new Set(['manchester', 'bedford', 'hooksett', 'goffstown', 'londonderry', 'auburn', 'nh']);
  const geoHits = tokens.filter(token => geoTokens.has(token));
  return geoHits.length >= 2 || (geoHits.length === 1 && tokens.length >= 4);
}

function declaredCanonicalVertical(candidate = {}, context = {}) {
  for (const field of ['vertical', 'industry', 'segment']) {
    const raw = candidate[field];
    if (!raw || isSearchDerivedField(raw, context)) continue;
    const normalized = normalizeVertical(raw);
    if (ENRICHABLE_SCOUT_VERTICALS.includes(normalized)) return normalized;
  }
  return null;
}

function classificationHaystack(candidate = {}, context = {}) {
  const parts = [
    asClassifyText(candidate.name),
    asClassifyText(candidate.company),
    asClassifyText(candidate.description),
    asClassifyText(candidate.businessType),
    asClassifyText(candidate.snippet),
    asClassifyText(signalText(candidate)),
    asClassifyText(placeTypeText(candidate)),
  ];

  for (const field of ['industry', 'vertical', 'segment', 'category']) {
    const value = candidate[field];
    if (!value || isSearchDerivedField(value, context)) continue;
    parts.push(asClassifyText(value));
  }

  return parts.filter(Boolean).join(' ');
}

function hasEvidence(text, patterns) {
  return patterns.some(pattern => pattern.test(text));
}

function detectContradictoryBusinessType(text) {
  for (const entry of CONTRADICTORY_BUSINESS_TYPES) {
    if (entry.pattern.test(text)) return entry.label;
  }
  return null;
}

function resolveReplenishmentVertical(candidate = {}, context = {}) {
  const declared = declaredCanonicalVertical(candidate, context);
  if (declared) return declared;

  const text = classificationHaystack(candidate, context);
  if (!text) return null;

  const placeTypes = asClassifyText(placeTypeText(candidate)).toLowerCase();
  const lodgingSignal = [...STR_PLACE_TYPES].some(type => placeTypes.includes(type));
  const realtySignal = [...PM_PLACE_TYPES].some(type => placeTypes.includes(type));

  if (hasEvidence(text, STR_EVIDENCE) || (lodgingSignal && /\b(rental|vacation|property|host|str)\b/i.test(text))) {
    return 'str_manager';
  }
  if (
    hasEvidence(text, PROPERTY_MANAGER_EVIDENCE)
    || (realtySignal && /\b(property|management|mgmt|leasing|multifamily|apartment)\b/i.test(text))
  ) {
    return 'property_manager';
  }
  if (hasEvidence(text, COMMERCIAL_OFFICE_EVIDENCE)) return 'commercial_office';
  return null;
}

function missionCompatibleVerticals(context = {}) {
  const missionSegment = normalizeVertical(
    context.missionSegment || context.segment || context.mission?.segment || ''
  );
  if (!missionSegment) return [...ENRICHABLE_SCOUT_VERTICALS];
  return MISSION_VERTICALS[missionSegment] || [...ENRICHABLE_SCOUT_VERTICALS];
}

function isMissionCompatible(vertical, context = {}) {
  const allowed = missionCompatibleVerticals(context);
  return allowed.includes(vertical);
}

function buildDiscoveryProvenance(candidate = {}, context = {}) {
  const discoveryQuery = asText(
    candidate.discoveryQuery ||
    context.discoveryQuery ||
    candidate._coverageWorkload?.query ||
    candidate.discoveryConcept ||
    context.discoveryConcept
  ) || null;
  const discoveryConcept = asText(
    candidate.discoveryConcept ||
    context.discoveryConcept ||
    candidate._coverageWorkload?.concept
  ) || null;
  const discoveryCity = asText(
    candidate.discoveryCity ||
    context.discoveryCity ||
    candidate._coverageWorkload?.city ||
    candidate.location
  ) || null;
  const discoverySource = asText(
    candidate.discoverySource ||
    context.discoverySource ||
    candidate.source ||
    candidate._coverageWorkload?.source
  ) || null;

  return {
    discoveryQuery,
    discoveryConcept,
    discoveryCity,
    discoverySource,
  };
}

function formatProvenanceNotes(baseNote, provenance = {}) {
  const payload = Object.fromEntries(
    Object.entries(provenance).filter(([, value]) => value)
  );
  if (!Object.keys(payload).length) return baseNote;
  return `${baseNote} | discovery: ${JSON.stringify(payload)}`;
}

function evaluateReplenishmentAdmission(candidate = {}, context = {}) {
  const name = asText(candidate.name || candidate.company);
  const website = asText(candidate.website || candidate.website_url || candidate.url);
  const domain = asText(candidate.domain);
  if (!name || (!domain && !website)) {
    return { admitted: false, reason: 'insufficient_business_fit' };
  }

  const provenance = buildDiscoveryProvenance(candidate, context);
  const admissionContext = { ...context, ...provenance };
  const text = classificationHaystack(candidate, admissionContext);

  const contradictory = detectContradictoryBusinessType(text);
  if (contradictory) {
    return { admitted: false, reason: 'contradictory_business_type', detail: contradictory };
  }

  const vertical = resolveReplenishmentVertical(candidate, admissionContext);
  if (!vertical) {
    return { admitted: false, reason: 'unclassifiable_vertical' };
  }

  if (!ENRICHABLE_SCOUT_VERTICALS.includes(vertical)) {
    return { admitted: false, reason: 'unclassifiable_vertical' };
  }

  if (!isMissionCompatible(vertical, context)) {
    return { admitted: false, reason: 'segment_mismatch', vertical };
  }

  const allowedCities = resolveMissionAllowedCities({
    allowedCities: context.allowedCities,
    missionCities: context.missionCities,
    cities: context.cities,
    geography: context.geography,
    service_area: context.service_area,
    clientConfig: context.clientConfig,
    region: context.region || context.missionRegion,
  });
  if (allowedCities.length) {
    const location = asText(candidate.location || candidate.address);
    const inScope = isLocationInMissionGeography({
      location,
      city: candidate.city,
      allowedCities,
    });
    if (!inScope) {
      return { admitted: false, reason: 'outside_geography', vertical };
    }
  }

  return {
    admitted: true,
    vertical,
    provenance,
  };
}

function createReplenishmentAdmissionCounters() {
  return {
    discovered: 0,
    evaluated: 0,
    fit: 0,
    admittedToEnrichment: 0,
    rejected: {
      outside_geography: 0,
      segment_mismatch: 0,
      contradictory_business_type: 0,
      unclassifiable_vertical: 0,
      suppressed: 0,
      owned_elsewhere: 0,
      valid_ownership_collision: 0,
      stale_ownership: 0,
      same_company_different_contact: 0,
      insufficient_business_fit: 0,
    },
    recovered: 0,
    alreadyQueued: 0,
  };
}

function recordReplenishmentRejection(counters, reason) {
  if (!counters || !reason) return;
  counters.rejected[reason] = (counters.rejected[reason] || 0) + 1;
}

function isCanonicalEnrichableVertical(vertical) {
  const normalized = normalizeVertical(vertical);
  return Boolean(normalized && ENRICHABLE_SCOUT_VERTICALS.includes(normalized));
}

/** Pre-#711 replenishment rows with query-slug verticals must not block fresh canonical admission. */
function isLegacyMalformedReplenishmentOwnership(row = {}) {
  if (String(row.source || '') !== 'max_buffer_replenishment') return false;
  if (Number(row.enrichment_attempts || 0) !== 0) return false;
  return !isCanonicalEnrichableVertical(row.vertical);
}

const LEGACY_RECONCILIATION_REMOVE_REASONS = Object.freeze([
  'outside_geography',
  'contradictory_business_type',
]);

function isLegacyReconciliationRemoveReason(reason) {
  return LEGACY_RECONCILIATION_REMOVE_REASONS.includes(reason);
}

function resolveLegacyReconciliationOutcome(admission = {}) {
  if (admission.admitted) {
    return {
      outcome: 'canonicalize',
      vertical: admission.vertical,
    };
  }
  const reason = admission.reason || 'unclassifiable_vertical';
  if (isLegacyReconciliationRemoveReason(reason)) {
    return {
      outcome: 'remove',
      reason,
      detail: admission.detail || null,
    };
  }
  return {
    outcome: 'hold',
    reason,
    detail: admission.detail || null,
  };
}

module.exports = {
  ENRICHABLE_SCOUT_VERTICALS,
  resolveReplenishmentVertical,
  evaluateReplenishmentAdmission,
  buildDiscoveryProvenance,
  formatProvenanceNotes,
  missionCompatibleVerticals,
  createReplenishmentAdmissionCounters,
  recordReplenishmentRejection,
  isCanonicalEnrichableVertical,
  isLegacyMalformedReplenishmentOwnership,
  LEGACY_RECONCILIATION_REMOVE_REASONS,
  isLegacyReconciliationRemoveReason,
  resolveLegacyReconciliationOutcome,
  _test: {
    classificationHaystack,
    isSearchDerivedField,
    detectContradictoryBusinessType,
  },
};
