'use strict';

const { normalizeVertical } = require('./normalize');
const { matchServiceAreaFromLocation, configuredServiceAreas } = require('./serviceArea');

const ENRICHABLE_SCOUT_VERTICALS = Object.freeze([
  'property_manager',
  'str_manager',
  'commercial_office',
]);

const STR_EVIDENCE = [
  /\bshort[\s-]?term rental\b/i,
  /\bvacation rental\b/i,
  /\bairbnb\b/i,
  /\bvrbo\b/i,
  /\bstr portfolio\b/i,
  /\bstr operator\b/i,
  /\bvacation propert(?:y|ies) management\b/i,
  /\brental management\b/i,
];

const PROPERTY_MANAGER_EVIDENCE = [
  /\bproperty management\b/i,
  /\bproperty manager\b/i,
  /\bresidential property management\b/i,
  /\bcommercial property management\b/i,
  /\brental property management\b/i,
  /\bmultifamily management\b/i,
  /\bapartment management\b/i,
];

const COMMERCIAL_OFFICE_EVIDENCE = [
  /\bcommercial office\b/i,
  /\boffice park\b/i,
  /\bbusiness center\b/i,
  /\boffice building management\b/i,
];

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

function classificationHaystack(candidate = {}, context = {}) {
  const parts = [
    candidate.name,
    candidate.company,
    candidate.description,
    candidate.businessType,
    candidate.snippet,
    signalText(candidate),
    placeTypeText(candidate),
  ];

  for (const field of ['industry', 'vertical', 'segment', 'category']) {
    const value = candidate[field];
    if (!value || isSearchDerivedField(value, context)) continue;
    parts.push(value);
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
  const text = classificationHaystack(candidate, context);
  if (!text) return null;

  if (hasEvidence(text, STR_EVIDENCE)) return 'str_manager';
  if (hasEvidence(text, PROPERTY_MANAGER_EVIDENCE)) return 'property_manager';
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

  const serviceAreas = configuredServiceAreas(context.clientConfig || context);
  if (serviceAreas.length) {
    const location = asText(candidate.location || candidate.address || provenance.discoveryCity);
    const matched = matchServiceAreaFromLocation(location, serviceAreas);
    if (!matched) {
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
      insufficient_business_fit: 0,
    },
  };
}

function recordReplenishmentRejection(counters, reason) {
  if (!counters || !reason) return;
  counters.rejected[reason] = (counters.rejected[reason] || 0) + 1;
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
  _test: {
    classificationHaystack,
    isSearchDerivedField,
    detectContradictoryBusinessType,
  },
};
