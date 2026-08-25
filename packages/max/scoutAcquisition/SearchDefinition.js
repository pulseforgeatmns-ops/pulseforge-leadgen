'use strict';

/**
 * SPEC-100A — translate Max's bounded acquisition delegation into a
 * durable, inspectable search definition and population statement.
 * Reuses Blueprint / Discovery Profile / targeting. Does not create a new ICP.
 */

const {
  asText,
  clone,
  isPlainObject,
  DEFAULT_COMMERCIAL_CLEANING_SEGMENTS,
} = require('./Types');
const { parseGeographyList } = require('./InvestigationProvenance');
const { MANCHESTER_GEO, seedDiscoveryProfiles } = require('../../capabilities/discovery/seedProfiles');
const { isRuntimeAim } = require('../../aim');

const BUSINESS_NEED_ALIASES = Object.freeze({
  commercial_cleaning: 'commercial_cleaning',
  cleaning: 'commercial_cleaning',
  janitorial: 'commercial_cleaning',
  office_cleaning: 'commercial_cleaning',
});

function normalizeBusinessNeed(value) {
  const raw = asText(value);
  if (!raw) return null;
  const key = raw.toLowerCase().replace(/[\s-]+/g, '_');
  return BUSINESS_NEED_ALIASES[key] || key;
}

function defaultSegmentsForNeed(businessNeed, supplied) {
  if (Array.isArray(supplied) && supplied.length) return supplied.map(asText).filter(Boolean);
  if (businessNeed === 'commercial_cleaning') {
    return DEFAULT_COMMERCIAL_CLEANING_SEGMENTS.slice();
  }
  return [];
}

function resolveProfile(input = {}) {
  const tenantId = asText(input.tenantId);
  const businessNeed = input.businessNeed;
  const segments = input.segments || [];
  const profiles = typeof seedDiscoveryProfiles === 'function' ? seedDiscoveryProfiles() : [];
  const tenantHits = profiles.filter((p) => {
    if (!p || p.status === 'inactive') return false;
    if (tenantId && Array.isArray(p.clientIds) && p.clientIds.length) {
      return p.clientIds.map(String).includes(String(tenantId));
    }
    return true;
  });
  const preferProperty =
    segments.length === 1 && /property/.test(String(segments[0] || '').toLowerCase());
  const ranked = tenantHits.slice().sort((a, b) => {
    const aPm = /property/.test(String(a.id || a.name || ''));
    const bPm = /property/.test(String(b.id || b.name || ''));
    if (preferProperty && aPm !== bPm) return aPm ? -1 : 1;
    const aClean = /cleaning/.test(String(a.id || a.name || ''));
    const bClean = /cleaning/.test(String(b.id || b.name || ''));
    if (businessNeed === 'commercial_cleaning' && aClean !== bClean) return aClean ? -1 : 1;
    return 0;
  });
  return ranked[0] || null;
}

function geographyFromLabel(label, profile) {
  const text = asText(label);
  const profileGeo = profile && profile.geography ? profile.geography : null;
  const cities = [];
  let state = null;
  let radiusMiles = null;
  if (profileGeo && text && String(profileGeo.label || '').toLowerCase().includes(
    String(text).split(',')[0].toLowerCase().trim()
  )) {
    state = profileGeo.state || null;
    radiusMiles = profileGeo.radiusMiles != null ? Number(profileGeo.radiusMiles) : null;
  }
  if (/manchester/i.test(text || '')) {
    return {
      label: text || MANCHESTER_GEO.label,
      cities: ['Manchester'],
      state: state || 'NH',
      radiusMiles: radiusMiles != null ? radiusMiles : 20,
      permittedNearby: MANCHESTER_GEO.cities.filter((c) => c !== 'Manchester'),
    };
  }
  const parts = parseGeographyList(text);
  for (const part of parts) {
    const city = String(part).split(',')[0].trim();
    if (city && !cities.includes(city)) cities.push(city);
  }
  return {
    label: text,
    cities,
    state,
    radiusMiles,
    permittedNearby: [],
  };
}

function buildPopulationStatement(input = {}) {
  if (input.aim && input.aim.mission && input.aim.mission.transformation) {
    const icp = input.aim.icp && input.aim.icp.company && input.aim.icp.company.reasoning;
    return (
      `Organizations that resemble this AIM ICP — ${input.aim.mission.transformation}` +
      (icp ? ` ICP reasoning: ${icp}` : '')
    );
  }
  const need = input.businessNeed || 'the delegated service';
  const geo = (input.geography && input.geography.label) || 'the approved service geography';
  const segments = input.segments || [];
  const narrowed = segments.length === 1
    ? ` Focus is currently limited to ${String(segments[0]).replace(/_/g, ' ')}.`
    : '';
  const needLabel =
    need === 'commercial_cleaning'
      ? 'recurring professional cleaning'
      : String(need).replace(/_/g, ' ');
  return (
    `Commercial organizations within ${geo} whose facility or operating characteristics ` +
    `plausibly require ${needLabel}.` +
    narrowed
  );
}

function companyCriteriaFor(businessNeed, populationStatement) {
  return {
    statement: populationStatement,
    commercialFacility: businessNeed === 'commercial_cleaning' || Boolean(businessNeed),
    relevantOperator: true,
  };
}

/**
 * @param {object} input
 * @returns {object} AcquisitionSearchDefinition
 */
function buildAcquisitionSearchDefinition(input = {}) {
  if (input.marketDefinition && input.projectFromMarketDefinition !== false) {
    return buildSearchDefinitionFromMarketDefinition(input.marketDefinition, input);
  }

  const delegation = isPlainObject(input.delegation) ? input.delegation : {};
  const target = isPlainObject(input.targetContext || delegation.targetContext)
    ? input.targetContext || delegation.targetContext
    : {};
  const business = isPlainObject(input.businessContext || delegation.businessContext)
    ? input.businessContext || delegation.businessContext
    : {};
  const tenantId = asText(
    input.tenantId || delegation.tenantId || input.authorizedTenantId
  );
  const businessNeed = normalizeBusinessNeed(
    target.businessType ||
      business.commercialCapability ||
      (business.approvedUnderstanding && business.approvedUnderstanding.commercialCapability)
  );
  const suppliedSegments = Array.isArray(target.segments)
    ? target.segments
    : Array.isArray(business.preferredSegments)
      ? business.preferredSegments
      : [];
  const operatorDirection =
    asText(input.operatorDirection) ||
    asText(business.operatorDirection && business.operatorDirection.focus) ||
    asText(business.operatorDirection && business.operatorDirection.text) ||
    asText(business.acquisitionDirection);
  const missionBound =
    Boolean(target.missionBound || business.missionObjectiveImmutable) &&
    Boolean(operatorDirection);
  const narrowedByOperator =
    /\bfocus on property|\bproperty managers?\b/i.test(String(operatorDirection || '')) ||
    (suppliedSegments.length === 1 && /property/.test(String(suppliedSegments[0] || '')));
  const segments = missionBound && suppliedSegments.length
    ? suppliedSegments.map(asText).filter(Boolean)
    : narrowedByOperator && suppliedSegments.length
      ? suppliedSegments.map(asText).filter(Boolean)
      : defaultSegmentsForNeed(businessNeed, suppliedSegments);

  const geoLabel = missionBound
    ? asText(target.geography) || asText(business.serviceGeography) || null
    : asText(target.geography) ||
      asText(business.serviceGeography) ||
      asText(business.approvedUnderstanding && business.approvedUnderstanding.serviceGeography) ||
      null;
  const profile = resolveProfile({ tenantId, businessNeed, segments });
  const geography = geoLabel ? geographyFromLabel(geoLabel, profile) : null;
  const exclusions = Array.isArray(business.exclusions)
    ? business.exclusions.map(asText).filter(Boolean)
    : [];
  const desiredSignals = Array.isArray(target.desiredSignals)
    ? target.desiredSignals.map(asText).filter(Boolean)
    : [];
  const rawAim = input.aim || business.aim || null;
  const aim = isRuntimeAim(rawAim) ? rawAim : null;
  const populationStatement = buildPopulationStatement({
    businessNeed,
    geography,
    segments,
    aim,
  });
  const invalidReason = aim
    ? !(aim.mission && aim.mission.known && aim.painOntology)
      ? 'AIM is incomplete — Scout will not search a market it does not understand.'
      : null
    : !geography || !geography.label
      ? 'Geography could not be resolved.'
      : !businessNeed && !segments.length
        ? 'Acquisition target definition is incomplete.'
        : null;

  return {
    tenantId,
    businessNeed,
    geography,
    segments,
    companyCriteria: companyCriteriaFor(businessNeed, populationStatement),
    exclusions,
    desiredSignals,
    createdFromDelegationId: asText(delegation.id || input.createdFromDelegationId),
    populationStatement,
    profileId: profile ? profile.id : null,
    operatorDirection: operatorDirection || null,
    missionBound,
    expansionRequiresAuthority: true,
    valid: !invalidReason,
    invalidReason,
    aim: aim || null,
    aimClientKey: aim ? aim.clientKey : asText(business.aimClientKey || business.clientKey) || null,
  };
}

/**
 * SPEC-178 / ADR-093 — project adapter-facing SearchDefinition from canonical MarketDefinition.
 * Segments are never inferred here; they mirror the market definition exactly.
 *
 * @param {object} marketDefinition
 * @param {object} [opts]
 * @returns {object}
 */
function buildSearchDefinitionFromMarketDefinition(marketDefinition = {}, opts = {}) {
  const md = marketDefinition || {};
  const delegation = isPlainObject(opts.delegation) ? opts.delegation : {};
  const target = isPlainObject(opts.targetContext || delegation.targetContext)
    ? opts.targetContext || delegation.targetContext
    : {};
  const business = isPlainObject(opts.businessContext || delegation.businessContext)
    ? opts.businessContext || delegation.businessContext
    : {};
  const tenantId = asText(
    opts.tenantId || md.tenantId || delegation.tenantId || opts.authorizedTenantId
  );
  const segmentKey = asText(md.segmentKey) || 'general';
  const segments = Array.isArray(md.segments) && md.segments.length
    ? md.segments.map(asText).filter(Boolean)
    : [segmentKey];

  const businessNeed = normalizeBusinessNeed(
    opts.businessNeed ||
      target.businessType ||
      business.commercialCapability ||
      (business.approvedUnderstanding && business.approvedUnderstanding.commercialCapability)
  );
  const operatorDirection =
    asText(opts.operatorDirection) ||
    asText(business.operatorDirection && business.operatorDirection.focus) ||
    asText(business.operatorDirection && business.operatorDirection.text) ||
    asText(business.acquisitionDirection) ||
    asText(md.missionGoal) ||
    null;
  const missionBound =
    Boolean(target.missionBound || business.missionObjectiveImmutable) &&
    Boolean(operatorDirection || md.missionGoal);
  const geoLabel =
    asText(md.geography) ||
    asText(target.geography) ||
    asText(business.serviceGeography) ||
    asText(business.approvedUnderstanding && business.approvedUnderstanding.serviceGeography) ||
    null;
  const profile = resolveProfile({ tenantId, businessNeed, segments });
  const geography = geoLabel ? geographyFromLabel(geoLabel, profile) : null;
  const exclusions = Array.isArray(business.exclusions)
    ? business.exclusions.map(asText).filter(Boolean)
    : Array.isArray(md.exclusions)
      ? md.exclusions.map(asText).filter(Boolean)
      : [];
  const desiredSignals = Array.isArray(target.desiredSignals)
    ? target.desiredSignals.map(asText).filter(Boolean)
    : [];
  const rawAim = opts.aim || business.aim || null;
  const aim = isRuntimeAim(rawAim) ? rawAim : null;
  const populationStatement = buildPopulationStatement({
    businessNeed,
    geography,
    segments,
    aim,
  });
  const invalidReason = aim
    ? !(aim.mission && aim.mission.known && aim.painOntology)
      ? 'AIM is incomplete — Scout will not search a market it does not understand.'
      : null
    : !geography || !geography.label
      ? 'Geography could not be resolved.'
      : !businessNeed && !segments.length
        ? 'Acquisition target definition is incomplete.'
        : null;

  return {
    tenantId,
    businessNeed,
    geography,
    segments,
    companyCriteria: companyCriteriaFor(businessNeed, populationStatement),
    exclusions,
    desiredSignals,
    createdFromDelegationId: asText(delegation.id || opts.createdFromDelegationId),
    populationStatement,
    profileId: profile ? profile.id : null,
    operatorDirection: operatorDirection || null,
    missionBound,
    expansionRequiresAuthority: true,
    valid: !invalidReason,
    invalidReason,
    aim: aim || null,
    aimClientKey: aim ? aim.clientKey : asText(business.aimClientKey || business.clientKey) || null,
    projectedFromMarketDefinition: true,
    marketDefinitionSegmentKey: segmentKey,
  };
}

function searchDefinitionFingerprint(definition) {
  if (!definition) return '';
  const geo = String((definition.geography && definition.geography.label) || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
  const segments = (definition.segments || [])
    .map((s) => String(s).toLowerCase().replace(/[^a-z0-9]+/g, '_'))
    .sort()
    .join(',');
  return `${definition.tenantId || ''}|${definition.businessNeed || ''}|${geo}|${segments}`;
}

function expansionSuggestion(definition, basicFitCount) {
  if (!definition || !definition.geography) return null;
  const nearby = definition.geography.permittedNearby || [];
  if (!nearby.length) {
    return (
      `The current search produced only ${basicFitCount} basic-fit compan` +
      `${basicFitCount === 1 ? 'y' : 'ies'}. Broadening the segment could increase the candidate universe.`
    );
  }
  return (
    `The current search produced only ${basicFitCount} basic-fit compan` +
    `${basicFitCount === 1 ? 'y' : 'ies'}. Expanding geography to ${nearby.slice(0, 3).join('/')} ` +
    `or broadening the segment could increase the candidate universe.`
  );
}

module.exports = {
  buildAcquisitionSearchDefinition,
  buildSearchDefinitionFromMarketDefinition,
  buildPopulationStatement,
  searchDefinitionFingerprint,
  expansionSuggestion,
  normalizeBusinessNeed,
  geographyFromLabel,
  clone,
};
