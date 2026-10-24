'use strict';

/**
 * SPEC-101 — available / supplied / consumed context layers.
 *
 * Available: what Max knew or could retrieve at delegation time.
 * Supplied: what Max actually included in the SpecialistDelegation.
 * Consumed: what the specialist successfully interpreted.
 *
 * These are diagnostic layers, not a second storage architecture.
 */

const { asText, clone, isPlainObject } = require('./Types');

const GEOGRAPHY_KEYS = Object.freeze([
  'geography',
  'serviceArea',
  'serviceGeography',
  'targetMarkets',
]);

function firstText(...values) {
  for (const value of values) {
    const text = asText(value);
    if (text) return text;
  }
  return null;
}

function asStringList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map(asText).filter(Boolean);
  const text = asText(value);
  return text ? [text] : [];
}

function locationKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function geographyPresent(value) {
  if (value == null) return false;
  if (typeof value === 'string') return Boolean(asText(value));
  if (Array.isArray(value)) return value.some((item) => geographyPresent(item));
  if (!isPlainObject(value)) return false;
  return Boolean(
    asText(value.label) ||
      asText(value.geography) ||
      asText(value.serviceArea) ||
      (Array.isArray(value.cities) && value.cities.length)
  );
}

function geographyLabel(value) {
  if (value == null) return null;
  if (typeof value === 'string') return asText(value);
  if (Array.isArray(value)) {
    const parts = value.map(geographyLabel).filter(Boolean);
    return parts.length ? parts.join(', ') : null;
  }
  if (!isPlainObject(value)) return null;
  return firstText(
    value.label,
    value.geography,
    value.serviceArea,
    Array.isArray(value.cities) ? value.cities.join(', ') : null
  );
}

function geographiesMatch(a, b) {
  const left = locationKey(geographyLabel(a));
  const right = locationKey(geographyLabel(b));
  if (!left || !right) return false;
  if (left === right) return true;
  const leftTokens = left.split(/\s+/).filter((t) => t.length > 2);
  const rightTokens = right.split(/\s+/).filter((t) => t.length > 2);
  return leftTokens.some((t) => right.includes(t)) || rightTokens.some((t) => left.includes(t));
}

function pickFromObject(src, keys) {
  const out = {};
  if (!isPlainObject(src)) return out;
  for (const key of keys) {
    if (src[key] !== undefined && src[key] !== null) out[key] = clone(src[key]);
  }
  return out;
}

/**
 * What Max knew or could retrieve when the delegation was created.
 * Prefer a persisted snapshot. Fall back to live sources only when
 * the snapshot is absent — and mark that inference so Max cannot
 * pretend the historical available layer was recorded.
 *
 * @param {object} input
 * @returns {object}
 */
function projectAvailableContext(input = {}) {
  const persisted =
    (input.delegation && input.delegation.availableContext) ||
    (input.delegation &&
      input.delegation.businessContext &&
      input.delegation.businessContext.maxAvailableContext) ||
    input.availableContext ||
    null;

  if (isPlainObject(persisted) && (persisted.recorded !== false || persisted.business || persisted.serviceArea)) {
    return {
      recorded: persisted.recorded !== false,
      inferred: persisted.inferred === true,
      business: asText(persisted.business),
      serviceArea: geographyLabel(persisted.serviceArea || persisted.geography),
      objective: asText(persisted.objective),
      segments: asStringList(persisted.segments),
      constraints: isPlainObject(persisted.constraints) ? clone(persisted.constraints) : {},
      sources: Array.isArray(persisted.sources) ? persisted.sources.slice() : [],
      raw: clone(persisted),
    };
  }

  const live = collectLiveAvailableContext(input);
  const hasAny =
    Boolean(live.business || live.serviceArea || live.objective || live.segments.length);
  return {
    recorded: false,
    inferred: hasAny,
    business: live.business,
    serviceArea: live.serviceArea,
    objective: live.objective,
    segments: live.segments,
    constraints: live.constraints,
    sources: live.sources,
    raw: hasAny ? live : null,
  };
}

/**
 * Collect what Max can see now (session / CIE / envelope).
 * Used at delegation time to persist the available snapshot, and
 * at interrogation time only as an inferred fallback.
 */
function collectLiveAvailableContext(input = {}) {
  const session =
    (input.session && input.session.context) ||
    input.sessionContext ||
    input.context ||
    {};
  const envelope = isPlainObject(input.envelope) ? input.envelope : {};
  const business = isPlainObject(input.businessContext)
    ? input.businessContext
    : isPlainObject(session.businessContext)
      ? session.businessContext
      : {};
  const target = isPlainObject(input.targetContext)
    ? input.targetContext
    : isPlainObject(session.targetContext)
      ? session.targetContext
      : {};
  const approved =
    (isPlainObject(input.approvedUnderstanding) && input.approvedUnderstanding) ||
    (isPlainObject(business.approvedUnderstanding) && business.approvedUnderstanding) ||
    (isPlainObject(session.approvedUnderstanding) && session.approvedUnderstanding) ||
    (session.businessBlueprint && session.businessBlueprint.approved) ||
    {};
  const cie =
    (isPlainObject(session.clientIntelligence) && session.clientIntelligence) ||
    (isPlainObject(envelope.clientIntelligence) && envelope.clientIntelligence) ||
    {};

  const sources = [];
  const serviceArea = firstText(
    target.geography,
    business.serviceGeography,
    approved.serviceGeography,
    approved.geography,
    cie.geography,
    cie.targetMarkets,
    session.serviceArea,
    session.geography,
    envelope.serviceArea,
    envelope.geography
  );
  if (target.geography) sources.push('targetContext.geography');
  else if (business.serviceGeography) sources.push('businessContext.serviceGeography');
  else if (approved.serviceGeography || approved.geography) sources.push('approvedUnderstanding');
  else if (cie.geography || cie.targetMarkets) sources.push('clientIntelligence');
  else if (session.serviceArea || session.geography) sources.push('session');

  const segments = asStringList(
    target.segments ||
      business.preferredSegments ||
      approved.preferredSegments ||
      approved.idealCustomers ||
      cie.idealCustomerList ||
      cie.idealCustomers
  );
  const businessName = firstText(
    approved.businessName,
    cie.businessName,
    cie.identity,
    session.clientName,
    session.businessName,
    envelope.clientName,
    business.notes
  );
  const constraints = {};
  const exclusions = asStringList(
    business.exclusions || approved.exclusions || cie.avoidCustomers
  );
  if (exclusions.length) constraints.exclusions = exclusions;
  if (business.commercialCapability || approved.commercialCapability) {
    constraints.recurringCommercial =
      /clean/i.test(
        String(business.commercialCapability || approved.commercialCapability || '')
      ) || Boolean(cie.commercialPreference);
  }

  return {
    business: businessName,
    serviceArea,
    objective: firstText(input.objective, input.question, session.objective),
    segments,
    constraints,
    sources,
  };
}

/**
 * Persistable snapshot of what Max knew at delegation time.
 */
function captureAvailableContext(input = {}) {
  const live = collectLiveAvailableContext(input);
  return {
    recorded: true,
    inferred: false,
    capturedAt: new Date().toISOString(),
    business: live.business,
    serviceArea: live.serviceArea,
    geography: live.serviceArea,
    objective: live.objective,
    segments: live.segments,
    constraints: live.constraints,
    sources: live.sources,
  };
}

/**
 * What Max actually put on the SpecialistDelegation.
 */
function projectSuppliedContext(delegation = {}) {
  const business = isPlainObject(delegation.businessContext)
    ? delegation.businessContext
    : {};
  const target = isPlainObject(delegation.targetContext) ? delegation.targetContext : {};
  const constraints = isPlainObject(delegation.constraints) ? delegation.constraints : {};
  const geography = firstText(
    target.geography,
    constraints.geography,
    business.serviceGeography
  );
  return {
    recorded: true,
    specialist: asText(delegation.specialist),
    capability: asText(delegation.capability),
    objective: asText(delegation.objective),
    reason: asText(delegation.reason),
    business: firstText(
      business.approvedUnderstanding && business.approvedUnderstanding.businessName,
      business.notes
    ),
    serviceArea: geography,
    geography,
    segments: asStringList(
      target.segments || constraints.targetSegments || business.preferredSegments
    ),
    businessType: firstText(target.businessType, business.commercialCapability),
    constraints: pickFromObject(constraints, [
      'geography',
      'targetSegments',
      'allowedChannels',
      'excludedChannels',
      'contactRestrictions',
    ]),
    exclusions: asStringList(business.exclusions),
    desiredSignals: asStringList(target.desiredSignals),
    requestedEvidence: asStringList(
      delegation.expectedReturn && delegation.expectedReturn.requireEvidence
        ? ['evidence']
        : []
    ),
  };
}

/**
 * What the specialist successfully interpreted.
 * Prefers an explicit consumedContext on the result payload.
 */
function projectConsumedContext(result = {}, delegation = {}) {
  const payload = isPlainObject(result.payload) ? result.payload : {};
  const explicit = isPlainObject(payload.consumedContext) ? payload.consumedContext : null;
  const investigation =
    (isPlainObject(payload.investigation) && payload.investigation) ||
    (isPlainObject(result.investigation) && result.investigation) ||
    null;
  const searchDefinition = isPlainObject(payload.searchDefinition)
    ? payload.searchDefinition
    : null;

  if (explicit) {
    return {
      recorded: true,
      inferred: false,
      geography: geographyLabel(explicit.geography || explicit.serviceArea),
      geographyResolved: explicit.geographyResolved !== false && geographyPresent(explicit.geography),
      segments: asStringList(explicit.segments),
      businessNeed: asText(explicit.businessNeed || explicit.businessType),
      valid: explicit.valid !== false,
      invalidReason: asText(explicit.invalidReason),
      raw: clone(explicit),
    };
  }

  const searchGeo = searchDefinition && searchDefinition.geography;
  const investigated = investigation && investigation.scope
    ? investigation.scope.geography ||
      (Array.isArray(investigation.scope.investigatedGeography)
        ? investigation.scope.investigatedGeography.join(', ')
        : null)
    : null;
  const requested = investigation && investigation.scope
    ? investigation.scope.requestedGeography
    : null;
  const invalidReason =
    (searchDefinition && searchDefinition.invalidReason) ||
    ((result.errors || []).find((e) => e && e.code === 'invalid_target') || {}).message ||
    (Array.isArray(result.uncertainties) ? result.uncertainties[0] : null) ||
    null;
  const geography = geographyLabel(searchGeo) || asText(investigated);
  const recorded = Boolean(searchDefinition || investigation || invalidReason);

  return {
    recorded,
    inferred: recorded && !explicit,
    geography,
    geographyResolved: Boolean(geography) && !/could not be resolved/i.test(String(invalidReason || '')),
    segments: asStringList(
      (searchDefinition && searchDefinition.segments) ||
        (investigation && investigation.scope && investigation.scope.segments)
    ),
    businessNeed: asText(
      (searchDefinition && searchDefinition.businessNeed) ||
        (delegation.targetContext && delegation.targetContext.businessType)
    ),
    valid: searchDefinition ? searchDefinition.valid === true : Boolean(geography),
    invalidReason: asText(invalidReason),
    requestedAsSeenBySpecialist: asText(requested),
    raw: searchDefinition || investigation || null,
  };
}

function contextFieldPresent(layer, field) {
  if (!layer) return false;
  if (field === 'geography' || field === 'serviceArea') {
    return geographyPresent(layer.serviceArea || layer.geography);
  }
  if (field === 'segments') return asStringList(layer.segments).length > 0;
  return Boolean(asText(layer[field]));
}

module.exports = {
  GEOGRAPHY_KEYS,
  firstText,
  asStringList,
  locationKey,
  geographyPresent,
  geographyLabel,
  geographiesMatch,
  projectAvailableContext,
  collectLiveAvailableContext,
  captureAvailableContext,
  projectSuppliedContext,
  projectConsumedContext,
  contextFieldPresent,
};
