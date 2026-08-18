'use strict';

/**
 * SPEC-100 — bounded business + target context for Scout.
 * Never send the entire Blueprint or Max state.
 */

const { asText, clone, isPlainObject } = require('./Types');

const BUSINESS_KEYS = Object.freeze([
  'serviceGeography',
  'commercialCapability',
  'preferredSegments',
  'acquisitionDirection',
  'exclusions',
  'offerContext',
  'operatorDirection',
  'approvedUnderstanding',
  'campaignLearnings',
  'targetMarket',
  'offer',
  'notes',
  'aimClientKey',
  'clientKey',
]);

function asStringList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map(asText).filter(Boolean);
  const text = asText(value);
  return text ? [text] : [];
}

function pickBounded(src, keys) {
  const out = {};
  if (!isPlainObject(src)) return out;
  for (const key of keys) {
    if (src[key] !== undefined) out[key] = clone(src[key]);
  }
  return out;
}

/**
 * Derive a bounded Scout envelope from workspace / CIE / operator context.
 * Drops full blueprint, session, and unrelated Max state.
 *
 * @param {object} input
 * @returns {{ businessContext: object, targetContext: object }}
 */
function buildBoundedScoutContext(input = {}) {
  const tenantId = asText(input.authorizedTenantId || input.tenantId || input.clientId);
  const rawBusiness = isPlainObject(input.businessContext) ? input.businessContext : {};
  const rawTarget = isPlainObject(input.targetContext) ? input.targetContext : {};
  const blueprint = isPlainObject(input.approvedUnderstanding)
    ? input.approvedUnderstanding
    : isPlainObject(input.blueprintSnapshot)
      ? input.blueprintSnapshot
      : isPlainObject(rawBusiness.approvedUnderstanding)
        ? rawBusiness.approvedUnderstanding
        : {};

  const geography =
    asText(rawTarget.geography) ||
    asText(rawBusiness.serviceGeography) ||
    asText(blueprint.serviceGeography) ||
    asText(blueprint.geography) ||
    (rawBusiness.targetMarket && asText(rawBusiness.targetMarket.geography)) ||
    null;

  const segments = asStringList(
    rawTarget.segments ||
      rawBusiness.preferredSegments ||
      (rawBusiness.targetMarket && rawBusiness.targetMarket.segments) ||
      blueprint.preferredSegments ||
      blueprint.idealCustomers
  );

  const businessType =
    asText(rawTarget.businessType) ||
    asText(rawBusiness.commercialCapability) ||
    asText(blueprint.commercialCapability) ||
    asText(blueprint.businessType) ||
    null;

  const desiredSignals = asStringList(rawTarget.desiredSignals);
  const exclusions = asStringList(
    rawBusiness.exclusions ||
      (rawBusiness.operatorDirection && rawBusiness.operatorDirection.excludedSegments) ||
      blueprint.exclusions
  );

  const businessContext = {
    ...pickBounded(rawBusiness, BUSINESS_KEYS),
    serviceGeography: geography,
    commercialCapability: businessType,
    preferredSegments: segments,
    exclusions,
    acquisitionDirection:
      asText(rawBusiness.acquisitionDirection) ||
      asText(blueprint.acquisitionDirection) ||
      asText(input.acquisitionDirection) ||
      null,
    offerContext:
      rawBusiness.offerContext ||
      rawBusiness.offer ||
      blueprint.offer ||
      null,
    operatorDirection: rawBusiness.operatorDirection || input.operatorDirection || null,
    approvedUnderstanding: pickApprovedSlice(blueprint),
    campaignLearnings: rawBusiness.campaignLearnings || input.campaignLearnings || null,
    notes: asText(rawBusiness.notes) || asText(input.notes),
  };

  const targetContext = {
    geography,
    segments,
    businessType,
    desiredSignals,
    entities: Array.isArray(rawTarget.entities) ? clone(rawTarget.entities) : [],
    priorDelegationId: asText(rawTarget.priorDelegationId || input.priorDelegationId),
    priorResultId: asText(rawTarget.priorResultId || input.priorResultId),
    seedCompanyId: asText(rawTarget.seedCompanyId || input.seedCompanyId),
    notes: asText(rawTarget.notes),
  };

  return {
    tenantId,
    businessContext,
    targetContext,
    bounded: true,
    omitted: ['fullBlueprint', 'fullMaxState', 'session', 'otherTenantIntelligence'],
  };
}

function pickApprovedSlice(blueprint) {
  if (!isPlainObject(blueprint)) return null;
  const slice = {};
  for (const key of [
    'businessName',
    'serviceGeography',
    'geography',
    'commercialCapability',
    'preferredSegments',
    'idealCustomers',
    'exclusions',
    'acquisitionDirection',
    'offer',
  ]) {
    if (blueprint[key] !== undefined) slice[key] = clone(blueprint[key]);
  }
  return Object.keys(slice).length ? slice : null;
}

function criteriaFingerprint(targetContext = {}, businessContext = {}) {
  const geo = String(
    (targetContext && targetContext.geography) ||
      (businessContext && businessContext.serviceGeography) ||
      ''
  )
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
  const segments = (
    (targetContext && targetContext.segments) ||
    (businessContext && businessContext.preferredSegments) ||
    []
  )
    .map((s) => String(s).toLowerCase().replace(/[^a-z0-9]+/g, '_').trim())
    .filter(Boolean)
    .sort();
  const seed = asText(targetContext && targetContext.seedCompanyId) || '';
  return `${geo}|${segments.join(',')}|${seed}`;
}

module.exports = {
  BUSINESS_KEYS,
  buildBoundedScoutContext,
  pickApprovedSlice,
  criteriaFingerprint,
  asStringList,
};
