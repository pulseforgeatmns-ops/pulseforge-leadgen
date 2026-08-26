'use strict';

/**
 * SPEC-181 — Evidence-Native Execution request contract.
 *
 * Providers receive structured evidence requirements — not search strings.
 * Query generation, pagination, retries, and localization are provider responsibilities.
 *
 * @example
 * {
 *   segment: 'short_term_rental',
 *   evidenceType: 'identity',
 *   geography: { cities: ['Manchester', 'Bedford', 'Hooksett'], state: 'NH' },
 *   investigationTaskId: 'task:identity',
 *   providerIds: ['google_maps'],
 * }
 */

const { asText } = require('../../max/scoutAcquisition/Types');
const { expandCitiesFromSearchDefinition } = require('./DiscoveryCoverageEngine');

/**
 * @typedef {object} EvidenceRequest
 * @property {string} segment — canonical segment key (e.g. short_term_rental)
 * @property {string} evidenceType — investigative evidence type (e.g. identity)
 * @property {object} geography
 * @property {string[]} geography.cities — city names without state suffix
 * @property {string|null} [geography.state]
 * @property {string|null} [geography.label] — operator geography label when present
 * @property {string} [investigationTaskId]
 * @property {string[]} [providerIds]
 * @property {string|null} [tenantId]
 */

function cityNameOnly(cityLabel) {
  const text = asText(cityLabel);
  if (!text) return '';
  return text.split(',')[0].trim().split(/\s+/)[0];
}

function inferStateFromLabel(label) {
  const text = asText(label) || '';
  if (!text) return null;
  const match = text.match(/\b([A-Z]{2})\b/);
  return match ? match[1] : null;
}

/**
 * Extract city names and state from a search definition geography block.
 * @param {object} searchDefinition
 * @returns {{ cities: string[], state: string|null, label: string|null }}
 */
function geographyFromSearchDefinition(searchDefinition = {}) {
  const geo = searchDefinition.geography || {};
  const label = asText(geo.label) || null;
  const state = asText(geo.state) || inferStateFromLabel(label);

  if (Array.isArray(geo.cities) && geo.cities.length) {
    return {
      cities: [...new Set(geo.cities.map(cityNameOnly).filter(Boolean))],
      state,
      label,
    };
  }

  const expanded = expandCitiesFromSearchDefinition(searchDefinition);
  if (expanded.length) {
    return {
      cities: [...new Set(expanded.map(cityNameOnly).filter(Boolean))],
      state: state || inferStateFromLabel(expanded[0]),
      label,
    };
  }

  if (label) {
    const city = cityNameOnly(label);
    return {
      cities: city ? [city] : [],
      state,
      label,
    };
  }

  return { cities: [], state, label };
}

function resolveSegment(searchDefinition = {}, marketDefinition = {}) {
  const fromMarket = Array.isArray(marketDefinition.segments) && marketDefinition.segments[0];
  if (fromMarket) return String(fromMarket);
  const fromSearch = Array.isArray(searchDefinition.segments) && searchDefinition.segments[0];
  if (fromSearch) return String(fromSearch);
  const fromIndustry = asText(marketDefinition.industry || searchDefinition.industry);
  if (fromIndustry) return fromIndustry.replace(/\s+/g, '_').toLowerCase();
  return '';
}

/**
 * Build an evidence-native provider request for an investigation task.
 * Scout never emits search strings — only evidence requirements.
 *
 * @param {object} task — investigation task from SPEC-180 plan
 * @param {object} searchDefinition
 * @param {object} marketDefinition
 * @returns {EvidenceRequest}
 */
function buildEvidenceRequest(task = {}, searchDefinition = {}, marketDefinition = {}) {
  const geography = geographyFromSearchDefinition(searchDefinition);
  return {
    segment: resolveSegment(searchDefinition, marketDefinition),
    evidenceType: asText(task.evidenceType),
    geography,
    investigationTaskId: asText(task.id) || null,
    providerIds: Array.isArray(task.providers)
      ? task.providers.map((p) => asText(p.providerId)).filter(Boolean)
      : [],
    tenantId: asText(searchDefinition.tenantId || marketDefinition.tenantId) || null,
  };
}

/**
 * Wrap a search definition with an evidence request for adapter dispatch.
 * @param {object} searchDefinition
 * @param {object} task
 * @param {object} marketDefinition
 * @returns {object}
 */
function scopeSearchDefinitionForTask(searchDefinition = {}, task = {}, marketDefinition = {}) {
  const evidenceRequest = buildEvidenceRequest(task, searchDefinition, marketDefinition);
  return {
    ...searchDefinition,
    evidenceRequest,
    _investigationTask: evidenceRequest.investigationTaskId,
    _evidenceType: evidenceRequest.evidenceType,
    _providerIds: evidenceRequest.providerIds,
  };
}

function isEvidenceRequest(value) {
  return (
    value &&
    typeof value === 'object' &&
    typeof value.segment === 'string' &&
    typeof value.evidenceType === 'string' &&
    value.geography &&
    Array.isArray(value.geography.cities)
  );
}

module.exports = {
  buildEvidenceRequest,
  scopeSearchDefinitionForTask,
  geographyFromSearchDefinition,
  resolveSegment,
  cityNameOnly,
  isEvidenceRequest,
};
