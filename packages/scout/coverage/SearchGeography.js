'use strict';

/**
 * Geography expansion helpers for Scout discovery.
 *
 * Dependency-neutral: must not import EvidenceRequest or DiscoveryCoverageEngine.
 * Extracted to break the CommonJS initialization cycle between those modules.
 */

const { expandGeography } = require('../../acquisition-mission/MissionPlanner');
const { MANCHESTER_GEO } = require('../../capabilities/discovery/seedProfiles');
const { parseGeographyList } = require('../../max/scoutAcquisition/InvestigationProvenance');
const { asText } = require('../../max/scoutAcquisition/Types');

/**
 * Expand mission geography into per-city search workloads. Geography is never executed literally.
 * @param {object} searchDefinition
 * @returns {string[]}
 */
function expandCitiesFromSearchDefinition(searchDefinition = {}) {
  const geo = searchDefinition.geography || {};
  const label = asText(geo.label);
  if (!label) return [];

  if (/greater\s+manchester/i.test(label)) {
    const state = geo.state || 'NH';
    return MANCHESTER_GEO.cities.map((city) => formatCityState(city, state));
  }

  // Multi-city missions execute each city independently with all concepts (SPEC-175).
  if (!/greater/i.test(label)) {
    if (Array.isArray(geo.cities) && geo.cities.length >= 1) {
      const state = geo.state || inferStateFromLabel(label);
      return dedupeCities(geo.cities.map((city) => formatCityState(city, state)));
    }
    const parsed = parseGeographyList(label);
    if (parsed.length > 1) {
      const state = geo.state || inferStateFromLabel(label);
      return dedupeCities(
        parsed.map((part) => formatCityState(String(part).split(',')[0].trim(), state))
      );
    }
    return [label];
  }

  const expanded = expandGeography(label, label);
  if (/greater\s+manchester/i.test(expanded.region || '')) {
    const state = geo.state || 'NH';
    return MANCHESTER_GEO.cities.map((city) => formatCityState(city, state));
  }
  if (expanded.cities && expanded.cities.length > 1) {
    const state = geo.state || inferStateFromLabel(label) || 'NH';
    return dedupeCities(expanded.cities.map((city) => formatCityState(city, state)));
  }

  const baseCities = Array.isArray(geo.cities) && geo.cities.length ? geo.cities.slice() : [];
  const nearby = Array.isArray(geo.permittedNearby) ? geo.permittedNearby.slice() : [];
  const merged = [...new Set([...baseCities, ...nearby])];
  if (merged.length > 1) {
    const state = geo.state || inferStateFromLabel(label);
    return dedupeCities(merged.map((city) => formatCityState(city, state)));
  }

  if (merged.length === 1) {
    const state = geo.state || inferStateFromLabel(label);
    return [formatCityState(merged[0], state)];
  }

  return [label];
}

function inferStateFromLabel(label) {
  const text = asText(label);
  if (/\bNH\b|New Hampshire/i.test(text)) return 'NH';
  if (/\bTN\b|Tennessee/i.test(text)) return 'TN';
  if (/\bWV\b|West Virginia/i.test(text)) return 'WV';
  if (/\bRI\b|Rhode Island/i.test(text)) return 'RI';
  return null;
}

function formatCityState(city, state) {
  const name = asText(city);
  if (!name) return '';
  if (/\b[A-Z]{2}\b/.test(name)) return name;
  return state ? `${name} ${state}` : name;
}

function dedupeCities(cities) {
  const seen = new Set();
  const out = [];
  for (const city of cities) {
    const key = city.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(city);
  }
  return out;
}

module.exports = {
  expandCitiesFromSearchDefinition,
  inferStateFromLabel,
  formatCityState,
  dedupeCities,
};
