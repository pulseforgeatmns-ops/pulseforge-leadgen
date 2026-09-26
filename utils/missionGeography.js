'use strict';

const { MANCHESTER_GEO } = require('../packages/capabilities/discovery/seedProfiles');
const { normalizeLocationPart } = require('./serviceArea');

const US_STATE_ABBR = new Set([
  'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS',
  'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY',
  'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV',
  'WI', 'WY', 'DC',
]);

function normalizeCanonicalCity(value) {
  const normalized = normalizeLocationPart(value);
  if (!normalized) return null;
  return normalized.replace(/\b(nh|new hampshire|ma|maine|me)\b/g, '').replace(/\s+/g, ' ').trim() || null;
}

function extractCityFromAddress(location) {
  const text = String(location || '').trim();
  if (!text) return null;

  const parts = text.split(',').map(part => part.trim()).filter(Boolean);
  for (let i = parts.length - 1; i >= 0; i -= 1) {
    const zipState = parts[i].match(/^(.+?)\s+([A-Za-z]{2})\s+\d{5}(?:-\d{4})?$/);
    if (zipState) {
      const city = normalizeCanonicalCity(zipState[1]);
      if (city) return city;
    }
    const stateOnly = parts[i].match(/^([A-Za-z]{2})$/);
    if (stateOnly && US_STATE_ABBR.has(stateOnly[1].toUpperCase()) && i > 0) {
      const city = normalizeCanonicalCity(parts[i - 1]);
      if (city) return city;
    }
    const cityState = parts[i].match(/^(.+?)\s+([A-Za-z]{2})$/);
    if (cityState && US_STATE_ABBR.has(cityState[2].toUpperCase())) {
      const city = normalizeCanonicalCity(cityState[1]);
      if (city) return city;
    }
  }

  if (parts.length >= 2) {
    const city = normalizeCanonicalCity(parts[parts.length - 2]);
    if (city) return city;
  }
  if (parts.length === 1) {
    const city = normalizeCanonicalCity(parts[0].replace(/\s+[A-Za-z]{2}\b.*$/, '').trim());
    if (city) return city;
  }
  return null;
}

function allowedCitiesFromGeographyLabel(label) {
  const text = String(label || '').trim();
  if (!text) return [];
  if (/greater\s+manchester/i.test(text)) return MANCHESTER_GEO.cities.slice();
  return [];
}

function resolveMissionAllowedCities(input = {}) {
  const buckets = [
    input.allowedCities,
    input.missionCities,
    input.cities,
    input.geography?.cities,
    input.structuredMission?.geography?.cities,
    input.service_area,
    input.clientConfig?.service_area,
  ];
  const merged = [];
  for (const bucket of buckets) {
    if (!Array.isArray(bucket)) continue;
    for (const city of bucket) {
      const normalized = normalizeCanonicalCity(city);
      if (normalized && !merged.includes(normalized)) merged.push(normalized);
    }
  }
  if (merged.length) return merged;
  const fromLabel = allowedCitiesFromGeographyLabel(input.region || input.geography?.region || input.geography?.label);
  return fromLabel.map(normalizeCanonicalCity).filter(Boolean);
}

/**
 * Canonical mission geography predicate — exact city membership only.
 * Fails closed when location is missing or no allowed city can be verified.
 */
function isLocationInMissionGeography({
  location,
  city,
  region: _region,
  allowedCities,
} = {}) {
  const allowed = (allowedCities || [])
    .map(normalizeCanonicalCity)
    .filter(Boolean);
  if (!allowed.length) return true;

  const explicitCity = city ? normalizeCanonicalCity(city) : null;
  const extractedCity = explicitCity || extractCityFromAddress(location);
  if (extractedCity && allowed.includes(extractedCity)) return true;

  const normalizedLocation = normalizeLocationPart(location);
  if (!normalizedLocation) return false;

  const paddedLocation = ` ${normalizedLocation} `;
  const ranked = allowed.slice().sort((a, b) => b.length - a.length);
  return ranked.some(cityName => paddedLocation.includes(` ${cityName} `));
}

function matchedMissionCity({
  location,
  city,
  allowedCities,
} = {}) {
  const allowed = (allowedCities || [])
    .map(normalizeCanonicalCity)
    .filter(Boolean);
  if (!allowed.length) return null;

  const explicitCity = city ? normalizeCanonicalCity(city) : null;
  const extractedCity = explicitCity || extractCityFromAddress(location);
  if (extractedCity && allowed.includes(extractedCity)) {
    return extractedCity.replace(/\b\w/g, ch => ch.toUpperCase());
  }

  const normalizedLocation = normalizeLocationPart(location);
  if (!normalizedLocation) return null;
  const paddedLocation = ` ${normalizedLocation} `;
  const ranked = allowed.slice().sort((a, b) => b.length - a.length);
  for (const cityName of ranked) {
    if (paddedLocation.includes(` ${cityName} `)) {
      return cityName.replace(/\b\w/g, ch => ch.toUpperCase());
    }
  }
  return null;
}

function isProspectServiceAreaConfirmed(row = {}, scope = {}) {
  if (row.service_area_match === false) return false;
  if (row.service_area_match === true) return true;

  const allowedCities = resolveMissionAllowedCities({
    allowedCities: scope.cities,
    missionCities: scope.cities,
    region: scope.region,
  });
  if (!allowedCities.length) return Boolean(String(row.service_area_match || '').trim());

  const location = [
    row.service_area_match,
    row.company_location,
    row.location,
  ].filter(value => value != null && value !== false).join(' ');

  return isLocationInMissionGeography({ location, allowedCities });
}

module.exports = {
  isLocationInMissionGeography,
  matchedMissionCity,
  extractCityFromAddress,
  normalizeCanonicalCity,
  resolveMissionAllowedCities,
  allowedCitiesFromGeographyLabel,
  isProspectServiceAreaConfirmed,
};
