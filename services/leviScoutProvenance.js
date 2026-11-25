'use strict';

const HOST_CITIES = Object.freeze({
  'data.cityofchicago.org': Object.freeze({ city: 'Chicago', provider: 'chicago_open_data_permits' }),
  'data.cityofnewyork.us': Object.freeze({ city: 'New York', provider: 'nyc_open_data_permits' }),
});

const PROVIDER_CITIES = Object.freeze({
  chicago_open_data_permits: 'Chicago',
  nyc_open_data_permits: 'New York',
});

function text(value) {
  return value == null ? '' : String(value).trim();
}

function hostOf(url) {
  try {
    return new URL(String(url)).hostname.toLowerCase();
  } catch (_) {
    return '';
  }
}

function urlsOf({ sourceUrls, permalink } = {}) {
  const listed = Array.isArray(sourceUrls) ? sourceUrls : [];
  return [...listed, permalink].filter(Boolean).map(String);
}

function normalizeNamedCity(value) {
  const explicit = text(value);
  if (/^chicago$/i.test(explicit)) return 'Chicago';
  if (/^new york$/i.test(explicit)) return 'New York';
  return null;
}

function trustedCitySignals({
  sourceUrls,
  permalink,
  location,
  provider,
  serviceAreaMatch,
} = {}) {
  const signals = [];
  const hosts = [...new Set(urlsOf({ sourceUrls, permalink }).map(hostOf).filter(Boolean))];
  for (const host of hosts) {
    if (!HOST_CITIES[host]) continue;
    signals.push({ city: HOST_CITIES[host].city, source: 'source_url_host', host, provider: HOST_CITIES[host].provider });
  }
  const providerName = text(provider);
  if (PROVIDER_CITIES[providerName]) {
    signals.push({
      city: PROVIDER_CITIES[providerName],
      source: 'provider',
      host: null,
      provider: providerName,
    });
  }
  const place = text(location || serviceAreaMatch);
  if (/,\s*chicago,\s*il\b/i.test(place) || /^chicago,\s*il\b/i.test(place)) {
    signals.push({
      city: 'Chicago',
      source: 'service_area_match',
      host: null,
      provider: 'chicago_open_data_permits',
    });
  }
  if (/,\s*new york,\s*ny\b/i.test(place) || /^new york,\s*ny\b/i.test(place)) {
    signals.push({
      city: 'New York',
      source: 'service_area_match',
      host: null,
      provider: 'nyc_open_data_permits',
    });
  }
  return signals;
}

function inferCityFromProvenance({
  city,
  sourceUrls,
  permalink,
  location,
  provider,
  serviceAreaMatch,
} = {}) {
  const signals = trustedCitySignals({ sourceUrls, permalink, location, provider, serviceAreaMatch });
  const uniqueCities = [...new Set(signals.map(item => item.city))];
  const explicitCity = normalizeNamedCity(city);
  const conflict = uniqueCities.length > 1
    || (explicitCity && uniqueCities.some(item => item !== explicitCity));
  const conflictingCities = [...new Set([
    ...(explicitCity ? [explicitCity] : []),
    ...uniqueCities,
  ])];

  if (explicitCity) {
    return Object.freeze({
      city: explicitCity,
      provider: explicitCity === 'Chicago' ? 'chicago_open_data_permits' : 'nyc_open_data_permits',
      source: 'explicit_city',
      conflict,
      conflictingCities: conflict ? Object.freeze(conflictingCities) : Object.freeze([]),
    });
  }

  if (uniqueCities.length === 1) {
    const hit = signals.find(item => item.city === uniqueCities[0]);
    return Object.freeze({
      city: hit.city,
      provider: hit.provider,
      source: hit.source,
      host: hit.host || null,
      conflict: false,
      conflictingCities: Object.freeze([]),
    });
  }
  if (uniqueCities.length > 1) {
    return Object.freeze({
      city: null,
      provider: text(provider) || null,
      source: 'ambiguous_source_hosts',
      conflict: true,
      conflictingCities: Object.freeze(uniqueCities),
      limitation: 'Multiple trusted source hosts were present; city was not inferred.',
    });
  }

  const explicit = text(city);
  return Object.freeze({
    city: explicit || null,
    provider: text(provider) || null,
    source: explicit ? 'unnormalized_city' : 'none',
    conflict: false,
    conflictingCities: Object.freeze([]),
  });
}

function applyCityToMetadata(metadata = {}, extras = {}) {
  const current = metadata && typeof metadata === 'object' ? { ...metadata } : {};
  const hostInference = inferCityFromProvenance({
    sourceUrls: current.sourceUrls,
    permalink: extras.permalink,
    location: extras.location,
    provider: current.provider,
    serviceAreaMatch: extras.serviceAreaMatch,
  });
  const inferred = inferCityFromProvenance({
    city: current.city,
    sourceUrls: current.sourceUrls,
    permalink: extras.permalink,
    location: extras.location,
    provider: current.provider,
    serviceAreaMatch: extras.serviceAreaMatch,
  });
  const existingCity = text(current.city);
  const conflict = Boolean(
    inferred.conflict
    || hostInference.conflict
    || (existingCity && hostInference.city && hostInference.city !== existingCity)
  );
  const conflictingCities = [...new Set([
    ...(existingCity ? [existingCity] : []),
    ...(inferred.conflictingCities || []),
    ...(hostInference.conflictingCities || []),
    ...(hostInference.city ? [hostInference.city] : []),
  ])];
  if (!existingCity && inferred.city && !conflict) {
    current.city = inferred.city;
    current.cityInference = Object.freeze({
      source: inferred.source,
      host: inferred.host || null,
      preservedOriginalCity: null,
      conflict: false,
    });
  } else if (conflict) {
    current.cityInference = Object.freeze({
      source: existingCity ? 'preserved_existing_city' : 'conflicting_trusted_evidence',
      conflict: true,
      existing: existingCity || null,
      inferred: hostInference.city || null,
      inferredSource: hostInference.source,
      conflictingCities: Object.freeze(conflictingCities),
    });
  }
  if (!text(current.provider) && inferred.provider && !conflict) current.provider = inferred.provider;
  return { metadata: current, inferred };
}

module.exports = {
  HOST_CITIES,
  PROVIDER_CITIES,
  inferCityFromProvenance,
  applyCityToMetadata,
};
