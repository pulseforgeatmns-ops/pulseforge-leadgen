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

function inferCityFromProvenance({
  city,
  sourceUrls,
  permalink,
  location,
  provider,
  serviceAreaMatch,
} = {}) {
  const explicit = text(city);
  if (/^chicago$/i.test(explicit)) {
    return Object.freeze({
      city: 'Chicago',
      provider: 'chicago_open_data_permits',
      source: 'explicit_city',
    });
  }
  if (/^new york$/i.test(explicit)) {
    return Object.freeze({
      city: 'New York',
      provider: 'nyc_open_data_permits',
      source: 'explicit_city',
    });
  }

  const hosts = [...new Set(urlsOf({ sourceUrls, permalink }).map(hostOf).filter(Boolean))];
  const matched = hosts.map(host => HOST_CITIES[host] ? { host, ...HOST_CITIES[host] } : null).filter(Boolean);
  const uniqueCities = [...new Set(matched.map(item => item.city))];
  if (uniqueCities.length === 1) {
    const hit = matched[0];
    return Object.freeze({
      city: hit.city,
      provider: hit.provider,
      source: 'source_url_host',
      host: hit.host,
    });
  }
  if (uniqueCities.length > 1) {
    return Object.freeze({
      city: null,
      provider: text(provider) || null,
      source: 'ambiguous_source_hosts',
      limitation: 'Multiple trusted source hosts were present; city was not inferred.',
    });
  }

  const providerName = text(provider);
  if (PROVIDER_CITIES[providerName]) {
    return Object.freeze({
      city: PROVIDER_CITIES[providerName],
      provider: providerName,
      source: 'provider',
    });
  }

  const place = text(location || serviceAreaMatch);
  if (/,\s*chicago,\s*il\b/i.test(place) || /^chicago,\s*il\b/i.test(place)) {
    return Object.freeze({
      city: 'Chicago',
      provider: 'chicago_open_data_permits',
      source: 'service_area_match',
    });
  }
  if (/,\s*new york,\s*ny\b/i.test(place) || /^new york,\s*ny\b/i.test(place)) {
    return Object.freeze({
      city: 'New York',
      provider: 'nyc_open_data_permits',
      source: 'service_area_match',
    });
  }

  return Object.freeze({
    city: explicit || null,
    provider: providerName || null,
    source: explicit ? 'unnormalized_city' : 'none',
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
  if (!existingCity && inferred.city) {
    current.city = inferred.city;
    current.cityInference = Object.freeze({
      source: inferred.source,
      host: inferred.host || null,
      preservedOriginalCity: null,
    });
  } else if (existingCity && hostInference.city && hostInference.city !== existingCity) {
    current.cityInference = Object.freeze({
      source: 'preserved_existing_city',
      existing: existingCity,
      inferred: hostInference.city,
      inferredSource: hostInference.source,
    });
  }
  if (!text(current.provider) && inferred.provider) current.provider = inferred.provider;
  return { metadata: current, inferred };
}

module.exports = {
  HOST_CITIES,
  PROVIDER_CITIES,
  inferCityFromProvenance,
  applyCityToMetadata,
};
