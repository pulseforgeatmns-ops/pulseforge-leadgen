function normalizeLocationPart(value) {
  return String(value == null ? '' : value)
    .trim()
    .toLocaleLowerCase('en-US')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function configuredServiceAreas(clientConfig) {
  return Array.isArray(clientConfig?.service_area)
    ? clientConfig.service_area.map(area => String(area || '').trim()).filter(Boolean)
    : [];
}

function matchServiceAreaLocality(locality, serviceAreas) {
  const normalizedLocality = normalizeLocationPart(locality);
  if (!normalizedLocality) return null;
  return (serviceAreas || []).find(area => normalizeLocationPart(area) === normalizedLocality) || null;
}

function matchServiceAreaFromLocation(location, serviceAreas) {
  const normalizedLocation = normalizeLocationPart(location);
  if (!normalizedLocation) return null;
  const paddedLocation = ` ${normalizedLocation} `;
  const candidates = (serviceAreas || [])
    .map(area => ({ area, normalized: normalizeLocationPart(area) }))
    .filter(candidate => candidate.normalized)
    .sort((a, b) => b.normalized.length - a.normalized.length);
  return candidates.find(candidate => paddedLocation.includes(` ${candidate.normalized} `))?.area || null;
}

function getAddressComponent(addressComponents, type) {
  const component = (Array.isArray(addressComponents) ? addressComponents : [])
    .find(item => Array.isArray(item?.types) && item.types.includes(type));
  if (!component) return null;
  return String(component.long_name || component.short_name || '').trim() || null;
}

function parsePlacesAddressComponents(addressComponents) {
  return {
    locality: getAddressComponent(addressComponents, 'locality'),
    administrativeAreaLevel1: getAddressComponent(addressComponents, 'administrative_area_level_1'),
    postalCode: getAddressComponent(addressComponents, 'postal_code'),
  };
}

module.exports = {
  configuredServiceAreas,
  matchServiceAreaFromLocation,
  matchServiceAreaLocality,
  normalizeLocationPart,
  parsePlacesAddressComponents,
};
