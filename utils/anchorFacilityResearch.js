'use strict';

// SPEC-SCOUT-P0-004. Pure observed-evidence decisions; no providers or writes.
const { createHash } = require('node:crypto');
const VERSION = 'anchor-facility-research-v1';
const FLAG = 'SCOUT_ANCHOR_FACILITY_RECOVERY_ENABLED';
const enabled = (tenantId, researchOnly, env = process.env) =>
  String(tenantId) === '10' && researchOnly === true && env[FLAG] === 'true';
const text = value => typeof value === 'string' ? value.trim() : '';
const norm = value => text(value).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
const unique = values => [...new Set(values.filter(Boolean))];
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');

function observedRows(candidate) {
  return [candidate, ...(Array.isArray(candidate.classificationObservations) ? candidate.classificationObservations : [])];
}
function observedText(candidate) {
  // Queries, segments, legacy verticals and industry labels are NOT facts.
  return observedRows(candidate).flatMap(row => [row.name, row.companyName, row.company,
    row.description, row.businessType, row.snippet]).filter(x => typeof x === 'string').join(' ');
}
function placeTypes(candidate) {
  return unique(observedRows(candidate).flatMap(row => row.placeTypes || row.types || row.place_types || [])
    .filter(x => typeof x === 'string').map(x => x.toLowerCase().replace(/[ -]+/g, '_')));
}
function verifiedAttributes(candidate) {
  const attributes = candidate.verifiedBusinessAttributes;
  return attributes?.verified === true && text(attributes.source) ? attributes : {};
}

function recognizeFacility(candidate) {
  const types = placeTypes(candidate), hay = observedText(candidate), verified = verifiedAttributes(candidate);
  if (/\bschool district\b/i.test(hay)) return { status: 'recognized', vertical: 'school', category: 'education_authority',
    reason: null, evidence: [{ field: 'observed_business_text', value: hay.slice(0, 600) }] };
  const found = [];
  const add = (vertical, category, pattern, names) => {
    const type = types.find(x => names.includes(x));
    if (type || pattern?.test(hay)) found.push({ vertical, category,
      evidence: type ? { field: 'google_place_type', value: type } : { field: 'observed_business_text', value: hay.slice(0, 600) } });
  };
  add('campus', 'higher_education', /\b(?:university|college|unh)\b/i, ['university', 'college']);
  add('school', 'education', /\b(?:school|academy)\b/i, ['school', 'primary_school', 'secondary_school']);
  add('dealership', 'dealership', /\b(?:auto|car|vehicle|automotive) dealership\b|\bdealer group\b/i, ['car_dealer']);
  add('manufacturer', 'manufacturing', /\bmanufactur(?:er|ing)\b|\bproduction (?:plant|facility)\b|\bfactory\b/i,
    ['manufacturer', 'factory']);
  if (verified.manufacturingOperations === true && !found.some(x => x.vertical === 'manufacturer'))
    found.push({ vertical: 'manufacturer', category: 'manufacturing', evidence: { field: 'verified_attribute', value: 'manufacturingOperations', source: verified.source } });
  add('warehouse', 'industrial_logistics', /\bwarehouse|\bdistribution (?:center|centre|facility)\b|\blogistics (?:facility|center|centre|hub|company)\b/i,
    ['warehouse', 'distribution_center', 'logistics_service', 'self_storage_facility']);
  add('restaurant', 'restaurant_foh', /\b(?:restaurant|cafe|bistro|food truck)\b/i, ['restaurant', 'cafe', 'food_truck']);
  add('property_manager', 'commercial_property_management', /\b(?:commercial |industrial )?property manag(?:ement|er)\b/i, []);
  add('commercial_office', 'commercial_facility', /\boffice park\b|\b(?:commercial|professional) office building\b|\bbusiness cent(?:er|re)\b/i, []);
  // A college can also carry Google's generic school type.
  const matches = found.some(x => x.vertical === 'campus') ? found.filter(x => x.vertical !== 'school') : found;
  if (matches.length !== 1) return { status: 'ambiguous', vertical: null, category: null,
    reason: matches.length ? 'conflicting_facility_categories' : 'facility_category_evidence_missing', evidence: matches.map(x => x.evidence) };
  return { status: 'recognized', reason: null, ...matches[0], evidence: [matches[0].evidence] };
}

function geographicObservation(candidate, allowedCities) {
  const address = text(candidate.formattedAddress || candidate.formatted_address || candidate.address || candidate.location);
  const components = candidate.addressComponents || candidate.address_components || [];
  const values = type => unique((Array.isArray(components) ? components : [])
    .filter(row => row?.types?.includes(type)).map(row => text(row.short_name || row.long_name)));
  const countries = values('country').map(x => /^(?:US|USA|United States)$/i.test(x) ? 'US' : x.toUpperCase());
  const states = values('administrative_area_level_1').map(x => /^(NH|New Hampshire)$/i.test(x) ? 'NH' : x.toUpperCase());
  const cities = [...values('locality'), ...values('postal_town')].map(norm);
  if (text(candidate.country)) countries.push(/^(US|USA|United States)$/i.test(candidate.country) ? 'US' : candidate.country.toUpperCase());
  if (text(candidate.state)) states.push(/^(NH|New Hampshire)$/i.test(candidate.state) ? 'NH' : candidate.state.toUpperCase());
  if (/\b(?:UK|United Kingdom|England|Scotland|Wales|Canada|France|Australia)\s*$/i.test(address)) countries.push('NON_US');
  else if (/\b(?:US|USA|United States)\s*$/i.test(address)) countries.push('US');
  const stateMatch = address.match(/(?:^|,)\s*([^,]+),?\s+(NH|New Hampshire|[A-Z]{2})\s*(?:\d{5}(?:-\d{4})?)?(?:,|$)/);
  if (stateMatch) {
    const state = /^(NH|New Hampshire)$/i.test(stateMatch[2]) ? 'NH' : stateMatch[2];
    states.push(state); cities.push(norm(stateMatch[1]));
  }
  // Explicit, unique US state + five-digit postal address can establish US.
  if (!countries.length && states.includes('NH') && /\b\d{5}(?:-\d{4})?\b/.test(address)) countries.push('US');
  const country = unique(countries), state = unique(states), city = unique(cities);
  const evidence = { address: address || null, countries: country, states: state, cities: city };
  if (country.some(x => x !== 'US')) return { status: country.includes('US') ? 'deferred' : 'ineligible', reason: 'outside_us', evidence };
  if (state.some(x => x !== 'NH')) return { status: state.includes('NH') ? 'deferred' : 'ineligible', reason: 'outside_new_hampshire', evidence };
  if (country.length !== 1 || state.length !== 1 || city.length !== 1)
    return { status: 'deferred', reason: 'geography_evidence_missing_or_conflicting', evidence };
  const allowed = unique((allowedCities || []).map(norm));
  if (!allowed.length) return { status: 'deferred', reason: 'approved_cities_missing', evidence };
  return { status: allowed.includes(city[0]) ? 'eligible' : 'ineligible',
    reason: allowed.includes(city[0]) ? null : 'outside_approved_towns', evidence };
}

function validateFacilityGeography(candidate, allowedCities) {
  const observations = observedRows(candidate).filter(row => row.address || row.location || row.formattedAddress || row.addressComponents || row.address_components);
  if (!observations.length) return geographicObservation(candidate, allowedCities);
  const results = observations.map(row => geographicObservation(row, allowedCities));
  if (results.some(x => x.status === 'ineligible') && results.some(x => x.status === 'eligible'))
    return { status: 'deferred', reason: 'conflicting_location_observations', evidence: results.map(x => x.evidence) };
  return results.find(x => x.status === 'ineligible') || results.find(x => x.status === 'deferred') || results[0];
}

function assessFacilitySuitability(candidate, recognition) {
  const hay = observedText(candidate), types = placeTypes(candidate), verified = verifiedAttributes(candidate);
  const excluded = reason => ({ status: 'unsuitable', reason, missingEvidence: [] });
  if (observedRows(candidate).some(x => /CLOSED_(?:PERMANENTLY|TEMPORARILY)/.test(x.businessStatus || x.business_status || '')))
    return excluded('premises_not_operational');
  if (/\b(?:online.only|virtual.only|mailbox.only|po box|p\.?o\.? box)\b/i.test(hay) || verified.physicalPremises === false)
    return excluded('no_service_premises');
  const institutionLabel = observedRows(candidate).flatMap(row => [row.name, row.companyName, row.company, row.businessType]).filter(Boolean).join(' ');
  if (recognition.vertical === 'school' && (/\b(?:driving|beauty|cosmetology|barber|martial arts|bjj|mma|karate|taekwondo|judo|music|gymnastics|dance)\b/i.test(institutionLabel)
    || /\b(?:music.only|only music|solely music)\b/i.test(hay)))
    return excluded('instructional_studio_not_institutional_school');
  if (recognition.vertical === 'warehouse' && (/\bself[ -]?storage\b|\bmailbox\b/i.test(hay) || types.includes('self_storage_facility')))
    return excluded('storage_or_mailbox_not_industrial_facility');
  if (recognition.vertical === 'manufacturer' && /\b(?:retailer|retail store|supplier|supplies|distributor|manufacturers? representative)\b/i.test(hay)
    && verified.manufacturingOperations !== true && !/\b(?:operates?|operating) (?:a |an )?(?:manufacturing|production|factory)\b/i.test(hay))
    return excluded('no_manufacturing_operations_evidenced');
  if (recognition.vertical === 'restaurant' && (/\bfood truck\b|\bmobile (?:food|catering)\b/i.test(hay) || types.includes('food_truck')))
    return excluded('mobile_food_business');
  const missing = [];
  if (recognition.status !== 'recognized') missing.push('facility_category');
  if (recognition.vertical === 'school' && !types.some(x => ['school', 'primary_school', 'secondary_school'].includes(x))
    && !/\b(?:elementary|primary|secondary|high|middle|charter|public|private|preparatory) school\b/i.test(hay)) missing.push('educational_program');
  if (/\b(?:administration|administrative|admissions|graduate school)\b/i.test(hay) && verified.distinctServicePremises !== true)
    missing.push('distinct_service_premises');
  if (recognition.category === 'education_authority' && verified.distinctServicePremises !== true) missing.push('distinct_service_premises');
  const address = text(candidate.address || candidate.location || candidate.formattedAddress || candidate.formatted_address);
  const components = candidate.addressComponents || candidate.address_components || [];
  if (verified.physicalPremises !== true && !address.split(',').some(part => /^\s*\d+[A-Za-z]?\s+[A-Za-z]/.test(part))
    && !(Array.isArray(components) && components.some(x => x.types?.includes('street_number')))) missing.push('physical_service_premises');
  if (recognition.vertical === 'property_manager' && !/\b(?:commercial|industrial|office|facilit(?:y|ies))\b/i.test(hay)
    && verified.commercialProcurement !== true) missing.push('commercial_facility_purchasing_responsibility');
  return { status: missing.length ? 'deferred' : 'suitable', reason: missing.length ? 'facility_evidence_missing' : null, missingEvidence: unique(missing) };
}

function facilityIdentity(candidate) {
  const placeId = text(candidate.placeId || candidate.place_id);
  const name = text(candidate.name || candidate.companyName || candidate.company);
  const address = text(candidate.address || candidate.location || candidate.formattedAddress || candidate.formatted_address);
  return { facilityId: placeId ? `google_places:${placeId}` : `observed:${digest([norm(name), norm(address)])}`,
    name, address: address || null, placeId: placeId || null };
}
function procurementDecision(candidate) {
  const verified = verifiedAttributes(candidate);
  const authority = text(verified.procurementAuthorityId);
  return { ...facilityIdentity(candidate), status: authority ? 'verified' : 'unverified',
    procurementAuthorityId: authority || null, evidenceSource: authority ? verified.source : null,
    independentPurchasingAuthority: authority ? verified.independentPurchasingAuthority === true : false,
    missingEvidence: authority ? [] : ['purchasing_authority_and_service_scope'] };
}

function evaluateFacilityResearch(candidate, context = {}) {
  const recognition = recognizeFacility(candidate);
  const suitability = assessFacilitySuitability(candidate, recognition);
  const geography = validateFacilityGeography(candidate, context.missionCities || context.allowedCities);
  const procurement = procurementDecision(candidate);
  const segments = context.missionSegments || [];
  const scopeAllowed = recognition.status !== 'recognized' ? segments.length > 0
    : recognition.vertical === 'restaurant' ? segments.includes('restaurant_foh') : segments.includes('managed_facilities');
  const identityComplete = Boolean(text(candidate.name || candidate.companyName || candidate.company)
    && text(candidate.website || candidate.website_url || candidate.url || candidate.domain));
  const rejected = geography.status === 'ineligible' || suitability.status === 'unsuitable' || !scopeAllowed;
  const admitted = !rejected && identityComplete && recognition.status === 'recognized' && suitability.status === 'suitable' && geography.status === 'eligible';
  const reason = geography.status === 'ineligible' ? 'outside_geography' : suitability.status === 'unsuitable' ? 'contradictory_business_type'
    : !scopeAllowed ? 'segment_mismatch' : geography.status === 'deferred' ? 'geography_ambiguous'
      : !identityComplete ? 'insufficient_business_fit' : admitted ? null : 'unclassifiable_vertical';
  return { admitted, vertical: recognition.vertical, reason, deferred: !admitted && !rejected,
    facilityDecision: { version: VERSION, recognition, suitability, geography, procurement,
      admission: { status: admitted ? 'eligible_for_research_retention' : rejected ? 'rejected' : 'deferred',
        independentSalesOpportunity: false, downstreamAuthorized: false } } };
}

// Relationship candidates only. No facility merge and no sales-opportunity group
// exists until explicit procurement evidence supports it.
function potentialProcurementGroups(candidates) {
  const groups = [];
  const hints = new Map();
  const add = (key, reason, row) => {
    if (!hints.has(key)) hints.set(key, { reason, facilities: [] });
    hints.get(key).facilities.push(procurementDecision(row));
  };
  for (const row of candidates) {
    const verified = verifiedAttributes(row);
    if (text(verified.procurementAuthorityId)) add(`verified:${verified.procurementAuthorityId}`, 'verified_authority_reference', row);
    let domain = text(row.domain || row.website || row.url).replace(/^https?:\/\//, '').split('/')[0].toLowerCase();
    const labels = domain.split('.');
    // Only a relationship hint, never ownership. Avoid guessing compound
    // public suffixes such as co.uk without a verified suffix registry.
    if (labels.length > 2 && ['com', 'org', 'net', 'edu', 'gov', 'example', 'test'].includes(labels.at(-1)))
      domain = labels.slice(-2).join('.');
    if (domain) add(`domain:${domain}`, 'shared_domain_unverified', row);
    const parts = text(row.address || row.location).split(',');
    const street = parts.findIndex(x => /^\s*\d+\s+[A-Za-z]/.test(x));
    const address = norm((street < 0 ? parts : parts.slice(street)).join(',').replace(/#\s*\w+|\b(?:ste|suite|unit)\s+\w+/gi, ''));
    if (address) add(`address:${address}`, 'co_location_only', row);
    if (/\b(?:school district|elementary school|middle school|high school)\b/i.test(observedText(row)))
      add('review:school_district', 'possible_district_relationship_requires_verification', row);
  }
  for (const [key, group] of hints) {
    const facilities = [...new Map(group.facilities.map(x => [x.facilityId, x])).values()];
    if (facilities.length > 1) groups.push({ key, reason: group.reason, verified: key.startsWith('verified:'),
      mergeFacilities: false, salesOpportunityGroupingAuthorized: false, facilities });
  }
  return groups;
}

module.exports = { VERSION, FLAG, enabled, observedText, recognizeFacility, validateFacilityGeography,
  assessFacilitySuitability, facilityIdentity, procurementDecision, evaluateFacilityResearch, potentialProcurementGroups };
