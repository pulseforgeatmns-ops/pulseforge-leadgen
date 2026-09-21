'use strict';

/**
 * Canonical website discovery for mission-bound candidates with company identity
 * but no website/domain. Runs before contact enrichment.
 *
 * Resolution order:
 *   A. Google Place Details (place_id)
 *   B. Upstream Scout/Max mission payloads
 *   C. Places text search (name + geography) with identity validation
 *   D. manual_review when confidence is insufficient
 *
 * Never fabricates domains from company names.
 */

const { legacyPlaceDetails, legacyTextSearch } = require('../../../utils/placesApi');
const { PLACES_FEATURES } = require('../../../utils/placesCostAttribution');
const { normalizeDomain: canonNormalizeDomain, classifyCompanyUrl } = require('../../../utils/canonicalEmailEligibility');
const { normalizeDomain, asText, isGooglePlaceId } = require('./CanonicalOutboundIdentity');
const {
  resolveMissionBoundWebsiteIntel,
  candidateWebsiteFields,
} = require('./MissionBoundWebsiteIntel');

const PLACE_DETAILS_FIELDS =
  'name,formatted_address,address_components,formatted_phone_number,website,place_id,rating,user_ratings_total,types';

const MIN_PERSIST_CONFIDENCE = 4;

const RESOLUTION_SOURCES = Object.freeze({
  PLACE_DETAILS: 'google_places.place_details',
  UPSTREAM: 'mission_bound.upstream',
  TEXT_SEARCH: 'google_places.text_search',
  MANUAL_REVIEW: 'manual_review',
});

function normalizeName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function nameTokens(value) {
  const stop = new Set(['llc', 'inc', 'corp', 'co', 'company', 'the', 'and', 'of', 'property', 'management', 'properties']);
  return normalizeName(value)
    .split(' ')
    .filter((token) => token.length > 1 && !stop.has(token));
}

function digits(value) {
  return String(value || '').replace(/\D/g, '');
}

function addressKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\b(road)\b/g, 'rd')
    .replace(/\b(street)\b/g, 'st')
    .replace(/\b(turnpike)\b/g, 'tpke')
    .replace(/[^a-z0-9]/g, '');
}

function geographyTokens(location = '') {
  const text = String(location || '').toLowerCase();
  const tokens = [];
  if (/manchester|nh|new hampshire|bedford|goffstown|hooksett|londonderry|auburn|nashville|tn|tennessee/.test(text)) {
    tokens.push('geo_match');
  }
  for (const part of text.split(/[,|]/).map((row) => row.trim()).filter(Boolean)) {
    if (part.length >= 3) tokens.push(part);
  }
  return tokens;
}

function namesAlign(expectedName, observedName) {
  const expected = normalizeName(expectedName);
  const observed = normalizeName(observedName);
  if (!expected || !observed) return false;
  if (expected === observed) return true;
  if (observed.includes(expected) || expected.includes(observed)) return true;
  const expectedTokens = nameTokens(expectedName);
  if (!expectedTokens.length) return false;
  const observedSet = new Set(nameTokens(observedName));
  return expectedTokens.every((token) => observedSet.has(token));
}

/**
 * Score identity alignment between a candidate and a Places result.
 * @returns {{ confidence: number, matchBasis: string[], evidence: object }}
 */
function scorePlaceIdentity(candidate = {}, placeDetails = {}) {
  const matchBasis = [];
  let confidence = 0;

  const candidatePlaceId = asText(candidate.placeId || candidate.place_id);
  const detailsPlaceId = asText(placeDetails.place_id);
  if (candidatePlaceId && detailsPlaceId && candidatePlaceId === detailsPlaceId) {
    confidence += 5;
    matchBasis.push('place_id');
  }

  const candidateName = candidate.company || candidate.name;
  const detailsName = placeDetails.name;
  if (namesAlign(candidateName, detailsName)) {
    confidence += 2;
    matchBasis.push('company_name');
  } else if (candidateName && detailsName) {
    return {
      confidence: 0,
      matchBasis: ['name_mismatch'],
      evidence: {
        candidateName,
        detailsName,
        placeId: detailsPlaceId || candidatePlaceId || null,
      },
    };
  }

  const candidatePhone = digits(candidate.phone);
  const detailsPhone = digits(placeDetails.formatted_phone_number);
  if (candidatePhone && detailsPhone && candidatePhone === detailsPhone) {
    confidence += 4;
    matchBasis.push('phone');
  }

  const candidateAddress = addressKey(candidate.address || candidate.location);
  const detailsAddress = addressKey(placeDetails.formatted_address);
  if (candidateAddress && detailsAddress && (detailsAddress.includes(candidateAddress) || candidateAddress.includes(detailsAddress))) {
    confidence += 5;
    matchBasis.push('address');
  }

  const geoTokens = geographyTokens(candidate.location || candidate.address || placeDetails.formatted_address);
  if (geoTokens.includes('geo_match')) {
    confidence += 1;
    matchBasis.push('geography');
  }

  return {
    confidence,
    matchBasis,
    evidence: {
      candidateName,
      detailsName: detailsName || null,
      placeId: detailsPlaceId || candidatePlaceId || null,
      formattedAddress: placeDetails.formatted_address || null,
      phone: placeDetails.formatted_phone_number || null,
    },
  };
}

function isPersistableConfidence(confidence) {
  return Number(confidence) >= MIN_PERSIST_CONFIDENCE;
}

function isCanonicalWebsite(url) {
  const classification = classifyCompanyUrl(url);
  return classification === 'official' || classification === 'hosted_builder';
}

function normalizeWebsite(url) {
  const raw = asText(url);
  if (!raw) return null;
  const website = raw.includes('://') ? raw : `https://${raw}`;
  const domain = normalizeDomain(website);
  if (!domain || !isCanonicalWebsite(website)) return null;
  return { website, domain };
}

function collectScoutEvidenceUrls(candidate = {}) {
  const urls = [];
  for (const trace of candidate.websiteTraces || []) {
    if (trace.website) urls.push(trace.website);
    if (trace.raw && String(trace.raw).includes('://')) urls.push(trace.raw);
  }
  return urls;
}

function buildDiscoveryInput(candidate = {}, crmRow = null) {
  return {
    placeId: candidate.placeId || crmRow?.google_place_id || null,
    companyName: candidate.company || crmRow?.company_name || null,
    location: candidate.location || null,
    phone: candidate.phone || null,
    address: candidate.address || null,
    scoutEvidenceUrls: collectScoutEvidenceUrls(candidate),
    crmCompanyId: crmRow?.company_id || null,
    crmDomain: crmRow?.domain || null,
    crmWebsite: crmRow?.website || crmRow?.website_url || null,
  };
}

async function fetchPlaceDetailsWebsite(placeId, deps = {}) {
  const apiKey = deps.apiKey || process.env.GOOGLE_PLACES_KEY || '';
  if (!placeId || !apiKey) return null;
  const traced = await legacyPlaceDetails({
    placeId,
    fields: PLACE_DETAILS_FIELDS,
    apiKey,
    fetchImpl: deps.fetchImpl,
    record: {
      caller: 'MissionBoundWebsiteDiscovery',
      feature: PLACES_FEATURES.SCRIPT,
      missionId: deps.missionId || null,
      tenantId: deps.tenantId || null,
    },
  }, deps.placesDeps || {});
  if (traced.googleStatus !== 'OK') return null;
  return traced.data?.result || null;
}

async function searchPlaceByIdentity(candidate, deps = {}) {
  const apiKey = deps.apiKey || process.env.GOOGLE_PLACES_KEY || '';
  if (!apiKey) return null;
  const query = [candidate.company, candidate.location || candidate.address, 'NH']
    .filter(Boolean)
    .join(' ');
  if (!query.trim()) return null;

  const traced = await legacyTextSearch({
    query,
    apiKey,
    fetchImpl: deps.fetchImpl,
    record: {
      caller: 'MissionBoundWebsiteDiscovery',
      feature: PLACES_FEATURES.SCRIPT,
      missionId: deps.missionId || null,
      tenantId: deps.tenantId || null,
    },
  }, deps.placesDeps || {});

  if (traced.googleStatus !== 'OK') return null;
  const results = traced.data?.results || [];
  if (!results.length) return null;

  const candidatePlaceId = asText(candidate.placeId);
  const exact = candidatePlaceId
    ? results.find((row) => asText(row.place_id) === candidatePlaceId)
    : null;
  if (exact) {
    return fetchPlaceDetailsWebsite(exact.place_id, deps);
  }

  let best = null;
  let bestScore = 0;
  for (const hit of results.slice(0, 5)) {
    const details = await fetchPlaceDetailsWebsite(hit.place_id, deps);
    if (!details) continue;
    const identity = scorePlaceIdentity(candidate, details);
    if (identity.confidence > bestScore) {
      best = details;
      bestScore = identity.confidence;
    }
  }
  return bestScore >= MIN_PERSIST_CONFIDENCE ? best : null;
}

function resolveUpstreamWebsite(candidate = {}) {
  const fields = candidateWebsiteFields(candidate);
  if (fields.domain || fields.website) {
    const normalized = normalizeWebsite(fields.website || fields.domain);
    if (normalized) {
      return {
        website: normalized.website,
        domain: normalized.domain,
        source: RESOLUTION_SOURCES.UPSTREAM,
        resolver: fields.websiteSource || 'mission_bound.upstream',
        confidence: 6,
        matchBasis: ['upstream_payload', fields.websiteFieldPath].filter(Boolean),
        evidence: {
          websiteFieldPath: fields.websiteFieldPath || null,
          websiteSource: fields.websiteSource || null,
          traces: candidate.websiteTraces || [],
        },
      };
    }
  }
  return null;
}

function resolutionFromPlaceDetails(candidate, placeDetails, source) {
  const normalized = normalizeWebsite(placeDetails?.website);
  if (!normalized) return null;
  const identity = scorePlaceIdentity(candidate, placeDetails);
  if (!isPersistableConfidence(identity.confidence)) {
    return {
      action: 'manual_review',
      source: RESOLUTION_SOURCES.MANUAL_REVIEW,
      resolver: source,
      confidence: identity.confidence,
      matchBasis: identity.matchBasis,
      evidence: {
        ...identity.evidence,
        rejectedWebsite: placeDetails.website,
        reason: 'identity_confidence_insufficient',
      },
      resolvedWebsite: normalized.website,
      resolvedDomain: normalized.domain,
    };
  }
  return {
    action: 'resolve',
    website: normalized.website,
    domain: normalized.domain,
    source,
    resolver: 'google_places',
    confidence: identity.confidence,
    matchBasis: identity.matchBasis,
    evidence: identity.evidence,
  };
}

/**
 * Discover canonical website/domain for one mission-bound candidate.
 * @returns {Promise<object>}
 */
async function discoverMissionBoundWebsite(candidate = {}, opts = {}) {
  const crmRow = opts.crmRow || null;
  const input = buildDiscoveryInput(candidate, crmRow);

  if (crmRow?.domain || crmRow?.website || crmRow?.website_url) {
    return {
      action: 'skip_existing_website',
      reason: 'crm_already_has_website',
      input,
      currentWebsite: crmRow.website || crmRow.website_url || null,
      currentDomain: crmRow.domain || null,
    };
  }

  const upstream = resolveUpstreamWebsite(candidate);
  if (upstream) {
    const identity = scorePlaceIdentity(candidate, {
      name: candidate.company,
      place_id: candidate.placeId,
      formatted_address: candidate.location || candidate.address,
      formatted_phone_number: candidate.phone,
    });
    const confidence = Math.max(upstream.confidence, identity.confidence);
    if (isPersistableConfidence(confidence)) {
      return {
        action: 'resolve',
        ...upstream,
        confidence,
        matchBasis: [...new Set([...(upstream.matchBasis || []), ...(identity.matchBasis || [])])],
      };
    }
  }

  if (input.placeId && isGooglePlaceId(input.placeId)) {
    const placeDetails = await fetchPlaceDetailsWebsite(input.placeId, opts);
    if (placeDetails?.website) {
      const resolution = resolutionFromPlaceDetails(
        candidate,
        placeDetails,
        RESOLUTION_SOURCES.PLACE_DETAILS
      );
      if (resolution) return resolution;
    }
  }

  const searched = await searchPlaceByIdentity(candidate, opts);
  if (searched?.website) {
    const resolution = resolutionFromPlaceDetails(
      candidate,
      searched,
      RESOLUTION_SOURCES.TEXT_SEARCH
    );
    if (resolution) return resolution;
  }

  return {
    action: 'manual_review',
    source: RESOLUTION_SOURCES.MANUAL_REVIEW,
    resolver: null,
    confidence: 0,
    matchBasis: [],
    evidence: {
      input,
      scoutEvidenceUrls: input.scoutEvidenceUrls,
      reason: 'no_confident_website_match',
    },
    reason: 'no_confident_website_match',
  };
}

function discoveryProvenance(candidate, missionId, resolution = {}) {
  return {
    mission_bound_website_discovery: {
      source: resolution.source || RESOLUTION_SOURCES.MANUAL_REVIEW,
      resolver: resolution.resolver || null,
      mission_id: missionId,
      candidate_id: candidate.candidateId || candidate.id || null,
      place_id: candidate.placeId || null,
      discovered_at: new Date().toISOString(),
      domain: resolution.domain || null,
      website: resolution.website || null,
      confidence: resolution.confidence ?? null,
      match_basis: resolution.matchBasis || [],
      evidence: resolution.evidence || null,
    },
  };
}

function buildDiscoveryPlan(candidate, crmRow, resolution, missionId) {
  const base = {
    company: candidate.company,
    candidateId: candidate.candidateId,
    placeId: candidate.placeId,
    currentWebsite: crmRow?.website || crmRow?.website_url || null,
    currentDomain: crmRow?.domain || null,
    resolvedWebsite: resolution.website || resolution.resolvedWebsite || null,
    resolvedDomain: resolution.domain || resolution.resolvedDomain || null,
    source: resolution.source || null,
    confidence: resolution.confidence ?? null,
    evidence: resolution.evidence || null,
    matchBasis: resolution.matchBasis || [],
  };

  if (resolution.action === 'skip_existing_website') {
    return { ...base, action: 'skip_existing_website', reason: resolution.reason, persisted: false };
  }

  if (resolution.action === 'manual_review') {
    return {
      ...base,
      action: 'manual_review',
      reason: resolution.reason || 'identity_confidence_insufficient',
      persisted: false,
    };
  }

  if (resolution.action !== 'resolve' || !resolution.domain) {
    return {
      ...base,
      action: 'manual_review',
      reason: resolution.reason || 'no_domain',
      persisted: false,
    };
  }

  if (crmRow?.domain && normalizeDomain(crmRow.domain) !== resolution.domain) {
    return {
      ...base,
      action: 'skip_existing_domain',
      reason: 'existing_stronger_domain',
      persisted: false,
    };
  }

  const updates = {};
  const reasons = [];
  if (!crmRow?.domain) {
    updates.companyDomain = resolution.domain;
    reasons.push('discover_company_domain');
  }
  if (!crmRow?.website) {
    updates.companyWebsite = resolution.website;
    reasons.push('discover_company_website');
  }
  if (!crmRow?.website_url) {
    updates.prospectWebsiteUrl = resolution.website;
    updates.prospectHasWebsite = true;
    reasons.push('discover_prospect_website_url');
  }

  if (!reasons.length) {
    return { ...base, action: 'noop_already_canonical', persisted: false };
  }

  return {
    ...base,
    action: 'persist',
    reasons,
    updates,
    provenance: discoveryProvenance(candidate, missionId, resolution),
    companyId: crmRow?.company_id,
    prospectId: crmRow?.prospect_id,
  };
}

async function applyDiscoveryPlan(db, plan, dryRun, clientId) {
  if (plan.action !== 'persist') {
    return { ...plan, persisted: false, dryRun };
  }
  if (dryRun) {
    return { ...plan, persisted: false, dryRun: true };
  }

  await db.query(
    `UPDATE companies
        SET domain = COALESCE(domain, $1),
            website = COALESCE(website, $2),
            enrichment_provenance = COALESCE(enrichment_provenance, '{}'::jsonb) || $3::jsonb,
            updated_at = NOW()
      WHERE id = $4
        AND client_id = $5`,
    [
      plan.updates.companyDomain,
      plan.updates.companyWebsite,
      JSON.stringify(plan.provenance),
      plan.companyId,
      clientId,
    ]
  );

  await db.query(
    `UPDATE prospects
        SET website_url = COALESCE(NULLIF(TRIM(website_url), ''), $1),
            has_website = CASE
              WHEN COALESCE(has_website, false) = true THEN has_website
              ELSE $2
            END,
            enrichment_provenance = COALESCE(enrichment_provenance, '{}'::jsonb) || $3::jsonb,
            updated_at = NOW()
      WHERE id = $4
        AND client_id = $5`,
    [
      plan.updates.prospectWebsiteUrl,
      plan.updates.prospectHasWebsite,
      JSON.stringify(plan.provenance),
      plan.prospectId,
      clientId,
    ]
  );

  return { ...plan, persisted: true, dryRun: false };
}

module.exports = {
  RESOLUTION_SOURCES,
  MIN_PERSIST_CONFIDENCE,
  PLACE_DETAILS_FIELDS,
  normalizeName,
  nameTokens,
  scorePlaceIdentity,
  isCanonicalWebsite,
  normalizeWebsite,
  buildDiscoveryInput,
  fetchPlaceDetailsWebsite,
  searchPlaceByIdentity,
  resolveUpstreamWebsite,
  discoverMissionBoundWebsite,
  discoveryProvenance,
  buildDiscoveryPlan,
  applyDiscoveryPlan,
  resolveMissionBoundWebsiteIntel,
};
