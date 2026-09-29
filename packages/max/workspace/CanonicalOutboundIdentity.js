'use strict';

/**
 * Canonical Max → Paige → Emmett identity helpers.
 * Candidate/company/external IDs stay distinct from CRM prospect UUIDs.
 * Google Place IDs are external identity, never prospects.id.
 */

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const PLACE_ID_RE = /^ChI[A-Za-z0-9_-]+$/;

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function isUuid(value) {
  return UUID_RE.test(asText(value));
}

function isGooglePlaceId(value) {
  return PLACE_ID_RE.test(asText(value));
}

function normalizeDomain(value) {
  const raw = asText(value);
  if (!raw) return null;
  try {
    const host = new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/^www\./i, '')
      .toLowerCase();
    return host || null;
  } catch {
    const domain = raw
      .replace(/^https?:\/\//i, '')
      .replace(/^www\./i, '')
      .split(/[/?#\s]/)[0]
      .replace(/[.,;:]+$/g, '')
      .toLowerCase();
    return domain || null;
  }
}

function uniqueKeys(values = []) {
  return [...new Set(values.map(asText).filter(Boolean))];
}

/**
 * CRM contact / people-record identity. Place IDs are never prospect UUIDs.
 */
function prospectIdentity(row = {}) {
  if (!row || typeof row !== 'object') return null;
  const ordered = [row.prospectId, row.contactId, row.crmProspectId, row.id];
  for (const raw of ordered) {
    const id = asText(raw);
    if (!id || isGooglePlaceId(id)) continue;
    return id;
  }
  return null;
}

function identityKeysFrom(row = {}) {
  return uniqueKeys([
    row.candidateId,
    row.id,
    row.companyId,
    row.placeId,
    row.place_id,
    row.externalId,
    row.crmProspectId,
    row.domain,
  ]);
}

/**
 * Lineage for one Max-ranked / Scout-discovered target.
 * candidateId is the SPEC-212 join key Paige stores on each variant.
 */
function canonicalOutboundIdentity(target = {}, extras = {}) {
  const placeId = asText(
    target.placeId
    || target.place_id
    || extras.placeId
    || (isGooglePlaceId(target.id) ? target.id : '')
    || (isGooglePlaceId(target.companyId) ? target.companyId : '')
    || (isGooglePlaceId(target.candidateId) ? target.candidateId : '')
  ) || null;

  const candidateId = asText(
    target.candidateId
    || target.id
    || target.companyId
    || placeId
    || extras.fallbackId
  ) || null;

  const companyId = asText(
    target.companyId
    || (!isGooglePlaceId(target.id) && isUuid(target.id) ? target.id : '')
    || (isGooglePlaceId(target.companyId) ? target.companyId : '')
    || placeId
    || candidateId
  ) || null;

  const crmProspectId = prospectIdentity(extras.prospect || {}) || null;
  const domain = normalizeDomain(
    target.domain || target.website || extras.website || extras.domain || extras.prospect?.website
  );

  return {
    candidateId,
    companyId,
    placeId,
    crmProspectId,
    domain,
    keys: uniqueKeys([candidateId, companyId, placeId, crmProspectId, domain]),
  };
}

function aliasCrmMapToIdentities(crmMaps = [], identities = []) {
  const out = new Map();
  for (const source of crmMaps) {
    if (!source) continue;
    const entries = source instanceof Map ? source.entries() : Object.entries(source);
    for (const [key, row] of entries) {
      if (key == null || !row) continue;
      out.set(String(key), row);
      const prospectKey = row.prospect_id || row.id;
      if (prospectKey != null) out.set(String(prospectKey), row);
      if (row.company_id != null) out.set(String(row.company_id), row);
      if (row.google_place_id) out.set(String(row.google_place_id), row);
      const rowDomain = normalizeDomain(row.domain || row.website);
      if (rowDomain) out.set(rowDomain, row);
    }
  }
  for (const identity of identities) {
    const keys = identityKeysFrom(identity);
    let hit = null;
    for (const key of keys) {
      hit = out.get(String(key));
      if (hit) break;
    }
    if (!hit) continue;
    for (const key of keys) out.set(String(key), hit);
  }
  return out;
}

module.exports = {
  UUID_RE,
  PLACE_ID_RE,
  asText,
  isUuid,
  isGooglePlaceId,
  isCanonicalProspectId: (value) => {
    const id = asText(value);
    return Boolean(id) && !isGooglePlaceId(id);
  },
  normalizeDomain,
  uniqueKeys,
  prospectIdentity,
  identityKeysFrom,
  canonicalOutboundIdentity,
  aliasCrmMapToIdentities,
};
