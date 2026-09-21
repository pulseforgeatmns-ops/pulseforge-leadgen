'use strict';

/**
 * Separate CRM UUID identity from Scout/external Place ID identity (SPEC-212).
 */

const { asText } = require('../../acquisition-mission/types');
const { normalizeDomain, identityKeysFrom } = require('./CanonicalOutboundIdentity');

function isGooglePlaceId(value) {
  const text = asText(value);
  if (!text) return false;
  return /^ChIJ[\w-]+$/i.test(text);
}

function isUuid(value) {
  const text = asText(value);
  if (!text) return false;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(text);
}

/**
 * Resolve mission-bound candidate identity without overloading companyId.
 * @returns {{
 *   candidateId: string,
 *   placeId: string|null,
 *   companyId: string|null,
 *   crmCompanyId: string|null,
 *   crmProspectId: string|null,
 *   domain: string|null,
 *   keys: string[],
 * }}
 */
function resolveMissionBoundIdentity({
  target = {},
  opp = {},
  prospect = null,
  fallbackId = null,
} = {}) {
  const rawId = asText(
    target.id
    || target.companyId
    || target.candidateId
    || opp.companyId
    || opp.id
    || prospect?.id
    || fallbackId
  ) || null;

  const explicitPlaceId = asText(
    target.placeId
    || target.place_id
    || opp.placeId
    || opp.place_id
    || prospect?.placeId
    || prospect?.place_id
  ) || null;

  const explicitCrmCompanyId = asText(
    target.crmCompanyId
    || target.crm_company_id
    || opp.crmCompanyId
    || opp.crm_company_id
    || prospect?.crmCompanyId
    || prospect?.company_id
  ) || null;

  const placeId = explicitPlaceId || (isGooglePlaceId(rawId) ? rawId : null);
  let crmCompanyId = explicitCrmCompanyId || null;
  if (!crmCompanyId && rawId && !isGooglePlaceId(rawId)) {
    crmCompanyId = isUuid(rawId) ? rawId : rawId;
  }

  const crmProspectId = asText(
    prospect?.prospectId
    || prospect?.crmProspectId
    || (prospect?.id && isUuid(prospect.id) ? prospect.id : null)
  ) || null;

  const candidateId = crmCompanyId || placeId || rawId || fallbackId;
  const companyId = crmCompanyId || placeId || (rawId ? String(rawId) : null) || (candidateId ? String(candidateId) : null);
  const domain = normalizeDomain(
    target.domain
    || target.website
    || target.url
    || opp.domain
    || opp.website
    || opp.url
    || prospect?.domain
    || prospect?.website
    || prospect?.website_url
  );

  const resolved = {
    candidateId: candidateId ? String(candidateId) : null,
    placeId: placeId ? String(placeId) : null,
    companyId: companyId ? String(companyId) : null,
    crmCompanyId: crmCompanyId ? String(crmCompanyId) : null,
    crmProspectId: crmProspectId ? String(crmProspectId) : null,
    domain,
    keys: identityKeysFrom({
      candidateId,
      id: candidateId,
      companyId,
      placeId,
      crmProspectId,
      domain,
    }),
  };

  return resolved;
}

module.exports = {
  isGooglePlaceId,
  isUuid,
  resolveMissionBoundIdentity,
};
