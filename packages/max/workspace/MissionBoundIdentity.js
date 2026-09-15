'use strict';

/**
 * Separate CRM UUID identity from Scout/external Place ID identity (SPEC-212).
 */

const { asText } = require('../../acquisition-mission/types');

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
 * @returns {{ candidateId: string, placeId: string|null, crmCompanyId: string|null, crmProspectId: string|null }}
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

  return {
    candidateId: candidateId ? String(candidateId) : null,
    placeId: placeId ? String(placeId) : null,
    crmCompanyId: crmCompanyId ? String(crmCompanyId) : null,
    crmProspectId: crmProspectId ? String(crmProspectId) : null,
  };
}

module.exports = {
  isGooglePlaceId,
  isUuid,
  resolveMissionBoundIdentity,
};
