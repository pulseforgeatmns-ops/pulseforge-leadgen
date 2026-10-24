'use strict';
const { normalizeDomain } = require('./canonicalEmailEligibility');
const text = value => String(value || '').trim();
const name = value => text(value).replace(/\s+/g, ' ').toLowerCase();

// Mission IDs can be company UUIDs, contact UUIDs, Place IDs or domains.
// Explicit CRM IDs are never interchangeable with those external aliases.
function governedRecipientBindingReason(item = {}, crm, message = {}) {
  if (!crm) return 'missing_row';
  const prospectId = text(crm.prospect_id || crm.id);
  const companyId = text(crm.company_id);
  const domain = normalizeDomain(crm.company_domain || crm.domain || crm.company_website || crm.website || crm.website_url);
  const keys = new Set([prospectId, companyId, text(crm.google_place_id), domain].filter(Boolean));
  if (item.crmProspectId && text(item.crmProspectId) !== prospectId) return 'crm_contact_binding_mismatch';
  if (item.crmCompanyId && text(item.crmCompanyId) !== companyId) return 'crm_company_binding_mismatch';
  for (const value of [item.candidateId, item.prospectId, item.paige?.candidateId, message.candidateId]) {
    if (value && !keys.has(text(value))) return 'candidate_crm_binding_mismatch';
  }
  for (const value of [item.companyId, message.companyId]) {
    if (value && ![companyId, text(crm.google_place_id), domain].filter(Boolean).includes(text(value))) return 'company_binding_mismatch';
  }
  for (const value of [item.company, item.companyName, item.paige?.companyName, message.companyName]) {
    if (value && name(value) !== name(crm.company_name || crm.company)) return 'message_company_context_mismatch';
  }
  if (item.domain && normalizeDomain(item.domain) !== domain) return 'company_domain_binding_mismatch';
  return null;
}
module.exports = { governedRecipientBindingReason };
