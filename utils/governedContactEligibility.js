'use strict';
const { canonicalOutboundEmailIneligibilityReason, normalizeDomain, emailDomain,
  isPersonalEmailProviderDomain } = require('./canonicalEmailEligibility');
const { createGovernedOutboundTenantContext } = require('../services/governedOutboundTenant');
const SENDABLE_CLASSES = Object.freeze(['VERIFIED_FOUNDER_EMAIL', 'VERIFIED_ROLE_EMAIL']);
function contactEvidence(row = {}) {
  const resolution = row.acquisition_metadata?.contactResolution || {};
  return { classification: resolution.finalState || resolution.classification || row.contact_classification || null,
    email: resolution.bestEmail || null, resolution };
}
function governedContactReason(row, policy = {}) {
  const base = canonicalOutboundEmailIneligibilityReason(row);
  if (base) return base;
  const evidence = contactEvidence(row);
  const tenantId = String(policy.tenantId || row.client_id || row.tenant_id || '10');
  if (policy.tenantId && (row.client_id || row.tenant_id)
    && String(row.client_id || row.tenant_id) !== String(policy.tenantId)) return 'contact_tenant_mismatch';
  if (row.is_synthetic === true) return 'synthetic_contact';
  const required = createGovernedOutboundTenantContext(tenantId).usesTenantMailboxTransport;
  if (!required) {
    // Verification establishes deliverability, not who owns the address.
    if (!String(row.enrichment_provenance?.email?.source || '').trim()) return 'missing_email_provenance';
    const binding = companyRecipientReason(row);
    if (binding) return binding;
  }
  if (!required && !evidence.classification) return null;
  if (!SENDABLE_CLASSES.includes(evidence.classification)) return 'contact_classification_not_sendable';
  if (required && !evidence.email) return 'contact_classification_address_missing';
  if (evidence.email && evidence.email.toLowerCase() !== String(row.email).toLowerCase()) return 'contact_classification_email_changed';
  const allowed = policy.allowedContactClassifications || (required ? ['VERIFIED_FOUNDER_EMAIL'] : SENDABLE_CLASSES);
  if (!allowed.includes(evidence.classification)) return 'contact_classification_not_authorized';
  return null;
}
function companyRecipientReason(row = {}) {
  const domain = normalizeDomain(row.company_domain || row.domain || row.company_website || row.website || row.website_url);
  if (!domain) return 'missing_company_domain';
  const recipientDomain = emailDomain(row.email);
  // Existing observed personal addresses remain supported. Unrelated corporate
  // domains need reviewed ownership evidence; a verifier cannot establish it.
  if (recipientDomain !== domain && !isPersonalEmailProviderDomain(recipientDomain)) return 'recipient_company_domain_mismatch';
  return null;
}
function founderFirst(a, b) {
  const rank = row => contactEvidence(row).classification === 'VERIFIED_FOUNDER_EMAIL' ? 0 : 1;
  return rank(a) - rank(b);
}
module.exports = { SENDABLE_CLASSES, contactEvidence, governedContactReason, companyRecipientReason, founderFirst };
