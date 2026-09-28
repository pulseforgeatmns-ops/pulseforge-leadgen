'use strict';
const { canonicalOutboundEmailIneligibilityReason } = require('./canonicalEmailEligibility');
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
  const required = createGovernedOutboundTenantContext(tenantId).usesTenantMailboxTransport;
  if (!required && !evidence.classification) return null;
  if (!SENDABLE_CLASSES.includes(evidence.classification)) return 'contact_classification_not_sendable';
  if (required && !evidence.email) return 'contact_classification_address_missing';
  if (evidence.email && evidence.email.toLowerCase() !== String(row.email).toLowerCase()) return 'contact_classification_email_changed';
  const allowed = policy.allowedContactClassifications || (required ? ['VERIFIED_FOUNDER_EMAIL'] : SENDABLE_CLASSES);
  if (!allowed.includes(evidence.classification)) return 'contact_classification_not_authorized';
  return null;
}
function founderFirst(a, b) {
  const rank = row => contactEvidence(row).classification === 'VERIFIED_FOUNDER_EMAIL' ? 0 : 1;
  return rank(a) - rank(b);
}
module.exports = { SENDABLE_CLASSES, contactEvidence, governedContactReason, founderFirst };
