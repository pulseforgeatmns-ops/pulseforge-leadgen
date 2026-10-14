'use strict';

// Standard Scout intake only. Bind the selected enrichment result before the
// lead's discovery sources and display fields are combined. No provider calls.
const { candidateRecord, classifyCandidate, mapVerificationResult } = require('./babrunContactClassification');
const { normalizeDomain, resolveOfficialEnrichmentDomain, stampEmailProvenance } = require('./canonicalEmailEligibility');

const text = value => typeof value === 'string' ? value.trim() : '';
const address = value => text(value).toLowerCase();
const PROVIDERS = new Set(['hunter', 'prospeo']);

function applyScoutEnrichment(lead, enriched, source, clientId, officialDomain) {
  Object.assign(lead, enriched);
  lead.source = [...(lead.source || []), source];
  if (Number(clientId) === 13) {
    // Read only this selected result, never stale lead.contact/title/sourceUrl.
    lead.scoutContactEvidence = {
      email: address(enriched.email),
      source,
      sourceUrl: text(enriched.sourceUrl) || null,
      contactName: text(enriched.contact) || null,
      contactTitle: text(enriched.title) || null,
      companyDomain: normalizeDomain(officialDomain),
    };
  }
  return lead;
}

function supportedPersonName(value) {
  const name = text(value).replace(/\s+/g, ' ');
  if (!name || !/^[\p{L}][\p{L}\p{M} .'-]*$/u.test(name)) return null;
  if (/\b(?:unknown|owner|founder|team|admin|contact|office|sales|support|info|hello)\b/i.test(name)) return null;
  return name;
}

function explicitFounderTitle(value) {
  // A search preference, assistant-to-owner or vice-president title is not
  // evidence of a founder/owner. Keep ambiguous titles for review.
  const roles = text(value).toLowerCase().split(/\s*(?:&|\/|,|\band\b)\s*/);
  return roles.some(role => /^(?:(?:co[- ]?)?(?:owner|founder)|president|principal|proprietor)$/.test(role))
    && roles.every(role => /^(?:(?:co[- ]?)?(?:owner|founder)|president|principal|proprietor|ceo|operator|managing director)$/.test(role));
}

function babrunScoutContactEvidence({ lead = {}, email, verification = {}, officialDomain, now = new Date().toISOString() } = {}) {
  const selectedEmail = address(email);
  const observed = lead.scoutContactEvidence;
  const domain = resolveOfficialEnrichmentDomain({ domain: officialDomain });
  const sameAddress = Boolean(selectedEmail && observed && address(observed.email) === selectedEmail);
  const sameCompany = Boolean(domain && normalizeDomain(observed?.companyDomain) === domain);
  const bound = sameAddress && sameCompany;
  const source = bound ? text(observed.source).toLowerCase() : null;
  const sourceUrl = bound ? text(observed.sourceUrl) || null : null;
  const firstParty = source === 'scraped' && Boolean(sourceUrl && normalizeDomain(sourceUrl) === domain);
  const supportedSource = PROVIDERS.has(source) || firstParty;
  const contactName = supportedSource ? supportedPersonName(observed.contactName) : null;
  const contactTitle = supportedSource ? text(observed.contactTitle) || null : null;
  const founder = contactName && explicitFounderTitle(contactTitle) ? contactName : '';
  const candidate = candidateRecord(selectedEmail, source || 'unattributed', sourceUrl || source, {
    firstParty: firstParty && Boolean(founder),
    founderAttribution: PROVIDERS.has(source) && Boolean(founder),
  });
  const mapped = mapVerificationResult({
    status: verification.emailStatus,
    reason: verification.rejectReason || verification.note || (verification.reject === true ? 'verification_rejected' : null),
  });
  const receipt = {
    ...mapped,
    // Inconsistent flags/statuses must never turn risky or unknown into verified.
    verified: mapped.verified === true && verification.emailVerified === true && verification.reject !== true,
    rejected: verification.reject === true,
    method: verification.emailVerificationMethod || null,
    verifiedAt: verification.verifiedAt || verification.verifierCheckedAt || null,
    checkedAt: verification.verifierCheckedAt || null,
    raw: verification.verifierResponse || null,
  };
  const finalState = classifyCandidate(candidate, receipt, founder);
  const bindingReason = !observed ? 'selected_contact_evidence_missing'
    : !sameAddress ? 'selected_email_changed'
      : !sameCompany ? 'selected_company_changed'
        : !supportedSource ? 'unsupported_contact_source' : null;
  return {
    contactName,
    jobTitle: contactTitle,
    acquisitionMetadata: {
      contactResolution: {
        resolvedAt: now,
        finalState,
        classification: finalState,
        bestEmail: selectedEmail,
        contactName,
        contactTitle,
        discoverySource: candidate.discoverySource,
        discoveryMethod: candidate.discoveryMethod,
        evidenceReason: bindingReason,
        verification: receipt,
        candidates: [{ ...candidate, classification: finalState, verification: receipt }],
      },
    },
    enrichmentProvenance: stampEmailProvenance({}, source, {
      email: selectedEmail,
      source_url: sourceUrl,
      verifier: receipt.method,
      status: receipt.status,
      resolved_at: now,
    }),
  };
}

module.exports = { applyScoutEnrichment, babrunScoutContactEvidence };
