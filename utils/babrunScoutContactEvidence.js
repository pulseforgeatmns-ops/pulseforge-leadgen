'use strict';

// Standard Scout intake only. Bind the selected enrichment result before the
// lead's discovery sources and display fields are combined. No provider calls.
const { candidateRecord, classifyCandidate, mapVerificationResult } = require('./babrunContactClassification');
const { normalizeDomain, resolveOfficialEnrichmentDomain, stampEmailProvenance } = require('./canonicalEmailEligibility');

const text = value => typeof value === 'string' ? value.trim() : '';
const address = value => text(value).toLowerCase();
const PROVIDERS = new Set(['hunter', 'prospeo']);

function selectedWebsiteEvidence(enriched, source, officialDomain) {
  const evidence = enriched.websiteOwnerEvidence;
  if (source !== 'scraped' || !evidence || evidence.version !== 1
    || !['html_person_record', 'json_ld_person'].includes(evidence.type)
    || typeof evidence.record !== 'string' || !evidence.record.length || evidence.record.length > 8192
    || !Number.isInteger(evidence.start) || evidence.start < 0
    || !Number.isInteger(evidence.end) || evidence.end > 1_000_000 || evidence.end - evidence.start !== evidence.record.length
    || typeof evidence.sourceUrl !== 'string' || evidence.sourceUrl.length > 2048 || !/^https?:\/\//i.test(evidence.sourceUrl)
    || address(evidence.email) !== address(enriched.email)
    || text(evidence.contactName) !== text(enriched.contact)
    || text(evidence.contactTitle) !== text(enriched.title)
    || evidence.sourceUrl !== enriched.sourceUrl
    || normalizeDomain(evidence.sourceUrl) !== normalizeDomain(officialDomain)) return null;
  // Copy only bounded source data, never caller-supplied verification/classification.
  return {
    version: 1, type: evidence.type, sourceUrl: evidence.sourceUrl,
    email: address(enriched.email), contactName: text(enriched.contact), contactTitle: text(enriched.title),
    start: evidence.start, end: evidence.end, record: evidence.record,
    ...(evidence.type === 'json_ld_person' && typeof evidence.jsonPointer === 'string' && evidence.jsonPointer.length <= 500
      ? { jsonPointer: evidence.jsonPointer } : {}),
  };
}

function applyScoutEnrichment(lead, enriched, source, clientId, officialDomain) {
  Object.assign(lead, enriched);
  lead.source = [...(lead.source || []), source];
  if (Number(clientId) === 13) {
    const websiteEvidence = selectedWebsiteEvidence(enriched, source, officialDomain);
    // Read only this selected result, never stale lead.contact/title/sourceUrl.
    lead.scoutContactEvidence = {
      email: address(enriched.email),
      source,
      sourceUrl: text(enriched.sourceUrl) || null,
      contactName: text(enriched.contact) || null,
      contactTitle: text(enriched.title) || null,
      companyDomain: normalizeDomain(officialDomain),
      ...(websiteEvidence ? { websiteEvidence } : {}),
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
  const websiteEvidence = firstParty ? selectedWebsiteEvidence({
    email: selectedEmail, contact: contactName, title: contactTitle, sourceUrl,
    websiteOwnerEvidence: observed.websiteEvidence,
  }, source, domain) : null;
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
        ...(websiteEvidence ? { websiteEvidence } : {}),
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
