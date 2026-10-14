'use strict';

// Pure shared Babrun classification contract. No providers, database or app imports.
const { isRolePattern } = require('./emailValidation');
const { isPersonalEmailProviderDomain } = require('./canonicalEmailEligibility');

const CONTACT_FINAL_STATE = Object.freeze({
  VERIFIED_FOUNDER_EMAIL: 'VERIFIED_FOUNDER_EMAIL',
  VERIFIED_ROLE_EMAIL: 'VERIFIED_ROLE_EMAIL',
  REVIEW_REQUIRED: 'REVIEW_REQUIRED',
  UNRESOLVED: 'UNRESOLVED',
});

const GENERIC_PREFIX_RE = /^(?:info|hello|contact|office|sales|support|admin|team)@/i;

function clean(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function founderFirstName(founder) {
  return clean(founder).split(/\s+/)[0]?.toLowerCase() || '';
}

function founderLastName(founder) {
  const parts = clean(founder).split(/\s+/).filter(Boolean);
  return parts.length > 1 ? parts[parts.length - 1].toLowerCase().replace(/[^a-z]/g, '') : '';
}

function localPart(email) {
  return clean(email).toLowerCase().split('@')[0] || '';
}

function emailDomain(email) {
  return clean(email).toLowerCase().split('@')[1] || '';
}

function isGenericRoleEmail(email) {
  return GENERIC_PREFIX_RE.test(clean(email).toLowerCase()) || isRolePattern(email);
}

function isFounderLocalPartMatch(email, founder) {
  const local = localPart(email).replace(/[^a-z]/g, '');
  const first = founderFirstName(founder).replace(/[^a-z]/g, '');
  const last = founderLastName(founder);
  if (!local || !first) return false;
  if (local === first) return true;
  if (last && (local === `${first}${last}` || local === `${first[0]}${last}`)) return true;
  return false;
}

function mapVerificationResult(result = {}) {
  const status = clean(result.status).toLowerCase();
  if (status === 'valid' || status === 'verified') {
    return { deliverability: 'valid', verified: true, status, reason: result.reason };
  }
  if (status === 'role') {
    return { deliverability: 'valid', verified: true, status, reason: 'role_pattern_mx_ok' };
  }
  if (status === 'invalid') {
    return { deliverability: 'invalid', verified: false, status, reason: result.reason };
  }
  if (status === 'catchall' || status === 'risky') {
    return { deliverability: status, verified: false, status, reason: result.reason };
  }
  if (result.reason === 'verifier_timeout') {
    return { deliverability: 'timeout', verified: false, status: 'unknown', reason: result.reason };
  }
  return { deliverability: 'unknown', verified: false, status: status || 'unknown', reason: result.reason };
}

function candidateRecord(email, discoveryMethod, discoverySource, extras = {}) {
  return {
    email: clean(email).toLowerCase(),
    discoveryMethod,
    discoverySource,
    patternGenerated: discoveryMethod === 'pattern_candidate',
    founderAttribution: extras.founderAttribution === true,
    firstParty: extras.firstParty === true,
    publicFounderSource: extras.publicFounderSource === true,
    roleGeneric: isGenericRoleEmail(email),
    personalProvider: isPersonalEmailProviderDomain(emailDomain(email)),
  };
}

function classifyCandidate(candidate, verification, founder) {
  if (!candidate?.email) return CONTACT_FINAL_STATE.UNRESOLVED;
  if (!verification?.verified) {
    if (verification?.deliverability === 'invalid') {
      return CONTACT_FINAL_STATE.UNRESOLVED;
    }
    if (
      verification?.deliverability === 'unknown'
      || verification?.deliverability === 'timeout'
      || verification?.deliverability === 'risky'
      || verification?.deliverability === 'catchall'
    ) {
      return CONTACT_FINAL_STATE.REVIEW_REQUIRED;
    }
    return CONTACT_FINAL_STATE.UNRESOLVED;
  }

  const founderOnFirstParty = candidate.firstParty
    && isFounderLocalPartMatch(candidate.email, founder);
  const attributedFounderRole = candidate.founderAttribution === true
    && candidate.patternGenerated !== true
    && isFounderLocalPartMatch(candidate.email, founder);
  const hasFounderEvidence = candidate.publicFounderSource
    || founderOnFirstParty
    || attributedFounderRole;

  if (hasFounderEvidence && !candidate.roleGeneric) {
    return CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL;
  }
  if (candidate.roleGeneric || isGenericRoleEmail(candidate.email)) {
    return CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL;
  }
  if (candidate.personalProvider && candidate.firstParty) {
    return CONTACT_FINAL_STATE.REVIEW_REQUIRED;
  }
  if (candidate.patternGenerated) {
    return CONTACT_FINAL_STATE.REVIEW_REQUIRED;
  }
  return CONTACT_FINAL_STATE.REVIEW_REQUIRED;
}

module.exports = { CONTACT_FINAL_STATE, clean, founderFirstName, founderLastName, localPart, emailDomain,
  isGenericRoleEmail, isFounderLocalPartMatch, candidateRecord, mapVerificationResult, classifyCandidate };
