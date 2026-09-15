'use strict';

/**
 * SPEC-254 — Outreach contact classification.
 * Contact type is NOT capacity; role inboxes must not pollute founder-address evidence.
 */

const { isRolePattern } = require('./emailValidation');

const OUTREACH_CONTACT_TYPE = Object.freeze({
  VERIFIED_FOUNDER_EMAIL: 'VERIFIED_FOUNDER_EMAIL',
  VERIFIED_ROLE_EMAIL: 'VERIFIED_ROLE_EMAIL',
  UNVERIFIED_EMAIL: 'UNVERIFIED_EMAIL',
});

function classifyOutreachContactType(email, opts = {}) {
  const normalized = typeof email === 'string' ? email.trim().toLowerCase() : '';
  if (!normalized || !normalized.includes('@')) {
    return OUTREACH_CONTACT_TYPE.UNVERIFIED_EMAIL;
  }
  if (opts.verified === false) {
    return OUTREACH_CONTACT_TYPE.UNVERIFIED_EMAIL;
  }
  if (isRolePattern(normalized)) {
    return OUTREACH_CONTACT_TYPE.VERIFIED_ROLE_EMAIL;
  }
  return OUTREACH_CONTACT_TYPE.VERIFIED_FOUNDER_EMAIL;
}

function isRoleContactType(contactType) {
  return contactType === OUTREACH_CONTACT_TYPE.VERIFIED_ROLE_EMAIL;
}

function isFounderContactType(contactType) {
  return contactType === OUTREACH_CONTACT_TYPE.VERIFIED_FOUNDER_EMAIL;
}

module.exports = {
  OUTREACH_CONTACT_TYPE,
  classifyOutreachContactType,
  isRoleContactType,
  isFounderContactType,
};
