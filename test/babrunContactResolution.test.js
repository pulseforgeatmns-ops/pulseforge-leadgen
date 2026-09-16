'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  classifyCandidate,
  mapVerificationResult,
  isFounderLocalPartMatch,
  pickBestCandidate,
  CONTACT_FINAL_STATE,
} = require('../scripts/lib/babrunContactResolution');

describe('babrun contact resolution classification', () => {
  it('does not promote pattern-only name match to founder verified', () => {
    const candidate = {
      email: 'max@thelemoncleaning.com',
      patternGenerated: true,
      firstParty: false,
      roleGeneric: false,
      personalProvider: false,
      publicFounderSource: false,
    };
    const verification = { verified: true, deliverability: 'valid', status: 'valid' };
    assert.equal(
      classifyCandidate(candidate, verification, 'Max Walls'),
      CONTACT_FINAL_STATE.REVIEW_REQUIRED
    );
  });

  it('classifies first-party founder local-part as verified founder email', () => {
    const candidate = {
      email: 'luis@venturalawncare.com',
      firstParty: true,
      patternGenerated: false,
      roleGeneric: false,
      personalProvider: false,
      publicFounderSource: false,
    };
    const verification = { verified: true, deliverability: 'valid', status: 'valid' };
    assert.equal(
      classifyCandidate(candidate, verification, 'Luis Ventura'),
      CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL
    );
  });

  it('classifies public directory founder attribution', () => {
    const candidate = {
      email: 'sebastian@ovopainting.com',
      publicFounderSource: true,
      firstParty: false,
      patternGenerated: false,
      roleGeneric: false,
      personalProvider: false,
    };
    const verification = { verified: true, deliverability: 'valid', status: 'valid' };
    assert.equal(
      classifyCandidate(candidate, verification, 'Sebastian Thomas'),
      CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL
    );
  });

  it('classifies first-party personal provider inbox as review required', () => {
    const candidate = {
      email: 'premier_gs@outlook.com',
      firstParty: true,
      personalProvider: true,
      patternGenerated: false,
      roleGeneric: false,
      publicFounderSource: false,
    };
    const verification = { verified: true, deliverability: 'valid', status: 'valid' };
    assert.equal(
      classifyCandidate(candidate, verification, 'Diego Louzada'),
      CONTACT_FINAL_STATE.REVIEW_REQUIRED
    );
  });

  it('maps role mx lookup to verified deliverability', () => {
    const mapped = mapVerificationResult({ status: 'role', valid: false, reason: 'role_pattern' });
    assert.equal(mapped.deliverability, 'valid');
    assert.equal(mapped.verified, true);
  });

  it('prefers observed first-party email over pattern candidate', () => {
    const best = pickBestCandidate([
      {
        email: 'contact@lemonhomecleaning.com',
        discoveryMethod: 'pattern_candidate',
        classification: CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL,
        verification: { verified: true },
      },
      {
        email: 'hello@thelemoncleaning.com',
        discoveryMethod: 'first_party_website',
        classification: CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL,
        verification: { verified: true },
        discoverySource: 'https://lemonhomecleaning.com/',
      },
    ]);
    assert.equal(best.email, 'hello@thelemoncleaning.com');
  });

  it('matches founder first name in local part', () => {
    assert.equal(isFounderLocalPartMatch('luis@venturalawncare.com', 'Luis Ventura'), true);
    assert.equal(isFounderLocalPartMatch('info@cchaulsjunk.com', 'Sirewl Cooper'), false);
  });
});
