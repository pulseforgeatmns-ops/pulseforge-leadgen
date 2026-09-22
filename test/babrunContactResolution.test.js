'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  classifyCandidate,
  mapVerificationResult,
  isFounderLocalPartMatch,
  pickBestCandidate,
  verificationDeliverabilityRank,
  attributionRank,
  isLikelyTypoDomain,
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

describe('babrun contact resolution ranking contract', () => {
  const riskyRoleInbox = {
    email: 'info@cchaulsjunk.com',
    discoveryMethod: 'first_party_website',
    discoverySource: 'https://www.cchaulsjunk.com/',
    firstParty: true,
    roleGeneric: true,
    patternGenerated: false,
    founderAttribution: false,
    publicFounderSource: false,
    personalProvider: false,
  };
  const invalidTypoRoleInbox = {
    email: 'info@cchauisjunk.com',
    discoveryMethod: 'first_party_website',
    discoverySource: 'https://www.cchaulsjunk.com/',
    firstParty: true,
    roleGeneric: true,
    patternGenerated: false,
    founderAttribution: false,
    publicFounderSource: false,
    personalProvider: false,
  };

  it('orders verification deliverability VALID > RISKY > UNKNOWN > INVALID', () => {
    assert.ok(verificationDeliverabilityRank({ deliverability: 'valid', verified: true }) < verificationDeliverabilityRank({ deliverability: 'risky' }));
    assert.ok(verificationDeliverabilityRank({ deliverability: 'risky' }) < verificationDeliverabilityRank({ deliverability: 'unknown' }));
    assert.ok(verificationDeliverabilityRank({ deliverability: 'unknown' }) < verificationDeliverabilityRank({ deliverability: 'invalid' }));
  });

  it('invalid candidate cannot outrank risky candidate', () => {
    const best = pickBestCandidate([
      {
        ...invalidTypoRoleInbox,
        classification: CONTACT_FINAL_STATE.UNRESOLVED,
        verification: { deliverability: 'invalid', verified: false, status: 'invalid' },
      },
      {
        ...riskyRoleInbox,
        classification: CONTACT_FINAL_STATE.REVIEW_REQUIRED,
        verification: { deliverability: 'risky', verified: false, status: 'risky' },
      },
    ], { officialDomain: 'cchaulsjunk.com' });
    assert.equal(best.email, 'info@cchaulsjunk.com');
  });

  it('invalid candidate cannot outrank unknown candidate', () => {
    const best = pickBestCandidate([
      {
        email: 'info@cchauisjunk.com',
        discoveryMethod: 'first_party_website',
        classification: CONTACT_FINAL_STATE.UNRESOLVED,
        verification: { deliverability: 'invalid', verified: false, status: 'invalid' },
      },
      {
        email: 'info@cchaulsjunk.com',
        discoveryMethod: 'first_party_website',
        classification: CONTACT_FINAL_STATE.REVIEW_REQUIRED,
        verification: { deliverability: 'unknown', verified: false, status: 'unknown' },
      },
    ], { officialDomain: 'cchaulsjunk.com' });
    assert.equal(best.email, 'info@cchaulsjunk.com');
  });

  it('valid generated pattern does not outrank valid observed first-party evidence', () => {
    const best = pickBestCandidate([
      {
        email: 'contact@lemonhomecleaning.com',
        discoveryMethod: 'pattern_candidate',
        patternGenerated: true,
        firstParty: false,
        classification: CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL,
        verification: { deliverability: 'valid', verified: true, status: 'valid' },
      },
      {
        email: 'hello@thelemoncleaning.com',
        discoveryMethod: 'first_party_website',
        firstParty: true,
        roleGeneric: true,
        classification: CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL,
        verification: { deliverability: 'valid', verified: true, status: 'valid' },
        discoverySource: 'https://lemonhomecleaning.com/',
      },
    ]);
    assert.equal(best.email, 'hello@thelemoncleaning.com');
  });

  it('classifies risky first-party role inbox as REVIEW_REQUIRED', () => {
    assert.equal(
      classifyCandidate(riskyRoleInbox, { deliverability: 'risky', verified: false, status: 'risky' }, 'Sirewl Cooper'),
      CONTACT_FINAL_STATE.REVIEW_REQUIRED
    );
  });

  it('classifies valid role inbox as VERIFIED_ROLE_EMAIL', () => {
    assert.equal(
      classifyCandidate(riskyRoleInbox, { deliverability: 'valid', verified: true, status: 'valid' }, 'Sirewl Cooper'),
      CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL
    );
  });

  it('requires founder attribution for VERIFIED_FOUNDER_EMAIL', () => {
    const roleOnly = {
      email: 'info@cchaulsjunk.com',
      firstParty: true,
      roleGeneric: true,
      patternGenerated: false,
      publicFounderSource: false,
      personalProvider: false,
    };
    assert.equal(
      classifyCandidate(roleOnly, { deliverability: 'valid', verified: true, status: 'valid' }, 'Sirewl Cooper'),
      CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL
    );
    assert.notEqual(
      classifyCandidate(roleOnly, { deliverability: 'valid', verified: true, status: 'valid' }, 'Sirewl Cooper'),
      CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL
    );
  });

  it('detects likely typo domain without collapsing candidate identity', () => {
    assert.equal(isLikelyTypoDomain('cchauisjunk.com', 'cchaulsjunk.com'), true);
    assert.equal(isLikelyTypoDomain('cchaulsjunk.com', 'cchaulsjunk.com'), false);
    assert.equal(isLikelyTypoDomain('otherbusiness.com', 'cchaulsjunk.com'), false);
  });

  it('ranks deterministically regardless of candidate input order', () => {
    const riskyEvaluated = {
      ...riskyRoleInbox,
      classification: CONTACT_FINAL_STATE.REVIEW_REQUIRED,
      verification: { deliverability: 'risky', verified: false, status: 'risky' },
    };
    const invalidEvaluated = {
      ...invalidTypoRoleInbox,
      classification: CONTACT_FINAL_STATE.UNRESOLVED,
      verification: { deliverability: 'invalid', verified: false, status: 'invalid' },
    };
    const forward = pickBestCandidate([invalidEvaluated, riskyEvaluated], { officialDomain: 'cchaulsjunk.com' });
    const reverse = pickBestCandidate([riskyEvaluated, invalidEvaluated], { officialDomain: 'cchaulsjunk.com' });
    assert.equal(forward.email, reverse.email);
    assert.equal(forward.email, 'info@cchaulsjunk.com');
  });

  it('CC Junk regression: risky observed first-party role inbox beats invalid typo', () => {
    const best = pickBestCandidate([
      {
        ...invalidTypoRoleInbox,
        classification: CONTACT_FINAL_STATE.UNRESOLVED,
        verification: { deliverability: 'invalid', verified: false, status: 'invalid', method: 'bouncer' },
      },
      {
        ...riskyRoleInbox,
        classification: CONTACT_FINAL_STATE.REVIEW_REQUIRED,
        verification: { deliverability: 'risky', verified: false, status: 'risky', method: 'bouncer' },
      },
    ], { officialDomain: 'cchaulsjunk.com' });
    assert.equal(best.email, 'info@cchaulsjunk.com');
    assert.equal(best.classification, CONTACT_FINAL_STATE.REVIEW_REQUIRED);
    assert.equal(best.verification.deliverability, 'risky');
  });

  it('prefers stronger attribution within equivalent verification quality', () => {
    assert.ok(attributionRank({ founderAttribution: true, firstParty: true }) < attributionRank({ publicFounderSource: true, founderAttribution: true }));
    assert.ok(attributionRank({ firstParty: true, roleGeneric: true }) < attributionRank({ patternGenerated: true }));
  });
});
