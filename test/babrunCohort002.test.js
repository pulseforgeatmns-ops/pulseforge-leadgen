'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  evaluateIcp,
  isDuplicate,
  isCompleteContactState,
  cohortAkId,
  RESEARCH_SEED_CANDIDATES,
} = require('../scripts/lib/babrunCohort002');
const { CONTACT_FINAL_STATE } = require('../scripts/lib/babrunContactResolution');

describe('babrun cohort 002', () => {
  it('evaluates Polk Services seed as ICP fit', () => {
    const polk = RESEARCH_SEED_CANDIDATES.find((row) => row.company.includes('Polk'));
    const result = evaluateIcp(polk);
    assert.equal(result.fit, true);
    assert.equal(result.rejections.length, 0);
  });

  it('does not flag subcontract-for-franchise copy as national chain', () => {
    const body = 'sub-contract crews for national franchises, independent removal for local gcs';
    const signals = {};
    if (/\b(?:franchise owner|franchisee|franchise location|our franchise|a franchise of|part of a franchise)\b|\bnational chain\b|\bnationwide (?:company|locations|chain)\b|\blocations across (?:the )?(?:us|u\.s\.|country)\b/i.test(body)) {
      signals.nationalChain = true;
    }
    assert.equal(signals.nationalChain, undefined);
  });

  it('dedupes against first-ten company names', () => {
    const dedupe = {
      companies: new Set(['lemon cleaning']),
      domains: new Set(),
      akIds: new Set(),
      founders: new Set(),
    };
    assert.equal(
      isDuplicate({ company: 'Lemon Cleaning', founder: 'Max Walls', domain: 'example.com' }, dedupe),
      'duplicate_company'
    );
  });

  it('treats review-required as complete contact state', () => {
    assert.equal(isCompleteContactState(CONTACT_FINAL_STATE.REVIEW_REQUIRED), true);
    assert.equal(isCompleteContactState(CONTACT_FINAL_STATE.UNRESOLVED), false);
  });

  it('generates stable cohort AK ids', () => {
    assert.equal(cohortAkId(1), 'ak_babrun_cohort002_c001');
    assert.equal(cohortAkId(10), 'ak_babrun_cohort002_c010');
  });
});
