'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  buildExecutiveSummary,
  composeCustomerConstraintPresentation,
} = require('../services/clientIntelligenceInterview');

describe('SPEC-237 exclusion presentation framing integrity', () => {
  it('preserves noun-phrase exclusions with natural framing', () => {
    assert.equal(
      composeCustomerConstraintPresentation('Babrun', 'idea-stage businesses'),
      'Babrun prefers to avoid idea-stage businesses'
    );
  });

  it('does not double-frame first-person exclusions', () => {
    const prose = composeCustomerConstraintPresentation(
      'Babrun',
      'we’d rather not work with people who are still at the idea stage'
    );
    assert.equal(prose, 'Babrun would rather not work with people who are still at the idea stage');
    assert.doesNotMatch(prose, /prefers to avoid we/i);
  });

  it('does not double-frame third-person exclusions', () => {
    const prose = composeCustomerConstraintPresentation(
      'Babrun',
      'The business avoids owners seeking only a quick lead-generation fix.'
    );
    assert.equal(prose, 'Babrun avoids owners seeking only a quick lead-generation fix');
    assert.doesNotMatch(prose, /prefers to avoid the business/i);
  });

  it('renders mixed noun and framed exclusions independently', () => {
    const prose = composeCustomerConstraintPresentation(
      'Babrun',
      'idea-stage businesses; we’d rather not work with people who expect someone else to run the business'
    );
    assert.equal(
      prose,
      'Babrun prefers to avoid idea-stage businesses; Babrun would rather not work with people who expect someone else to run the business'
    );
  });

  it('normalizes terminal punctuation without changing stored values', () => {
    const exclusions = ['people who are not ready to operate.;', 'idea-stage businesses.'];
    const original = [...exclusions];
    const prose = composeCustomerConstraintPresentation('Babrun', exclusions.join('; '));
    assert.doesNotMatch(prose, /\.;/);
    assert.deepEqual(exclusions, original);
  });

  it('does not deduplicate exclusion values', () => {
    const prose = composeCustomerConstraintPresentation(
      'Babrun',
      'idea-stage businesses; idea-stage businesses'
    );
    assert.equal((prose.match(/idea-stage businesses/gi) || []).length, 2);
  });

  it('keeps the Executive Brief path wired to the presentation helper', () => {
    const brief = buildExecutiveSummary({}, {
      normalizedFacts: {
        business_name: 'Babrun',
        ideal_customers: ['operating small businesses'],
        disqualified_customers: [
          'we’d rather not work with people who are still at the idea stage',
        ],
        geography: [],
        epistemic_states: {
          ideal_customers: 'KNOWN',
          disqualified_customers: 'KNOWN',
          geography: 'NOT_APPLICABLE',
        },
      },
    });
    const body = brief.sections.find((section) => section.id === 'whoYouServe').body;
    assert.match(body, /Babrun would rather not work with people/i);
    assert.doesNotMatch(body, /prefers to avoid we/i);
  });
});