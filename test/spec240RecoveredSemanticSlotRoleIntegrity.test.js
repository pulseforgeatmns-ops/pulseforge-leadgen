'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeRecoveredInterviewState,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  composeCustomerConstraintPresentation,
  EPISTEMIC_STATES,
} = require('../services/clientIntelligenceInterview');

const VALUE_ONE = 'is willing to change how they manage';
const VALUE_TWO = 'We’d rather not work with people who are still at the idea stage or don’t yet have a functioning business. Babrun is designed for owners who already have real operations';
const VALUE_THREE = 'should stay visible in the Blueprint';

function recoveredState(overrides = {}) {
  return {
    normalizedFacts: {
      business_name: 'Babrun',
      ideal_customers: ['existing operating small businesses'],
      ideal_customer_traits: ['has a small team', VALUE_ONE],
      disqualified_customers: ['idea-stage businesses', VALUE_ONE, VALUE_TWO, VALUE_THREE, 'cleaning/home services'],
      geography: [],
      epistemic_states: {
        ideal_customers: EPISTEMIC_STATES.KNOWN,
        ideal_customer_traits: EPISTEMIC_STATES.KNOWN,
        disqualified_customers: EPISTEMIC_STATES.KNOWN,
        geography: EPISTEMIC_STATES.NOT_APPLICABLE,
      },
      superseded_slots: [],
      ...overrides,
    },
    sectionState: {},
  };
}

describe('SPEC-240: Recovered Semantic Slot-Role Integrity', () => {
  it('preserves valid customer categories, traits, and exclusions in their proper slots', () => {
    const facts = normalizeRecoveredInterviewState(recoveredState()).normalizedFacts;
    assert.deepEqual(facts.ideal_customers, ['existing operating small businesses']);
    assert.deepEqual(facts.ideal_customer_traits, ['has a small team', VALUE_ONE]);
    assert.deepEqual(facts.disqualified_customers, ['idea-stage businesses']);
  });

  it('invalidates the AUDIT-123 role corruption without moving or splitting values', () => {
    const facts = normalizeRecoveredInterviewState(recoveredState()).normalizedFacts;
    assert.equal(facts.disqualified_customers.includes(VALUE_ONE), false);
    assert.equal(facts.disqualified_customers.includes(VALUE_TWO), false);
    assert.equal(facts.disqualified_customers.includes(VALUE_THREE), false);
    assert.equal(facts.disqualified_customers.includes('cleaning/home services'), false);
    assert.equal(facts.ideal_customer_traits.includes(VALUE_TWO), false);
    assert.equal(facts.ideal_customers.includes('cleaning/home services'), false);
  });

  it('marks an emptied recovered exclusion slot unknown and is idempotent', () => {
    const state = recoveredState({
      disqualified_customers: [VALUE_ONE, VALUE_TWO, VALUE_THREE, 'cleaning/home services'],
    });
    const once = normalizeRecoveredInterviewState(state);
    const twice = normalizeRecoveredInterviewState(once);
    assert.deepEqual(once.normalizedFacts.disqualified_customers, []);
    assert.equal(once.normalizedFacts.epistemic_states.disqualified_customers, EPISTEMIC_STATES.UNKNOWN);
    assert.ok(once.normalizedFacts.superseded_slots.includes('disqualified_customers'));
    assert.deepEqual(twice, once);
  });

  it('keeps malformed audit values out of sections and the executive summary', () => {
    const recovered = normalizeRecoveredInterviewState(recoveredState());
    const sections = sectionsFromNormalizedFacts(recovered.normalizedFacts, recovered.sectionState);
    const brief = buildExecutiveSummary(sections, { normalizedFacts: recovered.normalizedFacts });
    assert.equal(sections.avoidCustomers.summary, 'The business prefers to avoid idea-stage businesses. These constraints protect targeting quality and should stay visible in the Blueprint.');
    assert.doesNotMatch(JSON.stringify({ sections, brief }), /is willing to change how they manage|still at the idea stage or don’t yet have a functioning business/i);
    assert.doesNotMatch(
      brief.sections.find((section) => section.id === 'whoYouServe').body,
      /should stay visible in the Blueprint/i
    );
  });

  it('continues to render valid exclusions through the existing presentation helper', () => {
    assert.equal(
      composeCustomerConstraintPresentation('Babrun', 'idea-stage businesses'),
      'Babrun prefers to avoid idea-stage businesses'
    );
  });
});