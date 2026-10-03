'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  adaptNormalizedFactsKeys,
  ALIAS_TO_CANONICAL_FACT,
  BLUEPRINT_SECTION_TO_CANONICAL_FACT,
} = require('../lib/blueprintNormalizedFactsAdapter');
const {
  prepareNormalizedFactsForBrief,
  sectionsFromNormalizedFacts,
  emptyNormalizedFacts,
  EPISTEMIC_STATES,
} = require('../services/clientIntelligenceInterview');

describe('blueprintNormalizedFactsAdapter', () => {
  it('documents Blueprint section → canonical fact field mapping', () => {
    assert.equal(BLUEPRINT_SECTION_TO_CANONICAL_FACT.competitiveAdvantages[0], 'differentiation');
    assert.equal(BLUEPRINT_SECTION_TO_CANONICAL_FACT.brandVoice[0], 'brand_voice');
    assert.equal(ALIAS_TO_CANONICAL_FACT.avoid_customers, 'disqualified_customers');
    assert.equal(ALIAS_TO_CANONICAL_FACT.idealCustomers, 'ideal_customers');
  });

  it('folds camelCase-only persisted facts into canonical keys before composition', () => {
    const raw = {
      businessName: 'Studio Substral',
      businessDescription: 'Premium website redesign studio for owner-led local brands',
      services: ['website redesign', 'messaging', 'copywriting'],
      idealCustomers: [
        'owner-led businesses where a better website affects trust, lead flow, and sales',
        'small business owner / operator / decision-maker',
      ],
      avoidCustomers: ['cheap landing page / quick cosmetic tweak exclusion'],
      targetMarkets: ['Greater Manchester', 'southern New Hampshire'],
      competitiveAdvantages: 'clarity, speed, and a credible design process',
      brandVoice: 'clear, confident, and practical',
      campaignGoals:
        'Acquire at least one profitable website redesign client at $2,000+ from owner-led businesses in Greater Manchester / southern New Hampshire',
      successMetrics: ['qualified prospects identified', 'positive replies', 'discovery calls booked'],
      epistemic_states: {
        businessDescription: EPISTEMIC_STATES.KNOWN,
        services: EPISTEMIC_STATES.KNOWN,
        idealCustomers: EPISTEMIC_STATES.KNOWN,
        avoidCustomers: EPISTEMIC_STATES.KNOWN,
        targetMarkets: EPISTEMIC_STATES.KNOWN,
        competitiveAdvantages: EPISTEMIC_STATES.KNOWN,
        brandVoice: EPISTEMIC_STATES.KNOWN,
        campaignGoals: EPISTEMIC_STATES.KNOWN,
        successMetrics: EPISTEMIC_STATES.KNOWN,
      },
    };

    const prepared = prepareNormalizedFactsForBrief(raw);
    const sections = sectionsFromNormalizedFacts(prepared);

    assert.equal(prepared.business_name, 'Studio Substral');
    assert.ok(prepared.ideal_customers.length >= 2);
    assert.ok(prepared.disqualified_customers.length >= 1);
    assert.equal(prepared.differentiation, raw.competitiveAdvantages);
    assert.equal(prepared.brand_voice, raw.brandVoice);
    assert.equal(prepared.ninety_day_outcomes, raw.campaignGoals);
    assert.equal(prepared.epistemic_states.differentiation, EPISTEMIC_STATES.KNOWN);
    assert.equal(prepared.epistemic_states.ideal_customers, EPISTEMIC_STATES.KNOWN);

    for (const key of [
      'identity',
      'services',
      'idealCustomers',
      'avoidCustomers',
      'competitiveAdvantages',
      'brandVoice',
      'campaignGoals',
      'successMetrics',
    ]) {
      assert.notEqual(String(sections[key].summary || '').trim(), '', `expected ${key} summary`);
    }
    assert.match(sections.targetMarkets.summary, /Greater Manchester/i);
    assert.doesNotMatch(sections.targetMarkets.summary, /cheap landing page/i);
    assert.doesNotMatch(sections.successMetrics.summary, /good prospect should/i);
  });

  it('maps canonical projection avoid_customers into disqualified_customers', () => {
    const canonical = emptyNormalizedFacts();
    const adapted = adaptNormalizedFactsKeys(
      { avoid_customers: 'price-driven tire-kickers', epistemic_states: { avoid_customers: EPISTEMIC_STATES.KNOWN } },
      canonical
    );
    assert.deepEqual(adapted.disqualified_customers, ['price-driven tire-kickers']);
    assert.equal(adapted.epistemic_states.disqualified_customers, EPISTEMIC_STATES.KNOWN);
  });
});
