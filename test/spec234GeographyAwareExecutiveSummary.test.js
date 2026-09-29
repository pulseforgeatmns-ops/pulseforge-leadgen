'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { buildExecutiveSummary, EPISTEMIC_STATES } = require('../services/clientIntelligenceInterview');

const BUSINESS_GOAL =
  'Prove Babrun can reliably acquire the right founder customers for the 12-week program.';
const IDEAL_CUSTOMERS = [
  'existing operating small business',
  'cleaning/home services',
  'e-commerce',
  'fitness',
];
const EXCLUSIONS = ['idea-stage businesses', 'owners seeking only a quick lead-generation fix'];

function render(factOverrides = {}) {
  const normalizedFacts = {
    business_name: 'Babrun',
    ideal_customers: IDEAL_CUSTOMERS,
    ideal_customer_traits: ['generally fewer than 10 employees'],
    disqualified_customers: EXCLUSIONS,
    geography: [],
    differentiation: 'practical transformation-focused implementation',
    brand_voice: null,
    ninety_day_outcomes: BUSINESS_GOAL,
    success_metrics: ['qualified founder conversations'],
    epistemic_states: {
      ideal_customers: EPISTEMIC_STATES.KNOWN,
      disqualified_customers: EPISTEMIC_STATES.KNOWN,
      geography: EPISTEMIC_STATES.NOT_APPLICABLE,
      differentiation: EPISTEMIC_STATES.KNOWN,
      brand_voice: EPISTEMIC_STATES.UNKNOWN,
      ninety_day_outcomes: EPISTEMIC_STATES.KNOWN,
      success_metrics: EPISTEMIC_STATES.KNOWN,
    },
    hypotheses: {},
    ...factOverrides,
  };
  const brief = buildExecutiveSummary({}, { normalizedFacts, clientId: 1 });
  return Object.fromEntries(brief.sections.map((section) => [section.id, section]));
}

function assertNoGeographyStrategy(body) {
  assert.doesNotMatch(body, /geography chosen to match|geographic beachhead|location targeting|market geography/i);
}

describe('SPEC-234 geography-aware Executive Brief synthesis', () => {
  it('suppresses geography strategy for NOT_APPLICABLE Babrun geography while retaining ICP and exclusions', () => {
    const sections = render();
    assertNoGeographyStrategy(sections.whoYouServe.body);
    assert.match(sections.whoYouServe.body, /existing operating small business/i);
    assert.match(sections.whoYouServe.body, /idea-stage businesses/i);
  });

  it('suppresses geography strategy when geography is unknown', () => {
    const sections = render({
      epistemic_states: { ...renderFactsState(), geography: EPISTEMIC_STATES.UNKNOWN },
    });
    assertNoGeographyStrategy(sections.whoYouServe.body);
  });

  it('preserves explicit known geography and its fit conclusion', () => {
    const sections = render({
      geography: ['Greater Manchester'],
      epistemic_states: { ...renderFactsState(), geography: EPISTEMIC_STATES.KNOWN },
    });
    assert.match(sections.whoYouServe.body, /Greater Manchester/i);
    assert.match(sections.whoYouServe.body, /geography chosen to match that fit/i);
  });

  it('retains hypothesized geography as a hypothesis without a geographic strategy conclusion', () => {
    const sections = render({
      geography: ['regional founder businesses'],
      epistemic_states: { ...renderFactsState(), geography: EPISTEMIC_STATES.HYPOTHESIS },
      hypotheses: { geography: 'regional founder businesses' },
    });
    assert.match(sections.whoYouServe.body, /current hypothesis/i);
    assertNoGeographyStrategy(sections.whoYouServe.body);
  });

  it('does not restore superseded historical geography', () => {
    const sections = render({
      superseded_slots: ['geography'],
      geography: [],
      epistemic_states: { ...renderFactsState(), geography: EPISTEMIC_STATES.NOT_APPLICABLE },
    });
    assertNoGeographyStrategy(sections.whoYouServe.body);
  });
});

function renderFactsState() {
  return {
    ideal_customers: EPISTEMIC_STATES.KNOWN,
    disqualified_customers: EPISTEMIC_STATES.KNOWN,
    geography: EPISTEMIC_STATES.NOT_APPLICABLE,
    differentiation: EPISTEMIC_STATES.KNOWN,
    brand_voice: EPISTEMIC_STATES.UNKNOWN,
    ninety_day_outcomes: EPISTEMIC_STATES.KNOWN,
    success_metrics: EPISTEMIC_STATES.KNOWN,
  };
}