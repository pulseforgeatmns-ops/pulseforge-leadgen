'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildExecutiveSummary,
  sectionsFromNormalizedFacts,
  EPISTEMIC_STATES,
} = require('../services/clientIntelligenceInterview');

function studioSubstralFacts(overrides = {}) {
  return {
    business_name: 'Studio Substral',
    business_description:
      'premium website redesign studio for small businesses and owner-led companies that need a stronger, more credible online presence',
    services: ['today', 'website redesign', 'website design', 'landing pages'],
    growth_focus: 'commercial cleaning',
    ideal_customers: [
      'owner-led businesses in Greater Manchester',
      'professional service firms — law firms and accountants',
    ],
    ideal_customer_traits: ['A real operating business', 'A reachable decision-maker'],
    disqualified_customers: ['Price-driven clients looking for the cheapest possible website'],
    geography: ['Greater Manchester', 'southern New Hampshire'],
    differentiation: 'clarity, speed, and a credible design process',
    brand_voice: 'clear, confident, and practical',
    ninety_day_outcomes:
      'Acquire at least one profitable website redesign client at $2,000+ from owner-led businesses in Greater Manchester / southern New Hampshire',
    success_metrics: [
      'Getting them to engage. A good prospect should have an outdated or weak website',
      'Qualified prospects identified',
      'Positive replies',
      'Discovery calls booked',
      'A real operating business',
      'That is a weak signal. If they are talking about credibility',
    ],
    epistemic_states: {
      business_description: EPISTEMIC_STATES.KNOWN,
      services: EPISTEMIC_STATES.KNOWN,
      ideal_customers: EPISTEMIC_STATES.KNOWN,
      disqualified_customers: EPISTEMIC_STATES.KNOWN,
      geography: EPISTEMIC_STATES.KNOWN,
      differentiation: EPISTEMIC_STATES.KNOWN,
      brand_voice: EPISTEMIC_STATES.KNOWN,
      ninety_day_outcomes: EPISTEMIC_STATES.KNOWN,
      success_metrics: EPISTEMIC_STATES.KNOWN,
      growth_focus: EPISTEMIC_STATES.KNOWN,
    },
    hypotheses: {},
    evidence_statements: {},
    business_facts: {},
    transformation_areas: [],
    pains: [],
    learning_signals: [],
    excluded_metrics: [],
    superseded_slots: [],
    ...overrides,
  };
}

function briefForFacts(facts) {
  const sections = sectionsFromNormalizedFacts(facts);
  return buildExecutiveSummary(sections, { normalizedFacts: facts, clientId: 17 });
}

describe('Studio Substral Executive Business Brief', () => {
  it('does not mention commercial cleaning or Anchor objectives', () => {
    const brief = briefForFacts(studioSubstralFacts());
    const blob = JSON.stringify(brief);
    assert.doesNotMatch(blob, /commercial cleaning/i);
    assert.doesNotMatch(blob, /Anchor Cleaning/i);
  });

  it('renders coherent identity and services without Today artifacts', () => {
    const byId = Object.fromEntries(briefForFacts(studioSubstralFacts()).sections.map((s) => [s.id, s]));
    assert.match(byId.whoYouAre.body, /Studio Substral/i);
    assert.match(byId.whoYouAre.body, /website redesign|website design/i);
    assert.doesNotMatch(byId.whoYouAre.body, /\bToday is a Studio Substral\b/i);
    assert.doesNotMatch(byId.whoYouAre.body, /Services include today\b/i);
  });

  it('separates ICP, geography, and exclusions in Who You Serve', () => {
    const body = Object.fromEntries(briefForFacts(studioSubstralFacts()).sections.map((s) => [s.id, s]))
      .whoYouServe.body;
    assert.match(body, /ideal customers include/i);
    assert.match(body, /Greater Manchester/i);
    assert.match(body, /avoid|decline|price-driven|lowest price/i);
    assert.doesNotMatch(body, /particularly those responsible for professional service firms/i);
  });

  it('uses the Studio Substral ninety-day objective verbatim in substance', () => {
    const body = Object.fromEntries(briefForFacts(studioSubstralFacts()).sections.map((s) => [s.id, s]))
      .whereHeaded.body;
    assert.match(body, /website redesign client at \$2,000\+/i);
    assert.match(body, /Acquire at least one profitable/i);
  });

  it('keeps scorecard metrics valid and excludes sentence-fragment metrics', () => {
    const scorecard = Object.fromEntries(
      briefForFacts(studioSubstralFacts()).sections.map((s) => [s.id, s])
    ).recommendedScorecard;
    const names = (scorecard.items || []).map((row) => row.name).join('\n');
    assert.match(names, /Qualified Prospects|Positive Reply|Discovery Calls/i);
    assert.doesNotMatch(names, /Getting Them To Engage/i);
    assert.doesNotMatch(names, /Good Prospect Should/i);
    assert.doesNotMatch(names, /Real Operating Business/i);
    assert.doesNotMatch(names, /Weak Signal/i);
  });

  it('lists qualification signals separately from scorecard metrics', () => {
    const byId = Object.fromEntries(briefForFacts(studioSubstralFacts()).sections.map((s) => [s.id, s]));
    assert.ok(byId.qualificationSignals, 'expected Prospect Fit Criteria section');
    const signals = byId.qualificationSignals.items.join(' ');
    assert.match(signals, /outdated or weak website|operating business|decision-maker/i);
    const scoreNames = byId.recommendedScorecard.items.map((row) => row.name).join(' ');
    assert.doesNotMatch(scoreNames, /operating business/i);
  });

  it('does not list ideal customer or decline topics as missing when facts exist', () => {
    const learn = Object.fromEntries(briefForFacts(studioSubstralFacts()).sections.map((s) => [s.id, s]))
      .learnMore.items.join(' ');
    assert.doesNotMatch(learn, /ideal customer really is/i);
    assert.doesNotMatch(learn, /customers to decline/i);
    assert.doesNotMatch(learn, /commercial customer segment/i);
  });
});
