'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  prepareNormalizedFactsForBrief,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  EPISTEMIC_STATES,
} = require('../services/clientIntelligenceInterview');

/** Simulates mis-mapped persisted facts after Blueprint v1.1 regeneration. */
function studioSubstralCorruptedFacts() {
  return {
    business_name: 'Studio Substral',
    business_description:
      'Today is an especially service businesses, professional firms, contractors that need a stronger online presence',
    services: [
      'today',
      'website redesign',
      'messaging',
      'generic',
      'confusing',
      'visually weak',
      'slow',
      'mobile optimization',
      'copywriting',
    ],
    growth_focus: 'commercial cleaning',
    ideal_customers: [],
    idealCustomers: [
      'owner-led businesses where a better website affects trust, lead flow, and sales',
      'small business owner / operator / decision-maker',
    ],
    disqualified_customers: [],
    avoidCustomers: ['cheap landing page / quick cosmetic tweak exclusion'],
    geography: [
      'owner-led businesses where a better website affects trust, lead flow, and sales',
      'small business owner / operator / decision-maker',
      'local service businesses, contractors, trades, professional services, medical or wellness practices, property service companies, hospitality businesses',
      'cheap landing page / quick cosmetic tweak exclusion',
      'good real-world service but weak digital presentation',
      'Greater Manchester',
      'southern New Hampshire',
    ],
    vertical_focus: null,
    differentiation: 'clarity, speed, and a credible design process',
    brand_voice: 'clear, confident, and practical',
    ninety_day_outcomes:
      'Acquire at least one profitable website redesign client at $2,000+ from owner-led businesses in Greater Manchester / southern New Hampshire',
    success_metrics: [
      'Getting them to engage. A good prospect should have an outdated or weak website',
      'qualified prospects identified',
      'positive replies',
      'discovery calls booked',
      'A real operating business',
    ],
    epistemic_states: {
      business_description: EPISTEMIC_STATES.KNOWN,
      services: EPISTEMIC_STATES.KNOWN,
      ideal_customers: EPISTEMIC_STATES.UNRESOLVED,
      disqualified_customers: EPISTEMIC_STATES.UNRESOLVED,
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
  };
}

function blueprintSectionsFromCorruptedFacts() {
  const prepared = prepareNormalizedFactsForBrief(studioSubstralCorruptedFacts());
  return { prepared, sections: sectionsFromNormalizedFacts(prepared) };
}

describe('Studio Substral Blueprint section mapping (v1.1 regeneration)', () => {
  it('maps camelCase fact keys and fills idealCustomers / avoidCustomers', () => {
    const { prepared, sections } = blueprintSectionsFromCorruptedFacts();
    assert.ok(prepared.ideal_customers.length >= 2);
    assert.ok(prepared.disqualified_customers.length >= 1);
    assert.match(sections.idealCustomers.summary, /owner-led|decision-maker/i);
    assert.match(sections.avoidCustomers.summary, /cheap|cosmetic|landing page/i);
    assert.notEqual(String(sections.idealCustomers.summary || '').trim(), '');
    assert.notEqual(String(sections.avoidCustomers.summary || '').trim(), '');
  });

  it('keeps target markets to geography and vertical focus only', () => {
    const { sections } = blueprintSectionsFromCorruptedFacts();
    const markets = sections.targetMarkets.summary || '';
    assert.match(markets, /Greater Manchester/i);
    assert.match(markets, /southern New Hampshire|New Hampshire/i);
    assert.match(markets, /contractors|professional services|local service businesses/i);
    assert.doesNotMatch(markets, /cheap landing page/i);
    assert.doesNotMatch(markets, /small business owner \/ operator \/ decision-maker/i);
  });

  it('cleans identity and services without Today artifacts or ICP pain fragments', () => {
    const { sections } = blueprintSectionsFromCorruptedFacts();
    assert.doesNotMatch(sections.identity.summary, /\bToday is\b/i);
    assert.match(sections.identity.summary, /Studio Substral/i);
    assert.doesNotMatch(sections.services.summary, /Today the business delivers Today/i);
    assert.doesNotMatch(sections.services.summary, /\bgeneric\b|\bconfusing\b|\bvisually weak\b/i);
    assert.match(sections.services.summary, /website redesign/i);
  });

  it('partitions qualification prose out of success metrics', () => {
    const { prepared, sections } = blueprintSectionsFromCorruptedFacts();
    const metricsBlob = (prepared.success_metrics || []).join(' ');
    assert.match(metricsBlob, /qualified prospects identified/i);
    assert.doesNotMatch(metricsBlob, /good prospect should/i);
    assert.doesNotMatch(sections.successMetrics.summary, /real operating business/i);
  });

  it('aligns Executive Brief with the same prepared facts path', () => {
    const { prepared, sections } = blueprintSectionsFromCorruptedFacts();
    const brief = buildExecutiveSummary(sections, {
      normalizedFacts: studioSubstralCorruptedFacts(),
      clientId: 17,
    });
    const whoYouAre = brief.sections.find((s) => s.id === 'whoYouAre')?.body || '';
    const whoYouServe = brief.sections.find((s) => s.id === 'whoYouServe')?.body || '';
    assert.doesNotMatch(whoYouAre, /\bToday is\b/i);
    assert.match(whoYouServe, /ideal customers include/i);
    assert.match(whoYouServe, /Greater Manchester/i);
    assert.ok(prepared.ideal_customers.length >= 2);
  });
});
