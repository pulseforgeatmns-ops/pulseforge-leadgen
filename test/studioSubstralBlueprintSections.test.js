'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  prepareNormalizedFactsForBrief,
  rehydrateNormalizedFactsFromAnswers,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  mergeSupplementalIntoSections,
  pinComposedBlueprintSectionSummaries,
  EPISTEMIC_STATES,
  MESSAGE_TYPES,
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

/** Live Studio Substral session shape: answers hold substance; normalizedFacts slots were cleared. */
function studioSubstralAnswersOnlyInterviewState() {
  const answers = {
    identity:
      'The business is Studio Substral.\n\nToday, we build premium websites for small businesses, operators, and local service companies that need to look more credible online.',
    services:
      'Today, Studio Substral provides website redesign and launch services for small businesses that need a stronger, more credible online presence.',
    ideal_customers:
      'Studio Substral most wants to work with owner-led businesses where a better website can directly affect trust, lead flow, and sales.\n\nThe ideal customer is a small business owner, operator, or decision-maker who already has a real business but whose website feels outdated, generic, confusing, visually weak, or slow.',
    avoid_customers:
      'Yes. Studio Substral should avoid customers who are mainly looking for the cheapest possible website, a quick cosmetic patch, or a one-off task with no broader business value.',
    target_markets:
      'Greater Manchester and southern New Hampshire — local service businesses, contractors, trades, professional services, medical or wellness practices, property service companies, and hospitality businesses.',
    advantages:
      'A great-fit customer chooses Studio Substral when they care about credibility, clarity, and business impact more than simply getting the cheapest website.',
    brand_voice:
      'Studio Substral should sound clear, sharp, confident, and practical.\n\nThe tone should feel premium but not pretentious.',
    campaign_goals:
      'Over the next 90 days, this growth work would be successful if Studio Substral proves there is real demand for premium website redesigns among owner-led businesses in Greater Manchester and southern New Hampshire.',
    success_metrics: `We'll know Studio Substral is working if the outreach is producing real conversations with owner-led businesses that have obvious website credibility gaps.

The main numbers to watch are qualified prospects identified, prospects contacted, positive replies, discovery calls booked, proposals sent, proposals accepted, and total revenue closed.

The most important signal is not raw activity. It is whether we are finding businesses with real commercial pain and getting them to engage. A good prospect should have an outdated or weak website, a real operating business, a reachable decision-maker, and a clear reason a better site could improve trust, lead flow, or sales.`,
  };
  return {
    answers,
    normalizedFacts: {
      business_name: null,
      business_description: null,
      services: [],
      ideal_customers: [
        'growing owner-led brands in Greater Manchester',
        'lead flow',
        'getting referrals',
      ],
      disqualified_customers: [
        'Yes. Studio Substral should avoid customers who are mainly looking for the cheapest possible website',
      ],
      geography: ['Greater Manchester'],
      differentiation: null,
      brand_voice: null,
      ninety_day_outcomes: null,
      success_metrics: [
        "We'll know Studio Substral is working if the outreach is producing real conversations with owner-led businesses that have obvious website credibility gaps. The main numbers to watch are qualified prospects identified",
        'prospects contacted',
      ],
      epistemic_states: {
        business_description: EPISTEMIC_STATES.UNRESOLVED,
        services: EPISTEMIC_STATES.UNRESOLVED,
        ideal_customers: EPISTEMIC_STATES.KNOWN,
        disqualified_customers: EPISTEMIC_STATES.KNOWN,
        geography: EPISTEMIC_STATES.KNOWN,
        differentiation: EPISTEMIC_STATES.UNRESOLVED,
        brand_voice: EPISTEMIC_STATES.UNRESOLVED,
        ninety_day_outcomes: EPISTEMIC_STATES.UNRESOLVED,
        success_metrics: EPISTEMIC_STATES.KNOWN,
      },
      hypotheses: {},
      evidence_statements: {},
      business_facts: {},
      transformation_areas: [],
      pains: [],
      learning_signals: [],
      excluded_metrics: [],
      superseded_slots: [],
    },
    sectionState: {},
  };
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

  it('rehydrates cleared normalizedFacts from persisted guided answers (live session shape)', () => {
    const state = studioSubstralAnswersOnlyInterviewState();
    const rehydrated = rehydrateNormalizedFactsFromAnswers(state);
    const prepared = prepareNormalizedFactsForBrief(rehydrated);
    const sections = sectionsFromNormalizedFacts(prepared);
    assert.match(String(prepared.business_name || ''), /Studio Substral/i);
    assert.ok((prepared.services || []).some((s) => /website redesign/i.test(s)));
    assert.match(sections.identity.summary, /Studio Substral/i);
    assert.match(sections.services.summary, /website redesign/i);
    assert.match(sections.competitiveAdvantages.summary, /credibility|clarity|business impact/i);
    assert.match(sections.brandVoice.summary, /clear|confident|practical/i);
    assert.match(sections.campaignGoals.summary, /90 days|real demand|website redesign/i);
    assert.match(sections.idealCustomers.summary, /owner-led|decision-maker/i);
    assert.doesNotMatch(sections.idealCustomers.summary, /\blead flow,\s*getting referrals\b/i);
    assert.match(sections.avoidCustomers.summary, /cheapest|cosmetic|one-off/i);
    assert.match(sections.targetMarkets.summary, /Greater Manchester/i);
    assert.doesNotMatch(sections.targetMarkets.summary, /decision-maker who already/i);
    assert.match(sections.successMetrics.summary, /qualified prospects identified/i);
    assert.doesNotMatch(sections.successMetrics.summary, /good prospect should/i);
  });

  it('composes Blueprint v1.6 services as real offerings only (Studio Substral catalog)', () => {
    const evidence =
      'Studio Substral offers website redesign, messaging, visual design, mobile experience, calls to action, explain its value clearly, homepage and key page design, copywriting, mobile optimization, contact form or booking flow setup, basic SEO cleanup, launch support, and light post-launch refinement.';
    const prepared = prepareNormalizedFactsForBrief({
      business_name: 'Studio Substral',
      services: [
        'website redesign',
        'explain its value clearly',
        'mobile experience',
        'calls to action',
      ],
      geography: ['Greater Manchester'],
      vertical_focus: 'local service businesses',
      disqualified_customers: ['price shoppers'],
      differentiation: 'credibility',
      brand_voice: 'clear',
      ninety_day_outcomes: 'Acquire one client',
      success_metrics: ['qualified prospects identified'],
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
      },
      evidence_statements: { services: evidence },
      hypotheses: {},
      business_facts: {},
      transformation_areas: [],
      pains: [],
      learning_signals: [],
      excluded_metrics: [],
      superseded_slots: [],
    });
    const sections = sectionsFromNormalizedFacts(prepared);
    const services = sections.services.summary || '';

    assert.match(services, /website redesign, messaging, visual design, homepage and key-page design/i);
    assert.match(services, /mobile optimization, stronger calls to action/i);
    assert.match(services, /launch support, and light post-launch refinement/i);
    assert.doesNotMatch(services, /explain its value clearly/i);
    assert.doesNotMatch(services, /mobile experience/i);
  });

  it('v1.6 regeneration path keeps composed targetMarkets after supplemental merge', () => {
    const prepared = prepareNormalizedFactsForBrief({
      business_name: 'Studio Substral',
      services: ['website redesign'],
      geography: ['Greater Manchester', 'southern New Hampshire'],
      vertical_focus:
        'local service businesses, contractors and trades, professional services, medical and wellness practices, property service companies, hospitality businesses, and growing owner-led local brands',
      disqualified_customers: ['cheapest possible website'],
      differentiation: 'credibility',
      brand_voice: 'clear',
      ninety_day_outcomes: 'Acquire one client',
      success_metrics: ['qualified prospects identified'],
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
      },
      hypotheses: {},
      evidence_statements: {},
      business_facts: {},
      transformation_areas: [],
      pains: [],
      learning_signals: [],
      excluded_metrics: [],
      superseded_slots: [],
    });
    let sections = sectionsFromNormalizedFacts(prepared, {
      targetMarkets: {
        summary:
          'Ideal customers are owner-led businesses where a better website affects trust, lead flow, and sales.',
        confidence: 0.9,
        evidenceIds: [],
        unknowns: [],
      },
    });
    const icpNarrative =
      'Studio Substral most wants owner-led businesses where a better website can directly affect trust, lead flow, and sales.';
    sections = mergeSupplementalIntoSections(sections, [
      {
        text: icpNarrative,
        substance: icpNarrative,
        kind: MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT,
        confirmed: true,
        section: 'targetMarkets',
      },
    ]);
    sections = pinComposedBlueprintSectionSummaries(prepared, sections);
    const markets = sections.targetMarkets.summary || '';

    assert.match(
      markets,
      /^Priority markets are Greater Manchester and southern New Hampshire, with an initial focus on/i
    );
    assert.match(markets, /contractors and trades|contractors, trades/i);
    assert.match(markets, /growing owner-led local brands/i);
    assert.doesNotMatch(markets, /decision-maker|lead flow|trust, lead flow|Ideal customers are/i);
    assert.doesNotMatch(markets, /Geography and vertical focus here bound/i);
  });

  it('composes Blueprint v1.5 sections without transcript debris or duplicate boilerplate', () => {
    const prepared = prepareNormalizedFactsForBrief({
      business_name: 'Studio Substral',
      services: [
        'website redesign',
        'but to help the business look legitimate',
        'For now',
        'broken on mobile',
        'and or below the quality of the business itself',
      ],
      geography: ['Greater Manchester', 'southern New Hampshire'],
      vertical_focus:
        'local service businesses, contractors and trades, professional services, medical and wellness practices, property service companies, hospitality businesses, and growing owner-led local brands',
      disqualified_customers: [
        'customers who are mainly looking for the cheapest possible website. These constraints protect targeting quality and should stay visible in the Blueprint.',
      ],
      differentiation: 'clarity, speed, and a credible design process',
      brand_voice: 'clear, confident, and practical',
      ninety_day_outcomes: 'Acquire one profitable website redesign client at $2,000+',
      success_metrics: ['qualified prospects identified', 'discovery calls booked'],
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
      },
      hypotheses: {},
      evidence_statements: {
        services:
          'Studio Substral offers website redesign, messaging, visual design, mobile optimization, stronger calls to action, homepage and key page design, copywriting, contact and booking flow setup, basic SEO cleanup, launch support, and light post-launch refinement.',
        differentiation:
          'A great-fit customer chooses Studio Substral when they care about credibility, clarity, and business impact more than simply getting the cheapest website. that Studio Substral can connect design choices directly to trust and lead flow.',
      },
      business_facts: {},
      transformation_areas: [],
      pains: [],
      learning_signals: [],
      excluded_metrics: [],
      superseded_slots: [],
    });
    const sections = sectionsFromNormalizedFacts(prepared);

    assert.match(
      sections.services.summary,
      /Studio Substral delivers website redesign, messaging, visual design/i
    );
    assert.doesNotMatch(sections.services.summary, /but to help the business look legitimate/i);
    assert.doesNotMatch(sections.services.summary, /For now|broken on mobile|below the quality/i);

    const avoidClosing = 'These constraints protect targeting quality and should stay visible in the Blueprint.';
    assert.equal(
      (sections.avoidCustomers.summary.match(new RegExp(avoidClosing.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || [])
        .length,
      1
    );

    assert.match(
      sections.targetMarkets.summary,
      /Priority markets are Greater Manchester and southern New Hampshire, with an initial focus on/i
    );
    assert.match(sections.targetMarkets.summary, /contractors and trades|contractors, trades/i);
    assert.doesNotMatch(sections.targetMarkets.summary, /decision-maker|cheapest possible website|lead flow/i);
    assert.doesNotMatch(sections.targetMarkets.summary, /Geography and vertical focus here bound/i);

    assert.match(
      sections.competitiveAdvantages.summary,
      /Customers care about credibility, clarity, and business impact/i
    );
    assert.match(sections.competitiveAdvantages.summary, /connect design choices directly to trust and lead flow/i);
    assert.doesNotMatch(sections.competitiveAdvantages.summary, /\. that Studio Substral/i);
  });

  it('composes Blueprint v1.4 sections without wrapper duplication or cross-section bleed', () => {
    const answers = {
      services:
        'Studio Substral offers website redesign, messaging, visual design, mobile optimization, stronger calls to action, homepage and key page design, copywriting, contact and booking flow setup, basic SEO cleanup, launch support, and light post-launch refinement.',
      campaign_goals:
        'Over the next 90 days, this growth work would be successful if Studio Substral proves there is real demand for premium website redesigns, acquires at least one profitable website redesign client at $2,000+, and validates a repeatable local prospecting and outreach system.',
      success_metrics:
        'qualified prospects identified, discovery calls booked, total revenue closed, revenue closed',
    };
    const state = {
      answers,
      normalizedFacts: {
        business_name: 'Studio Substral',
        services: [],
        ninety_day_outcomes: null,
        success_metrics: ['total revenue closed', 'revenue closed'],
        geography: ['Greater Manchester', 'southern New Hampshire'],
        vertical_focus:
          'local service businesses, contractors, trades, professional services, medical or wellness practices, property service companies, hospitality businesses',
        differentiation:
          'Customers choose Studio Substral for credibility, clarity, and business impact over the cheapest website.',
        brand_voice: 'clear, sharp, confident, and practical',
        disqualified_customers: [
          'customers who are mainly looking for the cheapest possible website',
          'customers who are mainly looking for the cheapest possible website',
        ],
        epistemic_states: {
          services: EPISTEMIC_STATES.UNRESOLVED,
          ninety_day_outcomes: EPISTEMIC_STATES.UNRESOLVED,
          success_metrics: EPISTEMIC_STATES.KNOWN,
          geography: EPISTEMIC_STATES.KNOWN,
          differentiation: EPISTEMIC_STATES.KNOWN,
          brand_voice: EPISTEMIC_STATES.KNOWN,
          disqualified_customers: EPISTEMIC_STATES.KNOWN,
        },
        hypotheses: {},
        evidence_statements: {
          brand_voice:
            'Studio Substral should sound clear, sharp, confident, and practical.\n\nThe tone should feel premium but not pretentious.',
        },
        business_facts: {},
        transformation_areas: [],
        pains: [],
        learning_signals: [],
        excluded_metrics: [],
        superseded_slots: [],
      },
    };
    const prepared = prepareNormalizedFactsForBrief(rehydrateNormalizedFactsFromAnswers(state));
    const sections = sectionsFromNormalizedFacts(prepared);
    const blob = JSON.stringify(sections);

    assert.match(sections.services.summary, /messaging/i);
    assert.match(sections.services.summary, /copywriting/i);
    assert.match(sections.services.summary, /launch support/i);
    assert.doesNotMatch(sections.services.summary, /more credible online presence/i);

    assert.match(sections.campaignGoals.summary, /\$2,000\+/);
    assert.doesNotMatch(sections.campaignGoals.summary, /would be successful if/i);
    assert.doesNotMatch(sections.campaignGoals.summary, /Near-term growth goals focus on/i);

    assert.doesNotMatch(blob, /Competitive edge is described as/i);
    assert.doesNotMatch(blob, /Brand voice should read as/i);
    assert.match(sections.brandVoice.summary, /premium but not pretentious/i);

    assert.match(sections.targetMarkets.summary, /Greater Manchester/i);
    assert.match(sections.targetMarkets.summary, /contractors|professional services/i);
    assert.doesNotMatch(sections.targetMarkets.summary, /cheapest possible website/i);

    assert.doesNotMatch(sections.avoidCustomers.summary, /prefers to avoid The business prefers to avoid/i);
    assert.doesNotMatch(sections.avoidCustomers.summary, /,\s*and\s+or\s+/i);

    assert.match(sections.successMetrics.summary, /total revenue closed/i);
    assert.doesNotMatch(sections.successMetrics.summary, /total revenue closed, revenue closed/i);
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
