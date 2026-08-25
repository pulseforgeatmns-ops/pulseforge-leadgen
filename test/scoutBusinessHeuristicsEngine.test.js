'use strict';

/**
 * SPEC-162 — Business Heuristics Engine acceptance tests.
 * ADR-082 — Business judgment through reusable heuristics.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildUnderstanding,
  buildEvidence,
} = require('../packages/scout/synthesis/types');
const { buildBusinessHeuristic, OUTCOME_KINDS } = require('../packages/scout/heuristics/types');
const { cloneHeuristicLibrary, INITIAL_HEURISTICS } = require('../packages/scout/heuristics/HeuristicLibrary');
const {
  activateHeuristics,
  explainJudgment,
  buildRecommendationFromHeuristics,
  buildBusinessJudgmentReport,
  learnFromOutcome,
  detectHeuristicContradictions,
} = require('../packages/scout/heuristics/BusinessHeuristicsEngine');
const {
  applyBusinessJudgment,
  applyBusinessUnderstandingSynthesis,
  createInvestigationState,
} = require('../packages/scout/investigation/InvestigationState');
const { buildMissionIntelligenceReport } = require('../packages/scout/investigation/MissionIntelligenceReport');
const { synthesizeFromCandidates } = require('../packages/scout/synthesis/EvidenceSynthesisEngine');
const { runInvestigativeReasoningLoop } = require('../packages/scout/investigation/InvestigativeReasoningLoop');
const { buildSemanticMarketDefinition } = require('../packages/scout/intelligence/MarketDefinition');

function growthMarketUnderstanding() {
  return buildUnderstanding({
    entity: 'Downtown Manchester Corridor',
    entityId: 'downtown-corridor',
    kind: 'growth',
    assertions: ['Commercial activity appears to be increasing'],
    supportingEvidence: [
      buildEvidence({ source: 'news', observation: 'New Starbucks opened on Elm Street' }),
      buildEvidence({ source: 'permit', observation: 'Apartment construction permit approved for 200 units' }),
      buildEvidence({ source: 'news', observation: 'National retailer Target expanding into corridor' }),
    ],
    confidence: 0.78,
  });
}

function vendorInstabilityUnderstanding() {
  return buildUnderstanding({
    entity: 'ABC Property Management',
    entityId: 'abc-pm',
    kind: 'service_need',
    assertions: ['Vendor relationships may be weakening', 'Operations leadership changed recently'],
    supportingEvidence: [
      buildEvidence({ source: 'google_reviews', observation: 'Recent negative cleanliness reviews on Google' }),
      buildEvidence({ source: 'linkedin', observation: 'New Operations Manager hired last month' }),
      buildEvidence({ source: 'indeed', observation: 'Hiring facilities staff for property maintenance' }),
    ],
    confidence: 0.74,
  });
}

function contradictoryUnderstanding() {
  return buildUnderstanding({
    entity: 'Growing Property Co',
    entityId: 'growing-co',
    kind: 'growth',
    assertions: ['Growing company with expansion hiring', 'Long-standing vendor relationships maintained'],
    supportingEvidence: [
      buildEvidence({ source: 'news', observation: 'New Starbucks opened nearby in growing commercial corridor' }),
      buildEvidence({ source: 'permit', observation: 'Apartment construction permit approved for 200 units' }),
      buildEvidence({ source: 'news', observation: 'National retailer expanding into corridor' }),
      buildEvidence({ source: 'website', observation: 'Same cleaning company preferred vendor for 8 years' }),
      buildEvidence({ source: 'review', observation: 'Satisfied with long-standing vendor relationship' }),
    ],
    confidence: 0.82,
  });
}

function abcMultiHeuristicUnderstanding() {
  return buildUnderstanding({
    entity: 'ABC Property Management',
    entityId: 'abc-pm',
    kind: 'buying_signal',
    assertions: ['Operations manager hired', 'Negative cleanliness reviews', 'Market corridor growing'],
    supportingEvidence: [
      buildEvidence({ source: 'linkedin', observation: 'Operations Manager hired' }),
      buildEvidence({ source: 'google_reviews', observation: 'Recent negative cleanliness reviews' }),
      buildEvidence({ source: 'news', observation: 'New apartment construction nearby' }),
      buildEvidence({ source: 'chamber', observation: 'Chamber of Commerce member' }),
    ],
    confidence: 0.8,
  });
}

describe('SPEC-162 — Business Heuristics Engine', () => {
  it('BusinessHeuristic model includes reusable pattern fields', () => {
    const heuristic = buildBusinessHeuristic({
      id: 'test_heuristic',
      name: 'Test Heuristic',
      category: 'market_growth',
      description: 'Test pattern',
      triggerConditions: { patterns: [/growth/i], minMatches: 1 },
      implications: ['Test implication'],
      strength: 1,
    });

    assert.equal(heuristic.id, 'test_heuristic');
    assert.equal(heuristic.name, 'Test Heuristic');
    assert.ok(Array.isArray(heuristic.triggerConditions.patterns));
    assert.ok(Array.isArray(heuristic.implications));
    assert.equal(heuristic.strength, 1);
  });

  it('Scenario 1: three growth indicators activate Growth Market heuristic', () => {
    const judgment = activateHeuristics({
      businessUnderstandings: [growthMarketUnderstanding()],
    });

    const growth = judgment.activatedHeuristics.find((h) => h.heuristicId === 'growth_market');
    assert.ok(growth, 'Growth Market heuristic should activate');
    assert.ok(growth.score >= 0.7);
    assert.ok(growth.implications.some((i) => /commercial opportunity/i.test(i)));
    assert.ok(growth.triggeringEvidence.length >= 3);
    assert.equal(judgment.basedOnHeuristics, true);
    assert.equal(judgment.judgmentNotFromEvidence, true);
  });

  it('Scenario 2: vendor instability signals activate Vendor Instability heuristic', () => {
    const judgment = activateHeuristics({
      businessUnderstandings: [vendorInstabilityUnderstanding()],
    });

    const vendor = judgment.activatedHeuristics.find((h) => h.heuristicId === 'vendor_instability');
    assert.ok(vendor, 'Vendor Instability heuristic should activate');
    assert.ok(vendor.score >= 0.65);
    assert.ok(vendor.implications.some((i) => /vendor change/i.test(i)));
    assert.ok(vendor.triggeringEvidence.some((e) => /cleanliness|operations manager|facilities/i.test(e.observation)));

    const recommendation = buildRecommendationFromHeuristics(judgment);
    assert.equal(recommendation.basedOnHeuristics, true);
    assert.equal(recommendation.notDirectFromEvidence, true);
    assert.equal(recommendation.adr, 'ADR-082');
  });

  it('Scenario 3: contradictory heuristics preserved with reduced overall confidence', () => {
    const judgment = activateHeuristics({
      businessUnderstandings: [contradictoryUnderstanding()],
    });

    const growth = judgment.activatedHeuristics.find((h) => h.heuristicId === 'growth_market');
    const stability = judgment.activatedHeuristics.find((h) => h.heuristicId === 'vendor_stability');

    assert.ok(growth, 'Growth Market should activate');
    assert.ok(stability, 'Vendor Stability should activate');
    assert.ok(judgment.contradictions.length >= 1);

    const tension = judgment.contradictions.find(
      (c) =>
        (c.nameA === 'Growth Market' && c.nameB === 'Vendor Stability') ||
        (c.nameB === 'Growth Market' && c.nameA === 'Vendor Stability')
    );
    assert.ok(tension);
    assert.match(tension.tension, /BUT|vs/i);

    const withoutContradictionScore = judgment.activatedHeuristics.reduce((s, h) => s + h.score, 0) /
      judgment.activatedHeuristics.length;
    assert.ok(judgment.overallJudgment.confidence <= withoutContradictionScore);
    assert.match(judgment.overallJudgment.summary, /Mixed|tension|Review|long-standing/i);
  });

  it('Scenario 4: explainJudgment returns activated heuristics, evidence, and implications', () => {
    const judgment = activateHeuristics({
      businessUnderstandings: [abcMultiHeuristicUnderstanding()],
    });

    const explanation = explainJudgment(judgment, { entity: 'ABC Property Management' });

    assert.equal(explanation.spec, 'SPEC-162');
    assert.equal(explanation.businessJudgment, true);
    assert.ok(explanation.activatedHeuristics.length >= 2);
    assert.ok(explanation.overallJudgment.summary);
    assert.equal(explanation.judgmentNotFromEvidence, true);

    for (const item of explanation.activatedHeuristics) {
      assert.ok(item.name);
      assert.ok(item.score > 0);
      assert.ok(Array.isArray(item.implications));
      assert.ok(Array.isArray(item.evidence));
    }

    const vendor = explanation.activatedHeuristics.find((h) => h.name === 'Vendor Instability');
    if (vendor) {
      assert.ok(vendor.evidence.some((e) => /cleanliness|review/i.test(e.observation)));
      assert.ok(vendor.implications.some((i) => /vendor|cleaning/i.test(i)));
    }
  });

  it('Scenario 5: won deal strengthens heuristics; lost deal weakens without deletion', () => {
    const library = cloneHeuristicLibrary(INITIAL_HEURISTICS);
    const vendor = library.find((h) => h.id === 'vendor_instability');
    const initialStrength = vendor.strength;

    const won = learnFromOutcome(library, {
      outcome: OUTCOME_KINDS.WON,
      contributingHeuristicIds: ['vendor_instability'],
    });

    assert.equal(won.updated.length, 1);
    assert.ok(won.library.find((h) => h.id === 'vendor_instability').strength > initialStrength);
    assert.ok(won.library.length === INITIAL_HEURISTICS.length);

    const afterWonStrength = won.library.find((h) => h.id === 'vendor_instability').strength;
    const lost = learnFromOutcome(won.library, {
      outcome: OUTCOME_KINDS.LOST,
      contributingHeuristicIds: ['vendor_instability'],
    });

    assert.ok(lost.library.find((h) => h.id === 'vendor_instability').strength < afterWonStrength);
    assert.ok(lost.library.find((h) => h.id === 'vendor_instability').strength >= 0.3);
    assert.ok(lost.library.some((h) => h.id === 'vendor_instability'));
  });

  it('Scenario 6: Mission Intelligence Report separates Business Judgment from Understanding', () => {
    const synthesisResult = synthesizeFromCandidates({
      candidates: [
        {
          id: 'abc-pm',
          name: 'ABC Property Management',
          signals: [
            { source: 'linkedin', label: 'Operations Manager hired' },
            { source: 'google_reviews', label: 'Recent negative cleanliness reviews' },
            { source: 'news', label: 'New apartment construction nearby' },
          ],
        },
      ],
    });

    let state = createInvestigationState({
      missionId: 'spec162-1',
      marketDefinition: buildSemanticMarketDefinition({
        market: 'Property Management',
        geography: 'Manchester NH',
      }),
    });
    state = applyBusinessUnderstandingSynthesis(state, synthesisResult);

    const judgment = activateHeuristics({
      businessUnderstandings: [abcMultiHeuristicUnderstanding(), ...(state.businessUnderstandings || [])],
    });
    state = applyBusinessJudgment(state, judgment);

    const report = buildMissionIntelligenceReport({
      state,
      synthesisResult,
      judgmentResult: judgment,
    });

    assert.equal(report.heuristicsSpec, 'SPEC-162');
    assert.ok(report.businessUnderstanding);
    assert.ok(report.businessJudgment);
    assert.notEqual(report.businessUnderstanding, report.businessJudgment);
    assert.equal(report.businessJudgment.separatedFromUnderstanding, true);
    assert.equal(report.businessJudgment.judgmentNotFromEvidence, true);
    assert.ok(Array.isArray(report.businessJudgment.activatedHeuristics));
    assert.ok(report.businessJudgment.overallJudgment.summary);
    assert.equal(report.recommendation.basedOnHeuristics, true);
    assert.equal(report.judgmentFromHeuristics, true);
  });

  it('integrates with investigative reasoning loop end-to-end', async () => {
    const marketDefinition = buildSemanticMarketDefinition({
      market: 'Property Management',
      geography: 'Manchester NH',
    });

    const result = await runInvestigativeReasoningLoop({
      mission: { id: 'spec162-loop' },
      marketDefinition,
      candidates: [
        {
          id: 'abc-pm',
          name: 'ABC Property Management',
          signals: [
            { source: 'linkedin', label: 'Operations Manager hired' },
            { source: 'google_reviews', label: 'Recent negative cleanliness reviews on Google' },
            { source: 'indeed', label: 'Hiring facilities staff' },
            { source: 'news', label: 'Apartment construction nearby' },
          ],
        },
      ],
      coverageMetrics: { investigated: 1, qualified: 1 },
    });

    assert.ok(result.state.businessJudgment);
    assert.ok(result.report.businessJudgment);
    assert.equal(result.report.recommendation.basedOnHeuristics, true);
    assert.equal(result.report.recommendation.notDirectFromEvidence, true);
  });

  it('detectHeuristicContradictions identifies opposing heuristic pairs', () => {
    const library = cloneHeuristicLibrary(INITIAL_HEURISTICS);
    const activated = [
      {
        heuristicId: 'vendor_instability',
        name: 'Vendor Instability',
        score: 0.77,
      },
      {
        heuristicId: 'vendor_stability',
        name: 'Vendor Stability',
        score: 0.81,
      },
    ];

    const contradictions = detectHeuristicContradictions(activated, library);
    assert.equal(contradictions.length, 1);
    assert.equal(contradictions[0].nameA, 'Vendor Instability');
    assert.equal(contradictions[0].nameB, 'Vendor Stability');
  });

  it('buildBusinessJudgmentReport formats operator-facing judgment section', () => {
    const judgment = activateHeuristics({
      businessUnderstandings: [abcMultiHeuristicUnderstanding()],
    });
    const section = buildBusinessJudgmentReport(judgment);

    assert.equal(section.spec, 'SPEC-162');
    assert.ok(section.overallJudgment.summary);
    assert.ok(section.activatedHeuristics.length >= 1);
    assert.equal(section.separatedFromUnderstanding, true);
  });
});
