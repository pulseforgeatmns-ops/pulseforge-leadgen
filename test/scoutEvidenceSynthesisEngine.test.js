'use strict';

/**
 * SPEC-160 — Evidence Synthesis Engine acceptance tests.
 * ADR-080 — Understanding emerges from evidence.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildEvidence,
  buildUnderstanding,
  computeUnderstandingConfidence,
} = require('../packages/scout/synthesis/types');
const {
  normalizeCandidateEvidence,
  resolveEntityGroups,
  synthesizeFromCandidates,
  explainUnderstanding,
  reviseUnderstandingWithContradiction,
  applyEvidenceToUnderstandings,
  buildBusinessUnderstandingReport,
} = require('../packages/scout/synthesis/EvidenceSynthesisEngine');
const {
  applyBusinessUnderstandingSynthesis,
  createInvestigationState,
} = require('../packages/scout/investigation/InvestigationState');
const { buildMissionIntelligenceReport } = require('../packages/scout/investigation/MissionIntelligenceReport');
const { runInvestigativeReasoningLoop } = require('../packages/scout/investigation/InvestigativeReasoningLoop');
const { buildSemanticMarketDefinition } = require('../packages/scout/intelligence/MarketDefinition');
const { estimateCandidateUniverse } = require('../packages/scout/universe/CandidateUniverseEstimate');

function abcCandidate(overrides = {}) {
  return {
    id: 'abc-pm',
    name: 'ABC Property Management',
    website: 'https://abcvacationrentals.com',
    websiteDescription: 'Vacation Rental Management',
    signals: [
      { source: 'google_places', type: 'category', label: 'Business Category: Property Management' },
      { source: 'website', type: 'service', label: 'Vacation Rental Management services advertised' },
      { source: 'facebook', type: 'hiring', label: 'Hiring cleaners for guest turnover' },
    ],
    ...overrides,
  };
}

describe('SPEC-160 — Evidence Synthesis Engine', () => {
  it('Evidence model is atomic with source, observation, timestamp, confidence, provenance', () => {
    const evidence = buildEvidence({
      source: 'google_places',
      observation: 'Business Category: Property Management',
      provenance: { entityName: 'ABC Property Management' },
    });

    assert.ok(evidence.id);
    assert.equal(evidence.source, 'google_places');
    assert.match(evidence.observation, /Property Management/);
    assert.ok(evidence.timestamp);
    assert.ok(evidence.confidence > 0);
    assert.ok(evidence.provenance);
  });

  it('Scenario 1: three independent observations synthesize into one understanding', () => {
    const result = synthesizeFromCandidates({ candidates: [abcCandidate()] });

    assert.equal(result.understandings.length, 1);
    const understanding = result.understandings[0];
    assert.equal(understanding.entity, 'ABC Property Management');
    assert.ok(understanding.assertions.some((a) => /short-term rental|STR/i.test(a)));
    assert.ok(understanding.supportingEvidence.length >= 3);
    assert.ok(understanding.confidence >= 0.55);
    assert.ok(understanding.reasoning);
  });

  it('Scenario 2: contradictory evidence revises understanding and retains contradiction', () => {
    const prior = buildUnderstanding({
      entity: 'ABC Property Management',
      assertions: ['Commercial only'],
      supportingEvidence: [
        buildEvidence({ source: 'website', observation: 'Commercial cleaning only' }),
      ],
      confidence: 0.73,
    });

    const contradictory = buildEvidence({
      source: 'facebook',
      observation: 'Residential cleaning advertised for apartment turnovers',
    });

    const { understandings, revised } = applyEvidenceToUnderstandings([prior], [contradictory]);
    assert.equal(revised, true);

    const updated = understandings[0];
    assert.ok(updated.confidence < prior.confidence);
    assert.ok(updated.contradictoryEvidence.length >= 1);
    assert.ok(updated.assertions.some((a) => /Mixed commercial and residential/i.test(a)));
    assert.ok(updated.revisionHistory.length >= 1);
  });

  it('Scenario 3: operator traceability — why do you believe this?', () => {
    const result = synthesizeFromCandidates({ candidates: [abcCandidate()] });
    const explanation = explainUnderstanding(result.understandings[0]);

    assert.equal(explanation.entity, 'ABC Property Management');
    assert.ok(explanation.reasoning);
    assert.ok(explanation.supportingEvidence.length >= 3);
    assert.ok(explanation.supportingEvidence.every((e) => e.source && e.observation));
    assert.ok(explanation.confidence > 0);
  });

  it('Scenario 4: three business names merge into one entity understanding', () => {
    const candidates = [
      { id: 'a1', name: 'ABC Management', signals: [{ source: 'google_places', label: 'Property Management' }] },
      {
        id: 'a2',
        name: 'ABC Property Management',
        website: 'https://abc.com',
        signals: [{ source: 'website', label: 'Vacation Rental Management' }],
      },
      {
        id: 'a3',
        name: 'ABC Vacation Rentals LLC',
        signals: [{ source: 'facebook', label: 'Hiring cleaners' }],
      },
    ];

    const { groups, merges } = resolveEntityGroups(candidates);
    assert.equal(groups.length, 1);
    assert.ok(merges.length >= 2);

    const result = synthesizeFromCandidates({ candidates });
    assert.equal(result.summary.entityCount, 1);
    assert.ok(result.understandings.length >= 1);
    assert.equal(result.understandings[0].entity, 'ABC Vacation Rentals LLC');
  });

  it('Scenario 5: Mission Intelligence Report shows synthesized business understanding', () => {
    const market = buildSemanticMarketDefinition({
      mission: { objectiveText: 'Find STR operators', constraints: { locationHint: 'Manchester' } },
      geography: 'Manchester',
    });
    const synthesisResult = synthesizeFromCandidates({ candidates: [abcCandidate()] });
    const state = applyBusinessUnderstandingSynthesis(
      createInvestigationState({ mission: { id: 'm1' }, marketDefinition: market }),
      synthesisResult
    );

    const report = buildMissionIntelligenceReport({
      state,
      synthesisResult,
      candidates: [abcCandidate()],
    });

    assert.ok(report.businessUnderstanding);
    assert.equal(report.synthesizedNotRaw, true);
    assert.ok(Array.isArray(report.businessUnderstanding.businessUnderstanding));
    assert.ok(report.businessUnderstanding.confidence > 0);
    assert.ok(report.businessUnderstanding.buyingSignals >= 0);
    assert.equal(report.recommendation.basedOnUnderstanding, true);
    assert.equal(report.recommendation.notDirectFromEvidence, true);
    if (report.recommendation.basedOnHeuristics) {
      assert.equal(report.recommendation.adr, 'ADR-082');
      assert.ok(report.businessJudgment);
    } else {
      assert.equal(report.recommendation.adr, 'ADR-080');
      assert.match(report.recommendation.summary, /ABC Property Management/);
    }
  });

  it('confidence evolves with additional sources on understanding, not isolated facts', () => {
    const googleOnly = [
      buildEvidence({ source: 'google_places', observation: 'Business Category: Property Management' }),
    ];
    const withWebsite = [
      ...googleOnly,
      buildEvidence({ source: 'website', observation: 'Vacation Rental Management' }),
    ];
    const withFacebook = [
      ...withWebsite,
      buildEvidence({ source: 'facebook', observation: 'Hiring cleaners' }),
    ];
    const withMemory = [
      ...withFacebook,
      buildEvidence({ source: 'existing_pf', observation: 'Previous mission identified STR portfolio growth' }),
    ];

    const c1 = computeUnderstandingConfidence(googleOnly, []);
    const c2 = computeUnderstandingConfidence(withWebsite, []);
    const c3 = computeUnderstandingConfidence(withFacebook, []);
    const c4 = computeUnderstandingConfidence(withMemory, []);

    assert.ok(c1 < c2);
    assert.ok(c2 < c3);
    assert.ok(c3 < c4);
    assert.ok(c1 <= 0.45);
  });

  it('integrates with investigative reasoning loop end-to-end', async () => {
    const market = buildSemanticMarketDefinition({
      mission: { id: 'm-e2e', objectiveText: 'Find STR operators' },
      geography: 'Manchester',
    });

    const result = await runInvestigativeReasoningLoop({
      mission: { id: 'm-e2e', clientId: 1 },
      marketDefinition: market,
      universeEstimate: estimateCandidateUniverse({ marketDefinition: market }),
      coverageResult: {
        candidates: [abcCandidate()],
        coverage: { complete: true },
        searchHypotheses: [],
      },
    });

    assert.ok(result.state.businessUnderstandings.length >= 1);
    assert.ok(result.report.businessUnderstanding);
    assert.equal(result.report.synthesizedNotRaw, true);
  });
});
