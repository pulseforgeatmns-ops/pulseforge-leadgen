'use strict';

/**
 * SPEC-164 — Opportunity Intelligence Engine acceptance tests.
 * ADR-084 — Businesses grow by pursuing opportunities.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { buildEvidence, buildUnderstanding } = require('../packages/scout/synthesis/types');
const { buildSemanticMarketDefinition } = require('../packages/scout/intelligence/MarketDefinition');
const {
  createInvestigationState,
  applyBusinessJudgment,
  applyBusinessUnderstandingSynthesis,
} = require('../packages/scout/investigation/InvestigationState');
const { activateHeuristics } = require('../packages/scout/heuristics/BusinessHeuristicsEngine');
const { synthesizeFromCandidates } = require('../packages/scout/synthesis/EvidenceSynthesisEngine');
const { runInvestigativeReasoningLoop } = require('../packages/scout/investigation/InvestigativeReasoningLoop');
const { buildMissionIntelligenceReport } = require('../packages/scout/investigation/MissionIntelligenceReport');
const {
  evaluateOpportunities,
  rankOpportunities,
  compareOpportunities,
  explainWhyFirst,
  explainOvernightChanges,
  recalculateForMissionObjectives,
  buildOpportunityIntelligenceReport,
  OPPORTUNITY_CATEGORIES,
} = require('../packages/scout/opportunity');
const {
  explainWhyActToday,
  explainRankingComparison,
  explainWhatChanged,
  ensureOpportunityReasoning,
} = require('../packages/max/opportunity');

function abcUnderstanding() {
  return buildUnderstanding({
    entity: 'ABC Property Management',
    entityId: 'abc-pm',
    kind: 'service_need',
    assertions: [
      'Rapid portfolio growth',
      'Vendor instability signals present',
      'Ideal beachhead candidate',
    ],
    supportingEvidence: [
      buildEvidence({ source: 'google_reviews', observation: 'Recent negative cleanliness reviews on Google' }),
      buildEvidence({ source: 'linkedin', observation: 'New Operations Manager hired last month' }),
      buildEvidence({ source: 'indeed', observation: 'Hiring facilities staff for property maintenance' }),
      buildEvidence({ source: 'news', observation: '15% increase in managed properties this quarter' }),
    ],
    confidence: 0.82,
  });
}

function xyzUnderstanding() {
  return buildUnderstanding({
    entity: 'XYZ Property Services',
    entityId: 'xyz-ps',
    kind: 'growth',
    assertions: ['Good company with stable operations'],
    supportingEvidence: [
      buildEvidence({ source: 'website', observation: 'Same cleaning company preferred vendor for 8 years' }),
      buildEvidence({ source: 'review', observation: 'Satisfied with long-standing vendor relationship' }),
    ],
    confidence: 0.71,
  });
}

function buildTenQualifiedBusinesses() {
  const names = [
    'ABC Property Management',
    'XYZ Property Services',
    'Harbor Law Group',
    'Summit Accounting',
    'Riverside PM Co',
    'Peak Facilities',
    'Metro Office Partners',
    'Granite Property Co',
    'Oak Street Management',
    'Valley Commercial PM',
  ];
  return names.map((name, index) => ({
    id: `entity-${index}`,
    name,
    icpScore: 72 + (index % 5),
    people: index === 0 ? [{ name: 'Jane Ops', jobTitle: 'Operations Manager', decisionMaker: true }] : [],
    email: index < 3 ? `${name.split(' ')[0].toLowerCase()}@example.com` : null,
    signals:
      index === 0
        ? [
            { source: 'linkedin', label: 'Operations Manager hired' },
            { source: 'google_reviews', label: 'Recent negative cleanliness reviews' },
          ]
        : index === 1
          ? [{ source: 'website', label: 'Same cleaning company preferred vendor for 8 years' }]
          : [{ source: 'news', label: 'Steady operations' }],
  }));
}

function judgmentForTen() {
  const understandings = [
    abcUnderstanding(),
    xyzUnderstanding(),
    ...buildTenQualifiedBusinesses()
      .slice(2)
      .map((c) =>
        buildUnderstanding({
          entity: c.name,
          entityId: c.id,
          kind: 'growth',
          assertions: ['Qualified business profile'],
          supportingEvidence: [buildEvidence({ source: 'repository', observation: `${c.name} meets ICP criteria` })],
          confidence: 0.65,
        })
      ),
  ];
  return activateHeuristics({ businessUnderstandings: understandings });
}

describe('SPEC-164 — Opportunity Intelligence Engine', () => {
  it('Scenario 1: ten qualified businesses ranked by multidimensional reasoning', () => {
    const candidates = buildTenQualifiedBusinesses();
    const judgment = judgmentForTen();
    const opportunities = evaluateOpportunities({
      mission: { id: 'spec164-1', objectives: ['property management beachhead'] },
      businessUnderstandings: judgment.perEntity?.flatMap((e) => e.understandings || []) || [],
      judgmentResult: judgment,
      candidates,
    });

    assert.equal(opportunities.length, 10);
    assert.ok(opportunities[0].priority === 1);
    assert.ok(opportunities.every((o) => o.priority != null));
    assert.ok(opportunities.every((o) => Array.isArray(o.opportunityReasoning) && o.opportunityReasoning.length > 0));
    assert.ok(opportunities.every((o) => o.expectedBusinessValue?.level));
    assert.ok(opportunities.every((o) => o.timing?.level));
    assert.ok(opportunities.every((o) => !('leadScore' in o) && !('icpScore' in o)));
    assert.equal(opportunities[0].entity.name, 'ABC Property Management');
  });

  it('Scenario 2: operator asks why this one first — explains all dimensions', () => {
    const judgment = judgmentForTen();
    const opportunities = evaluateOpportunities({
      mission: { id: 'spec164-2' },
      businessUnderstandings: [abcUnderstanding(), xyzUnderstanding()],
      judgmentResult: judgment,
      candidates: buildTenQualifiedBusinesses().slice(0, 2),
    });

    const explanation = explainWhyFirst(opportunities[0], opportunities.slice(1));
    assert.equal(explanation.spec, 'SPEC-164');
    assert.equal(explanation.notScoreBased, true);
    assert.ok(explanation.businessValue);
    assert.ok(explanation.timing);
    assert.ok(explanation.strategicFit);
    assert.ok(explanation.probability);
    assert.ok(explanation.learningValue);
    assert.ok(explanation.opportunityReasoning.length > 0);
    assert.ok(explanation.recommendedAction);
    assert.ok(explanation.comparison);

    const maxExplanation = explainWhyActToday({
      missionReport: { topOpportunities: opportunities },
      entity: 'ABC Property Management',
    });
    assert.match(maxExplanation.summary, /ABC Property Management/i);
    assert.ok(maxExplanation.opportunityReasoning.length > 0);
  });

  it('Scenario 3: business expansion increases opportunity priority with recorded reason', () => {
    const before = evaluateOpportunities({
      mission: { id: 'spec164-3' },
      businessUnderstandings: [
        buildUnderstanding({
          entity: 'ABC Property Management',
          entityId: 'abc-pm',
          kind: 'service_need',
          assertions: ['Stable portfolio'],
          supportingEvidence: [
            buildEvidence({ source: 'website', observation: 'Manages 200 units' }),
          ],
          confidence: 0.6,
        }),
      ],
      judgmentResult: activateHeuristics({
        businessUnderstandings: [
          buildUnderstanding({
            entity: 'ABC Property Management',
            entityId: 'abc-pm',
            kind: 'service_need',
            assertions: ['Stable portfolio'],
            supportingEvidence: [
              buildEvidence({ source: 'website', observation: 'Manages 200 units' }),
            ],
            confidence: 0.6,
          }),
        ],
      }),
    });

    const after = evaluateOpportunities({
      mission: { id: 'spec164-3' },
      businessUnderstandings: [abcUnderstanding()],
      judgmentResult: activateHeuristics({ businessUnderstandings: [abcUnderstanding()] }),
    });

    assert.ok(after[0].category === OPPORTUNITY_CATEGORIES.IMMEDIATE || after[0].timing.level === 'high');
    assert.ok(after[0].opportunityReasoning.some((r) => /expansion|growth|portfolio|vendor|operations/i.test(r)));

    const movement = explainOvernightChanges(
      { topOpportunities: before },
      { topOpportunities: after }
    );
    assert.ok(movement.movements.length > 0);
    assert.match(movement.summary, /ABC Property Management/i);
    assert.equal(movement.explainsMovementNotJustEvidence, true);
  });

  it('Scenario 4: mission objective changes recalculate opportunity rankings', () => {
    const strUnderstanding = buildUnderstanding({
      entity: 'Coastal STR Management',
      entityId: 'coastal-str',
      kind: 'buying_signal',
      assertions: ['First STR customer opportunity in market'],
      supportingEvidence: [
        buildEvidence({ source: 'news', observation: 'First STR property management pilot launching locally' }),
      ],
      confidence: 0.68,
    });

    const judgment = activateHeuristics({
      businessUnderstandings: [abcUnderstanding(), xyzUnderstanding(), strUnderstanding],
    });
    const baseInput = {
      businessUnderstandings: [abcUnderstanding(), xyzUnderstanding(), strUnderstanding],
      judgmentResult: judgment,
      candidates: buildTenQualifiedBusinesses().slice(0, 3),
    };

    const standard = evaluateOpportunities({
      ...baseInput,
      mission: { id: 'm1', objectives: ['general growth'] },
    });

    const learningMission = evaluateOpportunities({
      ...baseInput,
      mission: {
        id: 'm2',
        firstInVertical: true,
        learningPriority: 'high',
        objectives: ['first STR customer'],
      },
    });

    const recalculated = recalculateForMissionObjectives(standard, {
      firstInVertical: true,
      learningPriority: 'high',
      objectives: ['first STR customer'],
    });

    const standardStr = standard.find((o) => o.entity.name === 'Coastal STR Management');
    const learningStr = learningMission.find((o) => o.entity.name === 'Coastal STR Management');
    assert.ok(standardStr);
    assert.ok(learningStr);
    assert.equal(standardStr.expectedLearningValue.level, 'medium');
    assert.equal(learningStr.expectedLearningValue.level, 'high');
    assert.deepEqual(
      recalculated.map((o) => ({ name: o.entity.name, priority: o.priority })),
      learningMission.map((o) => ({ name: o.entity.name, priority: o.priority }))
    );
    assert.notDeepEqual(
      standard.map((o) => ({
        name: o.entity.name,
        learning: o.expectedLearningValue.level,
      })),
      learningMission.map((o) => ({
        name: o.entity.name,
        learning: o.expectedLearningValue.level,
      }))
    );
  });

  it('Scenario 5: Mission Intelligence Report displays top opportunities with reasoning', () => {
    const synthesisResult = synthesizeFromCandidates({
      candidates: buildTenQualifiedBusinesses().slice(0, 3),
    });

    let state = createInvestigationState({
      missionId: 'spec164-5',
      marketDefinition: buildSemanticMarketDefinition({
        market: 'Property Management',
        geography: 'Manchester NH',
      }),
    });
    state = applyBusinessUnderstandingSynthesis(state, synthesisResult);
    state = applyBusinessUnderstandingSynthesis(state, {
      understandings: [abcUnderstanding()],
      summary: { count: 1 },
    });

    const judgment = activateHeuristics({
      businessUnderstandings: [abcUnderstanding(), ...(state.businessUnderstandings || [])],
    });
    state = applyBusinessJudgment(state, judgment);

    const report = buildMissionIntelligenceReport({
      state,
      mission: { id: 'spec164-5', objectives: ['beachhead'] },
      synthesisResult,
      judgmentResult: judgment,
      candidates: buildTenQualifiedBusinesses().slice(0, 3),
    });

    assert.equal(report.opportunitySpec, 'SPEC-164');
    assert.equal(report.opportunityAdr, 'ADR-084');
    assert.ok(Array.isArray(report.topOpportunities));
    assert.ok(report.topOpportunities.length > 0);
    assert.ok(report.opportunityIntelligence);
    assert.ok(report.recommendation.opportunityReasoning?.length > 0);
    assert.ok(report.recommendation.recommendedAction);
    assert.ok(report.recommendation.expectedOutcome);
    assert.ok(report.recommendation.basedOnOpportunityIntelligence);
    assert.ok(report.recommendation.confidence != null);

    const top = report.topOpportunities[0];
    assert.ok(top.expectedBusinessValue);
    assert.ok(top.timing);
    assert.ok(top.recommendedAction);
    assert.ok(top.expectedOutcome);
    assert.ok(top.opportunityReasoning.length > 0);
  });

  it('Scenario 6: what changed overnight explains opportunity movement', () => {
    const prior = evaluateOpportunities({
      businessUnderstandings: [
        buildUnderstanding({
          entity: 'ABC Property Management',
          entityId: 'abc-pm',
          kind: 'service_need',
          assertions: ['Developing relationship'],
          supportingEvidence: [
            buildEvidence({ source: 'linkedin', observation: 'Operations team stable' }),
          ],
          confidence: 0.55,
        }),
      ],
      judgmentResult: activateHeuristics({
        businessUnderstandings: [
          buildUnderstanding({
            entity: 'ABC Property Management',
            entityId: 'abc-pm',
            kind: 'service_need',
            assertions: ['Developing relationship'],
            supportingEvidence: [
              buildEvidence({ source: 'linkedin', observation: 'Operations team stable' }),
            ],
            confidence: 0.55,
          }),
        ],
      }),
    });

    const current = evaluateOpportunities({
      businessUnderstandings: [abcUnderstanding()],
      judgmentResult: activateHeuristics({ businessUnderstandings: [abcUnderstanding()] }),
    });

    const changes = explainWhatChanged({
      priorReport: { topOpportunities: prior },
      currentReport: { topOpportunities: current },
    });

    assert.ok(changes.movements.length > 0);
    assert.match(changes.summary, /ABC Property Management/i);
    assert.match(changes.summary, /immediate|developing|monitor/i);
    assert.ok(
      changes.movements[0].reasons.some((r) =>
        /operations manager|cleanliness|portfolio|vendor|facilities/i.test(r)
      )
    );
  });

  it('comparative reasoning: ABC ranked above XYZ', () => {
    const judgment = judgmentForTen();
    const opportunities = evaluateOpportunities({
      businessUnderstandings: [abcUnderstanding(), xyzUnderstanding()],
      judgmentResult: judgment,
      candidates: buildTenQualifiedBusinesses().slice(0, 2),
    });

    const comparison = compareOpportunities(opportunities[0], opportunities[1]);
    assert.equal(comparison.notScoreBased, true);
    assert.match(comparison.summary, /ABC Property Management/i);
    assert.match(comparison.summary, /XYZ Property Services/i);
    assert.ok(comparison.higher.advantages.length > 0);

    const maxComparison = explainRankingComparison({
      missionReport: { topOpportunities: opportunities },
      entityA: 'ABC Property Management',
      entityB: 'XYZ Property Services',
    });
    assert.match(maxComparison.summary, /ABC Property Management/i);
  });

  it('Max recommendation invariant: every recommendation includes opportunity reasoning', () => {
    const opportunities = evaluateOpportunities({
      businessUnderstandings: [abcUnderstanding()],
      judgmentResult: activateHeuristics({ businessUnderstandings: [abcUnderstanding()] }),
    });

    const bare = { kind: 'generic', summary: 'Contact top prospect.' };
    const enriched = ensureOpportunityReasoning(bare, {
      opportunityIntelligence: buildOpportunityIntelligenceReport({
        businessUnderstandings: [abcUnderstanding()],
        judgmentResult: activateHeuristics({ businessUnderstandings: [abcUnderstanding()] }),
      }),
    });

    assert.ok(enriched.opportunityReasoning?.length > 0);
    assert.equal(enriched.basedOnOpportunityIntelligence, true);
    assert.ok(enriched.recommendedAction);
  });

  it('integrates with investigative reasoning loop end-to-end', async () => {
    const result = await runInvestigativeReasoningLoop({
      mission: { id: 'spec164-loop', objectives: ['property management'] },
      marketDefinition: buildSemanticMarketDefinition({
        market: 'Property Management',
        geography: 'Manchester NH',
      }),
      candidates: [
        {
          id: 'abc-pm',
          name: 'ABC Property Management',
          people: [{ name: 'Jane Ops', jobTitle: 'Operations Manager', decisionMaker: true }],
          signals: [
            { source: 'linkedin', label: 'Operations Manager hired' },
            { source: 'google_reviews', label: 'Recent negative cleanliness reviews on Google' },
            { source: 'indeed', label: 'Hiring facilities staff' },
            { source: 'news', label: '15% increase in managed properties' },
          ],
        },
        {
          id: 'xyz-ps',
          name: 'XYZ Property Services',
          signals: [{ source: 'website', label: 'Same cleaning company preferred vendor for 8 years' }],
        },
      ],
      coverageMetrics: { investigated: 2, qualified: 2 },
    });

    assert.equal(result.report.opportunitySpec, 'SPEC-164');
    assert.ok(result.report.topOpportunities?.length >= 1);
    assert.ok(result.report.recommendation.opportunityReasoning?.length > 0);
    assert.equal(result.report.opportunityRankedNotScored, true);
  });
});
