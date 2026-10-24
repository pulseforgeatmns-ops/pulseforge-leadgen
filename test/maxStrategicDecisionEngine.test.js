'use strict';

/**
 * SPEC-165 — Strategic Decision Engine acceptance tests.
 * ADR-085 — Allocate finite effort toward the best business outcome.
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
const { evaluateOpportunities, buildOpportunityIntelligenceReport } = require('../packages/scout/opportunity');
const {
  evaluateTradeoff,
  allocateResources,
  buildStrategicDecision,
  presentStrategicDecision,
  ALLOCATION_KINDS,
} = require('../packages/max/decision');
const {
  ensureOpportunityReasoning,
  ensureStrategicDecision,
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

function twelveBusinesses() {
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
    'Northside Holdings',
    'Cedar Ridge Properties',
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

function understandingsForTwelve() {
  return [
    abcUnderstanding(),
    xyzUnderstanding(),
    ...twelveBusinesses()
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
}

function evaluateTwelve() {
  const understandings = understandingsForTwelve();
  return evaluateOpportunities({
    mission: { id: 'spec165', objectives: ['property management beachhead'] },
    businessUnderstandings: understandings,
    judgmentResult: activateHeuristics({ businessUnderstandings: understandings }),
    candidates: twelveBusinesses(),
  });
}

function evaluateAbcVsXyz() {
  const understandings = [abcUnderstanding(), xyzUnderstanding()];
  return evaluateOpportunities({
    mission: { id: 'spec165-abc', objectives: ['property management beachhead'] },
    businessUnderstandings: understandings,
    judgmentResult: activateHeuristics({ businessUnderstandings: understandings }),
    candidates: twelveBusinesses().slice(0, 2),
  });
}

function mixedDayInput(opportunities, extra = {}) {
  return {
    opportunities,
    mission: { id: 'spec165', objectives: ['property management beachhead'] },
    constraints: { availableHours: 4, availableAOs: 1 },
    competingWork: ['direct_mail'],
    pendingProposals: [{ name: 'Harbor Law Group proposal' }],
    scoutDiscoveries: true,
    remainingUnknowns: ['Decision-maker coverage for remaining queue'],
    ...extra,
  };
}

describe('SPEC-165 — Strategic Decision Engine', () => {
  it('Scenario 1: if we pursue ABC today — tradeoff card with ARR and confidence', () => {
    const opportunities = evaluateAbcVsXyz();
    assert.equal(opportunities[0].entity.name, 'ABC Property Management');
    assert.equal(opportunities[1].entity.name, 'XYZ Property Services');

    const abc = {
      ...opportunities[0],
      confidence: 0.81,
      estimatedARR: 2800,
    };

    const tradeoff = evaluateTradeoff({
      opportunity: abc,
      alternatives: opportunities.slice(1),
      competingWork: ['direct_mail'],
    });

    assert.ok(tradeoff.pros.includes('Highest recurring value'));
    assert.ok(tradeoff.pros.includes('Strong buying signals'));
    assert.ok(tradeoff.cons.some((c) => /Consumes 4 hours/i.test(c)));
    assert.ok(tradeoff.cons.some((c) => /Delays XYZ Property Services/i.test(c)));
    assert.ok(tradeoff.cons.some((c) => /Delays [Dd]irect mail/i.test(c)));
    assert.equal(tradeoff.expectedOutcome.label, '+$2,800 ARR');
    assert.equal(tradeoff.confidencePercent, 81);

    const decision = buildStrategicDecision(
      mixedDayInput(
        [abc, ...opportunities.slice(1), ...evaluateTwelve().slice(2)],
        { competingWork: ['direct_mail'] }
      )
    );
    const presented = presentStrategicDecision(decision);
    assert.match(presented.ifPursued.heading, /If we pursue ABC Property Management today/);
    assert.equal(presented.ifPursued.expectedOutcome, '+$2,800 ARR');
    assert.equal(presented.ifPursued.confidence, '81%');
    assert.equal(presented.notActivityBased, true);
  });

  it('Scenario 2: 1 AO / 4 hours / 12 opportunities — optimal mixed allocation', () => {
    const opportunities = evaluateTwelve();
    assert.equal(opportunities.length, 12);

    const decision = buildStrategicDecision(mixedDayInput(opportunities));
    assert.match(
      decision.capacityStatement,
      /You have 1 AO, 4 available hours, 12 opportunities\. Here's the optimal allocation/
    );

    const blocks = decision.allocation.blocks;
    const byActivity = Object.fromEntries(blocks.map((b) => [b.activity, b]));
    assert.equal(byActivity.phone?.hours, 2);
    assert.equal(byActivity.phone?.duration, '2 hours');
    assert.equal(byActivity.door_knocking?.hours, 1);
    assert.equal(byActivity.door_knocking?.duration, '1 hour');
    assert.equal(byActivity.proposal_follow_up?.hours, 0.5);
    assert.equal(byActivity.proposal_follow_up?.duration, '30 minutes');
    assert.equal(byActivity.scout_review?.hours, 0.5);
    assert.equal(byActivity.scout_review?.duration, '30 minutes');

    const totalHours = blocks.reduce((sum, b) => sum + b.hours, 0);
    assert.equal(totalHours, 4);
    assert.equal(decision.allocation.kind, ALLOCATION_KINDS.MIXED);

    const presented = presentStrategicDecision(decision);
    assert.equal(presented.today.heading, "Today's recommendation");
    assert.deepEqual(
      presented.today.blocks.map((b) => `${b.duration} ${b.activity}`),
      ['2 hours Phone', '1 hour Door knocking', '30 minutes Proposal follow-up', '30 minutes Review Scout discoveries']
    );
    assert.ok(decision.deferred.some((d) => /direct mail/i.test(d)));
  });

  it('Scenario 3: activities appear only because they maximize the mission objective', () => {
    const opportunities = evaluateTwelve();
    const decision = buildStrategicDecision(mixedDayInput(opportunities));

    assert.equal(decision.maximizesMissionObjective, true);
    assert.equal(decision.notActivityBased, true);
    assert.match(decision.explanation, /property management beachhead/i);
    assert.doesNotMatch(decision.explanation, /inherently good|phone is a good activity|door knocking is valuable in itself/i);

    for (const block of decision.allocation.blocks) {
      assert.equal(block.notInherentlyGood, true);
      assert.equal(block.maximizesMissionObjective, true);
      assert.ok(block.reason);
      assert.doesNotMatch(block.reason, /inherently good/i);
    }
  });

  it('Scenario 4: Mission Intelligence Report includes today\'s allocation and tradeoffs', () => {
    const candidates = twelveBusinesses().slice(0, 12);
    const synthesisResult = synthesizeFromCandidates({ candidates });
    let state = createInvestigationState({
      missionId: 'spec165-report',
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
      mission: { id: 'spec165-report', objectives: ['beachhead'] },
      synthesisResult,
      judgmentResult: judgment,
      candidates,
      constraints: { availableHours: 4, availableAOs: 1 },
      competingWork: ['direct_mail'],
      pendingProposals: 1,
      scoutDiscoveries: true,
    });

    assert.equal(report.decisionSpec, 'SPEC-165');
    assert.equal(report.decisionAdr, 'ADR-085');
    assert.ok(report.strategicDecision);
    assert.ok(report.strategicDecision.allocation.blocks.length >= 2);
    assert.ok(report.strategicDecision.tradeoff.pros.length > 0);
    assert.ok(report.strategicDecision.tradeoff.cons.length > 0);
    assert.ok(report.recommendation.basedOnStrategicDecision);
    assert.ok(report.recommendation.expectedBusinessOutcome);
    assert.ok(report.recommendation.strategicAllocation);
    assert.equal(report.basedOnStrategicDecision, true);
  });

  it('Scenario 5: constraint change recalculates the allocation', () => {
    const opportunities = evaluateTwelve();
    const fourHour = buildStrategicDecision(mixedDayInput(opportunities));
    const twoHour = buildStrategicDecision(
      mixedDayInput(opportunities, { constraints: { availableHours: 2, availableAOs: 1 } })
    );

    const fourTotal = fourHour.allocation.blocks.reduce((sum, b) => sum + b.hours, 0);
    const twoTotal = twoHour.allocation.blocks.reduce((sum, b) => sum + b.hours, 0);
    assert.equal(fourTotal, 4);
    assert.equal(twoTotal, 2);
    assert.notDeepEqual(
      fourHour.allocation.blocks.map((b) => `${b.activity}:${b.hours}`),
      twoHour.allocation.blocks.map((b) => `${b.activity}:${b.hours}`)
    );

    const twoAos = buildStrategicDecision(
      mixedDayInput(opportunities, { constraints: { availableHours: 4, availableAOs: 2 } })
    );
    assert.equal(twoAos.constraints.totalHours, 8);
    assert.equal(
      twoAos.allocation.blocks.reduce((sum, b) => sum + b.hours, 0),
      8
    );
  });

  it('Scenario 6: concentrated vs mixed — engine picks higher expected mission outcome', () => {
    const opportunities = evaluateTwelve();
    const mixed = allocateResources(mixedDayInput(opportunities));
    assert.equal(mixed.selected.kind, ALLOCATION_KINDS.MIXED);
    assert.ok(mixed.comparison.mixedNet >= mixed.comparison.concentratedNet);
    assert.match(mixed.comparison.summary, /mixed day|opportunity cost|concentrat/i);

    const single = evaluateOpportunities({
      mission: { id: 'spec165-single', objectives: ['close ABC'] },
      businessUnderstandings: [abcUnderstanding()],
      judgmentResult: activateHeuristics({ businessUnderstandings: [abcUnderstanding()] }),
      candidates: twelveBusinesses().slice(0, 1),
    });
    const concentrated = allocateResources({
      opportunities: single,
      constraints: { availableHours: 4, availableAOs: 1 },
      competingWork: [],
      pendingProposals: 0,
      scoutDiscoveries: false,
      remainingUnknowns: [],
    });
    assert.equal(concentrated.selected.kind, ALLOCATION_KINDS.CONCENTRATED);
    assert.match(concentrated.selected.blocks[0].label, /ABC Property Management/);
    assert.equal(concentrated.selected.blocks[0].hours, 4);
  });

  it('Scenario 7: why this mix cites mission objective, not channel preference', () => {
    const opportunities = evaluateTwelve();
    const result = allocateResources(mixedDayInput(opportunities));
    const decision = buildStrategicDecision(mixedDayInput(opportunities));
    assert.match(decision.explanation, /maximizes property management beachhead/i);
    assert.ok(result.selected.blocks.every((b) => /mission|pipeline|opportunit|reach|uncertainty|convert/i.test(b.reason)));
    assert.ok(result.selected.blocks.every((b) => !/because phone is good|always door knock/i.test(b.reason)));
  });

  it('Max recommendation invariant: every recommendation includes strategic decision', () => {
    const opportunities = evaluateTwelve();
    const report = {
      opportunityIntelligence: buildOpportunityIntelligenceReport({
        businessUnderstandings: understandingsForTwelve(),
        judgmentResult: activateHeuristics({ businessUnderstandings: understandingsForTwelve() }),
        candidates: twelveBusinesses(),
      }),
      topOpportunities: opportunities,
    };

    const bare = { kind: 'generic', summary: 'Call the top prospect.' };
    const enriched = ensureOpportunityReasoning(bare, report, mixedDayInput(opportunities));

    assert.ok(enriched.opportunityReasoning?.length > 0);
    assert.equal(enriched.basedOnOpportunityIntelligence, true);
    assert.equal(enriched.basedOnStrategicDecision, true);
    assert.ok(enriched.strategicAllocation?.blocks?.length > 0);
    assert.ok(enriched.tradeoffs?.pros?.length > 0);
    assert.ok(enriched.tradeoffs?.cons?.length > 0);
    assert.ok(enriched.expectedBusinessOutcome);
    assert.equal(enriched.notActivityBased, true);

    const alreadyEnriched = ensureStrategicDecision(enriched, report, mixedDayInput(opportunities));
    assert.equal(alreadyEnriched.basedOnStrategicDecision, true);
  });

  it('empty opportunity set does not crash — Max states the gap', () => {
    const decision = buildStrategicDecision({
      opportunities: [],
      constraints: { availableHours: 4, availableAOs: 1 },
    });
    assert.equal(decision.spec, 'SPEC-165');
    assert.equal(decision.opportunityCount, 0);
    assert.match(decision.capacityStatement, /0 opportunities/);
    assert.equal(decision.tradeoff, null);
  });

  it('integrates with investigative reasoning loop end-to-end', async () => {
    const result = await runInvestigativeReasoningLoop({
      mission: { id: 'spec165-loop', objectives: ['property management'] },
      marketDefinition: buildSemanticMarketDefinition({
        market: 'Property Management',
        geography: 'Manchester NH',
      }),
      candidates: twelveBusinesses().slice(0, 4).map((c, index) => ({
        ...c,
        signals:
          index === 0
            ? [
                { source: 'linkedin', label: 'Operations Manager hired' },
                { source: 'google_reviews', label: 'Recent negative cleanliness reviews on Google' },
                { source: 'indeed', label: 'Hiring facilities staff' },
                { source: 'news', label: '15% increase in managed properties' },
              ]
            : c.signals,
      })),
      coverageMetrics: { investigated: 4, qualified: 4 },
      competingWork: ['direct_mail'],
      pendingProposals: 1,
    });

    assert.equal(result.report.decisionSpec, 'SPEC-165');
    assert.ok(result.report.strategicDecision);
    assert.ok(result.report.recommendation.basedOnStrategicDecision);
    assert.ok(result.report.recommendation.opportunityReasoning?.length > 0);
  });
});
