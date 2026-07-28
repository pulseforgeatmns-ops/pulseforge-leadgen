'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  routeIntent,
  createMissionEngine,
  MISSION_TYPES,
  ROUTE_KINDS,
  createExecutionGraph,
  validateGraph,
  explainPlan,
  insertStage,
  removeStage,
  replaceStage,
  PLANNER_VERSION,
} = require('..');
const { BUILTIN_IDS: CAP_IDS, createBuiltinRegistry } = require('../../capabilities');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({
      discovery: { useFixture: true },
    }),
  });
}

describe('SPEC-041 IntentRouter — stage keywords do not collapse build objectives', () => {
  it('Build Campaign + Review + Ready to Print → campaign_creation (not campaign_review)', () => {
    const objective = [
      'Build Campaign 001 for Anchor Cleaning.',
      'Generate intelligence.',
      'Review.',
      'Ready to Print.',
    ].join(' ');
    const d = routeIntent(objective);
    assert.equal(d.kind, ROUTE_KINDS.MISSION);
    assert.equal(d.missionType, MISSION_TYPES.CAMPAIGN_CREATION);
  });

  it('focused Review Campaign 001 still routes to campaign_review', () => {
    const d = routeIntent('Review Campaign 001');
    assert.equal(d.missionType, MISSION_TYPES.CAMPAIGN_REVIEW);
  });
});

describe('SPEC-041 Mission Planner — objective-driven execution graph', () => {
  it('composes full pipeline from multi-outcome Build Campaign objective', () => {
    const engine = testEngine();
    const objective = [
      'Build Campaign 001 for Anchor Cleaning.',
      'Generate intelligence.',
      'Generate mail packages.',
      'Review.',
      'Ready to Print.',
    ].join(' ');
    const draft = engine.planner.plan({
      objective,
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.type, MISSION_TYPES.CAMPAIGN_CREATION);
    assert.ok(draft.plan.executionGraph);
    assert.equal(draft.plan.plannerVersion, PLANNER_VERSION);
    assert.ok(draft.plan.executionGraph.validation.ok);

    const ids = draft.plan.steps.map((s) => s.capabilityId);
    assert.ok(ids.includes(CAP_IDS.PROSPECT_DISCOVERY));
    assert.ok(ids.includes(CAP_IDS.COMPANY_ENRICHMENT));
    assert.ok(ids.includes(CAP_IDS.OPPORTUNITY_RANKING));
    assert.ok(ids.includes(CAP_IDS.CAMPAIGN_BUILDER));
    assert.ok(ids.includes(CAP_IDS.MAIL_PACKAGE_GENERATOR));
    assert.ok(ids.includes(CAP_IDS.CAMPAIGN_REVIEW));
    // Ready To Print is a planner gate — not a separate capability
    assert.ok(draft.plan.selectedStages.includes('ready_to_print'));
    assert.equal(draft.constraints.produceReadyToPrint, true);
    assert.ok(draft.plan.reviewGates.includes('campaign_review'));

    // Order: discovery before ranking before builder before mail before review
    const di = ids.indexOf(CAP_IDS.PROSPECT_DISCOVERY);
    const ri = ids.indexOf(CAP_IDS.OPPORTUNITY_RANKING);
    const ci = ids.indexOf(CAP_IDS.CAMPAIGN_BUILDER);
    const mi = ids.indexOf(CAP_IDS.MAIL_PACKAGE_GENERATOR);
    const revi = ids.indexOf(CAP_IDS.CAMPAIGN_REVIEW);
    assert.ok(di < ri && ri < ci && ci < mi && mi < revi);
  });

  it('stage keywords augment seed — do not replace campaign chain', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Build Campaign 001. Pause at review.',
      tenantId: '10',
      clientId: 10,
    });
    const ids = draft.plan.steps.map((s) => s.capabilityId);
    assert.ok(ids.includes(CAP_IDS.CAMPAIGN_BUILDER));
    assert.ok(ids.includes(CAP_IDS.CAMPAIGN_REVIEW));
    assert.ok(ids.length > 2);
  });

  it('focused mail package objective stays single-stage', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Generate mail packages for Campaign 001',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.type, MISSION_TYPES.MAIL_PACKAGE_GENERATION);
    assert.deepEqual(
      draft.plan.steps.map((s) => s.capabilityId),
      [CAP_IDS.MAIL_PACKAGE_GENERATOR]
    );
  });

  it('explainPlan answers why stages were included or skipped', () => {
    const graph = createExecutionGraph({
      objective: 'Build Campaign 001. Generate mail packages. Review.',
      missionType: MISSION_TYPES.CAMPAIGN_CREATION,
    });
    const explained = explainPlan(graph);
    assert.ok(explained.pipeline.includes('Discovery'));
    assert.equal(explained.answers.whyMailPackage.included, true);
    assert.equal(explained.answers.whyReviewRequired.included, true);
    assert.equal(explained.answers.whyNotDirectMail.included, false);
    assert.match(explained.answers.whyNotDirectMail.reason, /not required/i);
  });

  it('validateGraph rejects cycles and missing deps', () => {
    const ok = validateGraph(
      createExecutionGraph({
        objective: 'Build Campaign 001',
        missionType: MISSION_TYPES.CAMPAIGN_CREATION,
      })
    );
    assert.equal(ok.ok, true);

    const bad = validateGraph({
      nodes: [
        {
          id: 'a',
          name: 'A',
          capabilityId: 'x',
          dependencies: ['b'],
          reviewRequired: false,
          priority: 1,
        },
        {
          id: 'b',
          name: 'B',
          capabilityId: 'y',
          dependencies: ['a'],
          reviewRequired: false,
          priority: 2,
        },
      ],
      edges: [
        { from: 'a', to: 'b' },
        { from: 'b', to: 'a' },
      ],
    });
    assert.equal(bad.ok, false);
    assert.ok(bad.errors.some((e) => /cycle/i.test(e)));
  });

  it('insertStage / removeStage / replaceStage recompute graph', () => {
    let graph = createExecutionGraph({
      objective: 'Build Campaign 001',
      missionType: MISSION_TYPES.CAMPAIGN_CREATION,
    });
    assert.ok(!graph.selectedStages.includes('mail_package_generator'));
    graph = insertStage(graph, 'mail_package_generator', {
      reason: 'Operator added mail packages',
    });
    assert.ok(graph.selectedStages.includes('mail_package_generator'));
    graph = removeStage(graph, 'knowledge_update');
    assert.ok(!graph.selectedStages.includes('knowledge_update'));
    graph = replaceStage(graph, 'mail_package_generator', 'campaign_review');
    assert.ok(!graph.selectedStages.includes('mail_package_generator'));
    assert.ok(graph.selectedStages.includes('campaign_review'));
  });

  it('replan preserves completed stages and invalidates stale ones', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    // Simulate partial completion
    draft.plan.steps = draft.plan.steps.map((s, idx) =>
      idx === 0 ? { ...s, status: 'completed' } : s
    );
    const replanned = engine.planner.replan(draft, {
      constraints: { targetCount: 30 },
      staleCapabilityIds: ['prospect_discovery', 'opportunity_ranking'],
    });
    assert.equal(replanned.constraints.targetCount, 30);
    assert.ok(replanned.plan.replan);
    const discovery = replanned.plan.steps.find(
      (s) => s.capabilityId === CAP_IDS.PROSPECT_DISCOVERY
    );
    assert.equal(discovery.status, 'queued');
    assert.ok(
      replanned.plan.steps.every(
        (s) => s.status === 'queued' || s.status === 'completed'
      )
    );
  });

  it('mission creation fails when graph validation fails', () => {
    const engine = testEngine();
    assert.throws(
      () =>
        engine.planner.plan({
          objective: 'xyzzy completely unrelated nonsense',
          tenantId: '10',
          clientId: 10,
          missionType: 'not_a_real_type',
        }),
      (err) => {
        assert.match(String(err.message), /validation failed/i);
        assert.equal(err.code, 'MISSION_GRAPH_INVALID');
        return true;
      }
    );
  });
});
