'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  resolveArtifacts,
  deriveRequiredArtifacts,
  createExecutionGraph,
  createMissionEngine,
  parseIntent,
  MISSION_TYPES,
  PLANNER_VERSION,
  ARTIFACT_SOURCES,
} = require('..');
const {
  BUILTIN_IDS: CAP_IDS,
  createBuiltinRegistry,
  CAPABILITY_ARTIFACT_CONTRACTS,
} = require('../../capabilities');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({
      discovery: { useFixture: true },
    }),
  });
}

const CURRENT_LIST_OBJECTIVE = [
  'Build Campaign 001 for Anchor Cleaning using the current ProspectList.',
  'Execute the complete pipeline through Sales Intelligence.',
].join(' ');

describe('SPEC-051 Artifact Resolver', () => {
  it('prefers Current Mission over Capability Acquisition', () => {
    const result = resolveArtifacts({
      required: ['ProspectList'],
      availableArtifacts: [
        {
          type: 'ProspectList',
          source: 'current_mission',
          confidence: 'High',
          compatible: true,
        },
      ],
    });
    assert.equal(result.resolved.length, 1);
    assert.equal(result.resolved[0].source, ARTIFACT_SOURCES.CURRENT_MISSION);
    assert.ok(result.skippedStages.prospect_discovery);
    assert.ok(
      result.acquisitions.some((a) => a.strategy === 'use_existing')
    );
  });

  it('falls back to Discovery when ProspectList is missing', () => {
    const result = resolveArtifacts({
      required: ['ProspectList'],
      availableArtifacts: [],
    });
    assert.deepEqual(result.missing, ['ProspectList']);
    assert.ok(
      result.acquisitions.some(
        (a) =>
          a.strategy === 'capability_acquisition' &&
          a.stageId === 'prospect_discovery'
      )
    );
  });

  it('Mission Plan prospectList:current synthesizes an operator-explicit candidate', () => {
    const plan = parseIntent(CURRENT_LIST_OBJECTIVE);
    assert.equal(plan.parameters.prospectList, 'current');
    const result = resolveArtifacts({
      required: ['ProspectList'],
      missionPlan: plan,
    });
    assert.equal(result.resolved.length, 1);
    assert.equal(
      result.resolved[0].source,
      ARTIFACT_SOURCES.OPERATOR_EXPLICIT
    );
    assert.equal(result.resolved[0].pending, true);
  });

  it('ranks Operator Import above Previous Mission', () => {
    const result = resolveArtifacts({
      required: ['ProspectList'],
      availableArtifacts: [
        {
          type: 'ProspectList',
          source: 'operator_import',
          confidence: 'High',
          createdAt: '2026-07-01T00:00:00.000Z',
        },
      ],
      previousMissionArtifacts: [
        {
          type: 'ProspectList',
          source: 'previous_mission',
          confidence: 'High',
          createdAt: '2026-07-20T00:00:00.000Z',
        },
      ],
    });
    assert.equal(result.resolved[0].source, ARTIFACT_SOURCES.OPERATOR_IMPORT);
  });
});

describe('SPEC-051 Execution graph — Discovery skipped when ProspectList exists', () => {
  it('skips Discovery for campaign with current ProspectList parameter', () => {
    const plan = parseIntent(CURRENT_LIST_OBJECTIVE);
    const graph = createExecutionGraph({
      objective: CURRENT_LIST_OBJECTIVE,
      missionType: MISSION_TYPES.CAMPAIGN_CREATION,
      missionPlan: plan,
    });
    assert.ok(graph.validation.ok);
    assert.ok(!graph.selectedStages.includes('prospect_discovery'));
    assert.ok(graph.selectedStages.includes('campaign_builder'));
    assert.ok(graph.selectedStages.includes('sales_intelligence'));
    assert.ok(
      /Compatible ProspectList/i.test(
        graph.skippedStages.prospect_discovery || ''
      )
    );
    assert.ok(
      graph.artifactResolution.resolved.some((r) => r.type === 'ProspectList')
    );
    assert.ok(graph.reasoning.stagesSkippedByResolution.includes('prospect_discovery'));
  });

  it('keeps Discovery when no ProspectList is available', () => {
    const plan = parseIntent('Build Campaign 001 for Anchor Cleaning.');
    const graph = createExecutionGraph({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      missionType: MISSION_TYPES.CAMPAIGN_CREATION,
      missionPlan: plan,
    });
    assert.ok(graph.selectedStages.includes('prospect_discovery'));
    assert.ok(
      graph.artifactResolution.missing.includes('ProspectList') ||
        graph.artifactResolution.acquisitions.some(
          (a) =>
            a.artifactType === 'ProspectList' &&
            a.strategy === 'capability_acquisition'
        )
    );
  });

  it('skips Discovery when availableArtifacts supplies ProspectList', () => {
    const graph = createExecutionGraph({
      objective: 'Build Campaign 001.',
      missionType: MISSION_TYPES.CAMPAIGN_CREATION,
      availableArtifacts: [
        {
          type: 'ProspectList',
          source: 'operator_import',
          confidence: 'High',
          compatible: true,
          pending: false,
        },
      ],
    });
    assert.ok(!graph.selectedStages.includes('prospect_discovery'));
    assert.ok(graph.selectedStages.includes('campaign_builder'));
  });
});

describe('SPEC-051 MissionPlanner integration', () => {
  it('plan omits Discovery steps when ProspectList is current', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: CURRENT_LIST_OBJECTIVE,
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.plan.plannerVersion, PLANNER_VERSION);
    const ids = draft.plan.steps.map((s) => s.capabilityId);
    assert.ok(!ids.includes(CAP_IDS.PROSPECT_DISCOVERY));
    assert.ok(ids.includes(CAP_IDS.CAMPAIGN_BUILDER));
    assert.ok(ids.includes(CAP_IDS.SALES_INTELLIGENCE));
    assert.ok(draft.plan.artifactResolution);
    assert.ok(
      draft.plan.artifactResolution.resolved.some(
        (r) => r.type === 'ProspectList'
      )
    );
  });

  it('builtins declare requires and produces', () => {
    const registry = createBuiltinRegistry({ discovery: { useFixture: true } });
    const discovery = registry.get(CAP_IDS.PROSPECT_DISCOVERY);
    assert.ok(Array.isArray(discovery.requires));
    assert.ok(discovery.produces.includes('prospect_list'));
    assert.ok(CAPABILITY_ARTIFACT_CONTRACTS[CAP_IDS.CAMPAIGN_BUILDER]);
    const builder = registry.get(CAP_IDS.CAMPAIGN_BUILDER);
    assert.ok(builder.produces.includes('campaign'));
  });
});

describe('SPEC-051 deriveRequiredArtifacts', () => {
  it('unions consumes from selected stages', () => {
    const required = deriveRequiredArtifacts([
      'campaign_builder',
      'opportunity_ranking',
    ]);
    assert.ok(required.includes('ProspectList') || required.includes('OpportunityRanking') || required.length >= 1);
    assert.ok(required.includes('ProspectList'));
  });
});
