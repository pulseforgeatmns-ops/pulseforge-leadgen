'use strict';

/**
 * SPEC-JEV-006 — Guarded Active Routing Promotion
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const { applyJevActiveRoutingPromotion } = require('../jevActiveRouting');
const { DecisionService } = require('../../../decision-service/DecisionService');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const fixture = require('../../../../test/fixtures/decisionShadowEvent.json');

const ACTIVE_ENV = {
  DECISION_SHADOW_ENABLED: 'true',
  DECISION_PROVIDER: 'jev',
  JEV_ENABLED: 'true',
  JEV_API_KEY: 'test-key',
  JEV_ACTIVE_ROUTING_ENABLED: 'true',
  JEV_TIMEOUT_MS: '50',
};

const STATUS_QUESTION =
  'What is the current status and confidence of the Anchor STR mission?';

const inspectionDecision = Object.fromEntries(
  [
    'intent',
    'confidence',
    'mission_bound_probability',
    'approval_probability',
    'inspection_probability',
    'requires_human_clarification',
    'risk_if_misrouted',
    'recommended_route',
  ].map((key) => [key, fixture[key]])
);

function setupJevWorkspace(evaluate, env = ACTIVE_ENV) {
  const audits = [];
  const service = new DecisionService({
    env,
    provider: {
      name: 'jev',
      model: 'jev-test',
      evaluate:
        evaluate ||
        (async () => ({
          decision: inspectionDecision,
          model: 'jev-test',
        })),
    },
  });
  const runtime = createTestAmoRuntime();
  const mission = runtime.engine().create({
    id: 'MISSION_JEV006_ANCHOR_STR',
    tenantId: '10',
    objective:
      'Acquire one recurring commercial cleaning client from Anchor STR property managers in Manchester NH.',
    targetSegment: 'STR property managers',
    planApproved: true,
  });
  const workspace = createWorkspaceEngine({
    decisionService: service,
    disableLlm: true,
    acquisitionMissionRuntime: runtime,
    missionsEnabled: true,
    resolverEnabled: true,
  });
  const opened = workspace.open({
    tenantId: '10',
    missionId: mission.id,
    acquisitionMissionId: mission.id,
  });
  return {
    workspace,
    service,
    runtime,
    mission,
    sessionId: opened.sessionId,
    audits,
  };
}

describe('SPEC-JEV-006 — Guarded Active Routing Promotion', () => {
  it('applyJevActiveRoutingPromotion overrides reasoning ownership for safe inspection decisions', () => {
    const evaluation = {
      decision: inspectionDecision,
      assessment: { eligible: true, reason: null },
    };
    const promotion = applyJevActiveRoutingPromotion({
      workspaceOwnership: {
        owner: WORKSPACE_OWNERS.REASONING,
        reason: 'no_owner_claim',
        confidence: 0.5,
        specialist: null,
        fallback: true,
      },
      evaluation,
      question: STATUS_QUESTION,
      session: { context: { tenantId: '10', missionId: 'MISSION_JEV006_ANCHOR_STR' } },
      context: { tenantId: '10', missionId: 'MISSION_JEV006_ANCHOR_STR' },
      acquisitionMissionRuntime: createTestAmoRuntime(),
    });
    assert.equal(promotion.applied, true);
    assert.equal(promotion.workspaceOwnership.owner, WORKSPACE_OWNERS.MISSION_INSPECTION);
    assert.equal(promotion.audit.promoted, true);
    assert.equal(promotion.audit.production_route, 'conversation');
    assert.equal(promotion.audit.selected_route, 'inspection');
  });

  it('promotes high-confidence Jev inspection over reasoning for status questions', async () => {
    const { workspace, service, sessionId, runtime } = setupJevWorkspace();
    const productionWorkspace = createWorkspaceEngine({
      decisionService: workspace._decisionService,
      disableLlm: true,
      acquisitionMissionRuntime: runtime,
      missionsEnabled: true,
      resolverEnabled: true,
      resolveWorkspaceOwner: async (input) => {
        const resolved = await resolveWorkspaceOwner(input);
        if (input.question === STATUS_QUESTION) {
          return {
            owner: WORKSPACE_OWNERS.REASONING,
            reason: 'test_simulated_misroute',
            confidence: 0.5,
            specialist: null,
            fallback: true,
          };
        }
        return resolved;
      },
    });
    const opened = productionWorkspace.open({
      tenantId: '10',
      missionId: 'MISSION_JEV006_ANCHOR_STR',
      acquisitionMissionId: 'MISSION_JEV006_ANCHOR_STR',
    });
    const result = await productionWorkspace.ask({
      sessionId: opened.sessionId,
      question: STATUS_QUESTION,
      context: { tenantId: '10' },
    });
    await service.drain();
    assert.equal(result.workspaceOwnership.owner, 'mission_inspection');
    assert.equal(result.workspaceOwnership.reason, 'jev_active_routing_promotion');
    assert.match(result.prose, /status|confidence|Anchor|mission/i);
    assert.doesNotMatch(result.prose, /didn't catch a clear yes or no/i);
    assert.equal(result.context.jevActiveRouting.promoted, true);
    assert.equal(result.context.jevActiveRouting.jev_route, 'inspection');
    assert.equal(result.context.jevActiveRouting.selected_route, 'inspection');
  });

  it('does not promote when active routing is disabled', async () => {
    const { workspace, service, sessionId } = setupJevWorkspace(undefined, {
      ...ACTIVE_ENV,
      JEV_ACTIVE_ROUTING_ENABLED: 'false',
    });
    const result = await workspace.ask({
      sessionId,
      question: STATUS_QUESTION,
      context: { tenantId: '10' },
    });
    await service.drain();
    assert.notEqual(result.workspaceOwnership.reason, 'jev_active_routing_promotion');
  });

  it('does not promote execution flows when Jev recommends mission', async () => {
    const { workspace, service, sessionId } = setupJevWorkspace(async () => ({
      decision: {
        ...inspectionDecision,
        intent: 'mission_instruction',
        recommended_route: 'mission',
        confidence: 0.99,
        inspection_probability: 0.1,
        approval_probability: 0.1,
      },
      model: 'jev-test',
    }));
    const before = service;
    const result = await workspace.ask({
      sessionId,
      question: 'Execute the next Scout discovery stage now.',
      context: { tenantId: '10' },
    });
    await before.drain();
    assert.notEqual(result.workspaceOwnership.reason, 'jev_active_routing_promotion');
    assert.equal(result.context.jevActiveRouting.promoted, false);
  });

  it('does not bypass explicit approval capture when Jev inspection metadata is present', async () => {
    const { workspace, service, runtime, mission, sessionId } = setupJevWorkspace();
    const result = await workspace.ask({
      sessionId,
      question: 'Approved. Begin Scout discovery.',
      context: { tenantId: '10', missionId: mission.id },
    });
    await service.drain();
    assert.notEqual(result.workspaceOwnership.reason, 'jev_active_routing_promotion');
    const approved =
      (result.resolution &&
        /discovery_approved|acquisition_mission_discovery_approved/.test(
          result.resolution.reason || ''
        )) ||
      (result.operatorIntent &&
        result.operatorIntent.pendingDecisionResolution &&
        result.operatorIntent.pendingDecisionResolution.resolved === true);
    assert.equal(approved, true);
  });

  it('records production and Jev routes in active routing audit when blocked', async () => {
    const { workspace, service, sessionId } = setupJevWorkspace(async () => ({
      decision: {
        ...inspectionDecision,
        confidence: 0.5,
      },
      model: 'jev-test',
    }));
    const result = await workspace.ask({
      sessionId,
      question: STATUS_QUESTION,
      context: { tenantId: '10' },
    });
    await service.drain();
    assert.equal(result.context.jevActiveRouting.promoted, false);
    assert.equal(result.context.jevActiveRouting.blocked_reason, 'low_confidence');
    assert.ok(result.context.jevActiveRouting.production_route);
    assert.equal(result.context.jevActiveRouting.jev_route, 'inspection');
  });
});
