'use strict';

/**
 * SPEC-202 — Pending Decision Turn Ownership.
 * Regression: ambiguous pending replies must not escape into unrelated workspace reasoning.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS, EXECUTION_INTENTS } = amo;
const {
  resolvePendingOperatorDecision,
  RESOLUTION_OUTCOMES,
  pendingDecisionOwnsTurn,
  normalizePendingDecisionTypos,
} = require('../PendingDecisionResolver');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const {
  classifyOperatorCognition,
  THINKING_MODES,
} = require('../../operatorCognition');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { WORKSPACE_OWNERS } = require('../WorkspaceOwnershipResolver');
const { MISSION_RUNTIMES } = require('../MissionRuntimeDispatch');
const { contextualPendingAnswer } = require('../PendingDecisionTurn');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function discoveryApprovalMission(extra = {}) {
  return {
    id: 'MISSION_A0AE9CF8-4CC8-4F4F-B2F2-855E0E0A712C',
    stage: STAGES.DISCOVER,
    objective: OBJECTIVE,
    structuredMissionApproved: true,
    pendingOperatorDecision: {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
    },
    ...extra,
  };
}

describe('SPEC-202 — Pending Decision Turn Ownership', () => {
  describe('PendingDecisionResolver — typo normalization', () => {
    const mission = discoveryApprovalMission();

    const typoCases = [
      ['apprroved', 'approve_discovery'],
      ['aproved', 'approve_discovery'],
      ['approvd', 'approve_discovery'],
      ['yea', 'approve_discovery'],
      ['procede', 'approve_discovery'],
    ];

    for (const [utterance, action] of typoCases) {
      it(`normalizes "${utterance}" to ${action}`, () => {
        const resolution = resolvePendingOperatorDecision(utterance, mission);
        assert.equal(resolution.resolved, true);
        assert.equal(resolution.action, action);
        assert.equal(resolution.executionIntent, EXECUTION_INTENTS.APPROVE_DISCOVERY);
      });
    }

    it('does not normalize unrelated garbage', () => {
      const resolution = resolvePendingOperatorDecision('asdf', mission);
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.pending, true);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.AMBIGUOUS);
      assert.equal(resolution.prompt, 'Approve discovery?');
    });

    it('preserves unresolved state instead of discarding it', () => {
      const resolution = resolvePendingOperatorDecision('asdf', mission);
      assert.deepEqual(
        {
          pending: resolution.pending,
          resolved: resolution.resolved,
          outcome: resolution.outcome,
          decisionKind: resolution.decisionKind,
          missionId: resolution.missionId,
          prompt: resolution.prompt,
        },
        {
          pending: true,
          resolved: false,
          outcome: RESOLUTION_OUTCOMES.AMBIGUOUS,
          decisionKind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
          missionId: mission.id,
          prompt: 'Approve discovery?',
        }
      );
    });

    it('does not special-handle typos when no pending decision exists', () => {
      const resolution = resolvePendingOperatorDecision('apprroved', {
        id: 'm-none',
        pendingOperatorDecision: null,
      });
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.pending, undefined);
    });
  });

  describe('PendingDecisionResolver — ownership matrix', () => {
    const mission = discoveryApprovalMission();

    it('approved resolves to APPROVE_DISCOVERY', () => {
      const resolution = resolvePendingOperatorDecision('approved', mission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.action, 'approve_discovery');
    });

    it('what are the risks? stays pending with QUESTION outcome', () => {
      const resolution = resolvePendingOperatorDecision('what are the risks?', mission);
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.QUESTION);
      assert.equal(pendingDecisionOwnsTurn(resolution), true);
    });

    it('explicit subject change releases ownership', () => {
      const resolution = resolvePendingOperatorDecision(
        "show me today's pipeline instead",
        mission
      );
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.UNRELATED);
      assert.equal(pendingDecisionOwnsTurn(resolution), false);
    });

    it('plan approval not yet stays ambiguous pending ownership', () => {
      const planMission = {
        id: 'm-plan',
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
          prompt: 'Approve mission plan?',
        },
      };
      const resolution = resolvePendingOperatorDecision('not yet', planMission);
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.AMBIGUOUS);
      assert.equal(pendingDecisionOwnsTurn(resolution), true);
    });
  });

  describe('OperatorIntent — pending ownership beats generic cognition', () => {
    it('typo-corrected apprroved resolves and requests execution', async () => {
      const mission = discoveryApprovalMission();
      const intent = await analyzeOperatorIntent({
        question: 'apprroved',
        mission,
        resolveMission: false,
      });

      assert.equal(intent.executionRequested, true);
      assert.equal(intent.mutatesMission, true);
      assert.equal(intent.conversationIntent.via, 'pending_decision_resolved');
      assert.equal(intent.pendingDecisionResolution.resolved, true);
      assert.equal(intent.pendingDecisionResolution.action, 'approve_discovery');
    });

    it('unresolved garbage keeps pending ownership intent', async () => {
      const mission = discoveryApprovalMission();
      const intent = await analyzeOperatorIntent({
        question: 'asdf',
        mission,
        resolveMission: false,
      });

      assert.equal(intent.executionRequested, false);
      assert.equal(intent.mutatesMission, false);
      assert.equal(intent.conversationIntent.via, 'pending_decision_ownership');
      assert.ok(intent.pendingDecisionResolution);
      assert.equal(intent.pendingDecisionResolution.resolved, false);
      assert.equal(intent.pendingDecisionResolution.outcome, RESOLUTION_OUTCOMES.AMBIGUOUS);
    });

    it('no pending decision does not attach pending ownership', async () => {
      const intent = await analyzeOperatorIntent({
        question: 'apprroved',
        mission: { id: 'm-none' },
        resolveMission: false,
      });
      assert.equal(intent.pendingDecisionResolution, null);
    });
  });

  describe('E2E — production regression MISSION_A0AE9CF8', () => {
    let engine;
    let mission;
    let runtime;

    beforeEach(() => {
      engine = amo.createAcquisitionMissionEngine();
      mission = engine.create({
        id: 'MISSION_A0AE9CF8-4CC8-4F4F-B2F2-855E0E0A712C',
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
        planApproved: true,
      });
      runtime = createTestAmoRuntime({ engine });
    });

    it('apprroved either approves discovery or clarifies without Mike/HLA briefing', async () => {
      const workspace = createWorkspaceEngine({
        acquisitionMissionRuntime: runtime,
        missionsEnabled: true,
        resolverEnabled: true,
        disableLlm: true,
      });

      const opened = workspace.open({ tenantId: '10', missionId: mission.id });
      const session = workspace._sessions.get(opened.sessionId);
      session.context.missionId = mission.id;
      session.context.acquisitionMissionId = mission.id;

      const before = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(
        before.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL
      );

      const turn = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'apprroved',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(turn.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
      assert.equal(turn.workspaceOwnership.missionRuntime, MISSION_RUNTIMES.AMO);
      assert.doesNotMatch(turn.prose, /Mike/i);
      assert.doesNotMatch(turn.prose, /Campaign 001/i);
      assert.doesNotMatch(turn.prose, /today'?s briefing/i);
      assert.doesNotMatch(turn.prose, /I can investigate today'?s briefing/i);

      const after = engine.inspect(mission.id, { tenantId: '10' });
      const approved =
        turn.resolution.reason === 'acquisition_mission_discovery_approved' ||
        turn.resolution.reason === 'pending_decision_turn_ownership';
      assert.equal(approved, true);

      if (turn.resolution.reason === 'pending_decision_turn_ownership') {
        assert.match(turn.prose, /Approve discovery\?/i);
        assert.equal(after.mission.version, before.mission.version);
      } else {
        assert.match(turn.prose, /Mission Updated/i);
        assert.notEqual(after.mission.version, before.mission.version);
      }
    });

    it('asdf clarifies pending decision without mission mutation or unrelated briefing', async () => {
      const workspace = createWorkspaceEngine({
        acquisitionMissionRuntime: runtime,
        missionsEnabled: true,
        resolverEnabled: true,
        disableLlm: true,
      });

      const opened = workspace.open({ tenantId: '10', missionId: mission.id });
      const session = workspace._sessions.get(opened.sessionId);
      session.context.missionId = mission.id;
      session.context.acquisitionMissionId = mission.id;

      const before = engine.inspect(mission.id, { tenantId: '10' });
      const turn = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'asdf',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(turn.resolution.reason, 'pending_decision_turn_ownership');
      assert.match(turn.prose, /Approve discovery\?/i);
      assert.match(turn.prose, /didn't catch a clear yes or no/i);
      assert.doesNotMatch(turn.prose, /today'?s briefing/i);
      assert.doesNotMatch(turn.prose, /Mike/i);

      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(after.mission.version, before.mission.version);
    });

    it('what are the risks? answers in context and retains pending decision', async () => {
      const workspace = createWorkspaceEngine({
        acquisitionMissionRuntime: runtime,
        missionsEnabled: true,
        resolverEnabled: true,
        disableLlm: true,
      });

      const opened = workspace.open({ tenantId: '10', missionId: mission.id });
      const session = workspace._sessions.get(opened.sessionId);
      session.context.missionId = mission.id;
      session.context.acquisitionMissionId = mission.id;

      const turn = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'what are the risks?',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(turn.resolution.reason, 'pending_decision_turn_ownership');
      assert.match(turn.prose, /Approve discovery\?/i);
      assert.match(turn.prose, /Discovery runs Scout/i);
    });
  });

  describe('normalizePendingDecisionTypos — unit constraints', () => {
    it('leaves long unrelated phrases untouched', () => {
      const phrase =
        'show me today pipeline instead of approving this mission right now please';
      assert.equal(normalizePendingDecisionTypos(phrase), phrase);
    });
  });

  describe('contextualPendingAnswer', () => {
    it('answers discovery risk questions in context', () => {
      const answer = contextualPendingAnswer(
        'what are the risks?',
        {
          decisionKind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
        },
        { mission: discoveryApprovalMission() }
      );
      assert.match(answer, /Discovery runs Scout/i);
    });
  });
});
