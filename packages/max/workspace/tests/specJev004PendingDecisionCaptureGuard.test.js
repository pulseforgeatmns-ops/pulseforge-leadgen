'use strict';

/**
 * SPEC-JEV-004 — Pending Decision Capture Guard.
 * Status/inspection questions must not be swallowed as unclear yes/no replies.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  resolvePendingOperatorDecision,
  RESOLUTION_OUTCOMES,
  pendingDecisionOwnsTurn,
} = require('../PendingDecisionResolver');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { contextualPendingAnswer } = require('../PendingDecisionTurn');
const {
  CAPTURE_INTENTS,
  classifyPendingDecisionCaptureIntent,
  shouldAttemptPendingDecisionCapture,
  isHighConfidenceJevInspection,
  setPendingDecisionCaptureClassifierForTests,
  resetPendingDecisionCaptureClassifierForTests,
  listPendingDecisionCaptureGuardLog,
  clearPendingDecisionCaptureGuardLog,
} = require('../pendingDecisionCaptureGuard');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const ANCHOR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from Anchor STR property managers in Manchester NH.';
const STATUS_QUESTION =
  'What is the current status and confidence of the Anchor STR mission?';
const YES_NO_COPY = "I didn't catch a clear yes or no";

function discoveryApprovalMission(extra = {}) {
  return {
    id: extra.id || 'mission-anchor-str',
    stage: STAGES.DISCOVER,
    objective: ANCHOR_OBJECTIVE,
    structuredMissionApproved: true,
    pendingOperatorDecision: {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
    },
    ...extra,
  };
}

async function openPendingDiscoveryWorkspace(opts = {}) {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({
    id: opts.id || 'MISSION_ANCHOR_STR_JEV004',
    tenantId: '10',
    objective: ANCHOR_OBJECTIVE,
    targetSegment: 'STR property managers',
    planApproved: true,
  });
  const runtime = createTestAmoRuntime({ engine });
  const workspace = createWorkspaceEngine({
    acquisitionMissionRuntime: runtime,
    missionsEnabled: true,
    resolverEnabled: true,
    disableLlm: true,
  });
  const opened = workspace.open({
    tenantId: '10',
    missionId: mission.id,
    acquisitionMissionId: mission.id,
  });
  const session = workspace._sessions.get(opened.sessionId);
  session.context.missionId = mission.id;
  session.context.acquisitionMissionId = mission.id;
  if (opts.shadowDecision) {
    session.context.shadowDecision = opts.shadowDecision;
  }
  return { engine, mission, runtime, workspace, sessionId: opened.sessionId };
}

describe('SPEC-JEV-004 — Pending Decision Capture Guard', () => {
  beforeEach(() => {
    clearPendingDecisionCaptureGuardLog();
    resetPendingDecisionCaptureClassifierForTests();
  });

  afterEach(() => {
    resetPendingDecisionCaptureClassifierForTests();
  });

  describe('classifier', () => {
    const pending = {
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
    };

    const decisionCases = [
      'yes',
      'yes approve',
      'approved',
      'approve discovery',
      'go ahead',
      'do it',
      'start discovery',
      'begin scout discovery',
      'no',
      'reject',
      'hold off',
      'pause',
      'not yet',
      "don't approve",
    ];

    for (const message of decisionCases) {
      it(`classifies "${message}" as decision_response`, () => {
        assert.equal(
          classifyPendingDecisionCaptureIntent({ message, pendingDecision: pending }),
          CAPTURE_INTENTS.DECISION_RESPONSE
        );
        assert.equal(
          shouldAttemptPendingDecisionCapture({ message, pendingDecision: pending }),
          true
        );
      });
    }

    const inspectionCases = [
      STATUS_QUESTION,
      'What is the status?',
      'What are we waiting on?',
      'Why is it blocked?',
      'Show me the mission state.',
      'How many STR prospects are loaded for Rory?',
      'Do we have prospects for Rory?',
      'What happened with discovery?',
      'What is confidence right now?',
    ];

    for (const message of inspectionCases) {
      it(`does not capture "${message}"`, () => {
        const intent = classifyPendingDecisionCaptureIntent({
          message,
          pendingDecision: pending,
        });
        assert.notEqual(intent, CAPTURE_INTENTS.DECISION_RESPONSE, message);
        assert.notEqual(intent, CAPTURE_INTENTS.AMBIGUOUS_SHORT_RESPONSE, message);
        assert.equal(
          shouldAttemptPendingDecisionCapture({ message, pendingDecision: pending }),
          false,
          message
        );
      });
    }

    const clarificationCases = [
      'What exactly am I approving?',
      'What happens if I approve discovery?',
      'Why should I approve this?',
      'What does approve discovery mean?',
    ];

    for (const message of clarificationCases) {
      it(`classifies "${message}" as pending-decision clarification`, () => {
        assert.equal(
          classifyPendingDecisionCaptureIntent({ message, pendingDecision: pending }),
          CAPTURE_INTENTS.PENDING_DECISION_CLARIFICATION
        );
        assert.equal(
          shouldAttemptPendingDecisionCapture({ message, pendingDecision: pending }),
          false
        );
      });
    }

    it('treats sure/ok as short ambiguous responses', () => {
      assert.equal(
        classifyPendingDecisionCaptureIntent({ message: 'sure', pendingDecision: pending }),
        CAPTURE_INTENTS.AMBIGUOUS_SHORT_RESPONSE
      );
      assert.equal(
        classifyPendingDecisionCaptureIntent({ message: 'ok', pendingDecision: pending }),
        CAPTURE_INTENTS.AMBIGUOUS_SHORT_RESPONSE
      );
    });
  });

  describe('resolver + production failure', () => {
    const mission = discoveryApprovalMission();

    it('does not treat the Anchor STR status question as an unclear yes/no', () => {
      const resolution = resolvePendingOperatorDecision(STATUS_QUESTION, mission);
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.pending, true);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.UNRELATED);
      assert.equal(resolution.captureGuarded, true);
      assert.equal(resolution.captureIntent, CAPTURE_INTENTS.INSPECTION_OR_STATUS_QUESTION);
      assert.equal(pendingDecisionOwnsTurn(resolution), false);

      const logs = listPendingDecisionCaptureGuardLog();
      assert.equal(logs.length, 1);
      assert.equal(logs[0].event, 'PENDING_DECISION_CAPTURE_GUARDED');
      assert.equal(logs[0].spec, 'SPEC-JEV-004');
      assert.equal(logs[0].classification, CAPTURE_INTENTS.INSPECTION_OR_STATUS_QUESTION);
      assert.equal(logs[0].reason, 'operator_message_looks_like_status_or_inspection');
      assert.equal(logs[0].message_chars, STATUS_QUESTION.length);
      assert.ok(!Object.prototype.hasOwnProperty.call(logs[0], 'message'));
    });

    it('still resolves explicit discovery approval', () => {
      const resolution = resolvePendingOperatorDecision(
        'Approved. Begin Scout discovery.',
        mission
      );
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.action, 'approve_discovery');
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.AFFIRM);
      assert.equal(listPendingDecisionCaptureGuardLog().length, 0);
    });

    it('keeps hold-off on the pending-decision path', () => {
      const resolution = resolvePendingOperatorDecision('Hold off for now.', mission);
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.AMBIGUOUS);
      assert.equal(pendingDecisionOwnsTurn(resolution), true);
    });

    it('explains approval questions without resolving them', () => {
      const resolution = resolvePendingOperatorDecision(
        'What exactly am I approving?',
        mission
      );
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.QUESTION);
      assert.equal(pendingDecisionOwnsTurn(resolution), true);

      const answer = contextualPendingAnswer(
        'What exactly am I approving?',
        resolution,
        { mission }
      );
      assert.match(answer, /approve Scout discovery/i);
      assert.doesNotMatch(answer, new RegExp(YES_NO_COPY, 'i'));
    });

    it('does not capture prospect-count questions', () => {
      const resolution = resolvePendingOperatorDecision(
        'How many STR prospects are loaded for Rory?',
        mission
      );
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.UNRELATED);
      assert.equal(pendingDecisionOwnsTurn(resolution), false);
    });

    it('still asks for yes/no on short ambiguous replies', () => {
      for (const utterance of ['sure', 'ok', 'asdf']) {
        const resolution = resolvePendingOperatorDecision(utterance, mission);
        assert.equal(resolution.resolved, false, utterance);
        assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.AMBIGUOUS, utterance);
        assert.equal(pendingDecisionOwnsTurn(resolution), true, utterance);
      }
    });
  });

  describe('workspace e2e', () => {
    it('status question does not demand yes/no and leaves the pending decision', async () => {
      const { engine, mission, workspace, sessionId } = await openPendingDiscoveryWorkspace();
      const before = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(
        before.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL
      );

      const turn = await workspace.ask({
        sessionId,
        question: STATUS_QUESTION,
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.ok(turn && typeof turn.prose === 'string' && turn.prose.length > 0);
      assert.doesNotMatch(turn.prose, new RegExp(YES_NO_COPY, 'i'));
      assert.notEqual(
        turn.resolution && turn.resolution.reason,
        'pending_decision_turn_ownership'
      );
      assert.doesNotMatch(turn.prose, /^Approve discovery\?\s*$/i);

      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(
        after.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL
      );
      assert.equal(after.mission.version, before.mission.version);

      const logs = listPendingDecisionCaptureGuardLog();
      assert.ok(
        logs.some((row) => row.event === 'PENDING_DECISION_CAPTURE_GUARDED'),
        'expected capture-guarded audit row'
      );
    });

    it('explicit approval still resolves discovery', async () => {
      const { engine, mission, workspace, sessionId } = await openPendingDiscoveryWorkspace({
        id: 'MISSION_ANCHOR_STR_APPROVE',
      });

      const turn = await workspace.ask({
        sessionId,
        question: 'Approved. Begin Scout discovery.',
        context: { tenantId: '10', missionId: mission.id },
      });

      const after = engine.inspect(mission.id, { tenantId: '10' });
      const approved =
        turn.resolution.reason === 'acquisition_mission_discovery_approved' ||
        (turn.operatorIntent &&
          turn.operatorIntent.pendingDecisionResolution &&
          turn.operatorIntent.pendingDecisionResolution.resolved === true);
      assert.equal(approved || !after.mission.pendingOperatorDecision, true);
      assert.doesNotMatch(turn.prose, new RegExp(YES_NO_COPY, 'i'));
    });

    it('hold-off does not fall through to mission inspection', async () => {
      const { engine, mission, workspace, sessionId } = await openPendingDiscoveryWorkspace({
        id: 'MISSION_ANCHOR_STR_HOLD',
      });
      const before = engine.inspect(mission.id, { tenantId: '10' });

      const turn = await workspace.ask({
        sessionId,
        question: 'Hold off for now.',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(turn.resolution.reason, 'pending_decision_turn_ownership');
      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(
        after.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL
      );
      assert.equal(after.mission.version, before.mission.version);
    });

    it('clarifies What exactly am I approving? without resolving', async () => {
      const { engine, mission, workspace, sessionId } = await openPendingDiscoveryWorkspace({
        id: 'MISSION_ANCHOR_STR_CLARIFY',
      });

      const turn = await workspace.ask({
        sessionId,
        question: 'What exactly am I approving?',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(turn.resolution.reason, 'pending_decision_turn_ownership');
      assert.match(turn.prose, /approve Scout discovery|Approve discovery/i);
      assert.doesNotMatch(turn.prose, new RegExp(YES_NO_COPY, 'i'));
      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(
        after.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL
      );
    });

    it('prospect questions are not captured as pending answers', async () => {
      const { engine, mission, workspace, sessionId } = await openPendingDiscoveryWorkspace({
        id: 'MISSION_ANCHOR_STR_PROSPECTS',
      });
      const before = engine.inspect(mission.id, { tenantId: '10' });

      const turn = await workspace.ask({
        sessionId,
        question: 'How many STR prospects are loaded for Rory?',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.ok(turn && typeof turn.prose === 'string' && turn.prose.length > 0);
      assert.notEqual(
        turn.resolution && turn.resolution.reason,
        'pending_decision_turn_ownership'
      );
      assert.doesNotMatch(turn.prose, new RegExp(YES_NO_COPY, 'i'));
      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(
        after.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL
      );
      assert.equal(after.mission.version, before.mission.version);
    });

    it('short ambiguous replies still ask for an explicit yes/no', async () => {
      const { engine, mission, workspace, sessionId } = await openPendingDiscoveryWorkspace({
        id: 'MISSION_ANCHOR_STR_SURE',
      });

      const turn = await workspace.ask({
        sessionId,
        question: 'sure',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(turn.resolution.reason, 'pending_decision_turn_ownership');
      assert.match(turn.prose, new RegExp(YES_NO_COPY, 'i'));
      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(
        after.mission.pendingOperatorDecision.kind,
        OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL
      );
    });

    it('classifier failure cannot crash routing', async () => {
      setPendingDecisionCaptureClassifierForTests(() => {
        throw new Error('injected classifier failure');
      });
      const { engine, mission, workspace, sessionId } = await openPendingDiscoveryWorkspace({
        id: 'MISSION_ANCHOR_STR_GUARD_FAIL',
      });

      const turn = await workspace.ask({
        sessionId,
        question: STATUS_QUESTION,
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.ok(turn && typeof turn.prose === 'string' && turn.prose.length > 0);
      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.ok(after.mission);
      const errors = listPendingDecisionCaptureGuardLog().filter(
        (row) => row.event === 'PENDING_DECISION_CAPTURE_GUARD_ERROR'
      );
      assert.ok(errors.length >= 1);
      assert.match(errors[0].error, /injected classifier failure/);
    });
  });

  describe('Jev shadow signal', () => {
    it('uses synchronous Jev inspection metadata as an additional guard', () => {
      const mission = discoveryApprovalMission();
      const shadowDecision = {
        intent: 'status_check',
        recommended_route: 'inspection',
        inspection_probability: 0.93,
        approval_probability: 0.07,
        confidence: 0.99,
      };
      assert.equal(isHighConfidenceJevInspection(shadowDecision), true);

      // A short non-status phrase would otherwise be treated as an ambiguous
      // pending reply. Live Jev metadata, when already present, prevents capture.
      const resolution = resolvePendingOperatorDecision('got it', mission, {
        shadowDecision,
      });
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.UNRELATED);
      assert.equal(resolution.captureIntent, CAPTURE_INTENTS.INSPECTION_OR_STATUS_QUESTION);
      assert.equal(pendingDecisionOwnsTurn(resolution), false);
    });

    it('does not let Jev block an explicit approval', () => {
      const mission = discoveryApprovalMission();
      const resolution = resolvePendingOperatorDecision('Approved. Begin Scout discovery.', mission, {
        shadowDecision: {
          intent: 'status_check',
          recommended_route: 'inspection',
          inspection_probability: 0.93,
          approval_probability: 0.07,
          confidence: 0.99,
        },
      });
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.action, 'approve_discovery');
    });

    it('works without Jev metadata', () => {
      const mission = discoveryApprovalMission();
      const resolution = resolvePendingOperatorDecision(STATUS_QUESTION, mission);
      assert.equal(resolution.outcome, RESOLUTION_OUTCOMES.UNRELATED);
      assert.equal(pendingDecisionOwnsTurn(resolution), false);
    });
  });

  describe('operator intent', () => {
    it('lets status questions use normal cognition instead of pending ownership', async () => {
      const intent = await analyzeOperatorIntent({
        question: STATUS_QUESTION,
        mission: discoveryApprovalMission(),
        resolveMission: false,
      });
      assert.equal(intent.pendingDecisionResolution.outcome, RESOLUTION_OUTCOMES.UNRELATED);
      assert.notEqual(intent.conversationIntent.via, 'pending_decision_ownership');
      assert.equal(intent.executionRequested, false);
    });
  });
});
