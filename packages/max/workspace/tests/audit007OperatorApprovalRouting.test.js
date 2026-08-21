'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const { classifyMessage, MESSAGE_CLASS } = require('../../../mission-engine/classifyMessage');
const { isMissionExecutionCommand } = require('../ExecutionLanguageDetection');
const { detectExecutionAction } = require('../AcquisitionMissionExecution');
const { hasPendingDiscoveryApproval } = require('../AmoOperatorApproval');
const { WORKSPACE_OWNERS } = require('../WorkspaceOwnershipResolver');
const {
  listOperatorApprovalRoutingAuditLog,
  clearOperatorApprovalRoutingAuditLog,
  PIPELINES,
  BREAKPOINT_LEGACY_MISSION_FIRST,
} = require('../audit/OperatorApprovalRoutingAudit');
const { listMissionApprovalAuditLog, clearMissionApprovalAuditLog } = require('../audit/MissionApprovalAudit');

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester for law firms.';
const UTTERANCE = 'approved';

function stubLegacyMissionEngine() {
  return {
    activeMissionResolver: {
      resolveActiveMission: async () => null,
      resolve: async () => ({ action: 'intelligence' }),
      clearActiveMission: async () => {},
    },
    toCard: (m) => m,
  };
}

function eventsOf(name) {
  return listOperatorApprovalRoutingAuditLog().filter((row) => row.event === name);
}

describe('AUDIT-007 — Operator Approval Routing', () => {
  beforeEach(() => {
    clearOperatorApprovalRoutingAuditLog();
    clearMissionApprovalAuditLog();
  });

  it('classifiers: approved is an execution command, not EXECUTE_STAGE', () => {
    assert.equal(isMissionExecutionCommand(UTTERANCE), true);
    const classified = classifyMessage(UTTERANCE, {
      id: 'msn_active',
      status: 'planning',
      title: 'Campaign 001',
    });
    assert.equal(classified.classification, MESSAGE_CLASS.RESUME);
    assert.equal(classified.reason, 'default_resume_active');
    assert.notEqual(classified.classification, MESSAGE_CLASS.EXECUTE_STAGE);
  });

  it('AMO-only workspace consumes approved as discovery_approved and runs Scout', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const engine = createWorkspaceEngine({
      acquisitionMissionEngine: amoEngine,
      missionsEnabled: true,
      missionEngine: stubLegacyMissionEngine(),
      disableLlm: true,
    });

    const created = await engine.ask({
      question: ANCHOR_OBJECTIVE,
      context: { tenantId: '10', page: 'command-deck' },
    });
    assert.ok(created.mission);
    assert.ok(created.mission.pendingOperatorDecision);
    assert.equal(created.mission.pendingOperatorDecision.prompt, 'Approve discovery?');

    const snapshot = amoEngine.inspect(created.mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryApproval(snapshot), true);
    assert.equal(detectExecutionAction(UTTERANCE, snapshot), 'discovery_approved');

    clearOperatorApprovalRoutingAuditLog();
    clearMissionApprovalAuditLog();

    const turn = await engine.ask({
      sessionId: created.sessionId,
      question: UTTERANCE,
    });

    assert.equal(turn.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.match(turn.prose, /Mission Updated/i);
    assert.match(turn.prose, /Prioritization approval|Scout/i);
    assert.doesNotMatch(turn.prose, /Approve discovery\?/i);

    const entry = eventsOf('WORKSPACE_ENTRY')[0];
    assert.ok(entry);
    assert.equal(entry.question, UTTERANCE);

    const found = eventsOf('ACTIVE_MISSION_FOUND')[0];
    assert.equal(found.found, true);
    assert.equal(found.source, 'amo');
    assert.equal(found.runtime, 'AcquisitionMission');

    const pending = eventsOf('MISSION_PENDING_DECISION')[0];
    assert.equal(pending.found, true);
    assert.match(pending.prompt, /Approve discovery/i);

    const classifier = eventsOf('APPROVAL_CLASSIFIER')[0];
    assert.equal(classifier.missionExecutionCommand, true);
    assert.equal(classifier.executeStage, false);
    assert.equal(classifier.amoAction, 'discovery_approved');
    assert.equal(classifier.returnsTrue, true);

    const owner = eventsOf('OWNER_SELECTED')[0];
    assert.equal(owner.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);

    const pipeline = eventsOf('PIPELINE_SELECTED')[0];
    assert.equal(pipeline.pipeline, PIPELINES.ACQUISITION_MISSION);
    assert.equal(pipeline.claimedBy, 'maybeHandleAcquisitionMissionExecution');

    assert.ok(eventsOf('MISSION_APPROVAL_MATCH')[0]);
    assert.ok(eventsOf('MISSION_STAGE_EXECUTION')[0]);
    assert.equal(eventsOf('FALLBACK_REASON').length, 0);

    const approvalEvents = listMissionApprovalAuditLog().map((row) => row.event);
    assert.ok(approvalEvents.includes('MISSION_APPROVAL_MATCHED'));
  });

  it('legacy Mission-first return is the routing breakpoint when a SPEC-022 mission is bound', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const amoMission = amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    assert.ok(amoMission.pendingOperatorDecision);

    const missionEngine = createMissionEngine({
      registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
      resolverEnabled: true,
    });
    const workspace = createWorkspaceEngine({
      acquisitionMissionEngine: amoEngine,
      missionsEnabled: true,
      missionEngine,
      resolverEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: { tenantId: '10', page: 'command-deck', missionId: amoMission.id },
    });
    assert.ok(first.mission);
    await missionEngine.store.update({
      id: first.mission.id,
      status: 'planning',
    });

    clearOperatorApprovalRoutingAuditLog();
    clearMissionApprovalAuditLog();

    const turn = await workspace.ask({
      sessionId: first.sessionId,
      question: UTTERANCE,
    });

    assert.equal(turn.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(turn.resolution.action, 'resumed');
    assert.equal(turn.resolution.classification, MESSAGE_CLASS.RESUME);
    assert.match(turn.prose, /Continuing with the active Mission/i);

    const found = eventsOf('ACTIVE_MISSION_FOUND')[0];
    assert.equal(found.found, true);
    assert.equal(found.source, 'legacy');
    assert.equal(found.runtime, 'MissionEngine');

    const pending = eventsOf('MISSION_PENDING_DECISION')[0];
    assert.equal(pending.found, true);
    assert.equal(pending.missionId, amoMission.id);

    const classifier = eventsOf('APPROVAL_CLASSIFIER')[0];
    assert.equal(classifier.missionExecutionCommand, true);
    assert.equal(classifier.executeStage, false);
    assert.equal(classifier.amoAction, 'discovery_approved');

    const pipeline = eventsOf('PIPELINE_SELECTED')[0];
    assert.equal(pipeline.pipeline, PIPELINES.MISSION_ENGINE);
    assert.equal(pipeline.claimedBy, 'maybeHandleMissionFirstTurn');

    const fallback = eventsOf('FALLBACK_REASON')[0];
    assert.ok(fallback);
    assert.equal(fallback.reason, 'legacy_mission_first_preempted_amo');
    assert.equal(fallback.breakpoint, BREAKPOINT_LEGACY_MISSION_FIRST);
    assert.equal(fallback.claimedRuntime, 'MissionEngine');

    assert.equal(eventsOf('MISSION_APPROVAL_MATCH').length, 0);
    assert.equal(eventsOf('MISSION_STAGE_EXECUTION').length, 0);
    assert.equal(listMissionApprovalAuditLog().length, 0);

    const stillWaiting = amoEngine.get(amoMission.id, '10');
    assert.ok(stillWaiting.pendingOperatorDecision);
    assert.equal(stillWaiting.pendingOperatorDecision.prompt, 'Approve discovery?');
  });
});
