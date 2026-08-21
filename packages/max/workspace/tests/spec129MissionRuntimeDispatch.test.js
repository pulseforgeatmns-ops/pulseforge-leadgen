'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const { WORKSPACE_OWNERS } = require('../WorkspaceOwnershipResolver');
const { MESSAGE_CLASS } = require('../../../mission-engine/classifyMessage');
const {
  resolveMissionRuntime,
  logMissionRuntimeSelected,
  MISSION_RUNTIMES,
  clearMissionRuntimeAuditLog,
  listMissionRuntimeAuditLog,
} = require('../MissionRuntimeDispatch');
const {
  listMissionApprovalAuditLog,
  clearMissionApprovalAuditLog,
} = require('../audit/MissionApprovalAudit');

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester for law firms.';

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

function realLegacyEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    resolverEnabled: true,
  });
}

describe('SPEC-129 — Mission Runtime Dispatch', () => {
  beforeEach(() => {
    clearMissionRuntimeAuditLog();
    clearMissionApprovalAuditLog();
  });

  it('selects AMO when only an acquisition mission is active', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    const selected = await resolveMissionRuntime({
      question: 'approved',
      context: { tenantId: '10' },
      acquisitionMissionEngine: amoEngine,
      missionEngine: stubLegacyMissionEngine(),
      session: { id: 's-amo', context: { tenantId: '10', missionId: mission.id } },
      missionsEnabled: true,
      resolverEnabled: true,
    });
    assert.equal(selected.runtime, MISSION_RUNTIMES.AMO);
    assert.equal(selected.mission.id, mission.id);
    assert.equal(selected.reason, 'amo_pending_approval');
  });

  it('selects SPEC-022 when only a legacy mission is bound', async () => {
    const missionEngine = realLegacyEngine();
    const created = await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: '10',
      execute: false,
    });
    await missionEngine.activeMissionResolver.bindSession({
      sessionId: 's-legacy',
      mission: created,
    });
    await missionEngine.store.update({ id: created.id, status: 'planning' });

    const selected = await resolveMissionRuntime({
      question: 'approved',
      context: { tenantId: '10' },
      missionEngine,
      session: { id: 's-legacy', context: { tenantId: '10' } },
      missionsEnabled: true,
      resolverEnabled: true,
    });
    assert.equal(selected.runtime, MISSION_RUNTIMES.SPEC_022);
    assert.equal(selected.mission.id, created.id);
    assert.equal(selected.reason, 'legacy_mission');
  });

  it('does not let SPEC-022 preempt AMO pending approval when both missions exist', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const amoMission = amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });
    const missionEngine = realLegacyEngine();
    const legacy = await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: '10',
      execute: false,
    });
    await missionEngine.activeMissionResolver.bindSession({
      sessionId: 's-both',
      mission: legacy,
    });
    await missionEngine.store.update({ id: legacy.id, status: 'planning' });

    const selected = await resolveMissionRuntime({
      question: 'approved',
      context: { tenantId: '10', missionId: legacy.id },
      acquisitionMissionEngine: amoEngine,
      missionEngine,
      session: {
        id: 's-both',
        context: { tenantId: '10', missionId: legacy.id },
      },
      missionsEnabled: true,
      resolverEnabled: true,
    });

    assert.ok(amoMission.pendingOperatorDecision);
    assert.equal(selected.runtime, MISSION_RUNTIMES.AMO);
    assert.equal(selected.reason, 'amo_pending_approval');
    assert.equal(selected.mission.id, amoMission.id);
    assert.notEqual(selected.mission.id, legacy.id);
  });

  it('MISSION_RUNTIME_SELECTED never logs an ambiguous runtime', () => {
    logMissionRuntimeSelected({ runtime: 'maybe-both', reason: 'invalid' });
    logMissionRuntimeSelected({ runtime: MISSION_RUNTIMES.AMO, reason: 'amo_pending_approval' });
    logMissionRuntimeSelected({ runtime: MISSION_RUNTIMES.SPEC_022, reason: 'legacy_mission' });
    const events = listMissionRuntimeAuditLog().filter(
      (row) => row.event === 'MISSION_RUNTIME_SELECTED'
    );
    assert.equal(events[0].runtime, null);
    assert.equal(events[1].runtime, MISSION_RUNTIMES.AMO);
    assert.equal(events[2].runtime, MISSION_RUNTIMES.SPEC_022);
  });

  it('WorkspaceEngine.ask routes approved to AMO even when a SPEC-022 mission is bound', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const amoMission = amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });
    const missionEngine = realLegacyEngine();
    const legacy = await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: '10',
      execute: false,
    });
    await missionEngine.store.update({ id: legacy.id, status: 'planning' });

    const workspace = createWorkspaceEngine({
      acquisitionMissionEngine: amoEngine,
      missionsEnabled: true,
      missionEngine,
      resolverEnabled: true,
      disableLlm: true,
    });

    const opened = workspace.open({ tenantId: '10', page: 'command-deck' });
    const session = workspace._sessions.get(opened.sessionId);
    await missionEngine.activeMissionResolver.bindSession({
      sessionId: opened.sessionId,
      mission: legacy,
    });
    session.context.missionId = legacy.id;
    session.executionDomain = 'workspace';

    clearMissionRuntimeAuditLog();
    clearMissionApprovalAuditLog();

    const turn = await workspace.ask({
      sessionId: opened.sessionId,
      question: 'approved',
    });

    assert.equal(turn.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.match(turn.prose, /Mission Updated/i);
    assert.match(turn.prose, /Prioritization approval|Scout/i);
    assert.doesNotMatch(turn.prose, /Continuing with the active Mission/i);
    assert.doesNotMatch(turn.prose, /Approve discovery\?/i);
    assert.equal(turn.mission && turn.mission.id, amoMission.id);

    const runtimeEvents = listMissionRuntimeAuditLog().filter(
      (row) => row.event === 'MISSION_RUNTIME_SELECTED'
    );
    assert.ok(runtimeEvents.length >= 1);
    const selected = runtimeEvents[runtimeEvents.length - 1];
    assert.equal(selected.runtime, MISSION_RUNTIMES.AMO);

    const approvalEvents = listMissionApprovalAuditLog().map((row) => row.event);
    assert.ok(approvalEvents.includes('MISSION_APPROVAL_MATCHED'));
    assert.ok(approvalEvents.includes('MISSION_STAGE_EXECUTION_STARTED'));
    assert.ok(approvalEvents.includes('MISSION_STAGE_EXECUTION_COMPLETED'));

    const after = amoEngine.get(amoMission.id, '10');
    assert.equal(after.pendingOperatorDecision, null);
  });

  it('WorkspaceEngine.ask still resumes a legacy mission when no AMO is active', async () => {
    const missionEngine = realLegacyEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      resolverEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: { tenantId: '10', page: 'command-deck' },
    });
    assert.equal(first.route, 'mission');
    await missionEngine.store.update({
      id: first.mission.id,
      status: 'planning',
    });

    clearMissionRuntimeAuditLog();

    const second = await workspace.ask({
      sessionId: first.sessionId,
      question: 'approved',
    });

    assert.equal(second.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(second.resolution.action, 'resumed');
    assert.equal(second.resolution.classification, MESSAGE_CLASS.RESUME);
    assert.match(second.prose, /Continuing with the active Mission/i);

    const selected = listMissionRuntimeAuditLog().find(
      (row) => row.event === 'MISSION_RUNTIME_SELECTED'
    );
    assert.ok(selected);
    assert.equal(selected.runtime, MISSION_RUNTIMES.SPEC_022);
  });

  it('WorkspaceEngine.ask still executes legacy Discovery for Approved. Begin Scout discovery.', async () => {
    const missionEngine = realLegacyEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      resolverEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: { tenantId: '10', page: 'command-deck' },
    });
    await missionEngine.store.update({
      id: first.mission.id,
      status: 'planning',
    });

    clearMissionRuntimeAuditLog();

    const second = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Approved. Begin Scout discovery.',
    });

    assert.equal(second.resolution.action, 'executed');
    assert.equal(second.resolution.classification, MESSAGE_CLASS.EXECUTE_STAGE);
    const selected = listMissionRuntimeAuditLog().find(
      (row) => row.event === 'MISSION_RUNTIME_SELECTED'
    );
    assert.equal(selected.runtime, MISSION_RUNTIMES.SPEC_022);
  });
});
