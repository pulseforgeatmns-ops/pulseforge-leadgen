'use strict';
const { createTestAmoRuntime, runtimeProviderFromEngine, createHydratingTestRuntime } = require('./amoTestRuntime');

/**
 * AUDIT-008 — Active AMO Mission · stage=discover · pending Approve discovery · operator input="approved"
 * Regression test locking the deterministic workspace execution trace from operator approval to response.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const invocationTrace = {
  maybeHandleMissionFirstTurn: 0,
  maybeHandleAcquisitionMissionExecution: 0,
  composeResponse: 0,
  advanceDiscoveryAfterApproval: 0,
  buildDiscoveryApprovalProse: 0,
};

function instrumentExport(modPath, exportName, counterKey, { before, after } = {}) {
  const mod = require(modPath);
  const original = mod[exportName];
  if (typeof original !== 'function') {
    throw new Error(`instrumentExport: ${exportName} is not a function in ${modPath}`);
  }
  mod[exportName] = async function instrumentedExport(...args) {
    invocationTrace[counterKey] += 1;
    if (before) before(...args);
    const result = await original.apply(this, args);
    if (after) after(result, args);
    return result;
  };
  return original;
}

function instrumentSyncExport(modPath, exportName, counterKey) {
  const mod = require(modPath);
  const original = mod[exportName];
  if (typeof original !== 'function') {
    throw new Error(`instrumentSyncExport: ${exportName} is not a function in ${modPath}`);
  }
  mod[exportName] = function instrumentedSyncExport(...args) {
    invocationTrace[counterKey] += 1;
    return original.apply(this, args);
  };
  return original;
}

instrumentExport('../MissionFirstRouting.js', 'maybeHandleMissionFirstTurn', 'maybeHandleMissionFirstTurn');
instrumentSyncExport('../ResponseComposer.js', 'composeResponse', 'composeResponse');
instrumentExport('../AmoOperatorApproval.js', 'advanceDiscoveryAfterApproval', 'advanceDiscoveryAfterApproval');
instrumentSyncExport('../AmoOperatorApproval.js', 'buildDiscoveryApprovalProse', 'buildDiscoveryApprovalProse');

const amo = require('../../../acquisition-mission');
const { STAGES } = amo;
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const {
  detectExecutionAction,
  shouldExecuteDiscovery,
} = require('../AcquisitionMissionExecution');
const { resolveWorkspaceOwner, WORKSPACE_OWNERS } = require('../WorkspaceOwnershipResolver');
const {
  resolveMissionRuntime,
  MISSION_RUNTIMES,
  clearMissionRuntimeAuditLog,
  listMissionRuntimeAuditLog,
} = require('../MissionRuntimeDispatch');
const {
  clearMissionApprovalAuditLog,
  listMissionApprovalAuditLog,
} = require('../audit/MissionApprovalAudit');
const { createWorkspaceEngine } = require('../WorkspaceEngine');

function instrumentCachedExport(fileSuffix, exportName, counterKey) {
  for (const file of Object.keys(require.cache)) {
    if (!file.endsWith(fileSuffix)) continue;
    const mod = require.cache[file].exports;
    if (!mod || typeof mod[exportName] !== 'function') continue;
    if (mod[exportName].name === 'instrumentedExport') continue;
    const original = mod[exportName];
    mod[exportName] = async function instrumentedExport(...args) {
      invocationTrace[counterKey] += 1;
      return original.apply(this, args);
    };
  }
}

instrumentCachedExport('AcquisitionMissionExecution.js', 'maybeHandleAcquisitionMissionExecution', 'maybeHandleAcquisitionMissionExecution');

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function realLegacyEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    resolverEnabled: true,
  });
}

function resetTrace() {
  for (const key of Object.keys(invocationTrace)) {
    invocationTrace[key] = 0;
  }
}

describe('AUDIT-008 active AMO discovery approval execution trace', { concurrency: false }, () => {
  let amoEngine;
  let amoMission;
  let missionEngine;
  let workspace;
  let sessionId;

  beforeEach(() => {
    resetTrace();
    clearMissionRuntimeAuditLog();
    clearMissionApprovalAuditLog();

    amoEngine = amo.createAcquisitionMissionEngine();
    amoMission = amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });
  });

  function openAmoSession(engineOpts = {}) {
    workspace = createWorkspaceEngine({
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionsEnabled: true,
      resolverEnabled: true,
      disableLlm: true,
      ...engineOpts,
    });

    const opened = workspace.open({ tenantId: '10', page: 'command-deck' });
    sessionId = opened.sessionId;
    const session = workspace._sessions.get(sessionId);
    session.context.missionId = amoMission.id;
    session.context.acquisitionMissionId = amoMission.id;
    return session;
  }

  it('routes operator "approved" through AMO execution and leaves discover pending prioritization review', async () => {
    assert.equal(amoMission.stage, STAGES.DISCOVER);
    assert.ok(amoMission.pendingOperatorDecision);
    assert.equal(amoMission.pendingOperatorDecision.prompt, 'Approve discovery?');

    const session = openAmoSession();
    const snapshotBefore = amoEngine.inspect(amoMission.id, { tenantId: '10' });
    assert.equal(detectExecutionAction('approved', snapshotBefore), 'discovery_approved');
    assert.equal(shouldExecuteDiscovery('discovery_approved', snapshotBefore), true);

    const ownership = await resolveWorkspaceOwner({
      question: 'approved',
      session,
      context: session.context,
      missionsEnabled: true,
      resolverEnabled: true,
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(ownership.reason, 'active_mission_execution_command');

    const runtimeDecision = await resolveMissionRuntime({
      question: 'approved',
      session,
      context: session.context,
      missionsEnabled: true,
      resolverEnabled: true,
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
    });
    assert.equal(runtimeDecision.runtime, MISSION_RUNTIMES.AMO);
    assert.equal(runtimeDecision.reason, 'amo_pending_approval');
    assert.equal(runtimeDecision.mission.id, amoMission.id);

    const turn = await workspace.ask({
      sessionId,
      question: 'approved',
    });

    assert.equal(invocationTrace.maybeHandleAcquisitionMissionExecution, 1);
    assert.equal(invocationTrace.advanceDiscoveryAfterApproval, 1);
    // Live ask path renders through buildExecutionMissionResponse, not buildDiscoveryApprovalProse.
    assert.equal(invocationTrace.buildDiscoveryApprovalProse, 0);
    assert.equal(invocationTrace.maybeHandleMissionFirstTurn, 0);
    assert.equal(invocationTrace.composeResponse, 0);

    assert.equal(turn.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(turn.workspaceOwnership.missionRuntime, MISSION_RUNTIMES.AMO);
    assert.equal(turn.resolution.action, 'executed');
    assert.equal(turn.resolution.reason, 'acquisition_mission_discovery_approved');
    assert.equal(turn.route, 'intelligence');
    assert.notEqual(turn.route, 'mission');
    assert.equal(turn.mission.id, amoMission.id);

    assert.match(turn.prose, /Mission Updated/i);
    assert.doesNotMatch(turn.prose, /Continuing with the active Mission/i);
    assert.doesNotMatch(turn.prose, /Approve discovery\?/i);
    assert.equal(turn.structured.metadata.missionCommunication, true);
    assert.ok(turn.structured.metadata.missionCommunicationPayload);
    assert.equal(
      turn.structured.metadata.missionCommunicationPayload.headline,
      'Mission Updated'
    );
    assert.equal(turn.structured.metadata.acquisitionMission, true);
    assert.match(
      String(
        (turn.structured.metadata.reasoningEvidence &&
          turn.structured.metadata.reasoningEvidence.inference &&
          turn.structured.metadata.reasoningEvidence.inference[0]) ||
          ''
      ),
      /Operator approval consumed/i
    );

    const runtimeEvents = listMissionRuntimeAuditLog().filter(
      (row) => row.event === 'MISSION_RUNTIME_SELECTED'
    );
    assert.ok(runtimeEvents.length >= 1);
    assert.equal(runtimeEvents[runtimeEvents.length - 1].runtime, MISSION_RUNTIMES.AMO);
    assert.equal(runtimeEvents[runtimeEvents.length - 1].reason, 'amo_pending_approval');

    const approvalEvents = listMissionApprovalAuditLog().map((row) => row.event);
    assert.ok(approvalEvents.includes('MISSION_APPROVAL_MATCHED'));
    assert.ok(approvalEvents.includes('MISSION_APPROVAL_RECEIVED'));
    assert.ok(approvalEvents.includes('MISSION_APPROVAL_CONSUMED'));
    assert.ok(approvalEvents.includes('MISSION_STAGE_EXECUTION_STARTED'));
    assert.ok(approvalEvents.includes('MISSION_STAGE_EXECUTION_COMPLETED'));

    const after = amoEngine.get(amoMission.id, '10');
    assert.equal(after.stage, STAGES.DISCOVER);
    assert.equal(after.pendingOperatorDecision.kind, amo.OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL);
  });

  it('does not let bound SPEC-022 mission preempt AMO approval on the same turn', async () => {
    missionEngine = realLegacyEngine();
    openAmoSession({ missionEngine, resolverEnabled: true });

    const legacy = await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: '10',
      execute: false,
    });
    await missionEngine.activeMissionResolver.bindSession({ sessionId, mission: legacy });
    await missionEngine.store.update({ id: legacy.id, status: 'planning' });

    resetTrace();
    clearMissionRuntimeAuditLog();
    clearMissionApprovalAuditLog();

    const turn = await workspace.ask({
      sessionId,
      question: 'approved',
    });

    assert.equal(invocationTrace.maybeHandleAcquisitionMissionExecution, 1);
    assert.equal(invocationTrace.maybeHandleMissionFirstTurn, 0);
    assert.equal(invocationTrace.composeResponse, 0);
    assert.equal(turn.mission.id, amoMission.id);
    assert.notEqual(turn.mission.id, legacy.id);
    assert.equal(turn.workspaceOwnership.missionRuntime, MISSION_RUNTIMES.AMO);
    assert.notEqual(turn.resolution.action, 'resumed');

    const after = amoEngine.get(amoMission.id, '10');
    assert.equal(after.stage, STAGES.DISCOVER);
    assert.equal(after.pendingOperatorDecision.kind, amo.OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL);
  });
});
