'use strict';
const { createTestAmoRuntime } = require('./amoTestRuntime');

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
  isExplicitMissionResume,
} = require('../WorkspaceOwnershipResolver');
const { isMissionExecutionCommand } = require('../ExecutionLanguageDetection');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { clearWorkspaceOwnershipAuditLog } = require('../audit/WorkspaceOwnershipAudit');

const AUDIT_028_PROMPT = [
  'I want to acquire one recurring commercial cleaning client in Greater Manchester.',
  'Create or resume the appropriate Acquisition Mission.',
  'Execute every stage automatically.',
  'Pause only for human approval.',
].join('\n\n');

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

describe('SPEC-140 — Ownership-First Routing', () => {
  beforeEach(() => {
    clearWorkspaceOwnershipAuditLog();
  });

  it('detects explicit mission resume phrasing', () => {
    assert.equal(isExplicitMissionResume('Resume the acquisition mission').explicit, true);
    assert.equal(isExplicitMissionResume('Continue mission').explicit, true);
    assert.equal(
      isExplicitMissionResume('Create or resume the appropriate Acquisition Mission.').explicit,
      true
    );
    assert.equal(isExplicitMissionResume('Approved. Begin Discovery.').explicit, false);
  });

  it('execution language alone does not preempt acquisition objective ownership', () => {
    assert.equal(isMissionExecutionCommand('Execute every stage automatically.'), true);
    assert.equal(isMissionExecutionCommand('Pause only for human approval.'), true);
  });

  it('AUDIT-028 prompt selects mission_creation when AMO mission is active', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    amoEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
    });

    const ownership = await resolveWorkspaceOwner({
      question: AUDIT_028_PROMPT,
      context: { tenantId: '10' },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionEngine: stubLegacyMissionEngine(),
      missionsEnabled: true,
      resolverEnabled: true,
      session: { id: 'sess-audit-028', context: { tenantId: '10' } },
    });

    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.equal(ownership.reason, 'acquisition_objective_precedence');
    assert.notEqual(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
  });

  it('explicit resume selects mission_creation even when AMO mission is active', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    amoEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
      targetSegment: 'Law Firms',
    });

    const ownership = await resolveWorkspaceOwner({
      question: 'Resume the acquisition mission for Anchor Cleaning.',
      context: { tenantId: '10' },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionEngine: stubLegacyMissionEngine(),
      missionsEnabled: true,
      resolverEnabled: true,
      session: { id: 'sess-resume', context: { tenantId: '10' } },
    });

    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.equal(ownership.reason, 'explicit_mission_resume');
  });

  it('execution-only approval still selects active_mission when AMO is active', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    amoEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    const ownership = await resolveWorkspaceOwner({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10' },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionEngine: stubLegacyMissionEngine(),
      missionsEnabled: true,
      resolverEnabled: true,
      session: { id: 'sess-exec', context: { tenantId: '10' } },
    });

    assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(ownership.reason, 'active_mission_execution_command');
  });

  it('WorkspaceEngine.ask routes AUDIT-028 prompt to mission creation not execution', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    amoEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    const engine = createWorkspaceEngine({
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionsEnabled: true,
      missionEngine: stubLegacyMissionEngine(),
      disableLlm: true,
    });

    const result = await engine.ask({
      question: AUDIT_028_PROMPT,
      context: { tenantId: '10', clientId: '10' },
    });

    assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.match(result.prose, /Mission Created|Mission Resumed/);
    assert.doesNotMatch(result.prose, /Mission Updated/i);
  });
});
