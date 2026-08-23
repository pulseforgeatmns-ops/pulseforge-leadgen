'use strict';
const { createTestAmoRuntime, runtimeProviderFromEngine, createHydratingTestRuntime } = require('./amoTestRuntime');

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  isMissionExecutionCommand,
  isExplicitMissionExit,
  guardExecutionDomain,
  resolveActiveMissionLock,
} = require('../ActiveMissionGuard');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const {
  selectExecutionDomain,
  EXECUTION_DOMAINS,
} = require('../ExecutionDomain');
const {
  createWorkspaceOwnershipAudit,
  clearWorkspaceOwnershipAuditLog,
} = require('../audit/WorkspaceOwnershipAudit');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

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

describe('SPEC-127 — Active Mission Lock', () => {
  beforeEach(() => {
    clearWorkspaceOwnershipAuditLog();
  });

  it('detects mission execution commands', () => {
    assert.equal(isMissionExecutionCommand('Approved. Begin Discovery.'), true);
    assert.equal(isMissionExecutionCommand('continue'), true);
    assert.equal(isMissionExecutionCommand('Hello there'), false);
  });

  it('detects explicit mission exit commands', () => {
    assert.equal(isExplicitMissionExit("today's briefing").explicit, true);
    assert.equal(isExplicitMissionExit('new topic').explicit, true);
    assert.equal(isExplicitMissionExit('leave mission').explicit, true);
    assert.equal(isExplicitMissionExit('Approved. Begin Discovery.').explicit, false);
  });

  it('selects active_mission for execution commands when AMO is active', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const ownership = await resolveWorkspaceOwner({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10' },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionEngine: stubLegacyMissionEngine(),
      missionsEnabled: true,
      resolverEnabled: true,
      session: { id: 'sess-127', context: { tenantId: '10' } },
    });

    assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(ownership.reason, 'active_mission_execution_command');
  });

  it('blocks General Conversation domain while mission is active', () => {
    const decision = selectExecutionDomain('Hello', {
      previousDomain: EXECUTION_DOMAINS.WORKSPACE,
      activeMissionLock: true,
      explicitMissionExit: false,
    });
    assert.equal(decision.domain, EXECUTION_DOMAINS.WORKSPACE);
    assert.equal(decision.reason, 'active_mission_lock');
    assert.equal(decision.activeMissionGuard, true);
    assert.equal(decision.blockedDomain, EXECUTION_DOMAINS.GENERAL_CONVERSATION);
  });

  it('blocks Daily Briefing domain while mission is active', () => {
    const decision = selectExecutionDomain("What's in today's briefing?", {
      previousDomain: EXECUTION_DOMAINS.WORKSPACE,
      activeMissionLock: true,
      explicitMissionExit: false,
    });
    assert.equal(decision.domain, EXECUTION_DOMAINS.WORKSPACE);
    assert.equal(decision.reason, 'active_mission_lock');
    assert.equal(decision.blockedDomain, EXECUTION_DOMAINS.MORNING_BRIEFING);
  });

  it('allows Daily Briefing after explicit mission exit', () => {
    const decision = selectExecutionDomain("today's briefing", {
      previousDomain: EXECUTION_DOMAINS.WORKSPACE,
      activeMissionLock: true,
      explicitMissionExit: true,
    });
    assert.equal(decision.domain, EXECUTION_DOMAINS.MORNING_BRIEFING);
  });

  it('guardExecutionDomain blocks general conversation fallback', () => {
    const lock = {
      active: true,
      explicitExit: false,
      executionCommand: true,
      source: 'amo',
      missionId: 'mission_1',
    };
    const blocked = guardExecutionDomain(
      {
        domain: EXECUTION_DOMAINS.GENERAL_CONVERSATION,
        reason: 'general_conversation',
        previousDomain: EXECUTION_DOMAINS.WORKSPACE,
      },
      lock,
      'Approved. Begin Discovery.'
    );
    assert.equal(blocked.guarded, true);
    assert.equal(blocked.decision.domain, EXECUTION_DOMAINS.WORKSPACE);
    assert.equal(blocked.decision.reason, 'mission_execution_command');
  });

  it('WorkspaceEngine advances AMO on Approved. Begin Discovery.', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    const engine = createWorkspaceEngine({
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionsEnabled: true,
      missionEngine: stubLegacyMissionEngine(),
      disableLlm: true,
    });

    const opened = engine.open({ tenantId: '10', page: 'command-deck' });
    const session = engine._sessions.get(opened.sessionId);
    session.executionDomain = EXECUTION_DOMAINS.WORKSPACE;

    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Approved. Begin Discovery.',
    });

    assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(result.executionDomain, EXECUTION_DOMAINS.WORKSPACE);
    assert.equal(result.domainSwitch, null);
    assert.doesNotMatch(result.prose, /Switching from .* to General Conversation/i);
    assert.doesNotMatch(result.prose, /Today's Briefing/i);
    assert.match(result.prose, /Mission Updated/i);
    assert.match(result.prose, /Discover/i);
    assert.ok(result.mission);
  });

  it('legacy Approved. Begin Scout discovery executes Discovery stage', async () => {
    const missionEngine = createMissionEngine({
      registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
      resolverEnabled: true,
    });
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

    const second = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Approved. Begin Scout discovery.',
    });

    assert.equal(second.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(second.resolution.action, 'executed');
    assert.doesNotMatch(second.prose, /Switching from .* to General Conversation/i);
    assert.doesNotMatch(second.prose, /Today's Briefing/i);
  });

  it('emits ACTIVE_MISSION_GUARD and MISSION_OWNER_SELECTED audit events', async () => {
    clearWorkspaceOwnershipAuditLog();
    const amoEngine = amo.createAcquisitionMissionEngine();
    amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const engine = createWorkspaceEngine({
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionsEnabled: true,
      missionEngine: stubLegacyMissionEngine(),
      disableLlm: true,
    });

    const opened = engine.open({ tenantId: '10', page: 'command-deck' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Approved. Begin Discovery.',
    });

    const auditLog = require('../audit/WorkspaceOwnershipAudit').listWorkspaceOwnershipAuditLog();
    assert.ok(auditLog.some((row) => row.event === 'MISSION_OWNER_SELECTED'));
    assert.ok(
      auditLog.some(
        (row) =>
          row.event === 'WORKSPACE_OWNER_SELECTED' &&
          row.owner === WORKSPACE_OWNERS.ACTIVE_MISSION
      )
    );
  });

  it('resolveActiveMissionLock finds AMO mission for tenant', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
    });
    const lock = await resolveActiveMissionLock({
      question: 'continue',
      context: { tenantId: '10' },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionEngine: stubLegacyMissionEngine(),
      session: { id: 's1', context: { tenantId: '10' } },
    });
    assert.equal(lock.active, true);
    assert.equal(lock.source, 'amo');
    assert.equal(lock.missionId, mission.id);
  });
});
