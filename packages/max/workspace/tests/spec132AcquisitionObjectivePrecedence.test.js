'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const { clearWorkspaceOwnershipAuditLog } = require('../audit/WorkspaceOwnershipAudit');

const ACQUIRE_OBJECTIVE =
  'I want to acquire one recurring commercial cleaning client in Greater Manchester.';

const LEGACY_MISSION = {
  id: 'mission_legacy_001',
  title: 'Campaign 001 for Anchor Cleaning',
  objectiveText: 'Build Campaign 001 for Anchor Cleaning.',
  status: 'planning',
  type: 'campaign',
};

function stubLegacyResolver(activeMission = LEGACY_MISSION) {
  return {
    activeMissionResolver: {
      resolveActiveMission: async () => activeMission,
      resolve: async () => ({ action: 'intelligence' }),
      clearActiveMission: async () => {},
    },
    toCard: (m) => m,
  };
}

describe('SPEC-132 — Acquisition Objective Overrides Legacy Continuation', () => {
  beforeEach(() => {
    clearWorkspaceOwnershipAuditLog();
  });

  it('clean session: acquisition objective selects mission_creation', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: ACQUIRE_OBJECTIVE,
      context: { tenantId: '10' },
      session: { id: 'sess-clean', context: { tenantId: '10' } },
      missionEngine: stubLegacyResolver(null),
      missionsEnabled: true,
      resolverEnabled: true,
    });

    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.equal(ownership.reason, 'acquisition_objective_precedence');
  });

  it('session with bound SPEC-022 mission: acquisition objective beats legacy continuation', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: ACQUIRE_OBJECTIVE,
      context: { tenantId: '10' },
      session: { id: 'sess-legacy', context: { tenantId: '10' } },
      missionEngine: stubLegacyResolver(LEGACY_MISSION),
      missionsEnabled: true,
      resolverEnabled: true,
    });

    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.equal(ownership.reason, 'acquisition_objective_precedence');
    assert.notEqual(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
  });

  it('non-acquisition continuation still selects active_mission for legacy session', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: 'continue yesterday\'s mission',
      context: { tenantId: '10' },
      session: { id: 'sess-legacy-resume', context: { tenantId: '10' } },
      missionEngine: stubLegacyResolver(LEGACY_MISSION),
      missionsEnabled: true,
      resolverEnabled: true,
    });

    assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.match(ownership.reason, /^active_mission_/);
  });

  it('resume outreach still selects active_mission for legacy session', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: 'resume outreach',
      context: { tenantId: '10' },
      session: { id: 'sess-legacy-outreach', context: { tenantId: '10' } },
      missionEngine: stubLegacyResolver(LEGACY_MISSION),
      missionsEnabled: true,
      resolverEnabled: true,
    });

    assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.match(ownership.reason, /^active_mission_/);
  });

  it('WorkspaceEngine.ask creates AMO when legacy mission is bound to session', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const missionEngine = createMissionEngine({
      registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
      resolverEnabled: true,
    });

    const workspace = createWorkspaceEngine({
      acquisitionMissionEngine: amoEngine,
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
    assert.ok(first.mission);

    await missionEngine.store.update({
      id: first.mission.id,
      status: 'planning',
    });

    const second = await workspace.ask({
      sessionId: first.sessionId,
      question: ACQUIRE_OBJECTIVE,
      context: { tenantId: '10', clientId: '10' },
    });

    assert.equal(second.workspaceOwnership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.match(second.prose, /Mission Created|Mission Resumed/);
    assert.ok(second.mission);
    assert.equal(second.mission.objective, ACQUIRE_OBJECTIVE);
  });
});
