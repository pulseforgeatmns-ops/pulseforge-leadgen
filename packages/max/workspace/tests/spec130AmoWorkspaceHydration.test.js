'use strict';

/**
 * SPEC-130 — Hydrate AMO missions before workspace runtime resolution.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const { STAGES } = amo;
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  resolveAcquisitionActiveMission,
  resolveActiveMissionLock,
} = require('../ActiveMissionGuard');
const {
  resolveMissionRuntime,
  MISSION_RUNTIMES,
  clearMissionRuntimeAuditLog,
} = require('../MissionRuntimeDispatch');
const {
  ensureAmoTenantHydrated,
  listAmoHydrationAuditLog,
  clearAmoHydrationAuditLog,
  clearAmoHydrationCache,
} = require('../AmoWorkspaceHydration');
const {
  clearMissionApprovalAuditLog,
  listMissionApprovalAuditLog,
} = require('../audit/MissionApprovalAudit');
const { WORKSPACE_OWNERS } = require('../WorkspaceOwnershipResolver');
const {
  createHydratingTestRuntime,
  runtimeProviderFromEngine,
} = require('./amoTestRuntime');

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function realLegacyEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    resolverEnabled: true,
  });
}

function buildPersistedAmoFixture() {
  const source = amo.createAcquisitionMissionEngine();
  const mission = source.create({
    tenantId: '10',
    objective: ANCHOR_OBJECTIVE,
    targetSegment: 'Law Firms',
    planApproved: true,
  });
  return { source, mission };
}

describe('SPEC-130 — AMO workspace hydration', () => {
  beforeEach(() => {
    clearAmoHydrationAuditLog();
    clearAmoHydrationCache();
    clearMissionRuntimeAuditLog();
    clearMissionApprovalAuditLog();
  });

  it('ensureAmoTenantHydrated loads missions and emits audit events', async () => {
    const { source } = buildPersistedAmoFixture();
    const runtime = createHydratingTestRuntime(source, { persist: true, pool: null });

    const session = { id: 's-hydrate', context: { tenantId: '10' } };
    const result = await ensureAmoTenantHydrated({
      session,
      context: session.context,
      acquisitionMissionRuntime: runtime,
    });

    assert.equal(result.hydrated, true);
    assert.equal(result.missionsLoaded, 1);

    const events = listAmoHydrationAuditLog().map((row) => row.event);
    assert.ok(events.includes('AMO_HYDRATE_BEGIN'));
    assert.ok(events.includes('AMO_HYDRATE_COMPLETE'));
    assert.ok(events.includes('missionsLoaded'));

    const cached = await ensureAmoTenantHydrated({
      session,
      context: session.context,
      acquisitionMissionRuntime: runtime,
    });
    assert.equal(cached.skipped, 'session_cached');
  });

  it('resolveAcquisitionActiveMission hydrates an empty singleton and returns persisted mission', async () => {
    const { source, mission } = buildPersistedAmoFixture();
    const runtime = createHydratingTestRuntime(source, { persist: true, pool: null });
    const runtimeEngine = runtime.engine();

    assert.equal(runtimeEngine.list('10').length, 0);

    const resolved = await resolveAcquisitionActiveMission({
      context: { tenantId: '10' },
      session: { id: 's-resolve', context: { tenantId: '10' } },
      acquisitionMissionRuntime: runtime,
    });

    assert.equal(resolved.id, mission.id);
    assert.equal(runtimeEngine.list('10').length, 1);
    assert.ok(
      listAmoHydrationAuditLog().some((row) => row.event === 'AMO_ACTIVE_RESOLVED')
    );
  });

  it('resolveActiveMissionLock prefers hydrated AMO before legacy mission', async () => {
    const { source, mission } = buildPersistedAmoFixture();
    const runtime = createHydratingTestRuntime(source, { persist: true, pool: null });

    const missionEngine = realLegacyEngine();
    const legacy = await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: '10',
      execute: false,
    });
    await missionEngine.store.update({ id: legacy.id, status: 'planning' });

    const session = {
      id: 's-lock',
      context: { tenantId: '10', missionId: legacy.id },
    };
    await missionEngine.activeMissionResolver.bindSession({ sessionId: session.id, mission: legacy });

    const lock = await resolveActiveMissionLock({
      question: 'approved',
      session,
      context: session.context,
      acquisitionMissionRuntime: runtime,
      missionEngine,
      missionsEnabled: true,
      resolverEnabled: true,
    });

    assert.equal(lock.active, true);
    assert.equal(lock.source, 'amo');
    assert.equal(lock.missionId, mission.id);
  });

  it('runtime dispatch selects AMO after hydration when legacy is also bound', async () => {
    const { source, mission } = buildPersistedAmoFixture();
    const runtime = createHydratingTestRuntime(source, { persist: true, pool: null });

    const missionEngine = realLegacyEngine();
    const legacy = await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: '10',
      execute: false,
    });
    await missionEngine.store.update({ id: legacy.id, status: 'planning' });

    const runtimeDecision = await resolveMissionRuntime({
      question: 'approved',
      context: { tenantId: '10' },
      session: { id: 's-runtime', context: { tenantId: '10', missionId: legacy.id } },
      acquisitionMissionRuntime: runtime,
      missionEngine,
      missionsEnabled: true,
      resolverEnabled: true,
    });

    assert.equal(runtimeDecision.runtime, MISSION_RUNTIMES.AMO);
    assert.equal(runtimeDecision.reason, 'amo_pending_approval');
    assert.equal(runtimeDecision.mission.id, mission.id);
    assert.notEqual(runtimeDecision.mission.id, legacy.id);
  });

  it('Workspace ask executes Discovery after hydration with empty singleton store', async () => {
    const { source, mission } = buildPersistedAmoFixture();
    const runtime = createHydratingTestRuntime(source, { persist: true, pool: null });
    const runtimeEngine = runtime.engine();

    const missionEngine = realLegacyEngine();
    const legacy = await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: '10',
      execute: false,
    });
    await missionEngine.store.update({ id: legacy.id, status: 'planning' });

    const workspace = createWorkspaceEngine({
      runtimeProvider: () => runtime,
      missionEngine,
      missionsEnabled: true,
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

    assert.equal(runtimeEngine.list('10').length, 0);

    const turn = await workspace.ask({
      sessionId: opened.sessionId,
      question: 'approved',
    });

    assert.equal(runtimeEngine.list('10').length, 1);
    assert.equal(turn.workspaceOwnership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(turn.workspaceOwnership.missionRuntime, MISSION_RUNTIMES.AMO);
    assert.equal(turn.resolution.action, 'executed');
    assert.equal(turn.resolution.reason, 'acquisition_mission_discovery_approved');
    assert.equal(turn.mission.id, mission.id);

    const approvalEvents = listMissionApprovalAuditLog().map((row) => row.event);
    assert.ok(approvalEvents.includes('MISSION_APPROVAL_MATCHED'));
    assert.ok(approvalEvents.includes('MISSION_STAGE_EXECUTION_COMPLETED'));

    const after = runtimeEngine.get(mission.id, '10');
    assert.equal(after.stage, STAGES.UNDERSTAND);
    assert.equal(after.pendingOperatorDecision, null);
  });

  it('falls back to SPEC-022 only when hydration succeeds with no AMO missions', async () => {
    const runtimeEngine = amo.createAcquisitionMissionEngine();
    const runtime = createHydratingTestRuntime(runtimeEngine, { persist: false });
    runtime.hydrate = async () => runtimeEngine;

    const missionEngine = realLegacyEngine();
    const legacy = await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: '10',
      execute: false,
    });
    await missionEngine.store.update({ id: legacy.id, status: 'planning' });

    const session = { id: 's-legacy-only', context: { tenantId: '10', missionId: legacy.id } };
    await missionEngine.activeMissionResolver.bindSession({ sessionId: session.id, mission: legacy });

    const runtimeDecision = await resolveMissionRuntime({
      question: 'approved',
      session,
      context: session.context,
      acquisitionMissionRuntime: runtime,
      missionEngine,
      missionsEnabled: true,
      resolverEnabled: true,
    });

    assert.equal(runtimeDecision.runtime, MISSION_RUNTIMES.SPEC_022);
    assert.equal(runtimeDecision.mission.id, legacy.id);
  });
});
