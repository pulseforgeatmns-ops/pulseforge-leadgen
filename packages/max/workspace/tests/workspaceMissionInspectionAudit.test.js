'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  maybeHandleWorkspaceMissionInspection,
  PIPELINE_MISSION_INSPECTION,
} = require('../WorkspaceMissionInspection');
const {
  createWorkspaceMissionInspectionAudit,
  listWorkspaceMissionInspectionAuditLog,
  clearWorkspaceMissionInspectionAuditLog,
  buildOwnershipTrace,
} = require('../audit/WorkspaceMissionInspectionAudit');
const { createWorkspaceEngine } = require('../WorkspaceEngine');

function lawFirmMission(engine, overrides = {}) {
  const mission = engine.create({
    tenantId: '10',
    objective: 'Acquire commercial cleaning customers in Manchester NH.',
    targetSegment: 'Commercial Law Firms',
    campaign: 'Fall Outreach',
    confidence: 0.84,
    ...overrides,
  });
  engine.contribute(mission.id, {
    specialist: 'scout',
    payload: {
      companies: Array.from({ length: 61 }, (_, i) => ({ id: i + 1 })),
      evidence: ['places'],
      qualifiedCount: 61,
    },
  });
  engine.contribute(mission.id, {
    specialist: 'max',
    payload: {
      priorities: ['law firms'],
      objectiveReason: 'Commercial revenue remains primary objective.',
      recommendations: ['prioritize ops hires'],
    },
  });
  engine.progress(mission.id, { role: 'max' }, { stage: amo.STAGES.UNDERSTAND });
  engine.progress(mission.id, { role: 'max' }, { stage: amo.STAGES.PLAN });
  engine.progress(mission.id, { role: 'max' }, { stage: amo.STAGES.PREPARE });
  return mission;
}

describe('AUDIT-005 — Workspace Mission Inspection Integration', () => {
  beforeEach(() => {
    clearWorkspaceMissionInspectionAuditLog();
  });

  it('emits WORKSPACE_REQUEST at workspace entry', async () => {
    const audit = createWorkspaceMissionInspectionAudit();
    const amoEngine = amo.createAcquisitionMissionEngine();
    lawFirmMission(amoEngine);

    await maybeHandleWorkspaceMissionInspection({
      question: 'Why is the progress 40%?',
      context: { tenantId: '10' },
      acquisitionMissionEngine: amoEngine,
      audit,
      silentInspection: true,
    });

    const request = audit.log.find((row) => row.event === 'WORKSPACE_REQUEST');
    assert.ok(request);
    assert.equal(request.question, 'Why is the progress 40%?');
    assert.equal(request.workspace, 'max');
    assert.ok(request.timestamp);
    assert.equal(request.owner, 'WorkspaceEngine');
  });

  it('emits WORKSPACE_ACTIVE_MISSION with mission metadata', async () => {
    const audit = createWorkspaceMissionInspectionAudit();
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = lawFirmMission(amoEngine);

    await maybeHandleWorkspaceMissionInspection({
      question: 'Why is the progress 40%?',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionEngine: amoEngine,
      audit,
      silentInspection: true,
    });

    const active = audit.log.find((row) => row.event === 'WORKSPACE_ACTIVE_MISSION');
    assert.ok(active);
    assert.equal(active.missionFound, true);
    assert.equal(active.missionId, mission.id);
    assert.ok(active.stage);
    assert.ok(active.status);
  });

  it('attempts WORKSPACE_MISSION_INSPECTION and claims progress property', async () => {
    const audit = createWorkspaceMissionInspectionAudit();
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = lawFirmMission(amoEngine);

    const turn = await maybeHandleWorkspaceMissionInspection({
      question: 'Why is the progress 40%?',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionEngine: amoEngine,
      audit,
      silentInspection: true,
    });

    const inspection = audit.log.find((row) => row.event === 'WORKSPACE_MISSION_INSPECTION');
    assert.ok(inspection);
    assert.equal(inspection.attempted, true);

    const result = audit.log.find((row) => row.event === 'MISSION_INSPECTION_RESULT');
    assert.ok(result);
    assert.equal(result.claimed, true);
    assert.equal(result.property, 'progress');
    assert.equal(result.missionId, mission.id);

    assert.ok(turn);
    assert.equal(turn.reason, 'mission_inspection');
    assert.match(turn.prose, /Mission Progress/);
  });

  it('selects MissionInspection pipeline and blocks retrieval for claimed properties', async () => {
    const audit = createWorkspaceMissionInspectionAudit();
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = lawFirmMission(amoEngine);

    await maybeHandleWorkspaceMissionInspection({
      question: 'Why is the progress 40%?',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionEngine: amoEngine,
      audit,
      silentInspection: true,
    });

    const pipeline = audit.log.find((row) => row.event === 'WORKSPACE_PIPELINE');
    assert.ok(pipeline);
    assert.equal(pipeline.selectedPipeline, PIPELINE_MISSION_INSPECTION);
    assert.equal(pipeline.reason, 'property_claimed');

    const guard = audit.log.find((row) => row.event === 'MISSION_PROPERTY_GUARD');
    assert.ok(guard);
    assert.equal(guard.property, 'progress');
    assert.equal(guard.retrievalBlocked, true);
  });

  it('produces canonical ownership trace for claimed progress inspection', async () => {
    const audit = createWorkspaceMissionInspectionAudit();
    const amoEngine = amo.createAcquisitionMissionEngine();
    lawFirmMission(amoEngine);

    const turn = await maybeHandleWorkspaceMissionInspection({
      question: 'Why is the progress 40%?',
      context: { tenantId: '10' },
      acquisitionMissionEngine: amoEngine,
      audit,
      silentInspection: true,
    });

    assert.deepEqual(turn.ownershipTrace, [
      'Workspace Entry',
      'Mission Found',
      'Mission Inspection',
      'Property Claimed',
      'Mission Response',
      'Complete',
    ]);
  });

  it('WorkspaceEngine.ask() resolves mission_inspection owner before reasoning', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = lawFirmMission(amoEngine);
    const engine = createWorkspaceEngine({
      acquisitionMissionEngine: amoEngine,
      missionsEnabled: true,
      missionEngine: {
        activeMissionResolver: {
          resolveActiveMission: async () => null,
          resolve: async () => ({ action: 'intelligence' }),
          clearActiveMission: async () => {},
        },
        toCard: (m) => m,
      },
    });

    const result = await engine.ask({
      question: 'Why is the progress 40%?',
      context: { tenantId: '10', missionId: mission.id, clientId: '10' },
    });

    assert.equal(result.workspaceOwnership.owner, 'mission_inspection');
    assert.equal(result.domainDecision.reason, 'mission_inspection');
    assert.equal(result.executionContext.missionType, 'acquisition_mission');
    assert.equal(result.executionContext.missionId, mission.id);
    assert.equal(result.structured.metadata.missionInspection, true);
    assert.equal(result.structured.metadata.inspectionProperty, 'progress');
    assert.equal(result.structured.metadata.sourcesUsed.knowledge, false);

    const events = listWorkspaceMissionInspectionAuditLog();
    const inspectionAttempt = events.find((row) => row.event === 'WORKSPACE_MISSION_INSPECTION');
    assert.ok(inspectionAttempt);
    assert.equal(inspectionAttempt.attempted, true);

    const cognitiveIndex = events.findIndex((row) => row.event === 'WORKSPACE_MISSION_INSPECTION');
    const responseIndex = events.findIndex((row) => row.event === 'WORKSPACE_RESPONSE');
    assert.ok(cognitiveIndex >= 0);
    assert.ok(responseIndex > cognitiveIndex);

    const retrievalPipeline = events.find(
      (row) => row.event === 'WORKSPACE_PIPELINE' && row.selectedPipeline === 'Retrieval'
    );
    assert.equal(retrievalPipeline, undefined);
  });

  it('records attempted=false when no tenant is available', async () => {
    const audit = createWorkspaceMissionInspectionAudit();
    await maybeHandleWorkspaceMissionInspection({
      question: 'Why is the progress 40%?',
      context: {},
      audit,
    });

    const inspection = audit.log.find((row) => row.event === 'WORKSPACE_MISSION_INSPECTION');
    assert.ok(inspection);
    assert.equal(inspection.attempted, false);
    assert.equal(inspection.reason, 'no_tenant');
  });

  it('buildOwnershipTrace reflects fallback to retrieval when property is not claimed', () => {
    const trace = buildOwnershipTrace([
      { event: 'WORKSPACE_REQUEST' },
      { event: 'WORKSPACE_ACTIVE_MISSION', missionFound: true },
      { event: 'WORKSPACE_MISSION_INSPECTION', attempted: true },
      { event: 'MISSION_INSPECTION_RESULT', claimed: false },
      { event: 'WORKSPACE_PIPELINE', selectedPipeline: 'Retrieval' },
    ]);
    assert.deepEqual(trace, [
      'Workspace Entry',
      'Mission Found',
      'Mission Inspection',
      'Retrieval Selected',
    ]);
  });
});
