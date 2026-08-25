'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildOpenMissionAction,
  resolveMissionActionRuntime,
  MISSION_RUNTIMES,
} = require('../MissionActions');
const { buildOwnershipMissionResponse } = require('../AcquisitionOwnership');

describe('MissionActions — self-contained open_mission contract', () => {
  it('buildOpenMissionAction includes missionId and runtime for AMO', () => {
    const action = buildOpenMissionAction({
      missionId: 'amo-123',
      runtime: MISSION_RUNTIMES.AMO,
      label: 'Open mission workspace',
    });

    assert.equal(action.type, 'open_mission');
    assert.equal(action.payload.missionId, 'amo-123');
    assert.equal(action.payload.runtime, MISSION_RUNTIMES.AMO);
  });

  it('buildOpenMissionAction defaults legacy missions to SPEC-022', () => {
    const action = buildOpenMissionAction({ missionId: 'legacy-456' });

    assert.equal(action.payload.runtime, MISSION_RUNTIMES.SPEC_022);
  });

  it('resolveMissionActionRuntime never returns null', () => {
    assert.equal(resolveMissionActionRuntime(null), MISSION_RUNTIMES.SPEC_022);
    assert.equal(resolveMissionActionRuntime(undefined), MISSION_RUNTIMES.SPEC_022);
    assert.equal(resolveMissionActionRuntime('AMO'), MISSION_RUNTIMES.AMO);
  });

  it('buildOwnershipMissionResponse emits self-contained open_mission action', () => {
    const amo = require('../../../acquisition-mission');
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
      targetSegment: 'Law Firms',
    });
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const { structured } = buildOwnershipMissionResponse({
      mission: snapshot.mission,
      snapshot,
      created: false,
      ciEvidence: { attached: false, known: [] },
      question: 'Acquire commercial cleaning customers in Manchester NH.',
    });

    const openAction = structured.recommendedActions.find((row) => row.type === 'open_mission');
    assert.ok(openAction, 'expected open_mission action');
    assert.equal(openAction.payload.missionId, mission.id);
    assert.equal(openAction.payload.runtime, MISSION_RUNTIMES.AMO);
  });
});
