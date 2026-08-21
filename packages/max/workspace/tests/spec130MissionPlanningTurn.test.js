'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  isMissionPlanningTurn,
  looksLikeInspectionQuestion,
} = require('../MissionPlanningTurn');

describe('SPEC-130 — Mission Planning turn detection', () => {
  it('does not treat inspection questions as clarification answers', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers.',
      targetSegment: 'Law Firms',
    });

    assert.equal(mission.pendingOperatorDecision.kind, 'plan_clarification');
    assert.equal(looksLikeInspectionQuestion('Why is the progress 40%?'), true);
    assert.equal(isMissionPlanningTurn(mission, 'Why is the progress 40%?'), false);
    assert.equal(isMissionPlanningTurn(mission, 'What is blocking this mission?'), false);
    assert.equal(isMissionPlanningTurn(mission, 'Manchester NH'), true);
    assert.equal(isMissionPlanningTurn(mission, 'UK'), true);
  });

  it('claims Approve/Edit/Cancel on a ready plan, not progress questions', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
      targetSegment: 'Law Firms',
    });

    assert.equal(mission.pendingOperatorDecision.kind, 'plan_approval');
    assert.equal(isMissionPlanningTurn(mission, 'approved'), true);
    assert.equal(isMissionPlanningTurn(mission, 'Approved. Begin Discovery.'), true);
    assert.equal(isMissionPlanningTurn(mission, 'Why is the progress 40%?'), false);
  });
});
