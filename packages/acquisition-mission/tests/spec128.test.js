'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { createMission } = require('../Mission');

describe('SPEC-128 pending operator decision', () => {
  it('seeds pending plan approval on discover-stage missions', () => {
    const mission = createMission({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester.',
      targetSegment: 'Law Firms',
      stage: amo.STAGES.DISCOVER,
    });
    assert.ok(mission.pendingOperatorDecision);
    assert.equal(mission.pendingOperatorDecision.prompt, 'Approve mission plan?');
    assert.ok(mission.missionPlanDraft);
  });
});
