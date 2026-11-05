'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  CORRECTION_VERSION,
  correctedState,
} = require('../utils/laEndeavorsWorkspaceCorrection');

describe('LA Endeavors workspace correction', () => {
  it('injects the approved private-lending prospecting plan without removing unrelated state', () => {
    const input = {
      blueprintId: 'bp-la',
      blueprintVersion: 1,
      normalizedFacts: { identity: 'keep me' },
      growthConversation: {
        status: 'validation_target_ready',
        turns: [{ speaker: 'assistant', message: 'old generic response' }],
      },
      growthWork: { completedTaskIds: [], history: [] },
    };

    const result = correctedState(input, '2026-11-05T14:00:00.000Z');
    assert.equal(result.changed, true);
    assert.deepEqual(result.state.normalizedFacts, { identity: 'keep me' });
    assert.equal(
      result.state.validationTarget.title,
      'Asset-Backed Borrower Validation Target'
    );
    assert.match(
      result.state.validationTarget.first_30_day_success_definition,
      /10 new decision-ready borrower prospect records/i
    );
    assert.equal(result.state.firstGrowthPlanPreview.status, 'accepted');
    assert.equal(result.state.growthInfrastructureReadinessReport.outreachAuthorized, false);
    assert.deepEqual(result.state.growthWork.completedTaskIds, [
      'milestone:growth_conversation',
      'milestone:infrastructure_readiness',
      'milestone:campaign_ready',
    ]);
    assert.equal(
      result.state.laEndeavorsWorkspaceCorrection.version,
      CORRECTION_VERSION
    );
    const serialized = JSON.stringify(result.state);
    assert.doesNotMatch(serialized, /vendor frustration|walkthrough or estimate|weekly service|lowest-price shopping/i);
  });

  it('is idempotent after the correction marker is present', () => {
    const first = correctedState({}, '2026-11-05T14:00:00.000Z');
    const second = correctedState(first.state, '2026-11-05T15:00:00.000Z');
    assert.equal(second.changed, false);
    assert.equal(
      second.state.laEndeavorsWorkspaceCorrection.appliedAt,
      '2026-11-05T14:00:00.000Z'
    );
  });
});

