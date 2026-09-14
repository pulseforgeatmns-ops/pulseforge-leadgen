'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildObserveFollowUpMetadata,
  formatResearchNoteText,
  formatNeedsFollowUpSummary,
  dueDateFromTiming,
  researchNoteSource,
} = require('../services/observeReactionOperationalSync');

describe('observe reaction operational sync metadata', () => {
  it('preserves exact requested timing values in metadata', () => {
    const metadata = buildObserveFollowUpMetadata({
      reaction: {
        id: 'obsrx_obs_open_reeval_abcd',
        observationId: 'obs_open',
        evidenceType: 'human_open',
        evidenceStrength: 'engagement',
        updatedDisposition: 'seen',
        recommendedNextAction: 'wait',
        evaluationKind: 'cadence_reevaluation',
        reevaluationTriggerKind: 'historical_cadence_annotation',
        reevaluationTriggerId: 'cadence_ann_test',
        recommendedTiming: {
          kind: 'wait_until',
          waitDays: 4,
          dueAt: '2026-09-18T12:25:57.000Z',
          cadenceSource: 'prepared_sequence',
          cadenceProvenance: 'historical_annotation',
          reconstructed: true,
        },
        prospectId: '7adbb294-b94c-45c0-85df-e040f027ece0',
      },
      candidateState: {
        disposition: 'seen',
        evidenceStrength: 'engagement',
        recommendedNextAction: 'wait',
        recommendedTiming: {
          kind: 'wait_until',
          waitDays: 4,
          dueAt: '2026-09-18T12:25:57.000Z',
          cadenceSource: 'prepared_sequence',
          cadenceProvenance: 'historical_annotation',
          reconstructed: true,
        },
      },
      mission: { id: 'mission_ad7753b0-6def-441d-bb1a-3764656f5750' },
      execution: {
        id: 'amo_send_37a03a00-2686-4804-8360-9cf93edb52ba',
        prospect_id: '7adbb294-b94c-45c0-85df-e040f027ece0',
        prepared_artifact_revision: 'rev-backus-capacity-1',
      },
      preparedCadence: {
        steps: [{ day: 0 }, { day: 4 }, { day: 8 }, { day: 13 }],
        cadenceProvenance: 'historical_annotation',
        reconstructed: true,
      },
      annotation: { id: 'cadence_ann_test' },
    });

    assert.equal(metadata.disposition, 'seen');
    assert.equal(metadata.evidenceStrength, 'engagement');
    assert.equal(metadata.evidenceType, 'human_open');
    assert.equal(metadata.recommendedNextAction, 'wait');
    assert.equal(metadata.recommendedTiming.kind, 'wait_until');
    assert.equal(metadata.recommendedTiming.waitDays, 4);
    assert.equal(metadata.recommendedTiming.dueAt, '2026-09-18T12:25:57.000Z');
    assert.equal(metadata.recommendedTiming.cadenceSource, 'prepared_sequence');
    assert.equal(metadata.recommendedTiming.cadenceProvenance, 'historical_annotation');
    assert.equal(metadata.recommendedTiming.reconstructed, true);
    assert.deepEqual(metadata.sequenceStepDays, [0, 4, 8, 13]);
  });

  it('embeds metadata JSON in research note text', () => {
    const metadata = buildObserveFollowUpMetadata({
      reaction: {
        evidenceType: 'human_open',
        recommendedTiming: { kind: 'wait_until', waitDays: 4, dueAt: '2026-09-18T12:25:57.000Z' },
      },
      candidateState: { disposition: 'seen', recommendedNextAction: 'wait' },
    });
    const text = formatResearchNoteText(metadata);
    assert.match(text, /metadata:/);
    assert.match(text, /"waitDays": 4/);
    assert.match(text, /"kind": "wait_until"/);
  });

  it('formats needs_follow_up summary with due date', () => {
    const summary = formatNeedsFollowUpSummary({
      recommendedTiming: {
        waitDays: 4,
        dueAt: '2026-09-18T12:25:57.000Z',
        cadenceSource: 'prepared_sequence',
        cadenceProvenance: 'historical_annotation',
      },
    });
    assert.match(summary, /needs_follow_up/);
    assert.match(summary, /wait 4 day/);
    assert.match(summary, /2026-09-18T12:25:57/);
  });

  it('derives AO due date from timing dueAt', () => {
    assert.equal(dueDateFromTiming({ dueAt: '2026-09-18T12:25:57.000Z' }), '2026-09-18');
  });

  it('uses stable research note source per annotation', () => {
    assert.equal(researchNoteSource('cadence_ann_abc'), 'spec252_observe_reeval:cadence_ann_abc');
  });
});
