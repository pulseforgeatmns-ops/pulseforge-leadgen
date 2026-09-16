'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  MJ_ELECTRIC,
  PRIOR_SKIPPED_SCHEDULE_ID,
  buildReport,
} = require('../scripts/scheduleBabrunMjElectric');

describe('scheduleBabrunMjElectric script contract', () => {
  it('targets MJ Electric with canonical Babrun identifiers', () => {
    assert.equal(MJ_ELECTRIC.tenantId, '13');
    assert.equal(MJ_ELECTRIC.prospectId, 'c8c0282f-056b-42a3-8555-47c50d00c8a2');
    assert.equal(MJ_ELECTRIC.outreachAssetId, 'ak_babrun_outreach_final_10');
    assert.equal(MJ_ELECTRIC.recipientEmail, 'contact@mjelectricsandiego.com');
    assert.equal(MJ_ELECTRIC.authorizationSource, 'operator_approved_batch_continuation');
  });

  it('guards the prior skipped schedule id', () => {
    assert.equal(PRIOR_SKIPPED_SCHEDULE_ID, 'tosched_c1fb4d6b5b4de33b6a07fa69');
  });

  it('buildReport marks dry-run without direct send', () => {
    const report = buildReport({
      priorSkippedSchedule: {
        status: 'SKIPPED',
        skip_reason: 'emmett_spacing_violation',
        scheduled_for: '2026-09-16T13:15:00.000Z',
      },
      prospectEligibility: { exists: true, doNotContact: false, booked: false },
      suppression: null,
      asset: { lifecycleState: 'STAKEHOLDER_VALIDATED' },
      latestSuccessfulSend: null,
      envelope: { governorState: 'slow', minimumSpacingMinutes: 240, remainingCapacity: 1 },
      activeSchedules: [],
      activeReservations: [],
      eligibleTime: {
        verdict: 'AUTHORIZED_AT 2026-09-17T13:00:00.000Z',
        scheduledForIso: '2026-09-17T13:00:00.000Z',
        scheduledLocalEt: 'Wed, Sep 17, 9:00 AM ET',
      },
      schedulingEligibility: { eligible: true },
      schedulingBlocked: [],
    }, null, 'dry_run');

    assert.equal(report.mode, 'dry_run');
    assert.equal(report.M_directSendOccurred, false);
    assert.equal(report.I_authorizationResult.applied, false);
    assert.equal(report.N_priorSkippedScheduleUnchanged, true);
  });
});
