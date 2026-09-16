'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  MJ_ELECTRIC,
  PRIOR_SKIPPED_SCHEDULE_ID,
  extractRecipientEmail,
  deriveScheduleIdFromMessage,
  normalizeLatestSuccessfulSend,
  loadLatestSuccessfulSend,
  loadActiveSchedules,
  loadActiveReservations,
  loadPriorSkippedSchedule,
  loadMissionId,
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

describe('scheduleBabrunMjElectric tenant_outreach_messages schema', () => {
  it('extractRecipientEmail reads canonical recipients JSON', () => {
    assert.equal(
      extractRecipientEmail([{ role: 'recipient', email: 'contact@mjelectricsandiego.com' }]),
      'contact@mjelectricsandiego.com'
    );
    assert.equal(extractRecipientEmail(['Roque@Example.COM']), 'roque@example.com');
    assert.equal(extractRecipientEmail([]), null);
  });

  it('deriveScheduleIdFromMessage prefers join id then metadata', () => {
    assert.equal(
      deriveScheduleIdFromMessage({ schedule_id: 'tosched_join' }),
      'tosched_join'
    );
    assert.equal(
      deriveScheduleIdFromMessage({ metadata: { scheduleId: 'tosched_meta' } }),
      'tosched_meta'
    );
    assert.equal(
      deriveScheduleIdFromMessage({ metadata: { schedule_id: 'tosched_snake' } }),
      'tosched_snake'
    );
    assert.equal(deriveScheduleIdFromMessage({ metadata: {} }), null);
  });

  it('normalizeLatestSuccessfulSend maps real message columns only', () => {
    const normalized = normalizeLatestSuccessfulSend({
      id: 'msg_1',
      prospect_id: MJ_ELECTRIC.prospectId,
      sent_at: '2026-09-15T14:00:00.000Z',
      recipients: [{ role: 'recipient', email: MJ_ELECTRIC.recipientEmail }],
      outreach_asset_id: MJ_ELECTRIC.outreachAssetId,
      thread_id: 'tot_1',
      status: 'sent',
      schedule_id: 'tosched_abc',
    });

    assert.equal(normalized.id, 'msg_1');
    assert.equal(normalized.prospectId, MJ_ELECTRIC.prospectId);
    assert.equal(normalized.recipientEmail, MJ_ELECTRIC.recipientEmail);
    assert.equal(normalized.scheduleId, 'tosched_abc');
    assert.equal(normalized.recipient_email, undefined);
    assert.equal(normalized.schedule_id, undefined);
  });

  it('loadLatestSuccessfulSend queries tenant_outreach_messages without phantom columns', async () => {
    let capturedSql = '';
    const mockDb = {
      query: async (sql) => {
        capturedSql = String(sql);
        return {
          rows: [{
            id: 'msg_latest',
            prospect_id: MJ_ELECTRIC.prospectId,
            sent_at: '2026-09-15T14:00:00.000Z',
            recipients: [{ role: 'recipient', email: MJ_ELECTRIC.recipientEmail }],
            outreach_asset_id: MJ_ELECTRIC.outreachAssetId,
            thread_id: 'tot_latest',
            status: 'sent',
            metadata: {},
            schedule_id: 'tosched_from_join',
          }],
        };
      },
    };

    const latest = await loadLatestSuccessfulSend(mockDb);

    assert.match(capturedSql, /FROM tenant_outreach_messages m/);
    assert.match(capturedSql, /m\.recipients/);
    assert.match(capturedSql, /LEFT JOIN tenant_outreach_scheduled_sends s/);
    assert.match(capturedSql, /s\.outbound_message_id = m\.id/);
    assert.doesNotMatch(capturedSql, /tenant_outreach_messages[\s\S]*recipient_email/);
    assert.doesNotMatch(capturedSql, /m\.schedule_id/);
    assert.equal(latest.recipientEmail, MJ_ELECTRIC.recipientEmail);
    assert.equal(latest.scheduleId, 'tosched_from_join');
  });

  it('preflight parallel read helpers use pool-scoped db handles', async () => {
    let concurrent = 0;
    let maxConcurrent = 0;
    const mockPool = {
      query: async () => {
        concurrent += 1;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await new Promise((resolve) => setImmediate(resolve));
        concurrent -= 1;
        return { rows: [] };
      },
    };

    await Promise.all([
      loadLatestSuccessfulSend(mockPool),
      loadActiveSchedules(mockPool),
      loadActiveReservations(mockPool),
      loadPriorSkippedSchedule(mockPool),
      loadMissionId(mockPool),
    ]);

    assert.ok(maxConcurrent > 1, 'parallel preflight reads may execute concurrently on pool');
  });

  it('runPreflight Promise.all uses pool rather than a checked-out client', () => {
    const source = fs.readFileSync(
      path.join(__dirname, '..', 'scripts', 'scheduleBabrunMjElectric.js'),
      'utf8'
    );
    assert.match(source, /loadLatestSuccessfulSend\(pool\)/);
    assert.match(source, /loadProspectEligibility\(pool\)/);
    assert.match(source, /loadMissionId\(pool\)/);
    assert.doesNotMatch(source, /loadLatestSuccessfulSend\(client\)/);
  });
});
