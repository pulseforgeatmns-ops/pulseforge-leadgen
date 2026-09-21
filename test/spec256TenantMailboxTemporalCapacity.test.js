'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');

const {
  isValidPriorAnchor,
  findPriorSendAnchor,
  findScheduleSpacingConflicts,
  findInFlightSpacingConflicts,
  evaluateAuthorizationSpacing,
  evaluateExecutionSpacing,
  evaluateCapacityBudget,
  accountCapacityUnits,
} = require('../packages/emmett-outbound/TenantMailboxSpacing');
const {
  evaluateCapacityAuthorization,
  evaluateCapacityExecution,
  assessTenantMailboxCapacity,
  buildCapacityEnvelope,
} = require('../packages/emmett-outbound/TenantMailboxCapacity');
const {
  authenticationFromVerificationState,
  BOOTSTRAP_MODE,
} = require('../packages/emmett-outbound');
const {
  createPermissiveEnvelope,
  createMemoryCapacityGate,
  evaluateCapacityAuthorization: evaluateAuthFromService,
  ensureCapacitySchema,
  queryCanonicalCapacityAccounting,
  reconcileLegacyReservations,
  reservationIdForSchedule,
  authorizeTenantMailboxCapacity,
  validateExecutionCapacity,
} = require('../services/emmettTenantMailboxCapacity');
const { GOVERNOR_OUTCOMES, recommendCapacity, evaluateGovernor, createOutboundEngine } = require('../packages/emmett-outbound');

const BABRUN = Object.freeze({
  tenantId: '13',
  sendingIdentityId: 'tsi_13_babrun_fedir',
  schedule0900: 'tosched_c2b812b9f501e4eb23a6f641',
  schedule0915: 'tosched_c1fb4d6b5b4de33b6a07fa69',
  at0900: '2026-09-16T13:00:00.000Z',
  at0915: '2026-09-16T13:15:00.000Z',
  minSpacing: 240,
});

function bootstrapEnvelope(overrides = {}) {
  const envelope = createPermissiveEnvelope(BABRUN.tenantId, BABRUN.sendingIdentityId, {
    now: new Date('2026-09-16T12:00:00.000Z'),
    localDate: '2026-09-16',
    allowedSendWindow: { startHour: 9, endHour: 16, timezone: 'America/New_York' },
  });
  envelope.minimumSpacingMinutes = BABRUN.minSpacing;
  envelope.maxSendsPerDay = 2;
  envelope.remainingCapacity = 2;
  envelope.governorState = GOVERNOR_OUTCOMES.SLOW;
  return Object.assign(envelope, overrides);
}

describe('SPEC-256 temporal spacing contract', () => {
  it('future schedule never becomes prior-send anchor', () => {
    const events = [
      { id: BABRUN.schedule0900, scheduledFor: BABRUN.at0900, status: 'SCHEDULED' },
      { id: BABRUN.schedule0915, scheduledFor: BABRUN.at0915, status: 'SCHEDULED' },
    ];
    const prior = findPriorSendAnchor(events, BABRUN.at0900, { excludeScheduleId: BABRUN.schedule0900 });
    assert.equal(prior, null);
    assert.equal(isValidPriorAnchor(BABRUN.at0915, BABRUN.at0900), false);
  });

  it('prior successful send is the execution spacing anchor', () => {
    const events = [
      { id: BABRUN.schedule0900, scheduledFor: BABRUN.at0900, sentAt: BABRUN.at0900, status: 'SENT' },
      { id: BABRUN.schedule0915, scheduledFor: BABRUN.at0915, status: 'SCHEDULED' },
    ];
    const prior = findPriorSendAnchor(events, BABRUN.at0915, { excludeScheduleId: BABRUN.schedule0915 });
    assert.equal(prior.lastSendAt, BABRUN.at0900);
    assert.equal(prior.source, 'sent_at');
  });

  it('authorization rejects a too-close later schedule', () => {
    const conflicts = findScheduleSpacingConflicts(
      BABRUN.at0915,
      [{ id: BABRUN.schedule0900, scheduledFor: BABRUN.at0900, status: 'SCHEDULED' }],
      BABRUN.minSpacing
    );
    assert.equal(conflicts.length, 1);
    const result = evaluateAuthorizationSpacing(bootstrapEnvelope(), {
      scheduledFor: BABRUN.at0915,
      scheduleConflicts: conflicts,
    });
    assert.equal(result.allowed, false);
    assert.equal(result.code, 'emmett_spacing_violation');
  });

  it('authorization rejects a too-close earlier insertion', () => {
    const conflicts = findScheduleSpacingConflicts(
      BABRUN.at0900,
      [{ id: BABRUN.schedule0915, scheduledFor: BABRUN.at0915, status: 'SCHEDULED' }],
      BABRUN.minSpacing
    );
    assert.equal(conflicts.length, 1);
    const result = evaluateCapacityAuthorization(bootstrapEnvelope(), {
      scheduledFor: BABRUN.at0900,
      scheduleConflicts: conflicts,
    });
    assert.equal(result.allowed, false);
    assert.equal(result.code, 'emmett_spacing_violation');
  });

  it('schedules outside the spacing window are accepted', () => {
    const later = '2026-09-16T17:00:00.000Z';
    const conflicts = findScheduleSpacingConflicts(
      later,
      [{ id: BABRUN.schedule0900, scheduledFor: BABRUN.at0900, status: 'SCHEDULED' }],
      BABRUN.minSpacing
    );
    assert.equal(conflicts.length, 0);
    const result = evaluateAuthorizationSpacing(bootstrapEnvelope(), {
      scheduledFor: later,
      scheduleConflicts: conflicts,
    });
    assert.equal(result.allowed, true);
  });

  it('first Babrun 09:00 fixture is not blocked by the future 09:15 row', () => {
    const envelope = bootstrapEnvelope({ remainingCapacity: 0, currentScheduledCount: 2, maxSendsPerDay: 2 });
    const check = evaluateCapacityExecution(envelope, {
      scheduledFor: BABRUN.at0900,
      lastSendAt: BABRUN.at0915,
      alreadyConsumesCapacity: true,
    });
    assert.equal(isValidPriorAnchor(BABRUN.at0915, BABRUN.at0900), false);
    assert.equal(check.allowed, true);
    assert.equal(check.action, 'send');
  });

  it('hypothetical successful 09:00 send blocks 09:15 execution', () => {
    const envelope = bootstrapEnvelope({ remainingCapacity: 1, currentSentCount: 1, currentScheduledCount: 1, maxSendsPerDay: 2 });
    const prior = findPriorSendAnchor([
      { id: BABRUN.schedule0900, sentAt: BABRUN.at0900, scheduledFor: BABRUN.at0900, status: 'SENT' },
    ], BABRUN.at0915, { excludeScheduleId: BABRUN.schedule0915 });
    const check = evaluateCapacityExecution(envelope, {
      scheduledFor: BABRUN.at0915,
      lastSendAt: prior.lastSendAt,
      alreadyConsumesCapacity: true,
    });
    assert.equal(check.allowed, false);
    assert.equal(check.code, 'emmett_spacing_violation');
  });

  it('in-flight earlier EXECUTING send occupies the temporal slot', () => {
    const conflicts = findInFlightSpacingConflicts(
      BABRUN.at0915,
      [{ id: BABRUN.schedule0900, scheduledFor: BABRUN.at0900, status: 'EXECUTING' }],
      BABRUN.minSpacing,
      { excludeScheduleId: BABRUN.schedule0915 }
    );
    assert.equal(conflicts.length, 1);
    const check = evaluateExecutionSpacing(bootstrapEnvelope(), {
      scheduledFor: BABRUN.at0915,
      inFlightConflicts: conflicts,
    });
    assert.equal(check.allowed, false);
  });
});

describe('SPEC-256 preserves SPEC-255 bootstrap spacing on durable envelopes', () => {
  it('bootstrap-active assessment persists 240-minute spacing, not ramp 60', () => {
    const snapshot = {
      tenantId: '13',
      sendingIdentityId: BABRUN.sendingIdentityId,
      mailboxKind: 'tenant_smtp',
      deliverabilityObservability: 'limited',
      inboxAgeDays: 2,
      providerCeiling: 3,
      mailboxStatus: 'active',
      identityStatus: 'active',
      authentication: authenticationFromVerificationState({
        smtp: { status: 'verified' },
        spf: { status: 'present' },
        dkim: { status: 'present' },
        dmarc: { status: 'present' },
      }),
      warmup: { status: 'warming', dailyCap: 3, activeSendDays: 0, rampStage: 'early', reset: true },
      bounceRate: 0,
      replyRate: null,
      openRate: null,
      complaintRate: 0,
      sentToday: 0,
      scheduledSends: 2,
      recentSends: 0,
      totalOperationalSends: 0,
      blacklist: { listed: false },
    };
    const assessment = assessTenantMailboxCapacity(snapshot);
    assert.equal(assessment.capacity.mode, BOOTSTRAP_MODE);
    assert.equal(assessment.minimumSpacingMinutes, 240);
    assert.equal(assessment.allowedSendWindow.startHour, 9);
    assert.equal(assessment.allowedSendWindow.endHour, 16);
    const envelope = buildCapacityEnvelope(snapshot, assessment, { now: new Date('2026-09-16T12:00:00.000Z') });
    assert.equal(envelope.minimumSpacingMinutes, 240);
  });
});

describe('SPEC-256 canonical capacity accounting', () => {
  it('legacy schedule without reservation still consumes capacity', () => {
    const accounting = accountCapacityUnits({
      sentMessages: [],
      schedules: [
        { id: BABRUN.schedule0900, status: 'SCHEDULED' },
        { id: BABRUN.schedule0915, status: 'SCHEDULED' },
      ],
      reservations: [],
    });
    assert.equal(accounting.scheduled, 2);
    assert.equal(accounting.consumed, 2);
    assert.equal(accounting.remainingFor(2), 0);
  });

  it('reservation is not double-counted with its schedule', () => {
    const accounting = accountCapacityUnits({
      schedules: [{ id: BABRUN.schedule0900, status: 'SCHEDULED' }],
      reservations: [{ id: 'res_1', scheduleId: BABRUN.schedule0900, status: 'scheduled' }],
    });
    assert.equal(accounting.scheduled, 1);
    assert.equal(accounting.consumed, 1);
  });

  it('SENT is not double-counted with reservation or schedule', () => {
    const accounting = accountCapacityUnits({
      sentMessages: [{ id: 'msg_1', scheduleId: BABRUN.schedule0900 }],
      schedules: [{ id: BABRUN.schedule0900, status: 'SENT', outboundMessageId: 'msg_1' }],
      reservations: [{ id: 'res_1', scheduleId: BABRUN.schedule0900, status: 'sent' }],
    });
    assert.equal(accounting.sent, 1);
    assert.equal(accounting.consumed, 1);

    const unlinkedMessage = accountCapacityUnits({
      sentMessages: [{ id: 'msg_1' }],
      schedules: [{ id: BABRUN.schedule0900, status: 'SENT', outboundMessageId: 'msg_1' }],
      reservations: [{ id: 'res_1', scheduleId: BABRUN.schedule0900, status: 'sent' }],
    });
    assert.equal(unlinkedMessage.sent, 1);
    assert.equal(unlinkedMessage.consumed, 1);
  });

  it('FAILED, SKIPPED, and CANCELLED release capacity', () => {
    const accounting = accountCapacityUnits({
      schedules: [
        { id: 'a', status: 'FAILED' },
        { id: 'b', status: 'SKIPPED' },
        { id: 'c', status: 'CANCELLED' },
        { id: 'd', status: 'SCHEDULED' },
      ],
      reservations: [
        { id: 'ra', scheduleId: 'a', status: 'released' },
        { id: 'rb', scheduleId: 'b', status: 'skipped' },
      ],
    });
    assert.equal(accounting.scheduled, 1);
    assert.equal(accounting.consumed, 1);
    assert.equal(accounting.remainingFor(2), 1);
  });

  it('display and execution accounting agree on the same ledger', () => {
    const ledger = {
      sentMessages: [],
      schedules: [
        { id: BABRUN.schedule0900, status: 'SCHEDULED' },
        { id: BABRUN.schedule0915, status: 'SCHEDULED' },
      ],
      reservations: [],
    };
    const display = accountCapacityUnits(ledger);
    const execution = accountCapacityUnits(ledger);
    assert.equal(display.sent, execution.sent);
    assert.equal(display.scheduled, execution.scheduled);
    assert.equal(display.executing, execution.executing);
    assert.equal(display.consumed, execution.consumed);
    assert.equal(display.remainingFor(2), 0);
  });

  it('already-accounted execution is not blocked by remaining=0', () => {
    const envelope = bootstrapEnvelope({
      remainingCapacity: 0,
      currentScheduledCount: 2,
      currentSentCount: 0,
      maxSendsPerDay: 2,
    });
    const budget = evaluateCapacityBudget(envelope, { alreadyConsumesCapacity: true });
    assert.equal(budget.allowed, true);
    const newAuth = evaluateCapacityBudget(envelope, { alreadyConsumesCapacity: false });
    assert.equal(newAuth.allowed, false);
    assert.equal(newAuth.code, 'emmett_capacity_exhausted');
  });
});

describe('SPEC-256 memory gate concurrency and isolation', () => {
  it('concurrent conflicting authorization only permits one', async () => {
    const envelope = bootstrapEnvelope({ maxSendsPerDay: 1, remainingCapacity: 1, currentScheduledCount: 0 });
    envelope.minimumSpacingMinutes = BABRUN.minSpacing;
    const gate = createMemoryCapacityGate({ envelopes: [envelope] });
    const attempts = await Promise.allSettled([
      gate.authorize({
        tenantId: BABRUN.tenantId,
        sendingIdentityId: BABRUN.sendingIdentityId,
        scheduledFor: BABRUN.at0900,
      }),
      gate.authorize({
        tenantId: BABRUN.tenantId,
        sendingIdentityId: BABRUN.sendingIdentityId,
        scheduledFor: BABRUN.at0915,
      }),
    ]);
    const accepted = attempts.filter((row) => row.status === 'fulfilled');
    const rejected = attempts.filter((row) => row.status === 'rejected');
    assert.equal(accepted.length, 1);
    assert.equal(rejected.length, 1);
    assert.ok(['emmett_capacity_exhausted', 'emmett_spacing_violation'].includes(rejected[0].reason.code));
  });

  it('tenant isolation — other tenant commitments do not consume or conflict', async () => {
    const babrun = bootstrapEnvelope({ maxSendsPerDay: 1, remainingCapacity: 1 });
    babrun.minimumSpacingMinutes = BABRUN.minSpacing;
    const other = createPermissiveEnvelope('99', 'tsi_other');
    other.maxSendsPerDay = 1;
    other.remainingCapacity = 1;
    other.minimumSpacingMinutes = BABRUN.minSpacing;
    const gate = createMemoryCapacityGate({
      envelopes: [babrun, other],
      commitments: [{
        id: BABRUN.schedule0900,
        tenantId: BABRUN.tenantId,
        sendingIdentityId: BABRUN.sendingIdentityId,
        scheduledFor: BABRUN.at0900,
        status: 'SCHEDULED',
      }],
    });
    const allowed = await gate.authorize({
      tenantId: '99',
      sendingIdentityId: 'tsi_other',
      scheduledFor: BABRUN.at0900,
    });
    assert.ok(allowed.envelope);
    await assert.rejects(
      () => gate.authorize({
        tenantId: BABRUN.tenantId,
        sendingIdentityId: BABRUN.sendingIdentityId,
        scheduledFor: BABRUN.at0915,
      }),
      (err) => err.code === 'emmett_spacing_violation' || err.code === 'emmett_capacity_exhausted'
    );
  });

  it('memory execution of 09:00 is not skipped by a future 09:15 schedule', async () => {
    const envelope = bootstrapEnvelope({ maxSendsPerDay: 2, remainingCapacity: 0, currentScheduledCount: 2 });
    const gate = createMemoryCapacityGate({
      envelopes: [envelope],
      commitments: [
        { id: BABRUN.schedule0900, scheduleId: BABRUN.schedule0900, tenantId: BABRUN.tenantId, sendingIdentityId: BABRUN.sendingIdentityId, scheduledFor: BABRUN.at0900, status: 'SCHEDULED' },
        { id: BABRUN.schedule0915, scheduleId: BABRUN.schedule0915, tenantId: BABRUN.tenantId, sendingIdentityId: BABRUN.sendingIdentityId, scheduledFor: BABRUN.at0915, status: 'SCHEDULED' },
      ],
    });
    const first = await gate.validateExecution({
      id: BABRUN.schedule0900,
      tenantId: BABRUN.tenantId,
      sendingIdentityId: BABRUN.sendingIdentityId,
      scheduledFor: BABRUN.at0900,
    });
    assert.equal(first.eligible, true);
    await gate.finalize({
      id: BABRUN.schedule0900,
      tenantId: BABRUN.tenantId,
      sendingIdentityId: BABRUN.sendingIdentityId,
      scheduledFor: BABRUN.at0900,
      sentAt: BABRUN.at0900,
    }, 'SENT');
    const second = await gate.validateExecution({
      id: BABRUN.schedule0915,
      tenantId: BABRUN.tenantId,
      sendingIdentityId: BABRUN.sendingIdentityId,
      scheduledFor: BABRUN.at0915,
    });
    assert.equal(second.eligible, false);
    assert.equal(second.reason, 'emmett_spacing_violation');
  });

  it('legacy Brevo Emmett capacity path remains unchanged', () => {
    const engine = createOutboundEngine();
    const snapshot = {
      tenantId: '1',
      providerCeiling: 50,
      bounceRate: 0,
      replyRate: 0.05,
      openRate: 0.2,
      complaintRate: 0,
      recentSends: 25,
      sentToday: 2,
      authentication: { spf: true, dkim: true, dmarc: 'reject' },
      warmup: { status: 'healthy', dailyCap: 50 },
      blacklist: { listed: false },
    };
    const assessed = engine.assess({ tenantId: '1', snapshot, prospects: [] });
    assert.equal(assessed.capacity.kind, 'capacity');
    assert.ok(recommendCapacity(snapshot, assessed.health).recommended > 0);
    assert.equal(evaluateGovernor(snapshot, assessed.health, assessed.capacity).outcome, 'proceed');
    assert.equal(typeof evaluateAuthFromService, 'function');
  });
});

describe('SPEC-256 postgres accounting and reconciliation', () => {
  let postgres;
  let pool;
  let available = true;

  before(async () => {
    try {
      const { startDisposablePostgres } = require('./helpers/disposablePostgres');
      postgres = await startDisposablePostgres();
      pool = new Pool({ connectionString: postgres.connectionString });
      await pool.query(`
        CREATE TABLE tenant_outreach_messages (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL,
          sending_identity_id TEXT,
          direction TEXT,
          status TEXT,
          sent_at TIMESTAMPTZ
        );
        CREATE TABLE tenant_outreach_scheduled_sends (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL,
          sending_identity_id TEXT,
          status TEXT,
          scheduled_for TIMESTAMPTZ,
          outbound_message_id TEXT,
          timezone TEXT DEFAULT 'America/New_York'
        );
      `);
      await ensureCapacitySchema(pool);
    } catch (err) {
      available = false;
      console.warn('SPEC-256 postgres suite skipped:', err.message);
    }
  });

  after(async () => {
    if (pool) await pool.end().catch(() => {});
    if (postgres) await postgres.stop().catch(() => {});
  });

  async function seedEnvelope(envelopeId = 'env_babrun_256') {
    await pool.query(
      `INSERT INTO emmett_tenant_mailbox_capacity_envelopes (
          id, tenant_id, mailbox_integration_id, sending_identity_id, sender_email, sending_domain,
          local_date, computed_at, valid_from, valid_until, max_sends_per_day, minimum_spacing_minutes,
          allowed_send_window, governor_state, remaining_capacity
        ) VALUES ($1,$2,'tmi_13_babrun_hello',$3,'hello@babrun.com','babrun.com',
          '2026-09-16', '2026-09-16T12:00:00Z', '2026-09-16T12:00:00Z', '2026-09-17T00:00:00Z',
          2, 240,
          '{"startHour":9,"endHour":16,"timezone":"America/New_York"}'::jsonb, 'slow', 2)
        ON CONFLICT (id) DO UPDATE SET
          valid_until = EXCLUDED.valid_until,
          max_sends_per_day = EXCLUDED.max_sends_per_day,
          remaining_capacity = EXCLUDED.remaining_capacity`,
      [envelopeId, BABRUN.tenantId, BABRUN.sendingIdentityId]
    );
    return envelopeId;
  }

  it('legacy schedules consume capacity and reconciliation is idempotent', async (t) => {
    if (!available) return t.skip('disposable postgres unavailable');
    await pool.query('DELETE FROM tenant_outreach_scheduled_sends');
    await pool.query('DELETE FROM emmett_tenant_mailbox_capacity_reservations');
    await pool.query('DELETE FROM tenant_outreach_messages');
    const envelopeId = await seedEnvelope();
    await pool.query(
      `INSERT INTO tenant_outreach_scheduled_sends (id, tenant_id, sending_identity_id, status, scheduled_for)
       VALUES ($1,$3,$4,'SCHEDULED',$5), ($2,$3,$4,'SCHEDULED',$6)`,
      [BABRUN.schedule0900, BABRUN.schedule0915, BABRUN.tenantId, BABRUN.sendingIdentityId, BABRUN.at0900, BABRUN.at0915]
    );

    const before = await queryCanonicalCapacityAccounting(
      BABRUN.tenantId,
      BABRUN.sendingIdentityId,
      '2026-09-16',
      'America/New_York',
      pool
    );
    assert.equal(before.scheduled, 2);
    assert.equal(before.remainingFor(2), 0);

    const first = await reconcileLegacyReservations(BABRUN.tenantId, BABRUN.sendingIdentityId, envelopeId, pool);
    const second = await reconcileLegacyReservations(BABRUN.tenantId, BABRUN.sendingIdentityId, envelopeId, pool);
    assert.equal(first.created, 2);
    assert.equal(second.created, 0);

    const reservations = await pool.query(
      `SELECT schedule_id FROM emmett_tenant_mailbox_capacity_reservations
        WHERE tenant_id = $1 ORDER BY scheduled_for`,
      [BABRUN.tenantId]
    );
    assert.equal(reservations.rowCount, 2);
    assert.equal(reservations.rows[0].schedule_id, BABRUN.schedule0900);

    const after = await queryCanonicalCapacityAccounting(
      BABRUN.tenantId,
      BABRUN.sendingIdentityId,
      '2026-09-16',
      'America/New_York',
      pool
    );
    assert.equal(after.scheduled, 2);
    assert.equal(after.consumed, 2);
    assert.equal(reservationIdForSchedule(BABRUN.tenantId, BABRUN.schedule0900).startsWith('res_'), true);
  });

  it('concurrent postgres authorization cannot reserve conflicting slots', async (t) => {
    if (!available) return t.skip('disposable postgres unavailable');
    await pool.query('DELETE FROM tenant_outreach_scheduled_sends');
    await pool.query('DELETE FROM emmett_tenant_mailbox_capacity_reservations');
    await pool.query('DELETE FROM tenant_outreach_messages');
    await pool.query('DELETE FROM emmett_tenant_mailbox_capacity_envelopes');
    const envelopeId = await seedEnvelope('env_concurrent_256');
    await pool.query(
      `UPDATE emmett_tenant_mailbox_capacity_envelopes
          SET remaining_capacity = 1, current_scheduled_count = 0, max_sends_per_day = 1
        WHERE id = $1`,
      [envelopeId]
    );

    const attempts = await Promise.allSettled([
      authorizeTenantMailboxCapacity({
        tenantId: BABRUN.tenantId,
        sendingIdentityId: BABRUN.sendingIdentityId,
        scheduledFor: BABRUN.at0900,
      }, { pool, now: new Date('2026-09-16T12:00:00.000Z') }),
      authorizeTenantMailboxCapacity({
        tenantId: BABRUN.tenantId,
        sendingIdentityId: BABRUN.sendingIdentityId,
        scheduledFor: BABRUN.at0915,
      }, { pool, now: new Date('2026-09-16T12:00:00.000Z') }),
    ]);
    const accepted = attempts.filter((row) => row.status === 'fulfilled');
    const rejected = attempts.filter((row) => row.status === 'rejected');
    assert.equal(
      accepted.length,
      1,
      rejected.map((row) => row.reason && (row.reason.code || row.reason.message)).join(', ')
    );
    assert.equal(rejected.length, 1);
  });

  it('execution of 09:00 is not blocked by a future 09:15 row on postgres', async (t) => {
    if (!available) return t.skip('disposable postgres unavailable');
    await pool.query('DELETE FROM tenant_outreach_scheduled_sends');
    await pool.query('DELETE FROM emmett_tenant_mailbox_capacity_reservations');
    await pool.query('DELETE FROM tenant_outreach_messages');
    await pool.query('DELETE FROM emmett_tenant_mailbox_capacity_envelopes');
    await seedEnvelope('env_exec_256');
    await pool.query(
      `INSERT INTO tenant_outreach_scheduled_sends (id, tenant_id, sending_identity_id, status, scheduled_for)
       VALUES ($1,$3,$4,'SCHEDULED',$5), ($2,$3,$4,'SCHEDULED',$6)`,
      [BABRUN.schedule0900, BABRUN.schedule0915, BABRUN.tenantId, BABRUN.sendingIdentityId, BABRUN.at0900, BABRUN.at0915]
    );

    const first = await validateExecutionCapacity({
      id: BABRUN.schedule0900,
      tenantId: BABRUN.tenantId,
      sendingIdentityId: BABRUN.sendingIdentityId,
      scheduledFor: BABRUN.at0900,
      timezone: 'America/New_York',
    }, { pool, now: new Date(BABRUN.at0900) });
    assert.equal(first.eligible, true);

    await pool.query(
      `INSERT INTO tenant_outreach_messages (id, tenant_id, sending_identity_id, direction, status, sent_at)
       VALUES ('msg_0900',$1,$2,'OUTBOUND','sent',$3)`,
      [BABRUN.tenantId, BABRUN.sendingIdentityId, BABRUN.at0900]
    );
    await pool.query(
      `UPDATE tenant_outreach_scheduled_sends SET status = 'SENT', outbound_message_id = 'msg_0900' WHERE id = $1`,
      [BABRUN.schedule0900]
    );
    await pool.query(
      `UPDATE emmett_tenant_mailbox_capacity_reservations SET status = 'sent' WHERE schedule_id = $1`,
      [BABRUN.schedule0900]
    );

    const second = await validateExecutionCapacity({
      id: BABRUN.schedule0915,
      tenantId: BABRUN.tenantId,
      sendingIdentityId: BABRUN.sendingIdentityId,
      scheduledFor: BABRUN.at0915,
      timezone: 'America/New_York',
    }, { pool, now: new Date(BABRUN.at0915) });
    assert.equal(second.eligible, false);
    assert.equal(second.reason, 'emmett_spacing_violation');
  });
});
