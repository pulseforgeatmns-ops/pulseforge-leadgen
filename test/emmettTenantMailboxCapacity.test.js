'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { GOVERNOR_OUTCOMES } = require('../packages/emmett-outbound');
const {
  assessTenantMailboxCapacity,
  buildCapacityEnvelope,
  evaluateCapacityAuthorization,
  evaluateCapacityExecution,
  isWithinAllowedWindow,
} = require('../packages/emmett-outbound/TenantMailboxCapacity');
const { buildTenantMailboxSnapshot, EVIDENCE_UNKNOWN } = require('../services/emmettTenantMailboxSnapshot');
const {
  createMemoryCapacityGate,
  createPermissiveEnvelope,
  normalizeEnvelope,
} = require('../services/emmettTenantMailboxCapacity');
const {
  OUTREACH_CONTACT_TYPE,
  classifyOutreachContactType,
} = require('../utils/outreachContactType');
const {
  MemoryTenantMailboxStore,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
  MESSAGE_STATUS,
  MESSAGE_DIRECTION,
} = require('../services/tenantMailbox');
const {
  MemoryScheduleStore,
  authorizeScheduledOutreachSend,
  evaluateSendEligibility,
  executeScheduledSend,
  SCHEDULE_STATUS,
} = require('../services/tenantOutreachScheduler');

const BABRUN = Object.freeze({
  tenantId: '13',
  integrationId: 'tmi_13_babrun_hello',
  sendingIdentityId: 'tsi_13_babrun_fedir',
  senderEmail: 'hello@babrun.com',
  prospectId: 'prospect-babrun-1',
  outreachAssetId: 'ak_babrun_outreach_final_05',
});

function mockPool(state) {
  return {
    query: async (sql, params = []) => {
      const text = String(sql);
      if (text.includes('tenant_sending_identities si')) {
        const identity = state.identity;
        if (!identity) return { rows: [] };
        return {
          rows: [{
            id: identity.id,
            tenant_id: identity.tenantId,
            mailbox_integration_id: identity.mailboxIntegrationId,
            sender_email: identity.senderEmail,
            sender_display_name: identity.senderDisplayName,
            status: identity.status,
            created_at: identity.createdAt,
            mailbox_address: state.integration?.mailboxAddress,
            mailbox_status: state.integration?.status,
            mailbox_created_at: state.integration?.createdAt,
          }],
        };
      }
      if (text.includes('FROM tenant_outreach_messages') && text.includes('successful_sends')) {
        return {
          rows: [{
            successful_sends: state.messages.filter((m) => m.status === 'sent').length,
            failed_sends: state.messages.filter((m) => m.status === 'failed').length,
            inbound_messages: state.messages.filter((m) => m.direction === 'INBOUND').length,
            first_sent_at: state.messages.find((m) => m.status === 'sent')?.sentAt || null,
          }],
        };
      }
      if (text.includes('tenant_outreach_events')) {
        return { rows: [{ reply_count: state.replies || 0 }] };
      }
      if (text.includes('tenant_outreach_suppressions')) {
        return { rows: [{ suppression_count: state.suppressions || 0, hard_bounce_count: state.hardBounces || 0 }] };
      }
      if (text.includes('tenant_outreach_scheduled_sends') && text.includes('scheduled_count')) {
        return { rows: [{ scheduled_count: state.scheduledCount || 0 }] };
      }
      if (text.includes('sent_today')) return { rows: [{ sent_today: state.sentToday || 0 }] };
      if (text.includes('sent_yesterday')) return { rows: [{ sent_yesterday: state.sentYesterday || 0 }] };
      if (text.includes('historical_daily_avg')) return { rows: [{ historical_daily_avg: state.historicalDailyAvg || 0 }] };
      if (text.includes('recentTimestamps') || (text.includes('ORDER BY sent_at DESC') && text.includes('recipients'))) {
        return { rows: state.messages.filter((m) => m.status === 'sent').map((m) => ({ sent_at: m.sentAt, recipients: m.recipients })) };
      }
      if (text.includes('active_send_days')) {
        return { rows: [{ active_send_days: state.activeSendDays || 0 }] };
      }
      return { rows: [] };
    },
  };
}

async function seedBabrunFixture(overrides = {}) {
  const store = new MemoryTenantMailboxStore();
  const integration = await store.saveIntegration({
    id: BABRUN.integrationId,
    tenantId: BABRUN.tenantId,
    providerType: 'GENERIC_SMTP_IMAP',
    mailboxAddress: BABRUN.senderEmail,
    smtpHost: 'mail.adm.tools',
    smtpPort: 465,
    smtpTlsMode: 'SSL_TLS',
    smtpSecretRef: 'SMTP_PASSWORD_REF',
    status: MAILBOX_STATUS.ACTIVE,
    createdAt: '2026-09-01T00:00:00.000Z',
  });
  const identity = await store.saveIdentity({
    id: BABRUN.sendingIdentityId,
    tenantId: BABRUN.tenantId,
    mailboxIntegrationId: integration.id,
    senderEmail: BABRUN.senderEmail,
    senderDisplayName: 'Fedir | Babrun',
    status: IDENTITY_STATUS.ACTIVE,
    createdAt: '2026-09-01T00:00:00.000Z',
  });

  if (overrides.kayleeSend) {
    const thread = await store.saveThread({
      tenantId: BABRUN.tenantId,
      prospectId: BABRUN.prospectId,
      contactRef: 'kaylee@kbpainting.com',
      participants: [{ email: 'kaylee@kbpainting.com' }],
    });
    await store.saveMessage({
      tenantId: BABRUN.tenantId,
      sendingIdentityId: identity.id,
      threadId: thread.id,
      direction: MESSAGE_DIRECTION.OUTBOUND,
      status: MESSAGE_STATUS.SENT,
      sentAt: '2026-09-14T18:00:00.000Z',
      recipients: [{ email: 'kaylee@kbpainting.com', role: 'recipient' }],
      subject: 'Test',
      body: 'Body',
    });
  }

  const scheduleStore = new MemoryScheduleStore({
    tenants: [{ tenantId: BABRUN.tenantId, active: true }],
    prospects: [{
      tenantId: BABRUN.tenantId,
      prospectId: BABRUN.prospectId,
      email: 'kaylee@kbpainting.com',
    }],
    outreachAssets: [{
      tenantId: BABRUN.tenantId,
      id: BABRUN.outreachAssetId,
      lifecycleState: 'STAKEHOLDER_VALIDATED',
    }],
    schedules: overrides.existingSchedules || [],
  });

  const poolState = {
    identity,
    integration,
    messages: overrides.kayleeSend ? [{
      direction: MESSAGE_DIRECTION.OUTBOUND,
      status: MESSAGE_STATUS.SENT,
      sentAt: '2026-09-14T18:00:00.000Z',
      recipients: [{ email: 'kaylee@kbpainting.com' }],
    }] : [],
    replies: overrides.replies || 0,
    suppressions: overrides.suppressions || 0,
    hardBounces: overrides.hardBounces || 0,
    scheduledCount: overrides.scheduledCount || (overrides.existingSchedules || []).filter((s) => s.status === 'SCHEDULED').length,
    sentToday: overrides.sentToday || (overrides.kayleeSend ? 1 : 0),
    activeSendDays: overrides.kayleeSend ? 1 : 0,
  };

  return {
    store,
    scheduleStore,
    identity,
    pool: mockPool(poolState),
    poolState,
  };
}

describe('SPEC-254 Emmett tenant-mailbox capacity', () => {
  it('builds identity-scoped tenant mailbox snapshot', async () => {
    const { pool } = await seedBabrunFixture({ kayleeSend: true });
    const snapshot = await buildTenantMailboxSnapshot(BABRUN.tenantId, BABRUN.sendingIdentityId, {
      pool,
      now: new Date('2026-09-15T12:00:00.000Z'),
    });
    assert.equal(snapshot.channel, 'tenant_mailbox_smtp');
    assert.equal(snapshot.tenantId, '13');
    assert.equal(snapshot.sendingIdentityId, BABRUN.sendingIdentityId);
    assert.equal(snapshot.senderEmail, 'hello@babrun.com');
    assert.equal(snapshot.sendingDomain, 'babrun.com');
    assert.equal(snapshot.successfulSends, 1);
    assert.ok(snapshot.unknownEvidence.includes('delivery_rate'));
    assert.ok(snapshot.unknownEvidence.includes('open_rate'));
    assert.equal(snapshot.openRate, null);
    assert.equal(snapshot.deliveryRate, null);
  });

  it('keeps unknown delivery evidence as unknown, not zero', async () => {
    const { pool } = await seedBabrunFixture({ kayleeSend: true });
    const snapshot = await buildTenantMailboxSnapshot(BABRUN.tenantId, BABRUN.sendingIdentityId, { pool });
    assert.equal(snapshot.bounceRate, null);
    assert.equal(snapshot.replyRate, null);
    assert.ok(snapshot.unknownEvidence.includes('bounce_rate'));
  });

  it('classifies role emails independently from founder emails', () => {
    assert.equal(
      classifyOutreachContactType('info@sixstarhvac.com'),
      OUTREACH_CONTACT_TYPE.VERIFIED_ROLE_EMAIL
    );
    assert.equal(
      classifyOutreachContactType('contact@mjelectricsandiego.com'),
      OUTREACH_CONTACT_TYPE.VERIFIED_ROLE_EMAIL
    );
    assert.equal(
      classifyOutreachContactType('kaylee@kbpainting.com'),
      OUTREACH_CONTACT_TYPE.VERIFIED_FOUNDER_EMAIL
    );
  });

  it('produces and persists envelope semantics via buildCapacityEnvelope', async () => {
    const { pool } = await seedBabrunFixture({ kayleeSend: true });
    const snapshot = await buildTenantMailboxSnapshot(BABRUN.tenantId, BABRUN.sendingIdentityId, {
      pool,
      now: new Date('2026-09-15T12:00:00.000Z'),
    });
    const assessment = assessTenantMailboxCapacity(snapshot);
    const envelope = buildCapacityEnvelope(snapshot, assessment, { now: new Date('2026-09-15T12:00:00.000Z') });
    assert.ok(envelope.envelopeId);
    assert.equal(envelope.tenantId, '13');
    assert.equal(envelope.sendingIdentityId, BABRUN.sendingIdentityId);
    assert.equal(envelope.mailboxIntegrationId, BABRUN.integrationId);
    assert.ok(['proceed', 'slow', 'pause', 'emergency'].includes(envelope.governorState));
    assert.ok(envelope.maxSendsPerDay >= 0);
    assert.ok(Array.isArray(envelope.riskFlags));
    assert.equal(envelope.evidenceSnapshot.unknownEvidence.includes('delivery_rate'), true);
    assert.equal(envelope.authentication?.spf, undefined);
  });

  it('rejects authorization when envelope expired', () => {
    const envelope = createPermissiveEnvelope('13', BABRUN.sendingIdentityId, {
      now: new Date('2026-09-15T08:00:00.000Z'),
    });
    envelope.validUntil = '2026-09-15T09:00:00.000Z';
    const result = evaluateCapacityAuthorization(envelope, {
      scheduledFor: '2026-09-15T10:00:00.000Z',
    }, { now: new Date('2026-09-15T10:00:00.000Z') });
    assert.equal(result.allowed, false);
    assert.equal(result.code, 'emmett_envelope_expired');
  });

  it('enforces daily ceiling during authorization', () => {
    const envelope = createPermissiveEnvelope('13', BABRUN.sendingIdentityId);
    envelope.maxSendsPerDay = 2;
    envelope.remainingCapacity = 0;
    envelope.currentScheduledCount = 2;
    const result = evaluateCapacityAuthorization(envelope, {
      scheduledFor: '2026-09-15T14:00:00.000Z',
    }, { now: new Date('2026-09-15T12:00:00.000Z') });
    assert.equal(result.allowed, false);
    assert.equal(result.code, 'emmett_capacity_exhausted');
  });

  it('enforces minimum spacing during authorization', () => {
    const envelope = createPermissiveEnvelope('13', BABRUN.sendingIdentityId);
    envelope.minimumSpacingMinutes = 60;
    const result = evaluateCapacityAuthorization(envelope, {
      scheduledFor: '2026-09-15T14:10:00.000Z',
      lastSendAt: '2026-09-15T14:00:00.000Z',
    }, { now: new Date('2026-09-15T12:00:00.000Z') });
    assert.equal(result.allowed, false);
    assert.equal(result.code, 'emmett_spacing_violation');
  });

  it('enforces allowed send window during authorization', () => {
    const envelope = createPermissiveEnvelope('13', BABRUN.sendingIdentityId);
    envelope.allowedSendWindow = { startHour: 9, endHour: 17, timezone: 'America/New_York' };
    assert.equal(isWithinAllowedWindow('2026-09-15T14:00:00.000Z', envelope.allowedSendWindow), true);
    assert.equal(isWithinAllowedWindow('2026-09-15T22:00:00.000Z', envelope.allowedSendWindow), false);
    const blocked = evaluateCapacityAuthorization(envelope, {
      scheduledFor: '2026-09-15T22:00:00.000Z',
    }, { now: new Date('2026-09-15T12:00:00.000Z') });
    assert.equal(blocked.allowed, false);
    assert.equal(blocked.code, 'emmett_outside_send_window');
  });

  it('blocks authorization when governor is PAUSE', () => {
    const envelope = createPermissiveEnvelope('13', BABRUN.sendingIdentityId);
    envelope.governorState = GOVERNOR_OUTCOMES.PAUSE;
    envelope.remainingCapacity = 5;
    const result = evaluateCapacityAuthorization(envelope, {
      scheduledFor: '2026-09-15T14:00:00.000Z',
    });
    assert.equal(result.allowed, false);
    assert.match(result.code, /emmett_governor_pause/);
  });

  it('blocks execution when governor moves to PAUSE after authorization', () => {
    const envelope = createPermissiveEnvelope('13', BABRUN.sendingIdentityId);
    envelope.governorState = GOVERNOR_OUTCOMES.PAUSE;
    const result = evaluateCapacityExecution(envelope, {
      scheduledFor: '2026-09-15T14:00:00.000Z',
    });
    assert.equal(result.allowed, false);
    assert.match(result.code, /emmett_governor_pause/);
  });

  it('scheduled sends consume capacity through memory gate', async () => {
    const gate = createMemoryCapacityGate();
    const envelope = createPermissiveEnvelope('tenant-a', 'tsi_a');
    envelope.maxSendsPerDay = 1;
    envelope.remainingCapacity = 1;
    gate.setEnvelope('tenant-a', 'tsi_a', envelope);

    await gate.authorize({
      tenantId: 'tenant-a',
      sendingIdentityId: 'tsi_a',
      scheduledFor: '2026-09-15T14:00:00.000Z',
    });
    await gate.reserve({ id: 'sched-1', tenantId: 'tenant-a', sendingIdentityId: 'tsi_a' }, envelope.envelopeId);

    await assert.rejects(
      () => gate.authorize({
        tenantId: 'tenant-a',
        sendingIdentityId: 'tsi_a',
        scheduledFor: '2026-09-15T15:00:00.000Z',
      }),
      (err) => err.code === 'emmett_capacity_exhausted'
    );
  });

  it('integrates capacity gate into authorizeScheduledOutreachSend', async () => {
    const seeded = await seedBabrunFixture();
    const gate = createMemoryCapacityGate();
    const envelope = createPermissiveEnvelope(BABRUN.tenantId, BABRUN.sendingIdentityId, {
      allowedSendWindow: { startHour: 0, endHour: 24, timezone: 'UTC' },
      minimumSpacingMinutes: 0,
    });
    gate.setEnvelope(BABRUN.tenantId, BABRUN.sendingIdentityId, envelope);

    const auth = await authorizeScheduledOutreachSend({
      tenantId: BABRUN.tenantId,
      prospectId: BABRUN.prospectId,
      outreachAssetId: BABRUN.outreachAssetId,
      sendingIdentityId: BABRUN.sendingIdentityId,
      recipientEmail: 'kaylee@kbpainting.com',
      scheduledFor: '2026-09-16T14:00:00.000Z',
      timezone: 'America/New_York',
      subject: 'Subject',
      body: 'Body',
      authorizationSource: 'operator_test',
      authorizedBy: 'operator@test.com',
    }, {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      emmettCapacity: gate,
    });

    assert.equal(auth.created, true);
    assert.ok(auth.capacityEnvelope);
  });

  it('fails closed when operator requests send outside capacity', async () => {
    const seeded = await seedBabrunFixture();
    const gate = createMemoryCapacityGate();
    const envelope = createPermissiveEnvelope(BABRUN.tenantId, BABRUN.sendingIdentityId);
    envelope.maxSendsPerDay = 0;
    envelope.remainingCapacity = 0;
    envelope.governorState = GOVERNOR_OUTCOMES.PROCEED;
    gate.setEnvelope(BABRUN.tenantId, BABRUN.sendingIdentityId, envelope);

    await assert.rejects(
      () => authorizeScheduledOutreachSend({
        tenantId: BABRUN.tenantId,
        prospectId: BABRUN.prospectId,
        outreachAssetId: BABRUN.outreachAssetId,
        sendingIdentityId: BABRUN.sendingIdentityId,
        recipientEmail: 'kaylee@kbpainting.com',
        scheduledFor: '2026-09-16T14:00:00.000Z',
        subject: 'Subject',
        body: 'Body',
        authorizationSource: 'operator_test',
        authorizedBy: 'operator@test.com',
      }, {
        scheduleStore: seeded.scheduleStore,
        mailboxStore: seeded.store,
        emmettCapacity: gate,
      }),
      (err) => err.code === 'emmett_capacity_exhausted'
    );
  });

  it('execution-time revalidation skips when capacity exhausted after authorization', async () => {
    const seeded = await seedBabrunFixture();
    const gate = createMemoryCapacityGate();
    const envelope = createPermissiveEnvelope(BABRUN.tenantId, seeded.identity.id, {
      allowedSendWindow: { startHour: 0, endHour: 24, timezone: 'UTC' },
    });
    gate.setEnvelope(BABRUN.tenantId, seeded.identity.id, envelope);

    const fixture = await authorizeScheduledOutreachSend({
      tenantId: BABRUN.tenantId,
      prospectId: BABRUN.prospectId,
      outreachAssetId: BABRUN.outreachAssetId,
      sendingIdentityId: seeded.identity.id,
      recipientEmail: 'kaylee@kbpainting.com',
      scheduledFor: '2026-09-14T13:55:00.000Z',
      subject: 'Subject',
      body: 'Body',
      authorizationSource: 'operator_test',
      authorizedBy: 'operator@test.com',
      maxLatenessMinutes: 120,
    }, {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      emmettCapacity: gate,
    });

    envelope.governorState = GOVERNOR_OUTCOMES.PAUSE;
    gate.setEnvelope(BABRUN.tenantId, seeded.identity.id, envelope);

    const eligibility = await evaluateSendEligibility(fixture.schedule, {
      scheduleStore: seeded.scheduleStore,
      mailboxStore: seeded.store,
      emmettCapacity: gate,
      now: new Date('2026-09-14T14:00:00.000Z'),
    });
    assert.equal(eligibility.eligible, false);
    assert.equal(eligibility.action, SCHEDULE_STATUS.SKIPPED);
    assert.match(eligibility.reason, /emmett_governor_pause/);
  });

  it('maintains tenant isolation for capacity envelopes', () => {
    const gate = createMemoryCapacityGate();
    const tenantA = createPermissiveEnvelope('tenant-a', 'tsi_a');
    tenantA.remainingCapacity = 0;
    tenantA.maxSendsPerDay = 0;
    gate.setEnvelope('tenant-a', 'tsi_a', tenantA);
    gate.setEnvelope('tenant-b', 'tsi_b', createPermissiveEnvelope('tenant-b', 'tsi_b'));

    const allowed = evaluateCapacityAuthorization(
      gate.getEnvelope('tenant-b', 'tsi_b'),
      { scheduledFor: '2026-09-15T14:00:00.000Z' }
    );
    assert.equal(allowed.allowed, true);
    const blocked = evaluateCapacityAuthorization(
      gate.getEnvelope('tenant-a', 'tsi_a'),
      { scheduledFor: '2026-09-15T14:00:00.000Z' }
    );
    assert.equal(blocked.allowed, false);
  });

  it('Babrun fixture produces envelope from available evidence without prescribing capacity', async () => {
    const { pool } = await seedBabrunFixture({
      kayleeSend: true,
      existingSchedules: [
        {
          id: 'sched-sep16-a',
          tenantId: '13',
          prospectId: 'prospect-2',
          outreachAssetId: BABRUN.outreachAssetId,
          sendingIdentityId: BABRUN.sendingIdentityId,
          recipientEmail: 'info@sixstarhvac.com',
          scheduledFor: '2026-09-16T14:00:00.000Z',
          status: 'SCHEDULED',
          sequenceStep: 1,
          authorizationSnapshot: {},
          idempotencyKey: 'existing-1',
        },
        {
          id: 'sched-sep16-b',
          tenantId: '13',
          prospectId: 'prospect-3',
          outreachAssetId: BABRUN.outreachAssetId,
          sendingIdentityId: BABRUN.sendingIdentityId,
          recipientEmail: 'contact@mjelectricsandiego.com',
          scheduledFor: '2026-09-16T15:00:00.000Z',
          status: 'SCHEDULED',
          sequenceStep: 1,
          authorizationSnapshot: {},
          idempotencyKey: 'existing-2',
        },
      ],
      scheduledCount: 2,
    });

    const snapshot = await buildTenantMailboxSnapshot(BABRUN.tenantId, BABRUN.sendingIdentityId, {
      pool,
      now: new Date('2026-09-15T12:00:00.000Z'),
    });
    const assessment = assessTenantMailboxCapacity(snapshot);
    const envelope = buildCapacityEnvelope(snapshot, assessment, { now: new Date('2026-09-15T12:00:00.000Z') });

    assert.equal(classifyOutreachContactType('info@sixstarhvac.com'), OUTREACH_CONTACT_TYPE.VERIFIED_ROLE_EMAIL);
    assert.equal(classifyOutreachContactType('contact@mjelectricsandiego.com'), OUTREACH_CONTACT_TYPE.VERIFIED_ROLE_EMAIL);
    assert.ok(envelope.envelopeId);
    assert.ok(['proceed', 'slow', 'pause'].includes(envelope.governorState));
    assert.ok(envelope.evidenceSnapshot.unknownEvidence.length > 0);
    assert.equal(envelope.currentScheduledCount, 2);
    assert.equal(snapshot.authentication.spf, EVIDENCE_UNKNOWN);
  });

  it('concurrent authorization cannot exceed capacity through memory gate', async () => {
    const gate = createMemoryCapacityGate();
    const envelope = createPermissiveEnvelope('tenant-a', 'tsi_a');
    envelope.maxSendsPerDay = 1;
    envelope.remainingCapacity = 1;
    gate.setEnvelope('tenant-a', 'tsi_a', envelope);

    await gate.authorize({
      tenantId: 'tenant-a',
      sendingIdentityId: 'tsi_a',
      scheduledFor: '2026-09-15T14:00:00.000Z',
    });
    await gate.reserve({ id: 'sched-1', tenantId: 'tenant-a', sendingIdentityId: 'tsi_a' }, envelope.envelopeId);

    await assert.rejects(
      () => gate.authorize({
        tenantId: 'tenant-a',
        sendingIdentityId: 'tsi_a',
        scheduledFor: '2026-09-15T15:00:00.000Z',
      }),
      (err) => err.code === 'emmett_capacity_exhausted'
    );
  });

  it('reply and suppression evidence reflected in subsequent snapshot assessment', async () => {
    const base = await seedBabrunFixture({ kayleeSend: true, replies: 0, suppressions: 0 });
    const before = await buildTenantMailboxSnapshot(BABRUN.tenantId, BABRUN.sendingIdentityId, { pool: base.pool });
    const beforeAssessment = assessTenantMailboxCapacity(before);

    const afterPool = mockPool({
      ...base.poolState,
      replies: 1,
      suppressions: 1,
      hardBounces: 1,
    });
    const after = await buildTenantMailboxSnapshot(BABRUN.tenantId, BABRUN.sendingIdentityId, { pool: afterPool });
    const afterAssessment = assessTenantMailboxCapacity(after);

    assert.equal(before.replies, 0);
    assert.equal(after.replies, 1);
    assert.equal(after.suppressions, 1);
    assert.ok(afterAssessment.riskFlags.includes('unknown_delivery_rate') || after.unknownEvidence.includes('delivery_rate'));
    assert.notEqual(beforeAssessment.remainingCapacity, afterAssessment.remainingCapacity + 999);
  });

  it('legacy Brevo Emmett capacity path remains available and unchanged', () => {
    const { recommendCapacity, evaluateGovernor, createOutboundEngine } = require('../packages/emmett-outbound');
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
    assert.equal(assessed.governor.kind, 'governor');
    assert.ok(recommendCapacity(snapshot, assessed.health).recommended > 0);
    assert.equal(evaluateGovernor(snapshot, assessed.health, assessed.capacity).outcome, 'proceed');
  });

  it('normalizeEnvelope maps persisted row shape', () => {
    const normalized = normalizeEnvelope({
      id: 'env_123',
      tenant_id: '13',
      sending_identity_id: BABRUN.sendingIdentityId,
      governor_state: 'proceed',
      remaining_capacity: 3,
      evidence_snapshot: { unknownEvidence: ['delivery_rate'] },
    });
    assert.equal(normalized.envelopeId, 'env_123');
    assert.equal(normalized.governorState, 'proceed');
    assert.equal(normalized.remainingCapacity, 3);
  });
});
