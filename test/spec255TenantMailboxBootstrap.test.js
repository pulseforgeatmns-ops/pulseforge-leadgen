'use strict';

/**
 * SPEC-255 — Emmett tenant-mailbox bootstrap & auth evidence.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const eoi = require('../packages/emmett-outbound');
const {
  AUTH_STATE,
  authStateOf,
  authPass,
  authFail,
  authUnknown,
  authenticationFromVerificationState,
  allAuthPass,
  assessBootstrapEligibility,
  applyBootstrapCapacity,
  hasBootstrapNegativeEvidence,
  shouldExitBootstrap,
  resolveBootstrapAllowance,
  buildCapacityEnvelope,
  recommendCapacity,
  recommendCapacityNormal,
  scoreInboxHealth,
  evaluateGovernor,
  evaluateSend,
  GOVERNOR_OUTCOMES,
  PLAN_STATUS,
  BOOTSTRAP_MODE,
} = eoi;


function babrunVerificationState(overrides = {}) {
  return {
    smtp: { status: 'verified' },
    imap: { status: 'verified' },
    spf: { status: 'present' },
    dkim: { status: 'present' },
    dmarc: { status: 'present' },
    ...overrides,
  };
}

function youngTenantSnapshot(overrides = {}) {
  const auth = authenticationFromVerificationState(babrunVerificationState(overrides.verificationState));
  return {
    tenantId: '13',
    clientId: 13,
    sendingIdentityId: 'tsi_13_babrun_fedir',
    mailboxIntegrationId: 'tmi_13_babrun_hello',
    inboxAgeDays: 2,
    inboxAgeSource: 'created_at',
    providerCeiling: 3,
    mailboxStatus: 'active',
    identityStatus: 'active',
    deliverabilityObservability: 'limited',
    authentication: auth,
    warmup: { status: 'warming', dailyCap: 3, activeSendDays: 0, reset: true },
    bounceRate: 0,
    replyRate: null,
    openRate: null,
    complaintRate: 0,
    hardBounceCount: 0,
    blacklist: { listed: false },
    sentToday: 0,
    sentYesterday: 0,
    recentSends: 0,
    totalOperationalSends: 0,
    scheduledToday: 0,
    ...overrides,
    authentication: overrides.authentication || auth,
  };
}

describe('SPEC-255 auth evidence bridge', () => {
  it('maps persisted SPF/DKIM/DMARC present → PASS', () => {
    const auth = authenticationFromVerificationState(babrunVerificationState());
    assert.equal(authStateOf(auth.spf), AUTH_STATE.PASS);
    assert.equal(authStateOf(auth.dkim), AUTH_STATE.PASS);
    assert.equal(authStateOf(auth.dmarc), AUTH_STATE.PASS);
    assert.equal(authStateOf(auth.smtp), AUTH_STATE.PASS);
    assert.equal(auth.spf.provenance.source, 'verification_state');
  });

  it('maps explicit failed auth → FAIL', () => {
    const auth = authenticationFromVerificationState({
      spf: { status: 'missing' },
      dkim: { status: 'failed', code: 'dkim_missing' },
      dmarc: { status: 'missing' },
      smtp: { status: 'failed', message: 'auth failed' },
    });
    assert.equal(authStateOf(auth.spf), AUTH_STATE.FAIL);
    assert.equal(authStateOf(auth.dkim), AUTH_STATE.FAIL);
    assert.equal(authStateOf(auth.smtp), AUTH_STATE.FAIL);
  });

  it('maps absent auth evidence → UNKNOWN', () => {
    const auth = authenticationFromVerificationState({
      spf: { status: 'not_checked' },
      dkim: { status: 'not_checked', reason: 'dkim_selector_required' },
      dmarc: {},
    });
    assert.equal(authStateOf(auth.spf), AUTH_STATE.UNKNOWN);
    assert.equal(authStateOf(auth.dkim), AUTH_STATE.UNKNOWN);
    assert.equal(authStateOf(auth.dmarc), AUTH_STATE.UNKNOWN);
  });

  it('UNKNOWN never becomes false implicitly via authPass/authFail', () => {
    const unknown = { state: AUTH_STATE.UNKNOWN };
    assert.equal(authPass(unknown), false);
    assert.equal(authFail(unknown), false);
    assert.equal(authUnknown(unknown), true);
    assert.equal(authPass(false), false);
    assert.equal(authFail(false), true);
  });

  it('legacy boolean auth still works for Brevo clients', () => {
    const health = scoreInboxHealth({
      authentication: { spf: true, dkim: true, dmarc: 'reject' },
      warmup: { status: 'healthy' },
      inboxAgeDays: 47,
    });
    assert.ok(health.score >= 70);
  });
});

describe('SPEC-255 bootstrap eligibility and capacity', () => {
  it('new authenticated mailbox can enter BOOTSTRAP when normal capacity is zero', () => {
    const snapshot = youngTenantSnapshot();
    const health = scoreInboxHealth(snapshot);
    const normal = recommendCapacityNormal(snapshot, health);
    assert.equal(normal.recommended, 0);

    const capacity = recommendCapacity(snapshot, health);
    assert.equal(capacity.mode, BOOTSTRAP_MODE);
    assert.equal(capacity.bootstrap.active, true);
    assert.ok(capacity.recommended >= 1);
  });

  it('bootstrap capacity is bounded by warm-up ceiling', () => {
    const snapshot = youngTenantSnapshot({ warmup: { status: 'warming', dailyCap: 3, activeSendDays: 0 } });
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    assert.ok(capacity.recommended <= 3);
    assert.ok(capacity.recommended >= 1);
    assert.equal(capacity.ceiling, 3);
  });

  it('bootstrap envelope includes spacing and business-hours constraints', () => {
    const snapshot = youngTenantSnapshot();
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    const envelope = buildCapacityEnvelope({ snapshot, health, capacity });
    assert.ok(envelope.bootstrap.active);
    assert.ok(envelope.spacing.minSpacingMinutes >= 60);
    assert.ok(envelope.spacing.businessHours.startHour != null);
  });

  it('daily accounting is reflected in envelope remaining', () => {
    const snapshot = youngTenantSnapshot({ sentToday: 1, scheduledToday: 1 });
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    const governor = evaluateGovernor(snapshot, health, capacity);
    const envelope = buildCapacityEnvelope({
      snapshot,
      health,
      capacity,
      governor,
      sentToday: 1,
      scheduledToday: 1,
    });
    assert.equal(envelope.accounting.sentToday, 1);
    assert.equal(envelope.accounting.scheduledToday, 1);
    assert.ok(envelope.accounting.remaining <= envelope.recommended);
  });

  it('established mailbox never receives bootstrap floor', () => {
    const snapshot = youngTenantSnapshot({
      inboxAgeDays: 45,
      warmup: { status: 'healthy', dailyCap: 20, activeSendDays: 20 },
      recentSends: 80,
      totalOperationalSends: 80,
    });
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    assert.notEqual(capacity.mode, BOOTSTRAP_MODE);
    assert.notEqual(capacity.bootstrap?.active, true);
  });

  it('UNKNOWN critical auth does not qualify for bootstrap', () => {
    const snapshot = youngTenantSnapshot({
      authentication: authenticationFromVerificationState({
        smtp: { status: 'verified' },
        spf: { status: 'present' },
        dkim: { status: 'not_checked' },
        dmarc: { status: 'present' },
      }),
    });
    const health = scoreInboxHealth(snapshot);
    const normal = recommendCapacityNormal(snapshot, health);
    const capacity = recommendCapacity(snapshot, health);
    assert.equal(assessBootstrapEligibility(snapshot, health, normal).eligible, false);
    assert.notEqual(capacity.bootstrap?.active, true);
  });
});

describe('SPEC-255 governor and negative evidence', () => {
  it('negative evidence immediately PAUSEs bootstrap', () => {
    const snapshot = youngTenantSnapshot({ hardBounceCount: 1, bounceRate: 0.5, recentSends: 2 });
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    const governor = evaluateGovernor(snapshot, health, capacity);
    assert.equal(governor.halt, true);
    assert.ok([GOVERNOR_OUTCOMES.PAUSE, GOVERNOR_OUTCOMES.EMERGENCY].includes(governor.outcome));
  });

  it('complaint triggers emergency even during bootstrap', () => {
    const snapshot = youngTenantSnapshot({ complaintRate: 0.002 });
    const negative = hasBootstrapNegativeEvidence(snapshot);
    assert.equal(negative.blocked, true);
    assert.equal(negative.reason, 'complaint');
  });

  it('bootstrap exits into normal capacity when sufficient history exists', () => {
    const snapshot = youngTenantSnapshot({
      inboxAgeDays: 20,
      warmup: { status: 'healthy', dailyCap: 10, activeSendDays: 10 },
      recentSends: 20,
      totalOperationalSends: 20,
    });
    const health = scoreInboxHealth({ ...snapshot, replyRate: 0.05 });
    const exit = shouldExitBootstrap(snapshot, health);
    assert.equal(exit.exit, true);
    const capacity = recommendCapacity(snapshot, health);
    assert.notEqual(capacity.bootstrap?.active, true);
  });

  it('bootstrap governor outcome is SLOW not PAUSE when allowance > 0', () => {
    const snapshot = youngTenantSnapshot();
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    const governor = evaluateGovernor(snapshot, health, capacity);
    assert.equal(governor.outcome, GOVERNOR_OUTCOMES.SLOW);
    assert.equal(governor.halt, false);
    assert.equal(governor.slowCap, capacity.recommended);
  });
});

describe('SPEC-255 SPEC-254 envelope integration', () => {
  it('envelope is the canonical authority object for tenant outreach', () => {
    const snapshot = youngTenantSnapshot();
    const envelope = buildCapacityEnvelope({ snapshot });
    assert.equal(envelope.kind, 'capacity_envelope');
    assert.equal(envelope.spec, 'SPEC-255');
    assert.ok(envelope.governor.outcome);
    assert.ok(Array.isArray(envelope.decisiveReasoning));
    assert.ok(envelope.authentication.spf);
  });

  it('execution revalidation via evaluateSend respects bootstrap slow cap', () => {
    const snapshot = youngTenantSnapshot();
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    const governor = evaluateGovernor(snapshot, health, capacity);
    const approvedPlan = {
      status: PLAN_STATUS.APPROVED,
      localDate: snapshot.localDate || '2026-09-16',
      approvedCapacity: capacity.recommended,
    };
    const allowed = evaluateSend({
      governor,
      capacity,
      approvedPlan,
      candidate: {
        paige: { author: 'paige', subject: 'Hi', body: 'Body' },
        contentSource: 'paige',
      },
      sentToday: 0,
      localDate: approvedPlan.localDate,
    });
    assert.equal(allowed.allowed, true);

    const exhausted = evaluateSend({
      governor,
      capacity,
      approvedPlan,
      candidate: {
        paige: { author: 'paige', subject: 'Hi', body: 'Body' },
        contentSource: 'paige',
      },
      sentToday: capacity.recommended,
      localDate: approvedPlan.localDate,
    });
    assert.equal(exhausted.allowed, false);
    assert.equal(exhausted.code, 'capacity_exhausted');
  });

  it('tenant isolation — envelope carries identity scope', async () => {
    const { buildTenantMailboxInboxSnapshot } = require('../services/emmettTenantMailboxSnapshot');
    const snapshot = await buildTenantMailboxInboxSnapshot({
      tenantId: '13',
      sendingIdentityId: 'tsi_13_babrun_fedir',
      integration: {
        id: 'tmi_13_babrun_hello',
        status: 'active',
        verificationState: babrunVerificationState(),
        createdAt: '2026-09-13T00:00:00.000Z',
        mailboxAddress: 'hello@babrun.com',
      },
      identity: {
        id: 'tsi_13_babrun_fedir',
        status: 'active',
        senderEmail: 'hello@babrun.com',
        mailboxIntegrationId: 'tmi_13_babrun_hello',
        createdAt: '2026-09-13T00:00:00.000Z',
      },
      sendStats: { sentToday: 0, totalOperationalSends: 0, activeSendDays: 0 },
    }, { now: new Date('2026-09-15T14:00:00.000Z') });

    assert.equal(snapshot.sendingIdentityId, 'tsi_13_babrun_fedir');
    assert.equal(allAuthPass(snapshot.authentication), true);
    assert.equal(snapshot.deliverabilityObservability, 'limited');
  });
});

describe('SPEC-255 Babrun acceptance fixture (contract-driven)', () => {
  it('decision follows canonical contract from Babrun-like evidence', () => {
    const snapshot = youngTenantSnapshot({
      inboxAgeDays: 2,
      warmup: { status: 'warming', dailyCap: 3, activeSendDays: 0, reset: true },
    });
    const health = scoreInboxHealth(snapshot);
    const normal = recommendCapacityNormal(snapshot, health);
    const capacity = recommendCapacity(snapshot, health);
    const governor = evaluateGovernor(snapshot, health, capacity);
    const envelope = buildCapacityEnvelope({
      snapshot,
      health,
      capacity,
      governor,
      sendingIdentityId: 'tsi_13_babrun_fedir',
      mailboxIntegrationId: 'tmi_13_babrun_hello',
    });

    assert.equal(allAuthPass(snapshot.authentication), true);
    assert.equal(normal.recommended, 0, 'normal SPEC-117 math deadlocks at zero for young mailbox');
    assert.equal(capacity.mode, BOOTSTRAP_MODE);
    assert.ok(capacity.recommended >= 1);
    assert.equal(governor.outcome, GOVERNOR_OUTCOMES.SLOW);
    assert.ok(envelope.decisiveReasoning.some((line) => /bootstrap/i.test(line)));
    assert.ok(resolveBootstrapAllowance(snapshot) <= 3);
  });
});
