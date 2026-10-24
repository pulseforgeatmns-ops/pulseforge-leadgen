'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { policy, windowReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const {
  resolveOperatorProgramTotalCapForDelegation,
  resolveBoundedGrantHorizon,
  describeOperatorAuthorityEnvelope,
  countGrantWeekdaySlots,
  DEFAULT_BOUNDED_GRANT_HORIZON_DAYS,
} = require('../packages/emmett-outbound/OperatorDelegatedCapacity');
const { assessOperatingCapacity, LIMITING_FACTORS } = require('../packages/emmett-outbound/OperatingCapacity');
const { service } = require('../services/governedOutbound');
const { policy: buildPolicy } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const {
  buildDryRunReport,
  authoritySnapshot,
  OPERATING_GRANT,
} = require('../scripts/migrateBabrunOperatorDelegatedCapacity');

const BABRUN_BASE = {
  tenantId: '13',
  sourceMissionId: 'mission',
  senderEmail: 'hello@babrun.com',
  inboxIntegrationId: 'mailbox',
  sendingIdentityId: 'identity',
  dailyCap: 1,
  totalCap: 1,
  spacingMinutes: 240,
  startHour: 9,
  endHour: 16,
};

function babrunPolicy(extra = {}, now = new Date('2026-09-28T15:00:00Z')) {
  return policy({
    ...BABRUN_BASE,
    startsAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 14 * 86400000).toISOString(),
    ...extra,
  }, now);
}

test('delegated daily max above legacy totalCap requires migrated program totalCap', () => {
  const now = new Date('2026-09-28T15:00:00Z');
  const current = {
    ...BABRUN_BASE,
    startsAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 14 * 86400000).toISOString(),
  };
  assert.throws(
    () => babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 1 }, now),
    (err) => err.code === 'invalid_bounds',
  );
  const migratedTotal = resolveOperatorProgramTotalCapForDelegation(current, 20);
  assert.ok(migratedTotal >= 20);
  const envelope = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: migratedTotal }, now);
  assert.equal(envelope.operatorDelegatedMaximumDailyCapacity, 20);
  assert.equal(envelope.dailyCap, 1);
  assert.ok(envelope.totalCap >= 20);
});

test('activation safety grant totalCap is replaced for delegated ramp envelope', () => {
  const now = new Date('2026-09-28T15:00:00Z');
  const current = {
    ...BABRUN_BASE,
    startsAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 7 * 86400000).toISOString(),
  };
  const slots = countGrantWeekdaySlots(current);
  const migratedTotal = resolveOperatorProgramTotalCapForDelegation(current, 20);
  assert.equal(migratedTotal, Math.min(100, Math.max(20, 20 * Math.max(slots, 1))));
});

test('Anchor legacy policy without delegated field is unchanged by totalCap helper', () => {
  const now = new Date('2026-09-18T14:00:00Z');
  const anchor = {
    tenantId: '10',
    sourceMissionId: 'source',
    senderEmail: 'sender@anchor.example',
    inboxIntegrationId: 'inbox',
    aoOwnerIds: [1],
    startsAt: now.toISOString(),
    expiresAt: '2026-10-01T00:00:00Z',
    dailyCap: 5,
    totalCap: 100,
  };
  const p = policy(anchor, now);
  assert.equal(resolveOperatorProgramTotalCapForDelegation(p, 20), 100);
  assert.equal(p.operatorDelegatedMaximumDailyCapacity, undefined);
});

test('Emmett 4 with operator daily 20 yields authorization-limited capacity 4', () => {
  const grant = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 100 });
  const operating = assessOperatingCapacity({
    assessed: { capacity: { recommended: 4 }, governor: { outcome: 'proceed' }, health: { score: 80 } },
    policy: grant,
    now: new Date('2026-09-28T15:00:00.000Z'),
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.authorizationLimitedCapacity, 4);
  assert.equal(operating.operatorDelegatedMaximumDailyCapacity, 20);
});

test('program totalCap bounds cumulative sends after prior attempts', () => {
  const grant = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 25 });
  const operating = assessOperatingCapacity({
    assessed: { capacity: { recommended: 20 }, governor: { outcome: 'proceed' }, health: { score: 80 } },
    policy: grant,
    totalAttempted: 24,
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.remainingTotalAuthorization, 1);
  assert.equal(operating.authorizationLimitedCapacity, 1);
  assert.equal(operating.limitingFactor, LIMITING_FACTORS.AUTHORIZATION_REMAINING_TOTAL);
});

test('operator pause and expiry still fail closed', () => {
  const grant = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 100 });
  const paused = assessOperatingCapacity({
    assessed: { capacity: { recommended: 12 }, governor: { outcome: 'pause', halt: true } },
    policy: grant,
  });
  assert.equal(paused.authorizationLimitedCapacity, 0);
  assert.equal(paused.capacityLimitingAuthority, 'governor_pause');
  assert.equal(windowReason(grant, new Date('2026-10-15T15:00:00Z')), 'authorization_expired');
});

test('migration authority envelope reports effective operator ceilings', () => {
  const now = new Date('2026-09-28T15:00:00Z');
  const before = describeOperatorAuthorityEnvelope({
    dailyCap: 1,
    totalCap: 1,
  });
  assert.equal(before.operatorDelegatedMaximumDailyCapacity, null);
  assert.equal(before.effectiveOperatorDailyCeiling, 1);

  const migratedTotal = resolveOperatorProgramTotalCapForDelegation({
    ...BABRUN_BASE,
    startsAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + 14 * 86400000).toISOString(),
  }, 20);
  const after = describeOperatorAuthorityEnvelope({
    dailyCap: 1,
    totalCap: migratedTotal,
    operatorDelegatedMaximumDailyCapacity: 20,
  });
  assert.equal(after.effectiveOperatorDailyCeiling, 20);
  assert.equal(after.effectiveOperatorProgramCeiling, migratedTotal);
});

test('activation grant near expiry migrates to delegated 20, totalCap 100, expiresAt authorization+30d', async () => {
  const authNow = new Date('2026-10-04T13:00:00.000Z');
  const activation = {
    tenantId: '13',
    sourceMissionId: 'mission_validation_238a254c0cb798758b3519af',
    senderEmail: 'hello@babrun.com',
    inboxIntegrationId: 'tmi_13_babrun_hello',
    sendingIdentityId: 'tsi_13_babrun_fedir',
    dailyCap: 1,
    totalCap: 1,
    spacingMinutes: 240,
    startHour: 9,
    endHour: 16,
    allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'],
    startsAt: '2026-09-28T12:00:00.000Z',
    expiresAt: '2026-10-05T21:16:09.082Z',
  };
  let migratedPolicy = null;
  const programRow = {
    id: 'outbound_2629e4b153857a14d33c7765',
    policy: activation,
    policy_hash: 'old',
    scope_hash: 'scope',
    source_mission_id: activation.sourceMissionId,
  };
  const svc = service({ pool: { query: async () => ({ rows: [] }) }, tenantId: '13', now: () => authNow, adapters: {} });
  Object.assign(svc.store, {
    program: async () => programRow,
    migrateProgramPolicy: async (_program, nextPolicy) => {
      migratedPolicy = nextPolicy;
      return { id: programRow.id, policy: nextPolicy };
    },
  });
  const preview = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: DEFAULT_BOUNDED_GRANT_HORIZON_DAYS,
  }, { id: '1', role: 'admin' });
  assert.equal(preview.reviewRequired, true);
  assert.equal(preview.policy.operatorDelegatedMaximumDailyCapacity, 20);
  assert.equal(preview.policy.totalCap, 100);
  assert.equal(preview.policy.dailyCap, 1);
  assert.equal(preview.policy.spacingMinutes, 240);
  const horizon = resolveBoundedGrantHorizon(authNow, DEFAULT_BOUNDED_GRANT_HORIZON_DAYS);
  assert.equal(preview.policy.startsAt, horizon.startsAt);
  assert.equal(preview.policy.expiresAt, horizon.expiresAt);
  assert.equal(migratedPolicy, null);

  const applied = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: DEFAULT_BOUNDED_GRANT_HORIZON_DAYS,
    reviewHash: preview.reviewHash,
    authorizationInstant: preview.authorizationInstant,
  }, { id: '1', role: 'admin' });
  assert.equal(applied.policy.totalCap, 100);
});

test('Emmett recommended 0 yields effective authorization capacity 0', () => {
  const grant = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 100 });
  const operating = assessOperatingCapacity({
    assessed: { capacity: { recommended: 0 }, governor: { outcome: 'proceed' }, health: { score: 80 } },
    policy: grant,
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.authorizationLimitedCapacity, 0);
});

test('Emmett recommended 25 yields effective authorization capacity 20', () => {
  const grant = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 100 });
  const operating = assessOperatingCapacity({
    assessed: { capacity: { recommended: 25 }, governor: { outcome: 'proceed' }, health: { score: 80 } },
    policy: grant,
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.authorizationLimitedCapacity, 20);
  assert.equal(operating.capacityLimitingAuthority, 'operator_ceiling');
});

test('cumulative total authority remains bounded at 100 across remaining grant', () => {
  const grant = babrunPolicy({ operatorDelegatedMaximumDailyCapacity: 20, totalCap: 100 });
  const operating = assessOperatingCapacity({
    assessed: { capacity: { recommended: 20 }, governor: { outcome: 'proceed' }, health: { score: 80 } },
    policy: grant,
    totalAttempted: 99,
    schedule: { allowedSendWindow: { startHour: 0, endHour: 24 }, minSpacingMinutes: 30 },
  });
  assert.equal(operating.remainingTotalAuthorization, 1);
  assert.equal(operating.authorizationLimitedCapacity, 1);
});

test('migration preview preserves send history fields on program row (policy-only migration)', async () => {
  const authNow = new Date('2026-10-04T13:00:00.000Z');
  const activation = {
    ...BABRUN_BASE,
    startsAt: '2026-09-28T12:00:00.000Z',
    expiresAt: '2026-10-05T21:16:09.082Z',
  };
  const events = [];
  const programRow = {
    id: 'prog',
    policy: activation,
    scope_hash: 'scope',
    source_mission_id: 'mission',
    sent_history_marker: 'preserved',
  };
  const svc = service({ pool: { query: async () => ({ rows: [] }) }, tenantId: '13', now: () => authNow, adapters: {} });
  Object.assign(svc.store, {
    program: async () => programRow,
    migrateProgramPolicy: async (program, nextPolicy, actor, authorization) => {
      events.push({ type: 'migrate', authorization });
      return { ...program, policy: nextPolicy, sent_history_marker: program.sent_history_marker };
    },
    counts: async () => { throw new Error('counts must not run during migration'); },
    event: async () => { throw new Error('tick events must not run during migration'); },
  });
  const preview = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: 30,
  }, { id: '1', role: 'admin' });
  assert.equal(preview.reviewRequired, true);
  const applied = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: 30,
    reviewHash: preview.reviewHash,
    authorizationInstant: preview.authorizationInstant,
  }, { id: '1', role: 'admin' });
  assert.equal(applied.policy.totalCap, 100);
  assert.equal(events.length, 1);
  assert.equal(events[0].authorization.grantHorizonDays, 30);
  assert.equal(applied.sent_history_marker, 'preserved');
});

test('spacing remains Emmett-governed at 240 after operating grant migration preview', async () => {
  const authNow = new Date('2026-10-04T13:00:00.000Z');
  const svc = service({ pool: { query: async () => ({ rows: [] }) }, tenantId: '13', now: () => authNow, adapters: {} });
  Object.assign(svc.store, {
    program: async () => ({
      id: 'prog',
      policy: {
        ...BABRUN_BASE,
        startsAt: '2026-09-28T12:00:00.000Z',
        expiresAt: '2026-10-05T21:16:09.082Z',
      },
      scope_hash: 'scope',
      source_mission_id: 'mission',
    }),
    migrateProgramPolicy: async () => ({}),
  });
  const preview = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: 30,
  }, { id: '1', role: 'admin' });
  assert.equal(preview.policy.spacingMinutes, 240);
});

test('Anchor legacy grant totalCap helper unchanged when horizon renewal is Babrun-only', () => {
  const now = new Date('2026-09-18T14:00:00Z');
  const anchor = {
    tenantId: '10',
    sourceMissionId: 'source',
    senderEmail: 'sender@anchor.example',
    inboxIntegrationId: 'inbox',
    aoOwnerIds: [1],
    startsAt: now.toISOString(),
    expiresAt: '2026-10-01T00:00:00Z',
    dailyCap: 5,
    totalCap: 100,
  };
  const p = buildPolicy(anchor, now);
  assert.equal(resolveOperatorProgramTotalCapForDelegation(p, 20), 100);
});

test('migration dry-run is idempotent for the same authorization instant', async () => {
  const authNow = new Date('2026-10-04T13:00:00.000Z');
  const program = {
    id: 'prog',
    policy: {
      ...BABRUN_BASE,
      startsAt: '2026-09-28T12:00:00.000Z',
      expiresAt: '2026-10-05T21:16:09.082Z',
    },
    scope_hash: 'scope',
    source_mission_id: 'mission',
  };
  const svc = service({ pool: { query: async () => ({ rows: [] }) }, tenantId: '13', now: () => authNow, adapters: {} });
  Object.assign(svc.store, { program: async () => program });
  const first = await svc.migrateOperatorDelegatedCapacity(OPERATING_GRANT, { id: '1', role: 'admin' });
  const second = await svc.migrateOperatorDelegatedCapacity(OPERATING_GRANT, { id: '1', role: 'admin' });
  assert.equal(first.reviewHash, second.reviewHash);
  assert.deepEqual(first.policy, second.policy);
});

test('migration review path creates no send, schedule, or Emmett reservation side effects', async () => {
  const authNow = new Date('2026-10-04T13:00:00.000Z');
  const svc = service({
    pool: { query: async () => ({ rows: [] }) },
    tenantId: '13',
    now: () => authNow,
    adapters: {
      infrastructure: async () => { throw new Error('Emmett reservation must not run during migration review'); },
    },
  });
  Object.assign(svc.store, {
    program: async () => ({
      id: 'prog',
      policy: { ...BABRUN_BASE, startsAt: '2026-09-28T12:00:00.000Z', expiresAt: '2026-10-05T21:16:09.082Z' },
      scope_hash: 'scope',
      source_mission_id: 'mission',
    }),
    migrateProgramPolicy: async () => { throw new Error('migrate must not run without reviewHash'); },
    counts: async () => { throw new Error('counts must not run during migration review'); },
  });
  const preview = await svc.migrateOperatorDelegatedCapacity(OPERATING_GRANT, { id: '1', role: 'admin' });
  assert.equal(preview.reviewRequired, true);
});

test('dry-run report includes BEFORE/AFTER authority and preserved safety fields', () => {
  const program = {
    id: 'outbound_test',
    policy: {
      dailyCap: 1,
      totalCap: 1,
      startsAt: '2026-09-28T12:00:00.000Z',
      expiresAt: '2026-10-05T21:16:09.082Z',
      spacingMinutes: 240,
      startHour: 9,
      endHour: 16,
      timeZone: 'America/New_York',
      weekdays: [1, 2, 3, 4, 5],
    },
  };
  const preview = {
    reviewRequired: true,
    reviewHash: 'abc',
    migration: 'operator_delegated_maximum_daily_capacity',
    authority: {
      before: authoritySnapshot(program.policy),
      after: {
        dailyCap: 1,
        totalCap: 100,
        operatorDelegatedMaximumDailyCapacity: 20,
        startsAt: '2026-10-04T13:00:00.000Z',
        expiresAt: '2026-11-03T13:00:00.000Z',
        effectiveOperatorDailyCeiling: 20,
        effectiveOperatorProgramCeiling: 100,
      },
    },
    grantHorizon: { grantHorizonDays: 30 },
    policy: {
      ...program.policy,
      operatorDelegatedMaximumDailyCapacity: 20,
      totalCap: 100,
      senderEmail: 'hello@babrun.com',
      sendingIdentityId: 'tsi_13_babrun_fedir',
      inboxIntegrationId: 'tmi_13_babrun_hello',
      sourceMissionId: 'mission_validation_238a254c0cb798758b3519af',
      allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'],
    },
  };
  const report = buildDryRunReport({
    program,
    preview,
    emmett: { recommendedSafeDailyCapacity: 4, authorizationLimitedCapacity: 4, capacityLimitingAuthority: 'emmett' },
  });
  assert.equal(report.BEFORE.totalCap, 1);
  assert.equal(report.AFTER.totalCap, 100);
  assert.equal(report.AFTER.operatorDelegatedMaximumDailyCapacity, 20);
  assert.equal(report.expectedEffectiveDailyCapacity, 4);
  assert.equal(report.preservedSafetyFields.spacingMinutes, 240);
});

test('preview at T1 and apply with pinned authorizationInstant persists once', async () => {
  const authNow = new Date('2026-10-04T13:00:00.000Z');
  let migratedPolicy = null;
  const programRow = {
    id: 'outbound_test',
    policy: {
      ...BABRUN_BASE,
      startsAt: '2026-09-28T12:00:00.000Z',
      expiresAt: '2026-10-05T21:16:09.082Z',
    },
    policy_hash: 'old',
    scope_hash: 'scope',
    source_mission_id: 'mission',
  };
  const svc = service({
    pool: { query: async () => ({ rows: [] }) },
    tenantId: '13',
    now: () => authNow,
    adapters: {},
  });
  Object.assign(svc.store, {
    program: async () => programRow,
    migrateProgramPolicy: async (_program, nextPolicy, actor) => {
      migratedPolicy = nextPolicy;
      return {
        id: programRow.id,
        policy: nextPolicy,
        policy_hash: require('../packages/acquisition-mission/DailyOutboundPolicy').hash(nextPolicy),
        authorized_by: actor,
      };
    },
  });
  const preview = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: 30,
  }, { id: '3', role: 'admin' });
  assert.equal(preview.reviewRequired, true);
  assert.ok(preview.authorizationInstant);
  const applied = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: 30,
    reviewHash: preview.reviewHash,
    authorizationInstant: preview.authorizationInstant,
  }, { id: '3', role: 'admin' });
  assert.equal(applied.policy.operatorDelegatedMaximumDailyCapacity, 20);
  assert.equal(migratedPolicy.totalCap, 100);
});

test('preview T1 and apply recomputed at T2 without pinned instant fails policy_review_stale', async () => {
  const authNow = new Date('2026-10-04T13:00:00.000Z');
  let clock = authNow;
  const svc = service({
    pool: { query: async () => ({ rows: [] }) },
    tenantId: '13',
    now: () => clock,
    adapters: {},
  });
  Object.assign(svc.store, {
    program: async () => ({
      id: 'prog',
      policy: {
        ...BABRUN_BASE,
        startsAt: '2026-09-28T12:00:00.000Z',
        expiresAt: '2026-10-05T21:16:09.082Z',
      },
      scope_hash: 'scope',
      source_mission_id: 'mission',
    }),
    migrateProgramPolicy: async () => { throw new Error('migrate must not run'); },
  });
  const preview = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: 30,
  }, { id: '1', role: 'admin' });
  clock = new Date('2026-10-04T14:00:00.000Z');
  await assert.rejects(
    () => svc.migrateOperatorDelegatedCapacity({
      operatorDelegatedMaximumDailyCapacity: 20,
      totalCap: 100,
      grantHorizonDays: 30,
      reviewHash: preview.reviewHash,
    }, { id: '1', role: 'admin' }),
    (err) => err.code === 'policy_review_stale',
  );
});

test('supplied wrong reviewHash fails closed with policy_review_stale', async () => {
  const authNow = new Date('2026-10-04T13:00:00.000Z');
  const svc = service({
    pool: { query: async () => ({ rows: [] }) },
    tenantId: '13',
    now: () => authNow,
    adapters: {},
  });
  Object.assign(svc.store, {
    program: async () => ({
      id: 'prog',
      policy: {
        ...BABRUN_BASE,
        startsAt: '2026-09-28T12:00:00.000Z',
        expiresAt: '2026-10-05T21:16:09.082Z',
      },
      scope_hash: 'scope',
      source_mission_id: 'mission',
    }),
    migrateProgramPolicy: async () => { throw new Error('migrate must not run'); },
  });
  const preview = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: 30,
  }, { id: '1', role: 'admin' });
  await assert.rejects(
    () => svc.migrateOperatorDelegatedCapacity({
      operatorDelegatedMaximumDailyCapacity: 20,
      totalCap: 100,
      grantHorizonDays: 30,
      reviewHash: 'deadbeef',
      authorizationInstant: preview.authorizationInstant,
    }, { id: '1', role: 'admin' }),
    (err) => err.code === 'policy_review_stale',
  );
});

test('reviewRequired response cannot satisfy migrationApplySucceeded', () => {
  const { migrationApplySucceeded } = require('../scripts/migrateBabrunOperatorDelegatedCapacity');
  assert.equal(migrationApplySucceeded({ reviewRequired: true, reviewHash: 'x', policy: {} }), false);
  assert.equal(migrationApplySucceeded({ id: 'p', policy_hash: 'h', policy: { totalCap: 1 } }), true);
});

test('repeated apply with same reviewed proposal is idempotent', async () => {
  const authNow = new Date('2026-10-04T13:00:00.000Z');
  let migrateCalls = 0;
  const programRow = {
    id: 'prog',
    policy: {
      ...BABRUN_BASE,
      startsAt: '2026-09-28T12:00:00.000Z',
      expiresAt: '2026-10-05T21:16:09.082Z',
    },
    policy_hash: 'old',
    scope_hash: 'scope',
    source_mission_id: 'mission',
  };
  const svc = service({
    pool: { query: async () => ({ rows: [] }) },
    tenantId: '13',
    now: () => authNow,
    adapters: {},
  });
  Object.assign(svc.store, {
    program: async () => ({ ...programRow }),
    migrateProgramPolicy: async (_program, nextPolicy) => {
      migrateCalls += 1;
      programRow.policy = nextPolicy;
      programRow.policy_hash = require('../packages/acquisition-mission/DailyOutboundPolicy').hash(nextPolicy);
      return { ...programRow };
    },
  });
  const preview = await svc.migrateOperatorDelegatedCapacity({
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: 30,
  }, { id: '1', role: 'admin' });
  const applyInput = {
    operatorDelegatedMaximumDailyCapacity: 20,
    totalCap: 100,
    grantHorizonDays: 30,
    reviewHash: preview.reviewHash,
    authorizationInstant: preview.authorizationInstant,
  };
  await svc.migrateOperatorDelegatedCapacity(applyInput, { id: '1', role: 'admin' });
  assert.equal(migrateCalls, 1);
  await svc.migrateOperatorDelegatedCapacity(applyInput, { id: '1', role: 'admin' });
  assert.equal(migrateCalls, 1);
});
