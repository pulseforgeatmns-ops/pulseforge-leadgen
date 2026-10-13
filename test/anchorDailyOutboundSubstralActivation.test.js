'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { reviewStudioSubstralProgram } = require('../scripts/lib/studioSubstralProgramReview');
const { createGovernedProgram, emmettReadiness } = require('../scripts/studioSubstralMailboxActivation');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { service } = require('../services/governedOutbound');
const { buildCapacityEnvelope } = require('../packages/emmett-outbound/TenantMailboxCapacity');

const now = new Date('2026-10-13T12:00:00Z');
function input(overrides = {}) {
  return { sourceMissionId: 'substral-reviewed-redesign', startsAt: now.toISOString(),
    expiresAt: '2026-10-20T12:00:00Z', dailyCap: 2, totalCap: 10,
    spacingMinutes: 240, startHour: 9, endHour: 17,
    allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'], ...overrides };
}
function fixture(overrides = {}) {
  const row = { id: 'substral-reviewed-redesign', tenant_id: '17', stage: 'ready', status: 'ready',
    objective: 'Acquire a $2,000+ website redesign client in southern New Hampshire.',
    target_segment: 'Established local service businesses',
    payload: { structuredMission: { immutable: true,
      market: { segment: 'home_services', capability: 'website_redesign' },
      geography: { region: 'southern New Hampshire', cities: ['Manchester', 'Bedford'] } },
    constraints: { minimumProjectValue: 2000 } }, ...overrides };
  const queries = [];
  const db = { async query(sql, args) {
    queries.push({ sql, args });
    assert.match(sql.trim(), /^SELECT\b/);
    if (sql.includes('FROM acquisition_missions')) {
      assert.deepEqual(args, ['substral-reviewed-redesign', '17']);
      return { rows: row ? [row] : [] };
    }
    assert.match(sql, /FROM acquisition_outbound_programs/);
    assert.deepEqual(args, ['17']);
    return { rows: [] };
  } };
  return { row, db, queries };
}

test('program review is read-only and binds the explicit canonical mission, policy and mailbox', async () => {
  const { row, db, queries } = fixture();
  const review = await reviewStudioSubstralProgram(db, input(), now);
  assert.equal(queries.length, 1);
  assert.equal(review.policy.sourceMissionId, row.id);
  assert.equal(review.sourceMissionScope.objective, row.objective);
  assert.deepEqual(review.sourceMissionScope.structuredMission.geography, row.payload.structuredMission.geography);
  assert.equal(review.policy.tenantId, '17');
  assert.equal(review.policy.senderEmail, 'hello@studiosubstral.com');
  assert.equal(review.policy.inboxIntegrationId, 'tmi_17_substral_hello');
  assert.equal(review.policy.sendingIdentityId, 'tsi_17_substral_hello');
  assert.equal(review.scopeHash, hash(missionScope({ ...row.payload, tenantId: '17', objective: row.objective, targetSegment: row.target_segment })));
  assert.equal(review.reviewHash, hash({ policy: review.policy, scopeHash: review.scopeHash }));
  assert.equal(review.mode, 'shadow');
});

test('review requires an explicit source and every authority field instead of deriving defaults', async () => {
  for (const field of Object.keys(input())) {
    const proposed = input();
    delete proposed[field];
    const { db, queries } = fixture();
    await assert.rejects(reviewStudioSubstralProgram(db, proposed, now), { code: `program_review_${field}_required` });
    assert.equal(queries.length, 0);
  }
});

test('review rejects cross-tenant or noncanonical sending identities', async () => {
  for (const [field, value] of Object.entries({ tenantId: '13', senderEmail: 'hello@babrun.com',
    inboxIntegrationId: 'babrun', sendingIdentityId: 'babrun' })) {
    const { db, queries } = fixture();
    await assert.rejects(reviewStudioSubstralProgram(db, input({ [field]: value }), now), { code: 'substral_program_identity_mismatch' });
    assert.equal(queries.length, 0);
  }
});

test('review rejects an unapproved, cancelled, foreign or non-Ready mission', async () => {
  for (const [override, code] of [
    [{ payload: { structuredMission: { immutable: false } } }, 'approved_source_mission_required'],
    [{ payload: { structuredMission: { immutable: true }, planCancelled: true } }, 'approved_source_mission_required'],
    [{ status: 'cancelled' }, 'approved_source_mission_required'],
    [{ tenant_id: '13' }, 'approved_source_mission_required'],
    [{ stage: 'discover' }, 'ready_source_mission_required'],
  ]) {
    const { db } = fixture(override);
    await assert.rejects(reviewStudioSubstralProgram(db, input(), now), { code });
  }
  await assert.rejects(reviewStudioSubstralProgram({ query: async () => ({ rows: [] }) }, input(), now), { code: 'approved_source_mission_required' });
});

test('canonical null columns do not revive stale payload scope', async () => {
  const { db } = fixture({ target_segment: null, payload: { structuredMission: { immutable: true }, targetSegment: 'stale target' } });
  const review = await reviewStudioSubstralProgram(db, input(), now);
  assert.equal(review.sourceMissionScope.targetSegment, null);
});

test('changed offer, geography, or policy requires a fresh review hash before any activation', async () => {
  const first = fixture();
  const review = await reviewStudioSubstralProgram(first.db, input(), now);
  const mutations = [
    () => { first.row.objective = 'Different offer'; },
    () => { first.row.payload.structuredMission.geography.cities.push('Nashua'); },
  ];
  for (const mutate of mutations) {
    mutate();
    await assert.rejects(createGovernedProgram(input({ reviewHash: review.reviewHash }), 'jake', {
      pool: first.db, now, emmettReadiness: () => assert.fail('No readiness writes before approval'),
      productionService: () => assert.fail('No authorization before review'),
    }), { code: 'program_review_required' });
  }
  const clean = fixture();
  await assert.rejects(createGovernedProgram(input({ dailyCap: 3, reviewHash: review.reviewHash }), 'jake', {
    pool: clean.db, now, emmettReadiness: () => assert.fail('No readiness writes before approval'),
  }), { code: 'program_review_required' });
});

test('missing review hash cannot create a program or invoke Emmett', async () => {
  const { db, queries } = fixture();
  await assert.rejects(createGovernedProgram(input(), 'jake', {
    pool: db, now, emmettReadiness: () => assert.fail('No Emmett'),
    productionService: () => assert.fail('No program'),
  }), { code: 'program_review_required' });
  assert.equal(queries.length, 1);
});

test('reviewed activation uses the correct service signature and creates only a shadow program', async () => {
  const { db } = fixture();
  const review = await reviewStudioSubstralProgram(db, input(), now);
  let calls = 0;
  const result = await createGovernedProgram(input({ reviewHash: review.reviewHash }), 'jake', {
    pool: db, now,
    emmettReadiness: async options => {
      assert.equal(options.pool, db);
      return { pass: true, recommendedSafeDailyCapacity: 1 };
    },
    productionService(actualDb, options) {
      assert.equal(actualDb, db);
      assert.deepEqual(options, { tenantId: '17' });
      return { async authorize(proposed, actor) {
        calls += 1;
        assert.deepEqual(actor, { id: 'jake', role: 'admin' });
        assert.deepEqual(proposed, { ...review.policy, reviewHash: review.reviewHash });
        return { id: 'shadow-substral', mode: 'shadow', policy: review.policy };
      }, setMode: () => assert.fail('Never toggle an existing program'), tick: () => assert.fail('Never send') };
    },
  });
  assert.equal(calls, 1);
  assert.equal(result.mode, 'shadow');
  assert.equal(result.policy.dailyCap, 2, 'Emmett recommendation cannot rewrite reviewed operator authority');
  assert.equal(result.policy.totalCap, 10);
});

test('an existing program is never replaced or demoted', async () => {
  const { db } = fixture();
  const review = await reviewStudioSubstralProgram(db, input(), now);
  const original = db.query;
  db.query = (sql, args) => sql.includes('FROM acquisition_outbound_programs')
    ? { rows: [{ id: 'existing', mode: 'active' }] } : original(sql, args);
  await assert.rejects(createGovernedProgram(input({ reviewHash: review.reviewHash }), 'jake', {
    pool: db, now, emmettReadiness: () => assert.fail('No Emmett'),
    productionService: () => assert.fail('No replacement'),
  }), { code: 'governed_program_exists' });
});

test('blocked, invalid or zero Emmett capacity cannot create a program', async () => {
  for (const emmett of [{ pass: false, recommendedSafeDailyCapacity: 1 },
    { pass: true, recommendedSafeDailyCapacity: 0 }, { pass: true }]) {
    const { db } = fixture();
    const review = await reviewStudioSubstralProgram(db, input(), now);
    await assert.rejects(createGovernedProgram(input({ reviewHash: review.reviewHash }), 'jake', {
      pool: db, now, emmettReadiness: async () => emmett,
      productionService: () => assert.fail('No program'),
    }), { code: 'emmett_readiness_failed' });
  }
});

test('scope changes during the final governed authorization do not get automatically approved', async () => {
  const { db } = fixture();
  const review = await reviewStudioSubstralProgram(db, input(), now);
  let calls = 0;
  await assert.rejects(createGovernedProgram(input({ reviewHash: review.reviewHash }), 'jake', {
    pool: db, now, emmettReadiness: async () => ({ pass: true, recommendedSafeDailyCapacity: 1 }),
    productionService: () => ({ authorize: async () => { calls++; return { reviewRequired: true }; } }),
  }), { code: 'program_review_required' });
  assert.equal(calls, 1);
});

test('cancellation during readiness is rechecked by the final governed authorization', async () => {
  const { db, row } = fixture();
  const review = await reviewStudioSubstralProgram(db, input(), now);
  const svc = service({ pool: db, tenantId: '17', now: () => now,
    adapters: { loadMission: async () => ({ mission: { ...row.payload, id: row.id, tenantId: row.tenant_id,
      objective: row.objective, targetSegment: row.target_segment, stage: row.stage, status: row.status } }) } });
  svc.store.createProgram = () => assert.fail('Cancelled mission cannot create a program');
  await assert.rejects(createGovernedProgram(input({ reviewHash: review.reviewHash }), 'jake', {
    pool: db, now, productionService: () => svc,
    emmettReadiness: async () => {
      row.status = 'cancelled'; row.payload.planCancelled = true;
      return { pass: true, recommendedSafeDailyCapacity: 1 };
    },
  }), { code: 'approved_source_mission_required' });
});

test('readiness uses the real envelope capacity shape and the governor-adjusted ceiling', async () => {
  const snapshot = { tenantId: '17', sendingIdentityId: 'tsi_17_substral_hello',
    mailboxIntegrationId: 'tmi_17_substral_hello', senderEmail: 'hello@studiosubstral.com',
    authentication: {}, localDate: '2026-10-13' };
  const assessment = { capacity: { recommended: 8 }, maxSendsPerDay: 2,
    governor: { outcome: 'slow', halt: false }, health: {}, riskFlags: [],
    minimumSpacingMinutes: 240, allowedSendWindow: { startHour: 9, endHour: 17 } };
  const envelope = buildCapacityEnvelope(snapshot, assessment, { now });
  assert.equal(envelope.recommended, undefined);
  assert.equal(assessment.recommended, undefined);
  const db = {};
  const report = await emmettReadiness({ pool: db, produceCapacity: async (tenantId, identityId, options) => {
    assert.equal(tenantId, '17'); assert.equal(identityId, snapshot.sendingIdentityId);
    assert.equal(options.pool, db);
    return { snapshot, assessment, envelope };
  } });
  assert.equal(report.recommendedSafeDailyCapacity, 2);
  assert.equal(report.pass, true);
});

test('readiness rejects unknown, paused, halted, conflicting and zero-capacity governor results', async () => {
  for (const [governor, maxSendsPerDay, governorState] of [
    [{ outcome: 'pause', halt: false }, 2, 'pause'],
    [{ outcome: 'emergency', halt: true }, 2, 'emergency'],
    [{ outcome: 'proceed', halt: true }, 2, 'proceed'],
    [{ outcome: 'proceed', halt: false }, 0, 'proceed'],
    [{ outcome: 'proceed', halt: false }, NaN, 'proceed'],
    [{ outcome: 'proceed', halt: false }, 2, 'pause'],
    [{}, 2, 'proceed'],
  ]) {
    const report = await emmettReadiness({ produceCapacity: async () => ({ snapshot: {},
      assessment: { governor, maxSendsPerDay }, envelope: { governorState, maxSendsPerDay } }) });
    assert.equal(report.pass, false);
  }
});
