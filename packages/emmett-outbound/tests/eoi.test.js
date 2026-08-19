'use strict';

/**
 * SPEC-117 — Emmett Outbound Infrastructure Intelligence.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const eoi = require('../index');
const {
  scoreInboxHealth,
  recommendCapacity,
  evaluateGovernor,
  evaluateSend,
  acknowledgeHalt,
  createOutboundEngine,
  paceVerticals,
  recordOutcome,
  GOVERNOR_OUTCOMES,
  PLAN_STATUS,
  LEARNING_SINKS,
} = eoi;

function specExampleSnapshot(overrides = {}) {
  return {
    tenantId: 'fedir',
    clientId: 21,
    inboxAgeDays: 47,
    providerCeiling: 50,
    authentication: { spf: true, dkim: true, dmarc: 'reject' },
    warmup: { status: 'healthy', dailyCap: 50 },
    bounceRate: 0,
    replyRate: 0.094,
    openRate: 0.63,
    complaintRate: 0,
    blacklist: { listed: false },
    sentToday: 0,
    sentYesterday: 18,
    historicalDailyAvg: 16,
    recentSends: 80,
    ...overrides,
  };
}

function paigeCandidate(overrides = {}) {
  return {
    id: 101,
    email: 'owner@example.com',
    vertical: 'law_firm',
    icpScore: 82,
    maxPriority: 0.9,
    expectedResponse: 0.12,
    buyingSignalAt: new Date().toISOString(),
    contentSource: 'paige',
    paige: {
      author: 'paige',
      source: 'paige',
      subject: 'walkthrough next week',
      body: 'Worth a look at the common areas?',
      cta: 'reply',
    },
    ...overrides,
  };
}

describe('SPEC-117 inbox health', () => {
  it('never returns a bare score — reasons are always present', () => {
    const health = scoreInboxHealth(specExampleSnapshot());
    assert.ok(health.score >= 0 && health.score <= 100);
    assert.ok(health.reasons.length >= 1);
    assert.ok(health.factors.length >= 12);
    assert.equal(health.label, 'healthy');
    assert.ok(health.reasons.includes('No blacklist concerns'));
    for (const factor of health.factors) {
      assert.ok(factor.key);
      assert.ok(factor.reason);
      assert.ok(factor.max > 0);
    }
  });

  it('explains a degraded inbox instead of returning only 62', () => {
    const health = scoreInboxHealth(specExampleSnapshot({
      bounceRate: 0.035,
      sentToday: 40,
      sentYesterday: 12,
      warmup: { status: 'warming', dailyCap: 25 },
      recentSends: 40,
    }));
    assert.ok(health.score < 80);
    assert.ok(health.reasons.some((r) => /high bounce/i.test(r)));
    assert.ok(health.reasons.some((r) => /rapid increase/i.test(r)));
    assert.ok(health.reasons.some((r) => /warming/i.test(r)));
    assert.ok(health.reasons.some((r) => /no blacklist/i.test(r)));
  });
});

describe('SPEC-117 capacity intelligence', () => {
  it('reasons 22 from the spec example instead of gmail = 50', () => {
    const snapshot = specExampleSnapshot();
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    assert.equal(capacity.recommended, 22);
    assert.equal(capacity.confidence, 0.84);
    assert.match(capacity.statement, /recommend 22/);
    assert.ok(capacity.factors.some((f) => /47 days/.test(f)));
    assert.ok(capacity.factors.some((f) => /Properly authenticated/.test(f)));
    assert.ok(capacity.factors.some((f) => /9\.4%/.test(f)));
    assert.ok(capacity.ceiling <= 50);
    assert.notEqual(capacity.recommended, 50);
    assert.ok(capacity.tomorrow);
  });

  it('pauses entirely when reputation is unsafe', () => {
    const snapshot = specExampleSnapshot({ bounceRate: 0.05, recentSends: 40, complaintRate: 0 });
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    assert.equal(capacity.recommended, 0);
    assert.match(capacity.statement, /pausing entirely/i);
  });
});

describe('SPEC-117 Safe Send Governor', () => {
  it('returns proceed / slow / pause / emergency from evidence', () => {
    const healthy = specExampleSnapshot();
    const health = scoreInboxHealth(healthy);
    const capacity = recommendCapacity(healthy, health);
    const proceed = evaluateGovernor(healthy, health, capacity);
    assert.equal(proceed.outcome, GOVERNOR_OUTCOMES.PROCEED);

    const slow = evaluateGovernor(
      specExampleSnapshot({ warmup: { status: 'warming', dailyCap: 20 } }),
      { score: 72, reasons: ['Domain still warming'] },
      { recommended: 12 }
    );
    assert.equal(slow.outcome, GOVERNOR_OUTCOMES.SLOW);

    const pause = evaluateGovernor(
      specExampleSnapshot({ bounceRate: 0.025, recentSends: 30 }),
      { score: 55, bounceRate: 0.025 },
      { recommended: 4 }
    );
    assert.equal(pause.outcome, GOVERNOR_OUTCOMES.PAUSE);

    const emergency = evaluateGovernor(
      specExampleSnapshot({ blacklist: { listed: true, sources: ['example'] } }),
      { score: 20 },
      { recommended: 0 }
    );
    assert.equal(emergency.outcome, GOVERNOR_OUTCOMES.EMERGENCY);
    assert.match(emergency.reason, /blacklist/i);
  });

  it('rejects silent Max override of emergency', () => {
    const governor = evaluateGovernor(
      specExampleSnapshot({ complaintRate: 0.002 }),
      { score: 10, complaintRate: 0.002 },
      { recommended: 0 }
    );
    assert.equal(governor.outcome, GOVERNOR_OUTCOMES.EMERGENCY);
    assert.throws(
      () => acknowledgeHalt(governor, { role: 'max', name: 'Max' }, 'just send'),
      (err) => err.code === 'eoi_operator_acknowledgement_required'
    );
    const ack = acknowledgeHalt(governor, { role: 'operator', id: 'jacob' }, 'reviewed blacklist');
    assert.equal(ack.acknowledged, true);
    assert.equal(ack.emergencyCleared, false);
  });

  it('blocks send without operator approval even when healthy', () => {
    const snapshot = specExampleSnapshot();
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    const governor = evaluateGovernor(snapshot, health, capacity);
    const blocked = evaluateSend({
      governor,
      capacity,
      approvedPlan: null,
      candidate: paigeCandidate(),
      sentToday: 0,
    });
    assert.equal(blocked.allowed, false);
    assert.equal(blocked.code, 'operator_approval_required');
  });

  it('requires Paige content and DNC respect', () => {
    const snapshot = specExampleSnapshot();
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    const governor = evaluateGovernor(snapshot, health, capacity);
    const approvedPlan = { status: PLAN_STATUS.APPROVED, localDate: '2026-08-19', approvedCapacity: 22 };
    const noCopy = evaluateSend({
      governor,
      capacity,
      approvedPlan,
      candidate: { email: 'a@b.com', paige: null },
      localDate: '2026-08-19',
    });
    assert.equal(noCopy.code, 'paige_content_required');
    const dnc = evaluateSend({
      governor,
      capacity,
      approvedPlan,
      candidate: paigeCandidate({ dnc: true }),
      localDate: '2026-08-19',
    });
    assert.equal(dnc.code, 'dnc');
  });
});

describe('SPEC-117 queue and pacing', () => {
  it('builds a ranked queue of recommended N and interleaves verticals', () => {
    const engine = createOutboundEngine();
    const prospects = [
      paigeCandidate({ id: 1, vertical: 'law_firm', maxPriority: 0.95, icpScore: 90 }),
      paigeCandidate({ id: 2, vertical: 'law_firm', maxPriority: 0.9, icpScore: 88 }),
      paigeCandidate({ id: 3, vertical: 'accounting', maxPriority: 0.8, icpScore: 80 }),
      paigeCandidate({ id: 4, vertical: 'property_management', maxPriority: 0.7, icpScore: 75 }),
      paigeCandidate({ id: 5, vertical: 'med_spa', maxPriority: 0.6, icpScore: 70 }),
      paigeCandidate({ id: 6, vertical: 'restaurant', maxPriority: 0.5, icpScore: 68 }),
    ];
    const day = engine.planDay({
      tenantId: '21',
      clientId: 21,
      snapshot: specExampleSnapshot({ tenantId: '21' }),
      prospects,
    });
    assert.equal(day.queue.recommended, 22);
    assert.ok(day.queue.selectedCount <= 22);
    assert.ok(day.queue.items[0].maxPriority >= day.queue.items[day.queue.items.length - 1].maxPriority
      || day.queue.pacing.length >= 2);
    const paced = paceVerticals(prospects.map((p) => ({ ...p, vertical: p.vertical })));
    assert.notEqual(paced[0].vertical, paced[1].vertical);
    assert.equal(day.governor.outcome, GOVERNOR_OUTCOMES.PROCEED);
  });
});

describe('SPEC-117 operator approval and Max boundary', () => {
  it('lets the operator approve today\'s plan and then allows send', () => {
    const engine = createOutboundEngine();
    const day = engine.planDay({
      tenantId: '21',
      snapshot: specExampleSnapshot({ tenantId: '21' }),
      prospects: [paigeCandidate()],
    });
    assert.equal(day.plan.status, PLAN_STATUS.DRAFT);
    assert.throws(
      () => engine.approvePlan(day.plan.id, { role: 'max', name: 'Max' }),
      (err) => err.code === 'eoi_operator_acknowledgement_required'
    );
    const approved = engine.approvePlan(day.plan.id, { role: 'operator', id: 'jacob' });
    assert.equal(approved.status, PLAN_STATUS.APPROVED);
    const decision = engine.canSend({
      tenantId: '21',
      localDate: day.plan.localDate,
      candidate: paigeCandidate(),
      sentToday: 0,
      governor: day.governor,
      capacity: day.capacity,
    });
    assert.equal(decision.allowed, true);
    assert.equal(decision.outcome, GOVERNOR_OUTCOMES.PROCEED);
  });

  it('does not send after emergency even if a draft exists', () => {
    const engine = createOutboundEngine();
    const day = engine.planDay({
      tenantId: '21',
      snapshot: specExampleSnapshot({ tenantId: '21', blacklist: { listed: true } }),
      prospects: [paigeCandidate()],
    });
    assert.equal(day.governor.outcome, GOVERNOR_OUTCOMES.EMERGENCY);
    assert.throws(
      () => engine.approvePlan(day.plan.id, { role: 'operator', id: 'jacob' }),
      (err) => err.code === 'eoi_halt_blocks_approval'
    );
    const decision = engine.canSend({
      tenantId: '21',
      candidate: paigeCandidate(),
      governor: day.governor,
      capacity: day.capacity,
    });
    assert.equal(decision.allowed, false);
    assert.equal(decision.code, 'emergency');
  });
});

describe('SPEC-117 learning loop and tenancy', () => {
  it('routes outcomes to Paige, Scout, Max, and Emmett without auto-applying', () => {
    const engine = createOutboundEngine();
    const bounce = engine.ingestOutcome({
      tenantId: '21',
      clientId: 21,
      eventType: 'hard_bounce',
      prospectId: 9,
      vertical: 'law_firm',
    });
    assert.equal(bounce.outcome.type, 'bounce');
    assert.ok(bounce.learning.some((row) => row.sink === LEARNING_SINKS.SCOUT));
    assert.ok(bounce.learning.some((row) => row.sink === LEARNING_SINKS.EMMETT));
    assert.ok(bounce.learning.every((row) => row.autoApplied === false));

    const reply = engine.ingestOutcome({
      tenantId: '21',
      eventType: 'replied',
      prospectId: 9,
    });
    assert.ok(reply.learning.some((row) => row.sink === LEARNING_SINKS.PAIGE));
    assert.ok(reply.learning.some((row) => row.sink === LEARNING_SINKS.MAX));

    const other = engine.store.listLearning('99');
    assert.equal(other.length, 0);
    assert.ok(engine.store.listLearning('21').length >= 2);
  });

  it('maps meeting booked, opportunity, and revenue into Scout and Max', () => {
    const meeting = recordOutcome({ tenantId: '21', type: 'meeting_booked' });
    assert.ok(meeting.sinks.includes(LEARNING_SINKS.SCOUT));
    assert.ok(meeting.sinks.includes(LEARNING_SINKS.MAX));
    assert.equal(meeting.autoMutatesCampaign, false);
  });
});

describe('SPEC-117 specialist boundaries', () => {
  it('does not generate copy, pick ICP, or sell', () => {
    const source = fs.readFileSync(path.join(__dirname, '../index.js'), 'utf8');
    assert.match(source, /Reputation is capital/);
    const engineSrc = fs.readFileSync(path.join(__dirname, '../Engine.js'), 'utf8');
    assert.doesNotMatch(engineSrc, /subject:\s*['"]Hi /);
    assert.doesNotMatch(engineSrc, /SEQUENCES/);
  });

  it('documents Scout discovers / Max decides / Paige communicates / Emmett protects', () => {
    const spec = fs.readFileSync(
      path.join(__dirname, '../../../docs/specs/SPEC-117_Emmett_Outbound_Infrastructure_Intelligence.md'),
      'utf8'
    );
    assert.match(spec, /Scout discovers/);
    assert.match(spec, /Max decides/);
    assert.match(spec, /Paige communicates/);
    assert.match(spec, /Emmett protects and executes/);
    assert.match(spec, /Human approval remains mandatory/);
  });
});
