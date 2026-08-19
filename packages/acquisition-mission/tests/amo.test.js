'use strict';

/**
 * SPEC-118 — Acquisition Mission Orchestration.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const amo = require('../index');
const {
  createAcquisitionMissionEngine,
  assertContract,
  STAGES,
  SPECIALISTS,
  BLOCKER_KINDS,
} = amo;

function engine() {
  return createAcquisitionMissionEngine();
}

function lawFirmMission(amoEngine, overrides = {}) {
  return amoEngine.create({
    id: 'mission_481',
    tenantId: '10',
    clientId: 10,
    objective: 'Acquire commercial cleaning customers in Manchester.',
    targetSegment: 'Commercial Law Firms',
    campaign: 'Fall Outreach',
    priority: 'high',
    confidence: 0.82,
    owner: 'Operator',
    createdBy: 'max',
    constraints: ['Operator voice', 'Commercial only', 'Veteran discount available'],
    now: '2026-08-19T09:02:00.000Z',
    ...overrides,
  });
}

function toPrepare(amoEngine, missionId) {
  amoEngine.contribute(missionId, {
    specialist: SPECIALISTS.SCOUT,
    kind: 'discovery',
    at: '2026-08-19T09:05:00.000Z',
    payload: {
      companies: Array.from({ length: 61 }, (_, i) => ({ id: i + 1, name: `Firm ${i + 1}` })),
      prospects: [{ id: 1, name: 'Jordan', title: 'Office Manager' }],
      buyingSignals: ['Hiring operations manager'],
      decisionMakers: ['Office Manager'],
      confidence: 0.8,
      evidence: ['Google Places', 'website hire page'],
      qualifiedCount: 61,
    },
  });
  amoEngine.progress(missionId, { role: 'max' }, { stage: STAGES.UNDERSTAND });
  amoEngine.contribute(missionId, {
    specialist: SPECIALISTS.MAX,
    kind: 'prioritization',
    at: '2026-08-19T09:08:00.000Z',
    payload: {
      priorities: [{ segment: 'law_firm', rank: 1 }],
      objectives: [{ text: 'Generate walkthroughs.' }],
      objectiveReason: 'Commercial revenue remains primary objective.',
      timing: 'Fall',
      recommendations: ['Prioritize law firms with recent ops hires'],
      constraints: ['Operator voice', 'Commercial only', 'Veteran discount available'],
      delegation: { paige: 'variants', emmett: 'capacity' },
    },
  });
  amoEngine.progress(missionId, { role: 'max' }, { stage: STAGES.PLAN });
  amoEngine.progress(missionId, { role: 'max' }, { stage: STAGES.PREPARE });
  return amoEngine.inspect(missionId);
}

describe('SPEC-118 mission object', () => {
  it('creates a durable acquisition mission with the spec example fields', () => {
    const created = lawFirmMission(engine());
    assert.equal(created.id, 'mission_481');
    assert.equal(created.objective, 'Acquire commercial cleaning customers in Manchester.');
    assert.equal(created.targetSegment, 'Commercial Law Firms');
    assert.equal(created.campaign, 'Fall Outreach');
    assert.equal(created.priority, 'high');
    assert.equal(created.status, 'Discovering');
    assert.equal(created.confidence, 0.82);
    assert.equal(created.owner, 'Operator');
    assert.equal(created.createdBy, 'max');
    assert.equal(created.spec, 'SPEC-118');
  });
});

describe('SPEC-118 lifecycle', () => {
  it('progresses Discover → Understand → Plan → Prepare and records each transition', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    const snapshot = toPrepare(amoEngine, mission.id);
    assert.equal(snapshot.mission.stage, STAGES.PREPARE);
    assert.equal(snapshot.mission.status, 'Preparing');
    const transitions = snapshot.timeline.filter((row) => row.kind === 'stage_transition');
    assert.equal(transitions.length, 3);
    assert.ok(transitions.every((row) => row.label.includes('→')));
  });

  it('refuses specialists other than Max/operator from advancing the stage', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    amoEngine.contribute(mission.id, {
      specialist: 'scout',
      payload: { prospects: [{ id: 1 }], evidence: ['places'] },
    });
    assert.throws(
      () => amoEngine.progress(mission.id, { role: 'paige' }),
      (err) => err.code === 'amo_max_orchestrates'
    );
  });

  it('blocks Ready until Paige variants and Emmett capacity exist', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    toPrepare(amoEngine, mission.id);
    assert.throws(
      () => amoEngine.progress(mission.id, { role: 'max' }, { stage: STAGES.READY }),
      (err) => err.code === 'amo_stage_blocked'
    );
  });
});

describe('SPEC-118 capability contracts', () => {
  it('lets each specialist produce only contracted outputs', () => {
    assert.equal(assertContract('scout', { prospects: [{ id: 1 }], evidence: ['web'] }).ok, true);
    assert.equal(assertContract('max', { priorities: ['law firms'], recommendations: ['go'] }).ok, true);
    assert.equal(assertContract('paige', { variants: [{ label: 'B' }], cta: 'reply', subjects: ['Walkthrough'] }).ok, true);
    assert.equal(assertContract('emmett', { capacity: { recommended: 18 }, queue: [{ id: 1 }] }).ok, true);
  });

  it('rejects Scout messaging, Max copy, Paige recipients, and Emmett campaign copy', () => {
    assert.throws(() => assertContract('scout', { prospects: [], subject: 'hi' }), (err) => err.code === 'amo_contract_violation');
    assert.throws(() => assertContract('max', { priorities: ['x'], body: 'hello' }), (err) => err.code === 'amo_contract_violation');
    assert.throws(() => assertContract('paige', { variants: [{ label: 'B' }], recipients: ['a@x.com'] }), (err) => err.code === 'amo_contract_violation');
    assert.throws(() => assertContract('emmett', { capacity: { recommended: 18 }, cta: 'book' }), (err) => err.code === 'amo_contract_violation');
  });

  it('records rejected contributions on the timeline instead of attaching them', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    assert.throws(
      () => amoEngine.contribute(mission.id, {
        specialist: 'paige',
        payload: { variants: [{ label: 'B' }], recipients: [1, 2] },
      }),
      (err) => err.code === 'amo_contract_violation'
    );
    const snapshot = amoEngine.inspect(mission.id);
    assert.equal(snapshot.contributions.length, 0);
    assert.ok(snapshot.timeline.some((row) => row.kind === 'contract_rejected'));
  });
});

describe('SPEC-118 workspace', () => {
  it('shows Preparing at 68% with Scout/Max complete, Paige generating, Emmett waiting, operator approval required', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    const snapshot = toPrepare(amoEngine, mission.id);
    assert.equal(snapshot.workspace.status, 'Preparing');
    assert.equal(snapshot.workspace.progressPercent, 68);
    assert.match(snapshot.workspace.bar, /█/);
    assert.equal(snapshot.workspace.scout.label, 'Discovery Complete');
    assert.equal(snapshot.workspace.max.label, 'Prioritization Complete');
    assert.equal(snapshot.workspace.paige.label, 'Generating Variants');
    assert.equal(snapshot.workspace.emmett.label, 'Waiting');
    assert.equal(snapshot.workspace.operator.label, 'Approval Required');
    const prose = amoEngine.formatWorkspace(snapshot.workspace);
    assert.match(prose, /Commercial Law Firms/);
    assert.match(prose, /68%/);
    assert.match(prose, /Generating Variants/);
  });
});

describe('SPEC-118 shared context', () => {
  it('gives Paige the mission, objective, constraints, campaign, signals, and Max reasoning', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    toPrepare(amoEngine, mission.id);
    const context = amoEngine.context(mission.id);
    assert.equal(context.mission.campaign, 'Fall Outreach');
    assert.match(context.mission.objective, /walkthrough/i);
    assert.ok(context.mission.constraints.includes('Veteran discount available'));
    assert.ok(context.buyingSignals.length >= 1);
    assert.ok(context.priorityReasoning.length >= 1);
    const prose = amoEngine.formatSharedContext(context);
    assert.match(prose, /Commercial Law Firms/);
    assert.match(prose, /Fall Outreach/);
    assert.match(prose, /Scout evidence/);
    assert.match(prose, /Max evidence/);
    assert.doesNotMatch(prose, /^Generate email\.?$/m);
  });
});

describe('SPEC-118 timeline', () => {
  it('records every meaningful event in chronological order', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    toPrepare(amoEngine, mission.id);
    amoEngine.contribute(mission.id, {
      specialist: 'paige',
      at: '2026-08-19T09:14:00.000Z',
      payload: {
        variants: [{ label: 'Variant B', subject: 'Walkthrough', cta: 'reply' }],
        subjects: ['Walkthrough'],
        cta: 'reply',
        experiments: ['operational vs relationship'],
        hypotheses: ['ops language wins'],
      },
    });
    amoEngine.contribute(mission.id, {
      specialist: 'operator',
      kind: 'edit',
      at: '2026-08-19T09:17:00.000Z',
      payload: { field: 'cta', cta: 'book a walkthrough' },
    });
    amoEngine.contribute(mission.id, {
      specialist: 'emmett',
      at: '2026-08-19T09:19:00.000Z',
      payload: {
        capacity: { recommended: 18, remaining: 18 },
        queue: Array.from({ length: 18 }, (_, i) => ({ id: i + 1 })),
        sendRecommendations: ['Tuesday morning'],
        deliverability: { status: 'healthy' },
        queuedCount: 18,
      },
    });
    amoEngine.recordOutcome(mission.id, { type: 'open', at: '2026-08-19T10:04:00.000Z' });
    amoEngine.recordOutcome(mission.id, { type: 'reply', at: '2026-08-19T11:42:00.000Z' });
    amoEngine.recordOutcome(mission.id, { type: 'walkthrough_booked', at: '2026-08-19T14:10:00.000Z' });
    const lines = amoEngine.timeline(mission.id).map((row) => row.label);
    assert.ok(lines.includes('Mission Created'));
    assert.ok(lines.includes('Scout completed discovery'));
    assert.ok(lines.includes('Max ranked prospects'));
    assert.ok(lines.includes('Paige generated Variant B'));
    assert.ok(lines.includes('Operator edited CTA'));
    assert.ok(lines.includes('Emmett approved capacity'));
    assert.ok(lines.some((line) => /18 emails queued/.test(line)));
    assert.ok(lines.includes('First open'));
    assert.ok(lines.includes('Reply received'));
    assert.ok(lines.includes('Walkthrough booked'));
  });
});

describe('SPEC-118 health and blockers', () => {
  it('answers mission health instead of a vague outreach status', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine, { confidence: 0.87 });
    toPrepare(amoEngine, mission.id);
    amoEngine.contribute(mission.id, {
      specialist: 'paige',
      payload: { variants: [{ label: 'B' }], cta: 'reply', hypotheses: ['ops'] },
    });
    amoEngine.contribute(mission.id, {
      specialist: 'emmett',
      payload: {
        capacity: { recommended: 18, remaining: 18 },
        queue: [{ id: 1 }],
        sendRecommendations: ['pace verticals'],
      },
    });
    amoEngine.recordOutcome(mission.id, { type: 'reply' });
    amoEngine.recordOutcome(mission.id, { type: 'reply' });
    amoEngine.recordOutcome(mission.id, { type: 'reply' });
    amoEngine.recordOutcome(mission.id, { type: 'walkthrough_booked' });
    amoEngine.recordLearning(mission.id, {
      segment: 'law_firm',
      sends: 18,
      replies: 3,
      statement: 'Commercial firms responding better to operational messaging.',
    });
    const health = amoEngine.health(mission.id);
    assert.equal(health.confidence, 0.87);
    assert.equal(health.currentBlocker, 'Operator approval');
    assert.equal(health.risk, 'Low');
    assert.equal(health.capacityRemaining, 18);
    assert.equal(health.replies, 3);
    assert.equal(health.meetings, 1);
    assert.match(health.learning, /operational messaging/i);
    const prose = amoEngine.formatHealth(health);
    assert.match(prose, /Mission Health/);
    assert.match(prose, /Operator approval/i);
  });

  it('exposes explicit blockers including deliverability pause', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    amoEngine.setBlocker(mission.id, { kind: BLOCKER_KINDS.WAITING_FOR_DOMAIN_WARMUP });
    assert.equal(amoEngine.inspect(mission.id).blocker.kind, BLOCKER_KINDS.WAITING_FOR_DOMAIN_WARMUP);
    amoEngine.clearBlocker(mission.id, BLOCKER_KINDS.WAITING_FOR_DOMAIN_WARMUP);
    toPrepare(amoEngine, mission.id);
    amoEngine.contribute(mission.id, {
      specialist: 'emmett',
      payload: {
        capacity: { recommended: 0 },
        reputation: { atRisk: true },
        governor: { outcome: 'pause' },
      },
    });
    const snapshot = amoEngine.inspect(mission.id);
    assert.equal(snapshot.blocker.kind, BLOCKER_KINDS.PAUSED_DELIVERABILITY_RISK);
    assert.match(snapshot.blocker.label, /Deliverability Risk/);
  });
});

describe('SPEC-118 learning and explainability', () => {
  it('stores segment reply rates and a recommendation, never auto-applied', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    amoEngine.recordLearning(mission.id, { segment: 'Law firms', sends: 50, replies: 7 });
    amoEngine.recordLearning(mission.id, { segment: 'Property managers', sends: 50, replies: 3 });
    amoEngine.recordLearning(mission.id, { segment: 'Medical', sends: 50, replies: 2 });
    const learning = amoEngine.learning('10');
    assert.equal(learning.autoApplied, false);
    const law = learning.segments.find((row) => /law/i.test(row.segment));
    assert.equal(Math.round(law.replyRate * 100), 14);
    assert.match(learning.recommendation, /Increase Law firms allocation/i);
    const prose = amoEngine.formatLearning(learning);
    assert.match(prose, /14%/);
    assert.match(prose, /6%/);
    assert.match(prose, /4%/);
  });

  it('explains why the mission exists from evidence, not opinion', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine, { confidence: 0.84 });
    toPrepare(amoEngine, mission.id);
    amoEngine.contribute(mission.id, {
      specialist: 'emmett',
      payload: { capacity: { recommended: 22, remaining: 22 }, queue: [{ id: 1 }] },
    });
    amoEngine.recordLearning(null, {
      tenantId: '10',
      segment: 'law_firm',
      sends: 100,
      replies: 11,
    });
    const why = amoEngine.explainWhy(mission.id, { previousReplyRate: 0.11 });
    assert.equal(why.invented, false);
    assert.ok(why.reasons.some((row) => /commercial revenue remains primary objective/i.test(row)));
    assert.ok(why.reasons.some((row) => /61 qualified firms/.test(row)));
    assert.ok(why.reasons.some((row) => /Inbox capacity available/.test(row)));
    assert.ok(why.reasons.some((row) => /11% reply rate/.test(row)));
    assert.equal(why.confidence, 0.84);
    const answered = amoEngine.answerOperator('Why is this mission here?', { tenantId: '10', missionId: mission.id, previousReplyRate: 0.11 });
    assert.equal(answered.invented, false);
    assert.match(answered.prose, /Mission exists because/);
    assert.match(answered.prose, /61 qualified/);
  });

  it('answers how is outreach as mission health', () => {
    const amoEngine = engine();
    lawFirmMission(amoEngine);
    const answered = amoEngine.answerOperator('How is outreach?', { tenantId: '10' });
    assert.equal(answered.kind, 'health');
    assert.match(answered.prose, /Mission Health/);
  });
});

describe('SPEC-118 memory and isolation', () => {
  it('keeps cross-capability observations on the mission', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    amoEngine.recordObservation(mission.id, { specialist: 'scout', observation: 'Company hired Operations Manager.' });
    amoEngine.recordObservation(mission.id, { specialist: 'paige', observation: 'Variant C generated highest replies.' });
    amoEngine.recordObservation(mission.id, { specialist: 'emmett', observation: 'Tuesday mornings improve deliverability.' });
    amoEngine.recordObservation(mission.id, { specialist: 'max', observation: 'Recommend increasing campaign volume.' });
    const memory = amoEngine.inspect(mission.id).observations;
    assert.equal(memory.length, 4);
    assert.ok(memory.some((row) => /Operations Manager/.test(row.line)));
    assert.ok(memory.some((row) => /Variant C/.test(row.line)));
  });

  it('fail-closes cross-tenant mission reads', () => {
    const amoEngine = engine();
    const mission = lawFirmMission(amoEngine);
    assert.throws(
      () => amoEngine.inspect(mission.id, { tenantId: '99' }),
      (err) => err.code === 'amo_tenant_mismatch'
    );
    assert.equal(amoEngine.list('99').length, 0);
    assert.equal(amoEngine.list('10').length, 1);
  });
});

describe('SPEC-118 docs', () => {
  it('states Max manages missions and lists the lifecycle', () => {
    const spec = fs.readFileSync(
      path.join(__dirname, '../../../docs/specs/SPEC-118_Acquisition_Mission_Orchestration.md'),
      'utf8'
    );
    const adr = fs.readFileSync(
      path.join(__dirname, '../../../docs/adr/ADR-055_Max_Manages_Missions.md'),
      'utf8'
    );
    assert.match(spec, /Max doesn't manage agents/);
    assert.match(spec, /Discover/);
    assert.match(spec, /Waiting for Domain Warm-up/);
    assert.match(adr, /Max doesn't manage agents/);
    assert.match(adr, /SPEC-118/);
  });
});
