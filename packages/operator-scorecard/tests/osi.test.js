'use strict';

/**
 * SPEC-116 — Operator Scorecard Intelligence.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const osi = require('../index');
const {
  createScorecardEngine,
  createMemoryOsiStore,
  generateDraftScorecard,
  understandBusiness,
  getRuntimeScorecard,
  buildBriefScorecardSections,
  buildDailyBriefingScorecardSection,
  evaluateEvolution,
  SCORECARD_STATUS,
  METRIC_STATUS,
  BUSINESS_STAGES,
  PROFILES,
} = require('../index');

function babrunInput() {
  return {
    tenantId: 'babrun',
    clientId: 21,
    businessName: 'Babrun',
    businessGoal:
      'Validate founder transformation methodology and establish a repeatable acquisition process.',
    objectives: [
      'Validate founder transformation methodology',
      'Establish a repeatable acquisition process',
    ],
    profile: PROFILES.FOUNDER_TRANSFORMATION,
  };
}

function anchorInput() {
  return {
    tenantId: 'anchor',
    clientId: 10,
    businessName: 'Anchor Cleaning',
    businessGoal: 'Establish a repeatable commercial acquisition engine.',
    objectives: ['Establish a repeatable commercial acquisition engine'],
    profile: PROFILES.COMMERCIAL_CLEANING,
    normalizedFacts: {
      business_name: 'Anchor Cleaning',
      success_metrics: ['walkthroughs attended', 'conversion rate'],
    },
  };
}

describe('SPEC-116 reasoning pipeline', () => {
  it('reasons from business objectives before recommending metrics', () => {
    const understanding = understandBusiness(babrunInput());
    assert.equal(understanding.profile, PROFILES.FOUNDER_TRANSFORMATION);
    assert.equal(understanding.stage, BUSINESS_STAGES.MARKET_VALIDATION);
    assert.ok(understanding.objectives.some((o) => /transformation/i.test(o)));
    assert.deepEqual(understanding.objectives.length > 0, true);

    const draft = generateDraftScorecard(babrunInput());
    assert.equal(draft.status, SCORECARD_STATUS.DRAFT);
    assert.equal(draft.isRuntime, false);
    assert.deepEqual(draft.reasoning.pipeline, [
      'business_understanding',
      'business_objectives',
      'business_stage',
      'business_model',
      'outcome_intelligence',
      'draft_operator_scorecard',
    ]);
    assert.ok(draft.objectives.length >= 1);
  });

  it('fails closed without objectives or a blueprint', () => {
    assert.throws(
      () => generateDraftScorecard({ tenantId: 'empty' }),
      (err) => err.code === 'osi_insufficient_understanding'
    );
  });

  it('includes explainable fields on every recommendation', () => {
    const draft = generateDraftScorecard(babrunInput());
    assert.ok(draft.metrics.length >= 8);
    for (const metric of draft.metrics) {
      assert.ok(metric.name);
      assert.ok(metric.reason);
      assert.ok(metric.businessOutcome);
      assert.ok(metric.whyItBelongs);
      assert.ok(metric.category);
      assert.ok(metric.indicator === 'leading' || metric.indicator === 'lagging');
      assert.ok(metric.confidence > 0 && metric.confidence <= 1);
      assert.equal(metric.status, METRIC_STATUS.RECOMMENDED);
      assert.equal(metric.source, 'max');
    }
    const qualified = draft.metrics.find((m) => m.key === 'qualified_prospects');
    assert.ok(qualified);
    assert.match(qualified.reason, /ICP/i);
    assert.match(qualified.businessOutcome, /Repeatable Acquisition/i);
    assert.equal(Math.round(qualified.confidence * 100), 94);
  });

  it('produces the Babrun and Anchor example catalogs', () => {
    const babrun = generateDraftScorecard(babrunInput());
    const babrunNames = babrun.metrics.map((m) => m.name);
    for (const name of [
      'Qualified Prospects',
      'Outreach Response Rate',
      'Discovery Calls',
      'Pain Confirmation Rate',
      'ICP Confidence',
      'Pilot Enrollments',
      'Students Started',
    ]) {
      assert.ok(babrunNames.includes(name), `missing ${name}`);
    }

    const anchor = generateDraftScorecard(anchorInput());
    const anchorNames = anchor.metrics.map((m) => m.name);
    for (const name of [
      'Qualified Commercial Prospects',
      'Walkthrough Requests',
      'Walkthrough Completion Rate',
      'Proposal Acceptance Rate',
      'Monthly Recurring Clients',
      'Client Retention',
      'Cleaner Utilization',
    ]) {
      assert.ok(anchorNames.includes(name), `missing ${name}`);
    }
    assert.ok(anchorNames.some((n) => /walkthroughs attended/i.test(n) || /Walkthrough/i.test(n)));
    const conversion = anchor.metrics.find((m) => /conversion rate/i.test(m.name) || /conversion/i.test(m.reason));
    assert.ok(conversion);
  });
});

describe('SPEC-116 operator review and approval', () => {
  it('lets the operator accept, modify, remove, add, and reorder', () => {
    const engine = createScorecardEngine();
    const draft = engine.generateDraft(babrunInput());
    const pain = draft.metrics.find((m) => m.key === 'pain_confirmation_rate');
    const qualified = draft.metrics.find((m) => m.key === 'qualified_prospects');

    const accepted = engine.review(draft.id, qualified.id, { action: 'accept' }, { operator: 'fedir' });
    assert.equal(accepted.metric.status, METRIC_STATUS.ACCEPTED);

    const modified = engine.review(
      draft.id,
      qualified.id,
      { action: 'modify', name: 'Qualified Founder Prospects' },
      { operator: 'fedir' }
    );
    assert.equal(modified.metric.status, METRIC_STATUS.MODIFIED);
    assert.equal(modified.metric.name, 'Qualified Founder Prospects');
    assert.equal(modified.metric.original.name, 'Qualified Prospects');

    const removed = engine.review(
      draft.id,
      pain.id,
      { action: 'remove' },
      { operator: 'fedir' }
    );
    assert.equal(removed.metric.status, METRIC_STATUS.REMOVED);
    assert.match(removed.prompt, /Pain Confirmation Rate/);
    assert.match(removed.prompt, /Would you like to tell me why/);

    engine.provideRemovalReason(draft.id, pain.id, 'Already validated.');
    const added = engine.add(
      draft.id,
      { name: 'Referral Partners Created' },
      { operator: 'fedir' }
    );
    assert.equal(added.metric.status, METRIC_STATUS.ADDED);
    assert.equal(added.metric.source, 'operator');

    const current = engine.getScorecard(draft.id);
    const order = current.metrics.map((m) => m.id).reverse();
    const reordered = engine.reorder(draft.id, order, { operator: 'fedir' });
    assert.equal(reordered.metrics[0].id, order[0]);
  });

  it('does not auto-adopt recommendations and requires operator approval', () => {
    const engine = createScorecardEngine();
    const draft = engine.generateDraft(babrunInput());
    const runtimeBefore = engine.runtime('babrun');
    assert.equal(runtimeBefore.status, 'absent');
    assert.equal(runtimeBefore.definitionOfSuccess, null);

    const approved = engine.approve(draft.id, { operator: 'fedir' });
    assert.equal(approved.status, SCORECARD_STATUS.APPROVED);
    assert.equal(approved.isRuntime, true);
    assert.equal(approved.approvedBy, 'fedir');

    const runtime = engine.runtime('babrun');
    assert.equal(runtime.status, 'approved');
    assert.equal(runtime.source, 'operator_approved');
    assert.ok(runtime.metrics.length >= 1);
    assert.ok(runtime.metrics.every((m) => m.status !== METRIC_STATUS.RECOMMENDED));
  });

  it('never uses a draft as the reporting definition of success', () => {
    const draft = generateDraftScorecard(babrunInput());
    const runtime = getRuntimeScorecard(draft);
    assert.equal(runtime.status, 'absent');
    assert.equal(runtime.source, 'none');
    const briefing = buildDailyBriefingScorecardSection(draft);
    assert.equal(briefing.status, 'absent');
    assert.equal(briefing.definitionOfSuccess, null);
  });
});

describe('SPEC-116 learning and evolution', () => {
  it('turns operator feedback into future-recommendation learning', () => {
    const store = createMemoryOsiStore();
    const engine = createScorecardEngine({ store });
    const first = engine.generateDraft(babrunInput());
    const pain = first.metrics.find((m) => m.key === 'pain_confirmation_rate');
    engine.review(first.id, pain.id, { action: 'remove', reason: 'Already validated.' }, { operator: 'fedir' });
    engine.add(first.id, { name: 'Referral Partners Created' }, { operator: 'fedir' });
    engine.approve(first.id, { operator: 'fedir' });

    const learning = engine.learningFor('babrun');
    assert.ok(learning.some((row) => row.metricKey === 'pain_confirmation_rate' && row.suppress));
    assert.ok(learning.some((row) => /referral_partners_created/i.test(row.metricKey) && row.prioritize));

    const second = engine.generateDraft(babrunInput());
    assert.equal(
      second.metrics.some((m) => m.key === 'pain_confirmation_rate'),
      false
    );
    assert.ok(second.metrics.some((m) => m.key === 'referral_partners_created'));
    assert.equal(second.metrics[0].key, 'referral_partners_created');
  });

  it('recommends scorecard evolution without applying it', () => {
    const engine = createScorecardEngine();
    const draft = engine.generateDraft(babrunInput());
    engine.approve(draft.id, { operator: 'fedir' });
    const result = engine.evolve('babrun', { stage: BUSINESS_STAGES.OPERATIONAL_SCALE });
    assert.equal(result.autoApplied, false);
    assert.equal(result.needed, true);
    assert.match(result.message, /operational scale/i);
    assert.match(result.message, /Pain Confirmation Rate/);
    assert.match(result.message, /Student Completion Rate/);
    const still = engine.getApproved('babrun');
    assert.ok(still.metrics.some((m) => m.key === 'pain_confirmation_rate'));
  });
});

describe('SPEC-116 brief sections and tenant isolation', () => {
  it('distinguishes recommended, approved, and under-review metrics', () => {
    const engine = createScorecardEngine();
    const draft = engine.generateDraft(anchorInput());
    const sections = buildBriefScorecardSections(draft);
    assert.equal(sections[0].title, 'Recommended Operator Scorecard');
    assert.equal(sections[1].title, 'Operator Approved Scorecard');
    assert.equal(sections[2].title, 'Metrics Under Review');
    assert.match(sections[0].body, /stated business objectives/i);
    assert.match(sections[1].body, /not yet approved/i);
    assert.equal(sections[2].items.length, 0);
    const blob = JSON.stringify(sections);
    assert.match(blob, /walkthrough/i);
    assert.match(blob, /conversion rate/i);
    assert.match(blob, /Max may also want to explore/i);

    engine.approve(draft.id, { operator: 'aji' });
    const after = buildBriefScorecardSections(engine.getApproved('anchor'));
    assert.match(after[1].body, /explicitly approved/i);
    assert.ok(after[1].items.length >= 1);
    assert.equal(after[2].items.length, 0);
  });

  it('keeps scorecards tenant-scoped', () => {
    const engine = createScorecardEngine();
    engine.generateDraft(babrunInput());
    engine.generateDraft(anchorInput());
    assert.equal(engine.list('babrun').every((row) => row.tenantId === 'babrun'), true);
    assert.equal(engine.list('anchor').every((row) => row.tenantId === 'anchor'), true);
    assert.equal(engine.getApproved('babrun'), null);
    engine.approve(engine.getDraft('babrun').id, { operator: 'fedir' });
    assert.equal(engine.runtime('anchor').status, 'absent');
    assert.equal(engine.runtime('babrun').status, 'approved');
  });
});
