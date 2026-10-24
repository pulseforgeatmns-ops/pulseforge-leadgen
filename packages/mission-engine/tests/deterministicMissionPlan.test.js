'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  parseIntent,
  classifyUnit,
  PLAN_CATEGORIES,
  createMissionEngine,
  validateMissionPlan,
  summarizeMissionPlan,
  containsOperatorInstructionLeak,
  RESERVED_RUNTIME_FIELDS,
  createExecutionGraph,
  MISSION_TYPES,
} = require('..');
const { BUILTIN_IDS: CAP_IDS, createBuiltinRegistry } = require('../../capabilities');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({
      discovery: { useFixture: true },
    }),
  });
}

const SPEC049_OBJECTIVE = [
  'Build Campaign 001 for Anchor Cleaning using the current ProspectList.',
  'Execute the complete pipeline through Sales Intelligence.',
  'Review Human Test results and generated letters.',
].join(' ');

describe('SPEC-050 Intent Parser — Mission grammar', () => {
  it('classifies the Anchor Cleaning objective into Plan IR fields', () => {
    const plan = parseIntent(SPEC049_OBJECTIVE);
    assert.equal(plan.objective, 'Build Campaign 001');
    assert.equal(plan.subject, 'Anchor Cleaning');
    assert.equal(plan.parameters.prospectList, 'current');
    assert.equal(plan.parameters.campaign, '001');
    assert.ok(
      plan.execution.some((e) => e.stageId === 'campaign_builder')
    );
    assert.ok(
      plan.execution.some((e) => e.stageId === 'sales_intelligence')
    );
    assert.equal(plan.options.review, true);
    assert.equal(plan.options.approvalRequired, true);
    assert.ok(
      plan.notes.some((n) => /human\s+test/i.test(n) && /generated\s+letters/i.test(n))
    );
    assert.ok(
      !plan.notes.some((n) => /campaign_builder/i.test(n))
    );
  });

  it('puts Review Human Test / generated letters in Notes — never Execution', () => {
    const unit = classifyUnit(
      'Review Human Test results and generated letters.'
    );
    assert.equal(unit.category, PLAN_CATEGORIES.NOTES);
  });

  it('treats bare Review. as Options and requests Campaign Review stage', () => {
    const unit = classifyUnit('Review.');
    assert.equal(unit.category, PLAN_CATEGORIES.OPTIONS);
    assert.equal(unit.detail.options.review, true);
    assert.equal(unit.detail.requestCampaignReview, true);
  });

  it('stores unknown capability requests as Notes', () => {
    const plan = parseIntent(
      'Build Campaign 001. Teleport the prospects into hyperspace.'
    );
    assert.ok(
      plan.notes.some(
        (n) =>
          /no matching mission alias|hyperspace|suggested|teleport/i.test(n)
      )
    );
    assert.ok(
      !plan.execution.some((e) => /hyperspace|teleport/i.test(String(e.stageId)))
    );
  });
});

describe('SPEC-050 Mission Plan validation', () => {
  it('rejects reserved runtime fields populated from operator parameters', () => {
    const plan = parseIntent('Build Campaign 001');
    const tainted = {
      ...plan,
      parameters: { ...plan.parameters, companyName: 'Should Not Stick' },
    };
    const result = validateMissionPlan(tainted);
    assert.equal(result.ok, false);
    assert.ok(result.errors.some((e) => /reserved runtime field/i.test(e)));
  });

  it('lists reserved runtime field names', () => {
    assert.ok(RESERVED_RUNTIME_FIELDS.includes('company'));
    assert.ok(RESERVED_RUNTIME_FIELDS.includes('recipient'));
    assert.ok(RESERVED_RUNTIME_FIELDS.includes('decisionMaker'));
  });

  it('summarizeMissionPlan is operator-readable', () => {
    const plan = parseIntent(SPEC049_OBJECTIVE);
    const summary = summarizeMissionPlan(plan);
    assert.equal(summary.objective, 'Build Campaign 001');
    assert.equal(summary.reviewEnabled, true);
    assert.ok(summary.execution.includes('Campaign Builder'));
    assert.ok(summary.notes.length >= 1);
  });
});

describe('SPEC-050 Deterministic planning — free-form never becomes nodes', () => {
  it('does not create campaign_review from Human Test guidance', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: SPEC049_OBJECTIVE,
      tenantId: '10',
      clientId: 10,
    });
    assert.ok(draft.missionPlan);
    assert.ok(draft.plan.missionPlan);
    assert.ok(draft.plan.missionPlanSummary);
    assert.ok(!draft.plan.selectedStages.includes('campaign_review'));
    assert.ok(
      !draft.plan.steps.some((s) => /human\s+test|generated\s+letter/i.test(s.name))
    );
    assert.ok(
      draft.plan.steps.every((s) =>
        Object.values(CAP_IDS).includes(s.capabilityId)
      )
    );
    assert.ok(draft.plan.steps.map((s) => s.capabilityId).includes(CAP_IDS.CAMPAIGN_BUILDER));
    assert.ok(draft.plan.steps.map((s) => s.capabilityId).includes(CAP_IDS.SALES_INTELLIGENCE));
  });

  it('createExecutionGraph with Mission Plan ignores Notes keywords', () => {
    const plan = parseIntent(SPEC049_OBJECTIVE);
    const graph = createExecutionGraph({
      objective: SPEC049_OBJECTIVE,
      missionType: MISSION_TYPES.CAMPAIGN_CREATION,
      missionPlan: plan,
    });
    assert.ok(!graph.selectedStages.includes('campaign_review'));
    assert.ok(graph.reasoning.notesExcludedFromExecution >= 1);
  });

  it('still composes Review + Ready to Print when explicitly requested', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: [
        'Build Campaign 001 for Anchor Cleaning.',
        'Generate mail packages.',
        'Review.',
        'Ready to Print.',
      ].join(' '),
      tenantId: '10',
      clientId: 10,
    });
    assert.ok(draft.plan.selectedStages.includes('mail_package_generator'));
    assert.ok(draft.plan.selectedStages.includes('campaign_review'));
    assert.ok(draft.plan.selectedStages.includes('ready_to_print'));
  });
});

describe('SPEC-050 / ADR-034 — data isolation', () => {
  it('detects operator instruction leaks into artifact text', () => {
    const plan = parseIntent(SPEC049_OBJECTIVE);
    assert.equal(
      containsOperatorInstructionLeak(
        'Dear and generated letters for every prospect',
        plan
      ),
      true
    );
    assert.equal(
      containsOperatorInstructionLeak('Dear Jordan Hale,', plan),
      false
    );
  });

  it('executor context uses Mission Plan objective not raw notes', async () => {
    const engine = testEngine();
    const result = await engine.createFromObjective({
      objective: SPEC049_OBJECTIVE,
      tenantId: '10',
      clientId: 10,
      execute: true,
    });
    const mission = result.mission || result;
    assert.ok(mission.plan && mission.plan.missionPlan);
    assert.match(mission.objectiveText, /Human Test/);
    // Capabilities must not have been planned from the notes fragment
    assert.ok(!mission.plan.selectedStages.includes('campaign_review'));
    assert.equal(mission.plan.missionPlan.objective, 'Build Campaign 001');
  });
});
