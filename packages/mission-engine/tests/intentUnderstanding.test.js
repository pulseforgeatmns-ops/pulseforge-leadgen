'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  understandIntent,
  planFromIntent,
  planFromOperatorText,
  INTENT_CATEGORIES,
  INTENT_CONFIDENCE_THRESHOLD,
  summarizeMissionIntent,
  createMissionEngine,
} = require('..');
const {
  BUILTIN_IDS: CAP_IDS,
  createBuiltinRegistry,
} = require('../../capabilities');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({
      discovery: { useFixture: true },
    }),
  });
}

describe('SPEC-055 Intent Understanding', () => {
  it('resolves end-to-end execution audit as Campaign Diagnostics (not alias miss)', () => {
    const intent = understandIntent(
      'Run an end-to-end execution audit for Campaign 001.'
    );
    assert.equal(intent.matchedIntent, INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS);
    assert.equal(intent.needsClarification, false);
    assert.ok(intent.confidence >= INTENT_CONFIDENCE_THRESHOLD);
    assert.equal(intent.target.campaign, '001');
    assert.equal(intent.diagnostics, true);
    assert.match(intent.goal, /Diagnose Campaign 001/i);
  });

  it('resolves Discovery investigation from free-form why-question', () => {
    const intent = understandIntent("Why isn't Discovery finding anyone?");
    assert.equal(
      intent.matchedIntent,
      INTENT_CATEGORIES.DISCOVERY_INVESTIGATION
    );
    assert.equal(intent.diagnostics, true);
    assert.ok(intent.confidence >= 0.9);
  });

  it('resolves "what\'s wrong with Campaign" as Campaign Diagnostics', () => {
    const intent = understandIntent(
      "Let's see what's wrong with Campaign 001."
    );
    assert.equal(intent.matchedIntent, INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS);
    assert.equal(intent.target.campaign, '001');
  });

  it('resolves "Run the campaign" as Campaign Execution', () => {
    const intent = understandIntent('Run the campaign.');
    assert.equal(intent.matchedIntent, INTENT_CATEGORIES.CAMPAIGN_EXECUTION);
    assert.ok(intent.confidence >= INTENT_CONFIDENCE_THRESHOLD);
  });

  it('keeps Build Campaign as Campaign Creation with high confidence', () => {
    const intent = understandIntent(
      'Build Campaign 001 for Anchor Cleaning using the current ProspectList.'
    );
    assert.equal(intent.matchedIntent, INTENT_CATEGORIES.CAMPAIGN_CREATION);
    assert.equal(intent.target.campaign, '001');
    assert.equal(intent.parameters.prospectList, 'current');
    assert.equal(intent.target.subject, 'Anchor Cleaning');
  });

  it('surfaces alternate intents and clarification for ambiguous text', () => {
    const intent = understandIntent('Do the thing with the stuff.');
    assert.equal(intent.needsClarification, true);
    assert.ok(intent.clarificationPrompt);
    const summary = summarizeMissionIntent(intent);
    assert.equal(summary.needsClarification, true);
  });
});

describe('SPEC-055 Capability Planning from MissionIntent', () => {
  it('maps Campaign Execution → Direct Mail Execution', () => {
    const result = planFromOperatorText('Run the campaign for Campaign 001.');
    assert.equal(result.clarification, false);
    assert.ok(result.missionPlan);
    assert.ok(
      result.missionPlan.execution.some(
        (e) => e.stageId === 'direct_mail_execution'
      )
    );
    assert.equal(result.missionPlan.parameters.campaign, '001');
  });

  it('maps Campaign Diagnostics → Discovery Diagnostics + Campaign Review + Outcome Intelligence', () => {
    const result = planFromOperatorText(
      'Run an end-to-end execution audit for Campaign 001.',
      { registry: createBuiltinRegistry({ discovery: { useFixture: true } }) }
    );
    assert.equal(result.clarification, false);
    const stages = result.missionPlan.execution.map((e) => e.stageId);
    assert.ok(stages.includes('discovery_diagnostics'));
    assert.ok(stages.includes('campaign_review'));
    assert.ok(stages.includes('outcome_intelligence'));
    assert.equal(result.missionIntent.diagnostics, true);
  });

  it('maps Discovery Investigation → Discovery Diagnostics + Campaign Review', () => {
    const result = planFromOperatorText(
      "Why isn't Discovery finding anyone?",
      { registry: createBuiltinRegistry({ discovery: { useFixture: true } }) }
    );
    assert.equal(result.clarification, false);
    const stages = result.missionPlan.execution.map((e) => e.stageId);
    assert.ok(stages.includes('discovery_diagnostics'));
    assert.ok(stages.includes('campaign_review'));
    assert.ok(!stages.includes('prospect_discovery'));
  });

  it('does not invent capability nodes from unknown gibberish', () => {
    const result = planFromOperatorText('Teleport prospects into hyperspace.');
    assert.equal(result.clarification, true);
    assert.equal(result.missionPlan, null);
    assert.ok(
      Array.isArray(result.suggestedInterpretations) ||
        result.missionIntent.needsClarification
    );
  });

  it('preserves SPEC-050 multi-sentence campaign build via legacy parser path', () => {
    const text = [
      'Build Campaign 001 for Anchor Cleaning using the current ProspectList.',
      'Execute the complete pipeline through Sales Intelligence.',
      'Review Human Test results and generated letters.',
    ].join(' ');
    const result = planFromOperatorText(text);
    assert.equal(result.clarification, false);
    assert.equal(result.missionPlan.objective, 'Build Campaign 001');
    assert.ok(
      result.missionPlan.execution.some((e) => e.stageId === 'campaign_builder')
    );
    assert.ok(
      result.missionPlan.notes.some((n) => /human\s+test/i.test(n))
    );
  });
});

describe('SPEC-055 MissionPlanner two-stage planning', () => {
  it('plans execution audit without Unknown mission alias failure', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Run an end-to-end execution audit for Campaign 001.',
      tenantId: '10',
      clientId: 10,
    });
    assert.ok(draft.missionIntent);
    assert.equal(
      draft.missionIntent.matchedIntent,
      INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS
    );
    assert.ok(draft.missionPlan);
    assert.ok(draft.missionIntentSummary);
    assert.ok(draft.plan.missionIntentSummary);
    assert.ok(
      draft.plan.steps.some((s) => s.capabilityId === CAP_IDS.CAMPAIGN_REVIEW)
    );
    assert.ok(
      !String(JSON.stringify(draft.plan.planningDiagnostics || {})).match(
        /No matching mission alias for "Run an end-to-end/i
      )
    );
  });

  it('returns clarification draft for ambiguous requests', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Maybe do something somehow.',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.needsClarification, true);
    assert.ok(draft.plan.clarification);
    assert.equal(draft.plan.clarification.required, true);
    assert.ok(Array.isArray(draft.suggestedInterpretations));
    assert.equal(draft.missionPlan, null);
  });

  it('capabilities still consume MissionPlan — never MissionIntent language', () => {
    const intent = understandIntent('Run the campaign.');
    const { missionPlan } = planFromIntent(intent);
    assert.ok(missionPlan.objective);
    assert.ok(Array.isArray(missionPlan.execution));
    // MissionPlan is the executable contract
    assert.ok(missionPlan.execution.every((e) => e.stageId || e.capabilityId));
  });
});
