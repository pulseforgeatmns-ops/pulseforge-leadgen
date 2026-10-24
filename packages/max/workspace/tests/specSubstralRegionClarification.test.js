'use strict';

/**
 * Regression — Studio Substral region clarification + compound approval dispatches Scout.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { resolvePendingOperatorDecision } = require('../PendingDecisionResolver');
const { CAPTURE_INTENTS, classifyPendingDecisionCaptureIntent } = require('../pendingDecisionCaptureGuard');
const { maybeHandleAcquisitionMissionExecution } = require('../AcquisitionMissionExecution');
const {
  advancePlanClarification,
  hasPendingPlanClarification,
  hasPendingPlanApproval,
} = require('../AmoOperatorApproval');
const { hasPendingPlanClarification: hasPlanClarificationPredicate } = require('../../../acquisition-mission/PendingOperatorDecision');

const SUBSTRAL_OBJECTIVE =
  'Acquire one paid Studio Substral website assessment from an established business with a live website and a real decision about what to fix, rebuild, or leave alone.';

const COMPOUND_OPERATOR_MESSAGE =
  'Decision: region = Greater Manchester, NH and southern New Hampshire. Approval: approved. Proceed with Scout discovery for the first controlled Studio Substral batch. Outreach remains disabled.';

describe('Studio Substral — region clarification and Scout dispatch', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '17',
      objective: SUBSTRAL_OBJECTIVE,
      targetSegment: 'Established businesses with live websites — United States',
    });
  });

  it('starts with region clarification for nationwide Studio Substral objective', () => {
    const snapshot = engine.inspect(mission.id, { tenantId: '17' });
    assert.equal(hasPlanClarificationPredicate(snapshot), true);
    assert.match(
      snapshot.mission.pendingOperatorDecision.prompt,
      /Which region should this mission cover/i
    );
  });

  it('accepts free-text region answers without multiple-choice options', () => {
    const clarified = advancePlanClarification({
      engine,
      mission,
      tenantId: '17',
      question: COMPOUND_OPERATOR_MESSAGE,
    });
    assert.equal(clarified.matched, true);
    assert.equal(hasPendingPlanApproval(clarified.snapshot), true);
    assert.equal(clarified.snapshot.mission.missionPlanDraft.geography.region, 'Greater Manchester');
    assert.equal(hasPendingPlanClarification(clarified.snapshot), false);
  });

  it('compound turn clears clarification, locks plan, and creates Scout execution request', async () => {
    const snapshot = engine.inspect(mission.id, { tenantId: '17' });
    const operatorIntent = await analyzeOperatorIntent({
      question: COMPOUND_OPERATOR_MESSAGE,
      mission,
      snapshot,
      resolveMission: false,
      missionsEnabled: true,
    });

    assert.equal(operatorIntent.pendingDecisionResolution.action, 'clarify_plan');

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: COMPOUND_OPERATOR_MESSAGE,
      operatorIntent,
      context: { tenantId: '17', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
      allowFixtureFallback: true,
    });

    assert.ok(turn);
    assert.equal(turn.action, 'discovery_approved');
    assert.ok(turn.executionRequest);

    const after = engine.inspect(mission.id, { tenantId: '17' });
    assert.equal(after.mission.structuredMissionApproved, true);
    assert.equal(after.mission.structuredMission.geography.region, 'Greater Manchester');
    assert.equal(hasPlanClarificationPredicate(after), false);
    assert.ok(
      (after.contributions || []).some(
        (row) => row.specialist === 'scout' && row.kind === 'discovery'
      )
    );
    assert.notEqual(after.mission.pendingOperatorDecision?.kind, 'plan_clarification');
  });

  it('an explicit typed region updates only the bound mission and still requires plan approval', async () => {
    const other = engine.create({ tenantId: '10', objective: SUBSTRAL_OBJECTIVE });
    const otherBefore = engine.inspect(other.id, { tenantId: '10' });
    const question = 'region: Manchester NH';
    const operatorIntent = await analyzeOperatorIntent({
      question, mission, snapshot: engine.inspect(mission.id, { tenantId: '17' }),
      context: { tenantId: '17', missionId: mission.id }, resolveMission: false,
    });
    assert.equal(operatorIntent.pendingDecisionResolution.action, 'clarify_plan');
    assert.equal(operatorIntent.pendingDecisionResolution.missionId, mission.id);
    const turn = await maybeHandleAcquisitionMissionExecution({
      question, operatorIntent, context: { tenantId: '17', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true,
    });
    assert.equal(turn.action, 'plan_clarified');
    const after = engine.inspect(mission.id, { tenantId: '17' });
    assert.equal(after.mission.missionPlanDraft.geography.region, 'Greater Manchester');
    assert.equal(hasPendingPlanApproval(after), true);
    assert.equal(after.mission.structuredMissionApproved, false);
    assert.equal(after.contributions.some((row) => row.specialist === 'scout'), false);
    assert.deepEqual(engine.inspect(other.id, { tenantId: '10' }), otherBefore);
  });

  it('accepts an exact offered region choice without capturing a question about it', () => {
    const choosing = {
      ...mission,
      pendingOperatorDecision: {
        ...mission.pendingOperatorDecision,
        responseType: 'choice',
        choices: [{ id: 'manchester_nh', label: 'Manchester NH', value: { region: 'Greater Manchester' } }],
      },
    };
    for (const question of ['manchester_nh', 'Manchester NH']) {
      assert.equal(resolvePendingOperatorDecision(question, choosing).action, 'clarify_plan');
    }
    assert.equal(resolvePendingOperatorDecision('Why Manchester NH?', choosing).resolved, false);
    choosing.pendingOperatorDecision.choices = [{ id: 'portland_or', label: 'Portland OR', value: { region: 'Portland OR' } }];
    assert.equal(resolvePendingOperatorDecision('Portland OR', choosing).action, 'clarify_plan');
  });

  for (const question of ['Law firms', 'law_firm']) {
    it(`preserves exact offered non-geography choice: ${question}`, async () => {
      const choosing = engine.create({ tenantId: '17', objective: 'Acquire one cleaning client in Manchester NH.' });
      assert.equal(choosing.pendingOperatorDecision.field, 'market.segment');
      const context = { tenantId: '17', missionId: choosing.id };
      const operatorIntent = await analyzeOperatorIntent({
        question, mission: choosing, snapshot: engine.inspect(choosing.id, { tenantId: '17' }), context, resolveMission: false,
      });
      assert.equal(operatorIntent.pendingDecisionResolution.action, 'clarify_plan');
      const turn = await maybeHandleAcquisitionMissionExecution({
        question, operatorIntent, context,
        acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true,
      });
      assert.equal(turn.action, 'plan_clarified');
      const after = engine.inspect(choosing.id, { tenantId: '17' });
      assert.equal(after.mission.missionPlanDraft.market.segment, 'law_firm');
      assert.equal(after.mission.pendingOperatorDecision.kind, 'plan_approval');
      assert.equal(after.mission.structuredMissionApproved, false);
      assert.equal(after.contributions.some((row) => row.specialist === 'scout'), false);
    });
  }

  for (const region of ['Boston MA', 'Toronto ON', 'Portland OR']) {
    it(`preserves a clear typed city and region without approving the plan: ${region}`, async () => {
      const question = `region = ${region}`;
      const context = { tenantId: '17', missionId: mission.id };
      const operatorIntent = await analyzeOperatorIntent({
        question, mission, snapshot: engine.inspect(mission.id, { tenantId: '17' }),
        context, resolveMission: false,
      });
      const turn = await maybeHandleAcquisitionMissionExecution({
        question, operatorIntent, context,
        acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true,
      });
      assert.equal(turn.action, 'plan_clarified');
      const after = engine.inspect(mission.id, { tenantId: '17' });
      assert.equal(after.mission.missionPlanDraft.geography.region, region);
      assert.equal(hasPendingPlanApproval(after), true);
      assert.equal(after.mission.structuredMissionApproved, false);
      assert.equal(after.contributions.some((row) => row.specialist === 'scout'), false);
    });
  }

  for (const question of [
    'What is the current status and confidence of this mission?',
    'Show the current mission status',
    'What exactly am I approving?',
    'Should region = Manchester NH?',
    'region = Manchester NH?',
    'Show me today’s pipeline instead',
    'Tell me a joke',
    'approved',
    'region = approved',
    'region =',
  ]) {
    it(`leaves structured clarification unchanged for non-answer: ${question}`, async () => {
      const before = engine.inspect(mission.id, { tenantId: '17' });
      const capture = classifyPendingDecisionCaptureIntent({
        message: question, pendingDecision: mission.pendingOperatorDecision,
      });
      assert.notEqual(capture, CAPTURE_INTENTS.DECISION_RESPONSE);
      const resolution = resolvePendingOperatorDecision(question, mission);
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.executionIntent, undefined);
      const context = { tenantId: '17', missionId: mission.id };
      const operatorIntent = await analyzeOperatorIntent({ question, mission, snapshot: before, context, resolveMission: false });
      for (const intent of [operatorIntent, undefined]) {
        await maybeHandleAcquisitionMissionExecution({
          question, operatorIntent: intent, context,
          acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true,
        });
      }
      assert.deepEqual(engine.inspect(mission.id, { tenantId: '17' }), before);
    });
  }

  for (const question of [
    'region = not sure yet',
    'maybe Manchester NH',
    'Manchester NH or Boston MA',
    'region = maybe Manchester NH',
    'region = not Manchester NH',
    'region = Manchester NH or Boston MA',
    'region = Manchester NH and Boston MA',
    'region = Boston MA Toronto ON',
    'region = Boston MA / Toronto ON',
    'region = perhaps Toronto ON',
    'region = not Boston MA',
    'region = somewhere nearby',
    'region = Manchester NH. Do not approve this yet.',
    'region = Manchester NH. Approval: approved. Do not approve this yet.',
  ]) {
    it(`does not mutate the mission for uncertain or negated region turn: ${question}`, async () => {
      const before = engine.inspect(mission.id, { tenantId: '17' });
      const context = { tenantId: '17', missionId: mission.id };
      const operatorIntent = await analyzeOperatorIntent({
        question, mission, snapshot: before, context, resolveMission: false,
      });
      assert.equal(operatorIntent.pendingDecisionResolution?.resolved === true, false);
      await maybeHandleAcquisitionMissionExecution({
        question, operatorIntent, context,
        acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true,
      });
      assert.deepEqual(engine.inspect(mission.id, { tenantId: '17' }), before);
      await maybeHandleAcquisitionMissionExecution({
        question, context,
        acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true,
      });
      assert.deepEqual(engine.inspect(mission.id, { tenantId: '17' }), before);
    });
  }

  for (const prohibition of ['Do not begin discovery.', 'Do not run Scout discovery yet.']) {
    it(`can approve the plan while honoring the discovery prohibition: ${prohibition}`, async () => {
      const question = `region = Manchester NH. Approval: approved. ${prohibition}`;
      const context = { tenantId: '17', missionId: mission.id };
      const operatorIntent = await analyzeOperatorIntent({
        question, mission, snapshot: engine.inspect(mission.id, { tenantId: '17' }), context, resolveMission: false,
      });
      const turn = await maybeHandleAcquisitionMissionExecution({
        question, operatorIntent, context,
        acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true,
      });
      assert.equal(turn.action, 'plan_approved');
      const after = engine.inspect(mission.id, { tenantId: '17' });
      assert.equal(after.mission.structuredMissionApproved, true);
      assert.equal(after.mission.pendingOperatorDecision.kind, 'discovery_approval');
      assert.equal(after.contributions.some((row) => row.specialist === 'scout'), false);
    });
  }

  for (const condition of ['if the budget is confirmed', 'unless costs change', 'provided the budget is confirmed', 'once the budget is confirmed', 'after the budget is confirmed']) {
    it(`keeps conditional approval pending: ${condition}`, async () => {
      const question = `region = Manchester NH. Approval: approved ${condition}.`;
      const context = { tenantId: '17', missionId: mission.id };
      const operatorIntent = await analyzeOperatorIntent({
        question, mission, snapshot: engine.inspect(mission.id, { tenantId: '17' }), context, resolveMission: false,
      });
      const turn = await maybeHandleAcquisitionMissionExecution({
        question, operatorIntent, context,
        acquisitionMissionRuntime: createTestAmoRuntime({ engine }), allowFixtureFallback: true,
      });
      assert.equal(turn.action, 'plan_clarified');
      const after = engine.inspect(mission.id, { tenantId: '17' });
      assert.equal(after.mission.missionPlanDraft.geography.region, 'Greater Manchester');
      assert.equal(after.mission.structuredMissionApproved, false);
      assert.equal(after.mission.pendingOperatorDecision.kind, 'plan_approval');
      assert.equal(after.contributions.some((row) => row.specialist === 'scout'), false);
    });
  }
});
