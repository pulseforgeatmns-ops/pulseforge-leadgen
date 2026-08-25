'use strict';

/**
 * SPEC-168 — Canonical Objective Resolution acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { planMission, planFromObjective } = require('../../../acquisition-mission/MissionPlanner');
const { buildSharedContext } = require('../../../acquisition-mission/Context');
const { explainWhy } = require('../../../acquisition-mission/Explain');
const { formatCanonicalObjectiveDisplay } = require('../../../acquisition-mission/StructuredMission');
const { createMission } = require('../../../acquisition-mission/Mission');
const {
  resolveExecutionContract,
} = require('../OperatorObjectiveResolutionEngine');
const {
  resolveCanonicalObjective,
  classifyMessageLines,
  canonicalObjectiveText,
} = require('../ResolvedObjective');
const { maybeHandleAcquisitionOwnershipTurn } = require('../AcquisitionOwnership');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.';

const MIXED_PROMPT = [
  'Create a production acquisition mission.',
  'Objective: Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.',
  'Execute autonomously.',
  'Explain reasoning only when material.',
].join('\n');

const POLICY_ONLY_PROMPT = [
  'Create a production acquisition mission.',
  'Execute autonomously.',
  'Explain your reasoning naturally.',
].join('\n');

describe('SPEC-168 — Canonical Objective Resolution', () => {
  describe('Scenario 1 — Mixed prompt decomposes independently', () => {
    it('separates objective, execution policy, communication policy, and evaluation policy', () => {
      const { executionContract, resolvedObjective } = resolveExecutionContract({
        question: MIXED_PROMPT,
      });

      assert.match(resolvedObjective.objective, /Acquire one recurring commercial cleaning client/i);
      assert.equal(resolvedObjective.executionPolicy.autonomy, 'autonomous');
      assert.equal(resolvedObjective.executionPolicy.environment, 'production');
      assert.ok(resolvedObjective.communicationPolicy);
      assert.equal(resolvedObjective.evaluationPolicy.executiveBehavior, true);
      assert.ok(resolvedObjective.extractedFrom.objectiveLines.length >= 1);
      assert.ok(resolvedObjective.extractedFrom.policyLines.length >= 2);
      assert.equal(executionContract.executionPolicy.executionPolicy, 'autonomous');
    });
  });

  describe('Scenario 2 — Mission Planner consumes ResolvedObjective only', () => {
    it('planMission structures from canonical object without reparsing policies', () => {
      const resolved = resolveCanonicalObjective({ question: STR_OBJECTIVE });
      const planned = planMission(resolved);

      assert.equal(planned.resolvedObjective.objective, resolved.objective);
      assert.ok(planned.pipeline.includes('resolved_objective'));
      assert.ok(!planned.pipeline.includes('intent_analysis'));
      assert.equal(planned.draft.missionType, 'acquisition');
      assert.equal(planned.draft.market.segment, 'short_term_rental');
      assert.equal(planned.draft.geography.region, 'Greater Manchester');
    });
  });

  describe('Scenario 3 — Workspace Mission Understanding shows business objective only', () => {
    it('formatCanonicalObjectiveDisplay never includes operator prompt wrappers', () => {
      const mission = createMission({
        tenantId: '10',
        objective: STR_OBJECTIVE,
        resolvedObjective: resolveCanonicalObjective({ question: STR_OBJECTIVE }),
        skipMissionPlanning: true,
      });
      const display = formatCanonicalObjectiveDisplay(mission);
      assert.match(display, /Mission Understanding/i);
      assert.match(display, /Acquire one recurring commercial cleaning client/i);
      assert.doesNotMatch(display, /Execute autonomously/i);
      assert.doesNotMatch(display, /Create a production/i);
    });
  });

  describe('Scenario 4 — Explain consumes canonical objective', () => {
    it('explainWhy uses resolvedObjective.objective', () => {
      const resolved = resolveCanonicalObjective({ question: STR_OBJECTIVE });
      const mission = createMission({
        tenantId: '10',
        objective: resolved.objective,
        resolvedObjective: resolved,
        skipMissionPlanning: true,
      });
      const explain = explainWhy(mission, []);
      assert.ok(explain.reasons.some((row) => /Acquire one recurring commercial cleaning client/i.test(row)));
    });
  });

  describe('Scenario 5 — Clarification at objective resolution', () => {
    it('missing business objective triggers clarification, not mission type confusion', () => {
      const resolved = resolveCanonicalObjective({
        question: POLICY_ONLY_PROMPT,
        objectiveResolution: resolveExecutionContract({ question: POLICY_ONLY_PROMPT }).objectiveResolution,
        executionContract: resolveExecutionContract({ question: POLICY_ONLY_PROMPT }).executionContract,
      });

      assert.equal(resolved.ready, false);
      assert.equal(resolved.objective, '');
      assert.ok(resolved.ambiguities.some((row) => row.field === 'objective'));
      assert.equal(resolved.missionType, 'acquisition');
      assert.equal(resolved.executionPolicy.autonomy, 'autonomous');
    });

    it('classifyMessageLines keeps execution instructions out of objective lines', () => {
      const classified = classifyMessageLines(MIXED_PROMPT);
      assert.ok(classified.objectiveLines.some((line) => /Acquire one recurring/i.test(line)));
      assert.ok(classified.policyLines.some((line) => /Execute autonomously/i.test(line)));
      assert.ok(classified.policyLines.some((line) => /Create a production/i.test(line)));
    });
  });

  describe('Scenario 6 — Single source of truth', () => {
    it('mission creation, shared context, and planner share the same canonical objective', async () => {
      const amoEngine = amo.createAcquisitionMissionEngine();
      const runtime = createTestAmoRuntime({ engine: amoEngine });
      const resolved = resolveCanonicalObjective({ question: STR_OBJECTIVE });

      const turn = await maybeHandleAcquisitionOwnershipTurn({
        question: STR_OBJECTIVE,
        resolvedObjective: resolved,
        context: { tenantId: '10' },
        acquisitionMissionRuntime: runtime,
        persist: false,
      });

      assert.ok(turn.mission);
      assert.equal(turn.mission.objective, resolved.objective);
      assert.equal(turn.mission.resolvedObjective.objective, resolved.objective);

      const shared = buildSharedContext(turn.mission, []);
      assert.equal(shared.objective, resolved.objective);
      assert.equal(shared.mission.objective, resolved.objective);
      assert.equal(canonicalObjectiveText(turn.mission.resolvedObjective), resolved.objective);

      const planned = planMission(resolved);
      assert.equal(planned.draft.objective, resolved.objective);
    });
  });

  describe('Backward compatibility — planFromObjective resolves once', () => {
    it('still produces structured plans from plain objective text', () => {
      const planned = planFromObjective(STR_OBJECTIVE);
      assert.equal(planned.draft.missionType, 'acquisition');
      assert.equal(planned.resolvedObjective.objective, planned.draft.objective);
    });
  });
});
