'use strict';

/**
 * SPEC-JEV-005 — Mission Inspection State Consistency.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  STAGES,
  OPERATOR_DECISION_KINDS,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
} = amo;
const {
  resolveInspectionMission,
  ANCHOR_STR_CANONICAL_MISSION_ID,
  isAnchorStrObjective,
} = require('../../../acquisition-mission/resolveInspectionMission');
const {
  buildMissionInspectionSnapshot,
  validateMissionInspectionSnapshot,
  formatMissionInspectionProse,
} = require('../../../acquisition-mission/MissionInspectionSnapshot');
const { composeConversationalResponse } = require('../ConversationLayer');
const { maybeHandleWorkspaceMissionInspection } = require('../WorkspaceMissionInspection');
const { installTestAmoRuntime } = require('./amoTestRuntime');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.';

const ANCHOR_STATUS_QUESTION =
  'What is the current status and confidence of the Anchor STR mission?';

function makeStrMission(overrides = {}) {
  return {
    id: ANCHOR_STR_CANONICAL_MISSION_ID,
    tenantId: '10',
    objective: STR_OBJECTIVE,
    stage: STAGES.DISCOVER,
    status: 'active',
    confidence: 0.72,
    blockers: [],
    pendingOperatorDecision: null,
    ...overrides,
  };
}

function makeDailyMission() {
  return {
    id: 'mission_daily_0287c871bdea5ed6f3e59a1d',
    tenantId: '10',
    objective: 'Daily watch — Anchor Cleaning commercial pipeline',
    stage: STAGES.DISCOVER,
    status: 'active',
    confidence: 0.55,
    blockers: [],
    pendingOperatorDecision: {
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
      stage: STAGES.DISCOVER,
    },
  };
}

describe('SPEC-JEV-005 — Mission Inspection State Consistency', () => {
  describe('resolveInspectionMission', () => {
    it('named Anchor STR status selects canonical mission, not daily wrapper', () => {
      const canonical = makeStrMission({
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
          prompt: 'Approve discovery?',
        },
      });
      const daily = makeDailyMission();
      const resolution = resolveInspectionMission({
        tenantId: '10',
        operatorMessage: ANCHOR_STATUS_QUESTION,
        activeMission: daily,
        candidateMissions: [daily, canonical],
      });

      assert.equal(resolution.mission.id, ANCHOR_STR_CANONICAL_MISSION_ID);
      assert.notEqual(resolution.mission.id.startsWith('mission_daily_'), true);
      assert.ok(isAnchorStrObjective(resolution.mission.objective));
      assert.equal(resolution.resolution_type, 'named_exact');
      assert.equal(
        resolution.warnings.some((row) => row.code === 'named_mission_resolved_to_daily_wrapper'),
        false
      );
    });

    it('discloses when only daily wrapper exists for named Anchor STR request', () => {
      const daily = makeDailyMission();
      const resolution = resolveInspectionMission({
        tenantId: '10',
        operatorMessage: ANCHOR_STATUS_QUESTION,
        activeMission: daily,
        candidateMissions: [daily],
      });

      assert.equal(resolution.resolution_type, 'daily_wrapper');
      assert.match(resolution.mission.id, /^mission_daily_/);
      assert.ok(resolution.warnings.length > 0);
    });
  });

  describe('validateMissionInspectionSnapshot', () => {
    it('Scout finished + waiting for Scout triggers contradiction error', () => {
      const snapshot = {
        waiting_on: 'Scout',
        waiting_reason: 'Waiting for Scout',
        last_completed_specialist_action: {
          specialist: 'Scout',
          action: 'discovery',
          status: 'completed',
          phase: 'discovery',
        },
        pending_decision: null,
        is_daily_wrapper: false,
      };
      const validation = validateMissionInspectionSnapshot(snapshot);
      assert.equal(validation.ok, false);
      assert.ok(
        validation.warnings.some(
          (row) => row.code === 'specialist_finished_but_waiting_on_same_specialist'
        )
      );

      const prose = formatMissionInspectionProse(snapshot, validation);
      assert.match(prose, /conflicting mission state/i);
      assert.doesNotMatch(prose, /Scout finished its pass, but the mission is waiting on you/i);
    });

    it('waiting on operator + waiting reason points to Scout triggers contradiction', () => {
      const snapshot = {
        waiting_on: 'operator',
        waiting_reason: 'Waiting for Scout',
        pending_decision: { type: 'approve_discovery', prompt: 'Approve discovery?' },
        last_completed_specialist_action: null,
        is_daily_wrapper: false,
      };
      const validation = validateMissionInspectionSnapshot(snapshot);
      assert.equal(validation.ok, false);
      assert.ok(
        validation.warnings.some(
          (row) => row.code === 'operator_waiting_reason_points_to_specialist'
        )
      );
    });

    it('pending approve_discovery with no completed Scout discovery is consistent', () => {
      const inspectResult = {
        mission: makeStrMission({
          pendingOperatorDecision: {
            kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
            prompt: 'Approve discovery?',
          },
        }),
        contributions: [],
        workspace: { scout: { state: 'pending' } },
        health: { label: 'Healthy' },
        blocker: {
          kind: 'waiting_for_scout',
          label: 'Waiting for Scout',
          reason: 'Waiting for Scout',
          specialist: 'scout',
        },
      };
      const snapshot = buildMissionInspectionSnapshot(inspectResult, {
        resolution_type: 'named_exact',
        requested_mission_label: 'Anchor STR mission',
      });
      assert.equal(snapshot.waiting_on, 'operator');
      assert.equal(snapshot.pending_decision.type, 'approve_discovery');
      assert.equal(snapshot.last_completed_specialist_action, null);

      const prose = formatMissionInspectionProse(snapshot, { ok: true, warnings: [] });
      assert.match(prose, /waiting on operator approval to begin Scout discovery/i);
      assert.doesNotMatch(prose, /Scout finished/i);
      assert.doesNotMatch(prose, /Scout completed discovery/i);
    });

    it('Scout completed discovery with operator review does not say waiting for Scout', () => {
      const inspectResult = {
        mission: makeStrMission({
          stage: STAGES.DISCOVER,
          pendingOperatorDecision: {
            kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
            prompt: 'Approve prioritization?',
          },
        }),
        contributions: [
          {
            specialist: SPECIALISTS.SCOUT,
            kind: CONTRIBUTION_KINDS.DISCOVERY,
            payload: { companies: [{ id: 1 }], confidence: 0.68 },
            at: '2026-09-20T12:00:00.000Z',
          },
        ],
        workspace: { scout: { state: 'complete' } },
        health: { label: 'Healthy' },
        blocker: null,
      };
      const snapshot = buildMissionInspectionSnapshot(inspectResult, {
        resolution_type: 'named_exact',
        requested_mission_label: 'Anchor STR mission',
      });
      assert.equal(snapshot.last_completed_specialist_action.specialist, 'Scout');
      assert.equal(snapshot.waiting_on, 'operator');
      assert.doesNotMatch(String(snapshot.waiting_reason), /Waiting for Scout/i);

      const prose = formatMissionInspectionProse(snapshot, { ok: true, warnings: [] });
      assert.match(prose, /Scout completed discovery/i);
      assert.doesNotMatch(prose, /Waiting for Scout/i);
    });

    it('unknown confidence is explicit in response prose', () => {
      const snapshot = buildMissionInspectionSnapshot(
        {
          mission: makeStrMission({ confidence: null }),
          contributions: [],
          workspace: {},
          health: {},
        },
        { resolution_type: 'named_exact', requested_mission_label: 'Anchor STR mission' }
      );
      assert.equal(snapshot.confidence_available, false);
      const prose = formatMissionInspectionProse(snapshot, { ok: true, warnings: [] });
      assert.match(prose, /Confidence: unavailable/i);
    });
  });

  describe('ConversationLayer snapshot prose', () => {
    it('does not produce Scout finished / Waiting for Scout contradictory prose', () => {
      const inspectResult = {
        mission: makeStrMission({
          pendingOperatorDecision: {
            kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
            prompt: 'Approve discovery?',
          },
        }),
        contributions: [],
        workspace: { scout: { state: 'complete' } },
        health: { label: 'Healthy' },
        blocker: {
          kind: 'waiting_for_scout',
          label: 'Waiting for Scout',
          reason: 'Waiting for Scout',
          specialist: 'scout',
        },
      };
      const resolution = {
        resolution_type: 'named_exact',
        requested_mission_label: 'Anchor STR mission',
        tenant_id: '10',
      };
      const inspectionSnapshot = buildMissionInspectionSnapshot(inspectResult, resolution);
      const inspectionValidation = validateMissionInspectionSnapshot(inspectionSnapshot);

      const { prose } = composeConversationalResponse({
        question: ANCHOR_STATUS_QUESTION,
        conversationIntent: { intent: 'inspect' },
        snapshot: inspectResult,
        answered: { kind: 'inspection', mission: inspectResult.mission },
        inspectionSnapshot,
        inspectionValidation,
      });

      if (inspectionValidation.ok) {
        assert.doesNotMatch(prose, /Scout finished.*Waiting for Scout/is);
        assert.match(prose, /operator approval|Approve discovery/i);
      } else {
        assert.match(prose, /conflicting mission state/i);
      }
    });
  });

  describe('WorkspaceMissionInspection integration', () => {
    it('resolves canonical Anchor STR mission during inspection', async () => {
      const engine = amo.createAcquisitionMissionEngine();
      const runtime = installTestAmoRuntime({ engine });
      const canonical = engine.create({
        tenantId: '10',
        objective: STR_OBJECTIVE,
        targetSegment: 'Short-term rental operators',
      });
      assert.notEqual(canonical.id.startsWith('mission_daily_'), true);

      const daily = {
        id: 'mission_daily_test001',
        tenantId: '10',
        objective: 'Daily watch mission',
        stage: STAGES.DISCOVER,
        status: 'active',
      };
      engine.store.putMission(daily);

      const turn = await maybeHandleWorkspaceMissionInspection({
        question: ANCHOR_STATUS_QUESTION,
        context: { tenantId: '10', missionId: daily.id },
        acquisitionMissionRuntime: runtime,
        silentInspection: true,
      });

      assert.ok(turn);
      assert.equal(turn.reason, 'mission_inspection');
      assert.equal(turn.missionResolution.mission.id, canonical.id);
      assert.equal(turn.inspectionSnapshot.requested_mission_label, 'Anchor STR mission');
    });
  });
});
