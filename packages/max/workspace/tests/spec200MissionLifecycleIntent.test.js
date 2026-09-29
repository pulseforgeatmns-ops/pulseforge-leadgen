'use strict';

/**
 * SPEC-200 — Explicit Mission Lifecycle Intent.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const {
  MissionLifecycleIntent,
  resolveMissionLifecycleIntent,
  isVerbNegatedInClause,
} = require('../MissionLifecycleIntent');
const {
  hasExecutionLanguage,
  detectMissionExecutionLanguage,
  isMissionExecutionCommand,
} = require('../ExecutionLanguageDetection');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { maybeHandleAcquisitionOwnershipTurn } = require('../AcquisitionOwnership');
const { classifyOperatorCognition, THINKING_MODES } = require('../../operatorCognition');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.';

describe('SPEC-200 — Explicit Mission Lifecycle Intent', () => {
  describe('resolveMissionLifecycleIntent — phrase matrix', () => {
    const cases = [
      {
        utterance: 'Create a new mission for recurring STR clients',
        intent: MissionLifecycleIntent.CREATE_NEW,
      },
      {
        utterance: 'Create a brand-new mission for recurring STR clients',
        intent: MissionLifecycleIntent.CREATE_NEW,
      },
      {
        utterance:
          'Do not resume the old mission. Create a new one for recurring STR clients.',
        intent: MissionLifecycleIntent.CREATE_NEW,
      },
      {
        utterance: 'Start fresh with a new mission for recurring STR clients',
        intent: MissionLifecycleIntent.CREATE_NEW,
      },
      {
        utterance: 'Create another mission with the same objective',
        intent: MissionLifecycleIntent.CREATE_NEW,
      },
      {
        utterance: 'Resume the existing mission for recurring STR clients',
        intent: MissionLifecycleIntent.RESUME_EXISTING,
      },
      {
        utterance: 'Continue the mission',
        intent: MissionLifecycleIntent.CONTINUE_ACTIVE,
      },
      {
        utterance: STR_OBJECTIVE,
        intent: MissionLifecycleIntent.UNSPECIFIED,
      },
    ];

    for (const row of cases) {
      it(`classifies "${row.utterance.slice(0, 48)}..." as ${row.intent}`, () => {
        const resolved = resolveMissionLifecycleIntent(row.utterance);
        assert.equal(resolved.intent, row.intent);
      });
    }
  });

  describe('upstream parsing — clause-level negation', () => {
    it('does not treat "do not resume" alone as resume execution language', () => {
      assert.equal(isVerbNegatedInClause('Do not resume the old mission.', 'resume'), true);
      assert.equal(isMissionExecutionCommand('Do not resume the old mission.'), false);
      const cognition = classifyOperatorCognition('Do not resume the old mission.');
      assert.notEqual(cognition.intent, THINKING_MODES.RESUME);
    });

    it('does not globally suppress create when a separate clause negates resume', () => {
      const utterance =
        'Create a brand-new acquisition mission. Do not resume, reuse, or continue any existing mission.';
      assert.equal(hasExecutionLanguage(utterance), true);
      const detected = detectMissionExecutionLanguage(utterance);
      assert.equal(detected.matched, true);
      assert.equal(detected.reason, 'mission_create_command');
      assert.equal(
        resolveMissionLifecycleIntent(utterance).intent,
        MissionLifecycleIntent.CREATE_NEW
      );
    });
  });

  describe('analyzeOperatorIntent — lifecycle intent sealed once', () => {
    it('seals CREATE_NEW lifecycle intent on explicit create utterance', async () => {
      const intent = await analyzeOperatorIntent({
        question:
          'Create a brand-new acquisition mission. Do not resume any existing mission.',
        session: { id: 's1', context: { tenantId: '10' } },
        resolveMission: false,
      });
      assert.equal(intent.missionLifecycleIntent, MissionLifecycleIntent.CREATE_NEW);
    });
  });

  describe('maybeHandleAcquisitionOwnershipTurn — lifecycle overrides dedup', () => {
    let amoEngine;

    beforeEach(() => {
      amoEngine = amo.createAcquisitionMissionEngine();
    });

    it('production regression — explicit create yields new missionId despite identical objective', async () => {
      const existing = amoEngine.create({
        tenantId: '10',
        objective: STR_OBJECTIVE,
        targetSegment: 'Commercial',
      });

      const utterance = [
        'Create a brand-new acquisition mission.',
        'Do not resume, reuse, or continue any existing mission.',
        `Objective: ${STR_OBJECTIVE}`,
      ].join(' ');

      const turn = await maybeHandleAcquisitionOwnershipTurn({
        question: utterance,
        context: { tenantId: '10' },
        acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
        persist: false,
        missionLifecycleIntent: MissionLifecycleIntent.CREATE_NEW,
      });

      assert.ok(turn);
      assert.equal(turn.created, true);
      assert.equal(turn.reason, 'acquisition_mission_created');
      assert.notEqual(turn.mission.id, existing.id);
      assert.match(turn.prose, /Mission Created/);
      assert.equal(amoEngine.list('10').length, 2);
    });

    it('UNSPECIFIED still resumes similar acquisition mission (duplicate prevention)', async () => {
      const existing = amoEngine.create({
        tenantId: '10',
        objective: 'Acquire one recurring commercial cleaning client.',
        targetSegment: 'Commercial',
      });

      const turn = await maybeHandleAcquisitionOwnershipTurn({
        question: 'Acquire one recurring commercial cleaning client',
        context: { tenantId: '10' },
        acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
        persist: false,
        missionLifecycleIntent: MissionLifecycleIntent.UNSPECIFIED,
      });

      assert.ok(turn);
      assert.equal(turn.created, false);
      assert.equal(turn.reason, 'acquisition_mission_resumed');
      assert.equal(turn.mission.id, existing.id);
      assert.equal(amoEngine.list('10').length, 1);
    });

    it('RESUME_EXISTING finds and resumes matching mission', async () => {
      const existing = amoEngine.create({
        tenantId: '10',
        objective: STR_OBJECTIVE,
        targetSegment: 'Commercial',
      });

      const turn = await maybeHandleAcquisitionOwnershipTurn({
        question: `Resume the existing mission for ${STR_OBJECTIVE}`,
        context: { tenantId: '10' },
        acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
        persist: false,
        missionLifecycleIntent: MissionLifecycleIntent.RESUME_EXISTING,
      });

      assert.ok(turn);
      assert.equal(turn.created, false);
      assert.equal(turn.mission.id, existing.id);
      assert.match(turn.prose, /Mission Resumed/);
    });
  });
});
