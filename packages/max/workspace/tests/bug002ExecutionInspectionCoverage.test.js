'use strict';

/**
 * BUG-002 — Execution Inspection Coverage acceptance tests.
 *
 * Runtime guarantee: semantically equivalent pause/status/next/state questions
 * route through ExecutionInspectionRegistry, not generic acknowledgement.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  EXECUTION_INSPECTION_INTENTS,
  isExecutionInspectionQuestion,
  isExecutionExplanationQuestion,
  detectInspectionMode,
  matchExecutionInspectionIntent,
} = require('../ExecutionInspectionRegistry');
const { inspectExecutionState } = require('../ExecutionInspectionOperator');
const {
  createExecutionState,
  recordExecutionPaused,
  EXECUTION_STATUSES,
} = require('../ExecutionState');

const BUG002_PAUSE_FIXTURES = [
  'Why do you require mission plan approval?',
  'Why do you require mission plan approval before continuing autonomous execution?',
  "Why haven't you continued autonomous execution?",
  'What approval are you waiting for?',
  'Why are you blocked?',
  "Why can't you proceed?",
];

const BUG002_REGRESSION_FIXTURES = ['Why did you stop?'];

describe('BUG-002 — Execution Inspection Coverage', () => {
  it('registry defines pause, status, next-step, and full-state intents', () => {
    const ids = new Set(EXECUTION_INSPECTION_INTENTS.map((intent) => intent.id));
    assert.ok(ids.has('execution_pause_inspection'));
    assert.ok(ids.has('execution_status_inspection'));
    assert.ok(ids.has('execution_next_step_inspection'));
    assert.ok(ids.has('execution_state_inspection'));
    assert.ok(
      EXECUTION_INSPECTION_INTENTS.every((intent) => intent.id && intent.mode && intent.aliases?.length)
    );
  });

  describe('acceptance — pause-related phrasing routes to execution inspection', () => {
    for (const [index, question] of BUG002_PAUSE_FIXTURES.entries()) {
      it(`Test ${index + 1} — "${question}"`, () => {
        assert.equal(isExecutionInspectionQuestion(question), true, 'should detect inspection question');
        assert.equal(isExecutionExplanationQuestion(question), true, 'should detect pause explanation');
        assert.equal(detectInspectionMode(question), 'pause_explanation');
        assert.equal(matchExecutionInspectionIntent(question)?.id, 'execution_pause_inspection');
      });
    }
  });

  it('Test 6 (regression) — Why did you stop? still routes correctly', () => {
    for (const question of BUG002_REGRESSION_FIXTURES) {
      assert.equal(isExecutionInspectionQuestion(question), true);
      assert.equal(detectInspectionMode(question), 'pause_explanation');
    }
  });

  it('pause inspection reads Execution State — no generic acknowledgement', () => {
    let state = createExecutionState({ plan: { steps: [] } });
    state = recordExecutionPaused(state, {
      pauseReason:
        'Mission plan approval is currently required because the operator has not approved the proposed mission plan.',
      blockingContract: 'SPEC-118 Mission Plan Approval',
      nextStep: 'Approve the mission plan to continue autonomous execution.',
    });

    const question =
      'Why do you require mission plan approval before continuing autonomous execution?';
    const result = inspectExecutionState({ question, executionState: state });

    assert.notEqual(result.prose.trim(), 'Acknowledged.');
    assert.match(result.prose, /Execution Status:\s*paused/i);
    assert.match(result.prose, /Pause Reason:/i);
    assert.match(result.prose, /Mission plan approval is currently required/i);
    assert.match(result.prose, /Blocking Contract:/i);
    assert.match(result.prose, /SPEC-118 Mission Plan Approval/i);
    assert.match(result.prose, /Next Step:/i);
    assert.equal(state.status, EXECUTION_STATUSES.PAUSED);
  });

  it('status and next-step aliases route to correct inspection modes', () => {
    assert.equal(detectInspectionMode('What are you working on?'), 'current_activity');
    assert.equal(detectInspectionMode('What is your next step?'), 'next_step');
    assert.equal(detectInspectionMode('Show planner state.'), 'full_state');
    assert.equal(detectInspectionMode('Why are you waiting?'), 'pause_explanation');
  });
});
