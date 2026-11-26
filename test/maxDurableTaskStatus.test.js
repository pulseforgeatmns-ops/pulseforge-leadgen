'use strict';

/** MAX-P0-004 P6 — task-aware status presentation is honest (pure, no I/O). */
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { presentDurableTaskStatus, applyDurableTaskStatus } = require('../packages/max/workspace/DurableTaskStatus');

const STATES = ['requested', 'queued', 'claimed', 'running', 'awaiting_verification', 'completed', 'failed',
  'cancelled', 'recovery_required', 'blocked_dependency', 'escalated'];

test('only a completed (evidence-verified) task is reported as done', () => {
  for (const state of STATES) {
    const status = presentDurableTaskStatus({ id: 't1', state });
    assert.equal(status.done, state === 'completed', state);
    assert.equal(status.verified, state === 'completed', state);
    if (state !== 'completed') assert.doesNotMatch(status.sentence, /\bdone and verified\b/, state);
  }
});

test('in-flight and unverified states say they are not done', () => {
  for (const state of ['requested', 'queued', 'claimed', 'running']) {
    assert.match(presentDurableTaskStatus({ state }).sentence, /Not done yet/);
  }
  assert.match(presentDurableTaskStatus({ state: 'awaiting_verification' }).sentence, /not verified/);
});

test('escalation and interruption name the reason and require the operator', () => {
  const unresolved = presentDurableTaskStatus({ state: 'escalated', escalationReason: 'external_action_unresolved' });
  assert.equal(unresolved.needsOperator, true);
  assert.match(unresolved.sentence, /outcome must be reconciled/);
  assert.match(unresolved.sentence, /not be retried automatically/);
  assert.match(presentDurableTaskStatus({ state: 'escalated', escalationReason: 'cancelled_after_dispatch' }).sentence, /cancelled after an external action/);
  const interrupted = presentDurableTaskStatus({ state: 'recovery_required', recoveryReason: 'lease_expired' });
  assert.equal(interrupted.needsOperator, true);
  assert.match(interrupted.sentence, /until an operator revalidates/);
});

test('an unknown state is never reported as success', () => {
  const status = presentDurableTaskStatus({ state: 'something_new' });
  assert.equal(status.done, false);
  assert.equal(status.needsOperator, true);
  assert.match(status.sentence, /unknown; nothing is confirmed/);
  assert.equal(presentDurableTaskStatus(null), null);
});

test('applying status to a turn adds a visible notice unless the task is verifiably done', () => {
  const pending = applyDurableTaskStatus({ prose: 'Plan updated.', structured: { answer: 'Plan updated.' } }, { id: 't', state: 'awaiting_verification' });
  assert.match(pending.prose, /not verified/);
  assert.match(pending.structured.answer, /not verified/);
  assert.equal(pending.structured.metadata.durableTask.state, 'awaiting_verification');
  const done = applyDurableTaskStatus({ prose: 'Plan updated.', structured: { answer: 'Plan updated.' } }, { id: 't', state: 'completed' });
  assert.equal(done.prose, 'Plan updated.');
  assert.equal(done.structured.metadata.durableTask.done, true);
  const untouched = { prose: 'x', structured: {} };
  assert.equal(applyDurableTaskStatus(untouched, null), untouched, 'no task: response unchanged (default-off path)');
});
