'use strict';

/** MAX-P0-002 — release gate decision and test-ratchet parser. Hermetic; no network. */

const test = require('node:test');
const assert = require('node:assert/strict');
const { decide, DEFAULT_REQUIRED } = require('../scripts/ci/verifyReleaseCandidate');
const { failingTests } = require('../scripts/ci/testRatchet');

const SHA = 'a'.repeat(40);
const ok = (name, extra = {}) => ({ name, status: 'completed', conclusion: 'success', event: 'push', run_number: 1, ...extra });

test('allows the branch tip when every required workflow succeeded', () => {
  const v = decide({ sha: SHA, branchTip: SHA, runs: [ok(DEFAULT_REQUIRED[0]), ok('Identity workflow safety')] });
  assert.equal(v.allowed, true, v.reasons.join(','));
});

test('refuses a commit that is not the branch tip', () => {
  const v = decide({ sha: SHA, branchTip: 'b'.repeat(40), runs: [ok(DEFAULT_REQUIRED[0])] });
  assert.equal(v.allowed, false);
  assert.ok(v.reasons.some(r => r.startsWith('not_branch_tip')));
});

test('refuses when any workflow for the commit failed, even a non-required one', () => {
  const v = decide({ sha: SHA, branchTip: SHA, runs: [ok(DEFAULT_REQUIRED[0]), ok('PR CI (SPEC-CI-LEAN-001)', { conclusion: 'failure' })] });
  assert.equal(v.allowed, false);
  assert.ok(v.reasons.includes('failed:PR CI (SPEC-CI-LEAN-001):failure'));
});

test('refuses when the required workflow is missing, running or cancelled', () => {
  assert.ok(decide({ sha: SHA, branchTip: SHA, runs: [] }).reasons.some(r => r.startsWith('required_missing')));
  assert.ok(decide({ sha: SHA, branchTip: SHA, runs: [ok(DEFAULT_REQUIRED[0], { status: 'in_progress', conclusion: null })] })
    .reasons.some(r => r.startsWith('still_running')));
  assert.equal(decide({ sha: SHA, branchTip: SHA, runs: [ok(DEFAULT_REQUIRED[0], { conclusion: 'cancelled' })] }).allowed, false);
});

test('a later successful re-run of the same workflow supersedes an earlier failure', () => {
  const v = decide({ sha: SHA, branchTip: SHA, runs: [
    ok(DEFAULT_REQUIRED[0], { conclusion: 'failure', run_number: 1 }),
    ok(DEFAULT_REQUIRED[0], { run_number: 2 }),
  ] });
  assert.equal(v.allowed, true, v.reasons.join(','));
});

test('refuses a short or malformed SHA', () => {
  assert.equal(decide({ sha: 'abc123', branchTip: 'abc123', runs: [ok(DEFAULT_REQUIRED[0])] }).allowed, false);
});

test('ratchet parser keys nested failures and ignores TODO', () => {
  const tap = [
    '# Subtest: suite A',
    '    # Subtest: inner case',
    '    not ok 1 - inner case',
    'not ok 1 - suite A',
    '# Subtest: pending',
    'not ok 2 - pending # TODO later',
    '# Subtest: passes',
    'ok 3 - passes',
  ].join('\n');
  assert.deepEqual(failingTests(tap), ['suite A > inner case', 'suite A']);
});
