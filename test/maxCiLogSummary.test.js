'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { summarizeNodeTestLog, MAX_FAILURES, MAX_DETAILS, MAX_LINE } = require('../.github/scripts/summarize-node-test-log');

test('summarizes TAP failures and totals without noisy fixture output', () => {
  const summary = summarizeNodeTestLog([
    'TAP version 13', '# huge fixture JSON that must not dominate the summary',
    '# Subtest: preserves approvals', 'not ok 1 - preserves approvals', '  ---',
    "  location: 'test/example.test.js:12:3'", '  failureType: testCodeFailure',
    "  error: 'unexpected approval'", '  code: ERR_ASSERTION', '  name: AssertionError',
    '  stack: |-', '    TestContext.<anonymous> (test/example.test.js:14:5)', '  ...',
    '# tests 2', '# suites 0', '# pass 1', '# fail 1', '# skipped 0',
  ].join('\n'));
  assert.match(summary, /not ok 1 - preserves approvals/);
  assert.match(summary, /example\.test\.js:12:3/);
  assert.match(summary, /unexpected approval/);
  assert.match(summary, /TestContext/);
  assert.match(summary, /pass: 1/);
  assert.match(summary, /fail: 1/);
  assert.doesNotMatch(summary, /huge fixture JSON/);
});

test('supports local spec reporter failures without double-counting early output', () => {
  const summary = summarizeNodeTestLog('✖ approval test (1ms)\nℹ tests 1\nℹ pass 0\nℹ fail 1\n✖ failing tests:\n\ntest at test/a.test.js:4:1\n✖ approval test (1ms)\n  TypeError: missing function\n');
  assert.match(summary, /Failed entries: 1/);
  assert.match(summary, /TypeError: missing function/);
  assert.match(summary, /test at test\/a/);
});

test('keeps successful totals and reports skipped tests explicitly', () => {
  const summary = summarizeNodeTestLog('ok 1 - test\n# tests 3\n# pass 2\n# fail 0\n# skipped 1\n');
  assert.match(summary, /tests: 3/);
  assert.match(summary, /fail: 0/);
  assert.match(summary, /skipped: 1/);
  assert.doesNotMatch(summary, /Failed entries/);
});

test('bounds failure count, detail count and long lines', () => {
  const huge = 'x'.repeat(10000);
  const summary = summarizeNodeTestLog(Array.from({ length: MAX_FAILURES + 20 }, (_, i) =>
    `not ok ${i + 1} - ${huge}\n${Array(MAX_DETAILS + 10).fill(`  error: ${huge}`).join('\n')}`
  ).join('\n'));
  assert.match(summary, /20 more entries/);
  assert.ok(summary.length < (MAX_FAILURES * (MAX_DETAILS + 1) + 5) * (MAX_LINE + 10));
  assert.doesNotMatch(summary, /x{241}/);
});

test('shows a bounded prefixed tail for startup failure before TAP output', () => {
  const summary = summarizeNodeTestLog('::error::sample\nError: runner did not start\n');
  assert.match(summary, /No test totals found/);
  assert.match(summary, /runner did not start/);
  assert.doesNotMatch(summary, /^::/m);
});
