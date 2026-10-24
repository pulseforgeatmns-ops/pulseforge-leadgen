'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { readBrief, validateBrief, doctor, REA_VERSION } = require('../../../scripts/signalReaLab');

test('Signal REA investigation brief is bounded and fails closed', () => {
  const brief = validateBrief(readBrief());
  assert.equal(brief.deadline, '2026-10-14');
  assert.equal(brief.status, 'COMPLETE_PUBLIC_WEB');
  assert.equal(brief.constraints.allowTrade, false);
  assert.equal(brief.constraints.allowWalletConnection, false);
  assert.equal(brief.constraints.allowWalletSignature, false);
  assert.equal(brief.constraints.feedProspectiveResearch, false);
  assert.deepEqual(brief.questions.map(question => question.id), ['Q1', 'Q2', 'Q3']);
});

test('Signal REA investigation rejects an execution-enabled brief', () => {
  const brief = structuredClone(readBrief());
  brief.constraints.allowTrade = true;
  assert.throws(() => validateBrief(brief), /unsafe_brief:allowTrade_must_be_false/);
});

test('REA doctor is pinned and reports scoped Codex readiness', () => {
  let observed;
  const fakeSpawn = (command, args) => {
    observed = { command, args };
    return {
      status: 0,
      stdout: JSON.stringify({
        healthy: true,
        identity: { cli_package_version: REA_VERSION, expected_skill_version: '37' },
        scope_checks: [{ name: 'node', detail: '24.19.0' }]
      }),
      stderr: ''
    };
  };
  const result = doctor(fakeSpawn);
  assert.equal(observed.command, 'npx');
  assert.ok(observed.args.includes(`rea-agents@${REA_VERSION}`));
  assert.deepEqual(observed.args.slice(-6), [
    'doctor', '--client', 'codex', '--skill', '--format', 'json'
  ]);
  assert.equal(result.healthy, true);
  assert.equal(result.reaVersion, REA_VERSION);
});
