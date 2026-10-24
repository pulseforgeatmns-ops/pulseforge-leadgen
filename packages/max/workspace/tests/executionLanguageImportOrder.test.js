'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

// A fresh process is essential: a populated require cache hides the unfinished
// CommonJS exports that previously broke one of these modules in either order.
for (const first of ['ExecutionDomain', 'ExecutionLanguageDetection']) {
  test(`execution routing is safe when ${first} loads first`, () => {
    const script = `
      const assert = require('node:assert/strict');
      require('./${first}');
      const { selectExecutionDomain, EXECUTION_DOMAINS } = require('./ExecutionDomain');
      const { isMissionExecutionCommand } = require('./ExecutionLanguageDetection');

      assert.equal(isMissionExecutionCommand(''), false);
      assert.equal(isMissionExecutionCommand('approved'), true);
      assert.equal(isMissionExecutionCommand('Do not resume the old mission.'), false);
      assert.equal(isMissionExecutionCommand('does Paige have pending posts for me to approve?'), false);

      const diagnostic = 'Run an end-to-end execution audit for Campaign 001.';
      assert.equal(selectExecutionDomain(diagnostic).domain, EXECUTION_DOMAINS.MISSION_DIAGNOSTICS);
      assert.equal(isMissionExecutionCommand(diagnostic), false);

      const lock = { activeMissionLock: true, explicitMissionExit: false };
      const greeting = selectExecutionDomain('Hello', lock);
      assert.equal(greeting.domain, EXECUTION_DOMAINS.WORKSPACE);
      assert.equal(greeting.reason, 'active_mission_lock');
      assert.equal(greeting.blockedDomain, EXECUTION_DOMAINS.GENERAL_CONVERSATION);

      const briefing = selectExecutionDomain("What's in today's briefing?", lock);
      assert.equal(briefing.domain, EXECUTION_DOMAINS.WORKSPACE);
      assert.equal(briefing.reason, 'active_mission_lock');
      assert.equal(briefing.blockedDomain, EXECUTION_DOMAINS.MORNING_BRIEFING);
      const exit = selectExecutionDomain("today's briefing", { ...lock, explicitMissionExit: true });
      assert.equal(exit.domain, EXECUTION_DOMAINS.MORNING_BRIEFING);

      // The locked fallback may call language detection, whose own domain
      // classification must stay unlocked and terminate without losing approval.
      const approval = selectExecutionDomain('approved', lock);
      assert.equal(approval.domain, EXECUTION_DOMAINS.WORKSPACE);
      assert.equal(approval.reason, 'mission_execution_command');
    `;
    const result = spawnSync(process.execPath, ['-e', script], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      timeout: 10000,
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
    assert.doesNotMatch(result.stderr, /circular dependency/i);
  });
}
