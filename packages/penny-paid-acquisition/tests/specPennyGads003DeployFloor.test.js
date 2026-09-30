'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const DEPLOY_FLOOR_SHA = 'd54b6a52976e9a3a4afa1ebf9522a5d0a988b770';

function localGitHeadAtOrAfterFloor(floorSha, headSha) {
  if (!floorSha || !headSha) return false;
  try {
    execFileSync('git', ['merge-base', '--is-ancestor', floorSha, headSha], {
      cwd: path.join(__dirname, '../../..'),
      stdio: 'ignore',
    });
    return true;
  } catch {
    return false;
  }
}

describe('SPEC-PENNY-GADS-003 — deploy floor SHA check', () => {
  it('uses git ancestry instead of lexicographic SHA ordering', () => {
    const head = 'b8819b40a9f03a8102361f1cd22bd436d8587e81';
    assert.equal(head.slice(0, 7) >= DEPLOY_FLOOR_SHA.slice(0, 7), false);
    assert.equal(localGitHeadAtOrAfterFloor(DEPLOY_FLOOR_SHA, head), true);
  });
});
