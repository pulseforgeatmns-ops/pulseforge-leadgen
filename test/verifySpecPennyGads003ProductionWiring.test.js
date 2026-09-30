'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  DEPLOY_FLOOR_SHA,
  sameSha,
  compareStatusAtOrAfter,
} = require('../scripts/verifySpecPennyGads003ProductionWiring');

describe('SPEC-PENNY-GADS-003 deploy floor comparison', () => {
  it('treats identical and prefix SHAs as the same commit', () => {
    assert.equal(sameSha(DEPLOY_FLOOR_SHA, DEPLOY_FLOOR_SHA), true);
    assert.equal(sameSha(DEPLOY_FLOOR_SHA.slice(0, 7), DEPLOY_FLOOR_SHA), true);
    assert.equal(sameSha(null, DEPLOY_FLOOR_SHA), false);
  });

  it('uses GitHub compare ancestry, not lexicographic SHA order', () => {
    assert.equal(compareStatusAtOrAfter('ahead'), true);
    assert.equal(compareStatusAtOrAfter('identical'), true);
    assert.equal(compareStatusAtOrAfter('behind'), false);
    assert.equal(compareStatusAtOrAfter('diverged'), false);
    const laterMain = 'b8819b40a9f03a8102361f1cd22bd436d8587e81';
    assert.equal(laterMain.slice(0, 7) >= DEPLOY_FLOOR_SHA.slice(0, 7), false);
  });
});
