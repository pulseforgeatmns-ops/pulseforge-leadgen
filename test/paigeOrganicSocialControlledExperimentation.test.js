'use strict';

const { describe, test } = require('node:test');
const assert = require('node:assert/strict');
const {
  ANCHOR_CLIENT_ID,
  DEFAULT_BACKLOG_MIN,
  EXPLORATION_RATE,
  BACKLOG_APPROVAL_STATES,
  createMemoryOrganicSocialStore,
  maintainAnchorContentBacklog,
  planBacklogCandidate,
  computeExplorationBounds,
  rebalanceControlledExploration,
} = require('../packages/capabilities/paigeOrganicSocial');

describe('Paige organic social controlled experimentation', () => {
  test('maintains at least one exploration slot when backlog meets default minimum', async () => {
    for (let run = 0; run < 30; run += 1) {
      const store = createMemoryOrganicSocialStore();
      await maintainAnchorContentBacklog({ clientId: ANCHOR_CLIENT_ID }, { store });
      const backlog = await store.listBacklog(ANCHOR_CLIENT_ID);
      assert.ok(backlog.length >= DEFAULT_BACKLOG_MIN);
      const explorers = backlog.filter((b) => b.exploration);
      const { min, max } = computeExplorationBounds(backlog.length);
      assert.ok(explorers.length >= min, `run ${run}: expected min ${min}, got ${explorers.length}`);
      assert.ok(explorers.length <= max, `run ${run}: expected max ${max}, got ${explorers.length}`);
      assert.ok(
        backlog.every((b) => b.approvalState === BACKLOG_APPROVAL_STATES.DRAFT),
        'approval boundary stays draft until operator review'
      );
    }
  });

  test('rebalance fails closed on unbounded explicit exploration flags', async () => {
    const store = createMemoryOrganicSocialStore();
    for (let i = 0; i < DEFAULT_BACKLOG_MIN; i += 1) {
      const candidate = await planBacklogCandidate({
        clientId: ANCHOR_CLIENT_ID,
        seed: `${ANCHOR_CLIENT_ID}:forced-explore:${i}`,
        exploration: true,
      }, { store });
      await store.insertBacklogItem(candidate);
    }
    const before = await store.listBacklog(ANCHOR_CLIENT_ID);
    assert.equal(before.filter((b) => b.exploration).length, DEFAULT_BACKLOG_MIN);

    await rebalanceControlledExploration(before, ANCHOR_CLIENT_ID, { store });
    const after = await store.listBacklog(ANCHOR_CLIENT_ID);
    const { max } = computeExplorationBounds(after.length);
    assert.equal(after.filter((b) => b.exploration).length, max);
    assert.match(
      after.find((b) => b.exploration).planningRationale,
      /controlled experimentation/i
    );
  });

  test('deterministic exploration selection is stable for a fixed seed', async () => {
    const store = createMemoryOrganicSocialStore();
    const first = await planBacklogCandidate({
      clientId: ANCHOR_CLIENT_ID,
      seed: `${ANCHOR_CLIENT_ID}:stable-seed`,
    }, { store });
    const second = await planBacklogCandidate({
      clientId: ANCHOR_CLIENT_ID,
      seed: `${ANCHOR_CLIENT_ID}:stable-seed`,
    }, { store });
    assert.equal(first.exploration, second.exploration);
    assert.notEqual(typeof first.exploration, 'undefined');
  });

  test('small backlogs do not force exploration when below default minimum', async () => {
    const store = createMemoryOrganicSocialStore();
    const candidate = await planBacklogCandidate({
      clientId: ANCHOR_CLIENT_ID,
      seed: `${ANCHOR_CLIENT_ID}:solo`,
      exploration: false,
    }, { store });
    await store.insertBacklogItem(candidate);
    const items = await store.listBacklog(ANCHOR_CLIENT_ID);
    const { min } = computeExplorationBounds(items.length);
    assert.equal(min, 0);
    await rebalanceControlledExploration(items, ANCHOR_CLIENT_ID, { store });
    const after = await store.listBacklog(ANCHOR_CLIENT_ID);
    assert.equal(after.filter((b) => b.exploration).length, 0);
  });
});
