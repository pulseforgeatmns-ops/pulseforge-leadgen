'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  SnapshotEngine,
  InMemorySnapshotRepository,
  SerializingSnapshotRepository,
} = require('..');
const { snapshot } = require('./helpers');

function fakeEvaluation(snap) {
  return {
    recommendation: snap.recommendation,
    explanation: { recommendationId: snap.recommendation.id },
    report: {
      strategyResults: snap.strategyResults,
      context: { builtAt: snap.timestamp },
      normalizedScores: {},
    },
  };
}

describe('Snapshot generation + replay', () => {
  it('captures append-only deterministic snapshots', async () => {
    const repo = new InMemorySnapshotRepository();
    const engine = new SnapshotEngine({ repository: repo });
    const base = snapshot({
      timestamp: '2026-07-20T12:00:00.000Z',
      score: 71,
      confidence: 40,
      claims: ['c1'],
      evidence: ['e1'],
    });

    const a = await engine.capture({
      tenantId: base.tenantId,
      companyId: base.companyId,
      timestamp: base.timestamp,
      evaluation: fakeEvaluation(base),
    });

    assert.equal(a.score, 71);
    assert.equal(a.confidence, 40);
    assert.deepEqual(a.claims, ['c1']);
    assert.ok(a.id.startsWith('snap:'));

    assert.throws(() => repo.append(a), /append-only|already exists/);

    const replay = await engine.replay(base.tenantId, base.companyId);
    assert.equal(replay.length, 1);
    assert.equal(replay[0].id, a.id);
  });

  it('replay returns chronological order', async () => {
    const repo = new InMemorySnapshotRepository();
    const engine = new SnapshotEngine({ repository: repo });
    const times = [
      '2026-07-20T12:00:00.000Z',
      '2026-07-22T12:00:00.000Z',
      '2026-07-24T12:00:00.000Z',
    ];
    for (const timestamp of times) {
      const s = snapshot({ timestamp, score: 50 });
      await engine.capture({
        tenantId: s.tenantId,
        companyId: s.companyId,
        timestamp,
        evaluation: fakeEvaluation(s),
      });
    }
    const replay = await engine.replay('10', 'co-1');
    assert.deepEqual(
      replay.map((r) => r.timestamp),
      times
    );
  });

  it('snapshots are immutable copies on read', async () => {
    const repo = new InMemorySnapshotRepository();
    const engine = new SnapshotEngine({ repository: repo });
    const s = snapshot({ score: 60, claims: ['c1'] });
    const captured = await engine.capture({
      tenantId: s.tenantId,
      companyId: s.companyId,
      timestamp: s.timestamp,
      evaluation: fakeEvaluation(s),
    });
    captured.score = 999;
    captured.claims.push('mutated');
    const again = await repo.getById(s.tenantId, captured.id);
    assert.equal(again.score, 60);
    assert.deepEqual(again.claims, ['c1']);
  });
});

describe('Repository parity', () => {
  it('InMemory and Serializing repositories agree on append/list/latest', async () => {
    const a = new InMemorySnapshotRepository();
    const b = new SerializingSnapshotRepository();
    const engineA = new SnapshotEngine({ repository: a });
    const engineB = new SnapshotEngine({ repository: b });

    for (const [i, timestamp] of [
      '2026-07-20T12:00:00.000Z',
      '2026-07-22T12:00:00.000Z',
    ].entries()) {
      const s = snapshot({
        timestamp,
        score: 70 + i * 11,
        confidence: 40 + i * 5,
        claims: i === 0 ? ['c1'] : ['c1', 'c2'],
        evidence: i === 0 ? ['e1'] : ['e1', 'e2'],
      });
      const evaln = fakeEvaluation(s);
      const ca = await engineA.capture({
        tenantId: s.tenantId,
        companyId: s.companyId,
        timestamp,
        evaluation: evaln,
      });
      // Reset seq on B by using a fresh engine... actually both engines have separate seq.
      // Capture on B with same evaluation — ids may differ by seq pad if engines diverge.
      // For parity of *content*, compare score/claims via list, not ids from different engines.
      await engineB.capture({
        tenantId: s.tenantId,
        companyId: s.companyId,
        timestamp,
        evaluation: evaln,
      });
      void ca;
    }

    const listA = await a.listByCompany('10', 'co-1');
    const listB = await b.listByCompany('10', 'co-1');
    assert.equal(listA.length, listB.length);
    for (let i = 0; i < listA.length; i += 1) {
      assert.equal(listA[i].score, listB[i].score);
      assert.equal(listA[i].confidence, listB[i].confidence);
      assert.deepEqual(listA[i].claims, listB[i].claims);
      assert.deepEqual(listA[i].evidence, listB[i].evidence);
      assert.equal(listA[i].timestamp, listB[i].timestamp);
    }
    assert.equal((await a.latest('10', 'co-1')).score, 81);
    assert.equal((await b.latest('10', 'co-1')).score, 81);
  });
});
