'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { DiffEngine, ChangeDetector, CHANGE_TYPES } = require('..');
const { snapshot, strat } = require('./helpers');

describe('Diff correctness', () => {
  it('computes score/confidence deltas and claim/evidence sets', () => {
    const from = snapshot({
      id: 'snap-a',
      timestamp: '2026-07-20T12:00:00.000Z',
      score: 71,
      confidence: 40,
      claims: ['c1'],
      evidence: ['e1'],
      strategyResults: [strat('opportunity', 20, 40)],
    });
    const to = snapshot({
      id: 'snap-b',
      timestamp: '2026-07-22T12:00:00.000Z',
      score: 82,
      confidence: 55,
      claims: ['c1', 'c2'],
      evidence: ['e1', 'e2'],
      strategyResults: [
        strat('opportunity', 40, 60, {
          supportingEvidence: [
            { id: 'e2', kind: 'evidence', summary: 'Hiring Operations Manager', sourceId: null, sourceType: null, confidence: 0.9 },
          ],
        }),
      ],
      priority: 'critical',
    });

    const diff = new DiffEngine().diff(from, to);
    assert.equal(diff.scoreDelta, 11);
    assert.equal(diff.confidenceDelta, 15);
    assert.deepEqual(diff.newClaims, ['c2']);
    assert.deepEqual(diff.removedClaims, []);
    assert.deepEqual(diff.newEvidence, ['e2']);
    assert.equal(diff.recommendation.priorityChanged, true);
    assert.ok(diff.strategyChanges.some((s) => s.strategy === 'opportunity' && s.scoreDeltaChange === 20));
  });

  it('diffs are deterministic', () => {
    const from = snapshot({ score: 50, claims: ['b', 'a'] });
    const to = snapshot({
      id: 'snap-2',
      timestamp: '2026-07-21T12:00:00.000Z',
      score: 60,
      claims: ['a', 'c'],
    });
    const engine = new DiffEngine();
    const d1 = engine.diff(from, to);
    const d2 = engine.diff(from, to);
    assert.equal(engine.fingerprint(d1), engine.fingerprint(d2));
    assert.deepEqual(d1.newClaims, ['c']);
    assert.deepEqual(d1.removedClaims, ['b']);
  });
});

describe('Change detection', () => {
  it('detects meaningful score, opportunity, hiring, and contradiction changes', () => {
    const from = snapshot({
      score: 71,
      confidence: 60,
      claims: ['c1'],
      evidence: ['e1'],
      strategyResults: [
        strat('opportunity', 10, 40),
        strat('risk', -5, 40, {
          contradictingEvidence: [
            { id: 'x1', kind: 'evidence', summary: 'old risk', sourceId: null, sourceType: null, confidence: 0.5 },
          ],
        }),
      ],
    });
    const to = snapshot({
      id: 'snap-b',
      timestamp: '2026-07-22T12:00:00.000Z',
      score: 82,
      confidence: 45,
      claims: ['c1', 'c2'],
      evidence: ['e1', 'e2'],
      strategyResults: [
        strat('opportunity', 35, 55, {
          supportingEvidence: [
            {
              id: 'e2',
              kind: 'evidence',
              summary: 'New Operations Manager hiring',
              sourceId: null,
              sourceType: null,
              confidence: 0.88,
            },
          ],
        }),
        strat('risk', -5, 40, {
          contradictingEvidence: [
            { id: 'x1', kind: 'evidence', summary: 'old risk', sourceId: null, sourceType: null, confidence: 0.5 },
            { id: 'x2', kind: 'evidence', summary: 'existing vendor contract', sourceId: null, sourceType: null, confidence: 0.7 },
          ],
        }),
      ],
    });

    const diff = new DiffEngine().diff(from, to);
    const changes = new ChangeDetector().detect(diff, from, to);
    const types = changes.map((c) => c.type);

    assert.ok(types.includes(CHANGE_TYPES.SCORE_INCREASED));
    assert.ok(types.includes(CHANGE_TYPES.CONFIDENCE_DECREASED));
    assert.ok(types.includes(CHANGE_TYPES.NEW_CLAIM));
    assert.ok(types.includes(CHANGE_TYPES.NEW_EVIDENCE));
    assert.ok(types.includes(CHANGE_TYPES.NEW_OPPORTUNITY_SIGNAL));
    assert.ok(types.includes(CHANGE_TYPES.NEW_HIRING_SIGNAL));
    assert.ok(types.includes(CHANGE_TYPES.NEW_CONTRADICTION));
  });
});
