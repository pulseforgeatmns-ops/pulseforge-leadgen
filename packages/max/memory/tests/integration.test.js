'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { createMaxReasoningRuntime } = require('../..');
const { seedReasoningFixture, AS_OF } = require('../../tests/fixtures');
const { NODE_TYPES } = require('../../../knowledge');

describe('Memory + ReasoningEngine integration', () => {
  it('remember() captures live evaluations and detects motion after graph update', async () => {
    const fixture = await seedReasoningFixture();
    const max = createMaxReasoningRuntime({ knowledge: fixture.knowledge });

    const first = await max.remember({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
      timestamp: '2026-07-20T12:00:00.000Z',
    });
    assert.ok(first.snapshot);
    assert.equal(first.previous, null);
    assert.ok(first.diff.isInitial);

    // Add new hiring claim/evidence → score motion
    const evidence = await fixture.knowledge.evidence.createEvidence({
      tenantId: fixture.tenantId,
      sourceType: 'website',
      sourceId: 'https://lodgism.com/careers-ops',
      summary: 'Hiring new Operations Manager; overflow confidence increased; website updated',
      confidence: 0.92,
    });
    await fixture.knowledge.evidence.attachEvidence(
      fixture.tenantId,
      evidence.id,
      fixture.company.id
    );
    await fixture.knowledge.claims.createClaim({
      tenantId: fixture.tenantId,
      statement: 'New Operations Manager hiring; overflow confidence increased',
      subjectId: fixture.company.id,
      evidenceIds: [evidence.id],
    });

    const second = await max.remember({
      tenantId: fixture.tenantId,
      companyId: fixture.company.id,
      asOf: AS_OF,
      timestamp: '2026-07-22T12:00:00.000Z',
    });

    assert.ok(second.previous);
    assert.equal(typeof second.diff.scoreDelta, 'number');
    assert.ok(second.changes.length >= 1);
    assert.ok(second.temporalExplanation.chain.why);
    assert.ok(second.evolution.history.length === 2);

    const history = await max.memory.scoreHistory(fixture.tenantId, fixture.company.id);
    assert.equal(history.length, 2);
    void NODE_TYPES;
  });
});
