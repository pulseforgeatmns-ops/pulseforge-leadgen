'use strict';

/**
 * SPEC-212 — CAPACITY persistence must keep Paige-derived message bindings.
 * Regression: sanitizeQueueItem used to drop candidateId / bindingScope /
 * attributableIntelligence, so EXECUTE fail-closed after a real PREPARE.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  MESSAGE_BINDING_SCOPES,
  BINDING_VALIDATION_RESULTS,
  validateProspectMessageBindings,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advanceAcquisitionApproach,
  advancePaigeVariants,
  advanceEmmettCapacity,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  sanitizeQueueItem,
  sanitizePaigeBinding,
} = require('../../max/workspace/EmmettCapacityExecution');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function durableReload(payload) {
  return JSON.parse(JSON.stringify(payload));
}

function findLatest(contributions, specialist, kind) {
  return [...contributions]
    .reverse()
    .find((row) => row.specialist === specialist && row.kind === kind) || null;
}

describe('SPEC-212 — CAPACITY persistence preserves message bindings', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  async function throughPersistedCapacity() {
    await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    await advanceDiscoveryAfterApproval({
      engine, mission, tenantId: '10', question: 'Approved.', allowFixtureFallback: true,
    });
    await advancePrioritizationAfterApproval({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', question: 'Approved.',
    });
    await advanceMaxPrioritization({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
    });
    await advanceAcquisitionApproach({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
    });
    await advancePaigeVariants({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
    });
    await advanceEmmettCapacity({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
    });
    return engine.inspect(mission.id, { tenantId: '10' });
  }

  it('sanitizeQueueItem keeps Paige SPEC-212 fields and projects safe copy', () => {
    const before = {
      prospectId: 'co-harbor',
      email: 'alex@harborlaw.com',
      sendable: true,
      subject: 'must-not-persist',
      contentSource: 'paige',
      paige: {
        author: 'paige',
        source: 'paige',
        ready: true,
        variantLabel: 'Primary - Harbor Law',
        subject: 'Harbor Law walkthrough',
        body: 'Alex, Harbor Law intake is up.',
        cta: 'Reply',
        candidateId: 'co-harbor',
        variantId: 'paige_v_co_harbor',
        bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
        attributableIntelligence: { companyName: 'Harbor Law', rationale: 'intake volume' },
      },
    };

    const after = sanitizeQueueItem(before);
    assert.equal(after.prospectId, 'co-harbor');
    assert.equal(after.subject, undefined);
    assert.equal(after.paige.candidateId, 'co-harbor');
    assert.equal(after.paige.bindingScope, MESSAGE_BINDING_SCOPES.PROSPECT);
    assert.deepEqual(after.paige.attributableIntelligence, {
      companyName: 'Harbor Law',
      rationale: 'intake volume',
    });
    assert.equal(after.paige.variantId, 'paige_v_co_harbor');
    assert.equal(after.paige.variantLabel, 'Primary - Harbor Law');
    assert.equal(after.paige.subject, 'Harbor Law walkthrough');
    assert.equal(after.paige.body, 'Alex, Harbor Law intake is up.');
    assert.equal(after.paige.cta, 'Reply');
    assert.equal(after.sendable, true);
    assert.equal(after.sendBlocker, null);
    assert.equal(sanitizePaigeBinding(before.paige).candidateId, 'co-harbor');
  });

  it('does not invent SPEC-212 fields when Paige never bound the item', () => {
    const after = sanitizeQueueItem({
      prospectId: 'unbound',
      paige: { author: 'paige', source: 'paige', ready: false },
    });
    assert.equal(after.paige.candidateId, undefined);
    assert.equal(after.paige.bindingScope, undefined);
    assert.equal(after.paige.attributableIntelligence, undefined);
  });

  it('Paige VARIANTS → persisted CAPACITY reload passes SPEC-212 without re-inject', async () => {
    const snapshot = await throughPersistedCapacity();
    assert.equal(snapshot.mission.stage, STAGES.READY);

    const paige = findLatest(snapshot.contributions, SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS);
    const emmett = findLatest(snapshot.contributions, SPECIALISTS.EMMETT, CONTRIBUTION_KINDS.CAPACITY);
    assert.ok(paige?.payload?.variants?.length, 'Paige VARIANTS required');
    assert.ok(emmett?.payload, 'Emmett CAPACITY required');

    const reloaded = durableReload(emmett.payload);
    const items = reloaded.queue?.items || [];
    assert.ok(items.length >= 1, 'CAPACITY queue must be non-empty');

    for (const item of items) {
      assert.equal(item.subject, undefined);
      assert.equal(item.body, undefined);
      assert.equal(item.cta, undefined);
      assert.ok(item.paige?.subject, 'persisted paige.subject');
      assert.ok(item.paige?.body, 'persisted paige.body');
      assert.ok(item.paige?.cta, 'persisted paige.cta');
      assert.ok(item.paige?.candidateId, 'persisted paige.candidateId');
      assert.equal(item.paige.bindingScope, MESSAGE_BINDING_SCOPES.PROSPECT);
      assert.ok(item.paige.attributableIntelligence, 'persisted attributableIntelligence');
      const itemId = String(item.id || item.candidateId || item.prospectId || item.companyId);
      assert.equal(itemId, String(item.paige.candidateId));
      const variant = paige.payload.variants.find(
        (row) => String(row.candidateId) === String(item.paige.candidateId)
      );
      assert.ok(variant, 'queue item binds a real Paige variant');
      assert.deepEqual(
        item.paige.attributableIntelligence,
        variant.attributableIntelligence
      );
    }

    const validation = validateProspectMessageBindings(reloaded);
    assert.equal(validation.valid, true);
    assert.equal(validation.result, BINDING_VALIDATION_RESULTS.VALID);
    assert.equal(validation.violations.length, 0);
  });
});
