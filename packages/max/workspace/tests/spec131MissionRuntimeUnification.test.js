'use strict';

/**
 * SPEC-131 — Mission Runtime Unification.
 * One mission store for acquisition campaigns: AMO service only.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const {
  resetEngine,
  createMission,
  listMissions,
  toCommandDeckCard,
  isAcquisitionLegacyMissionType,
} = require('../../../../services/acquisitionMission');
const { maybeHandleAcquisitionOwnershipTurn } = require('../AcquisitionOwnership');
const { advanceDiscoveryAfterApproval, advancePlanAfterApproval } = require('../AmoOperatorApproval');
const { composeOperations } = require('../../commandDeck/sections/Operations');
const { ensureAmoTenantHydrated, clearAmoHydrationCache } = require('../AmoWorkspaceHydration');

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester for law firms.';

describe('SPEC-131 — Mission Runtime Unification', () => {
  beforeEach(() => {
    resetEngine();
    clearAmoHydrationCache();
  });

  it('createMission persists through the service facade (single store)', async () => {
    const mission = await createMission(
      {
        tenantId: '10',
        objective: ANCHOR_OBJECTIVE,
        targetSegment: 'Law Firms',
      },
      { persist: false }
    );

    assert.ok(mission.id);
    const listed = await listMissions('10', { persist: false });
    assert.equal(listed.length, 1);
    assert.equal(listed[0].id, mission.id);
    assert.equal(listed[0].objective, ANCHOR_OBJECTIVE);
  });

  it('AcquisitionOwnership uses createMission instead of engine.create', async () => {
    const turn = await maybeHandleAcquisitionOwnershipTurn({
      question: ANCHOR_OBJECTIVE,
      context: { tenantId: '10' },
      persist: false,
    });

    assert.ok(turn);
    assert.equal(turn.created, true);
    assert.ok(turn.mission.id);

    const listed = await listMissions('10', { persist: false });
    assert.equal(listed.length, 1);
    assert.equal(listed[0].id, turn.mission.id);
  });

  it('toCommandDeckCard maps AMO missions for Operations queue', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const card = toCommandDeckCard(mission);
    assert.equal(card.runtime, 'AMO');
    assert.equal(card.type, 'acquisition');
    assert.equal(card.id, mission.id);
    assert.equal(card.progress.currentStage, 'discover');
    assert.ok(card.progress.percent >= 0);

    const { operations } = composeOperations({ missions: [card] });
    assert.equal(operations.missions.length, 1);
    assert.equal(operations.missions[0].runtime, 'AMO');
    assert.equal(operations.empty, false);
  });

  it('filters acquisition types from legacy mission engine cards', () => {
    assert.equal(isAcquisitionLegacyMissionType({ type: 'acquisition_search' }), true);
    assert.equal(isAcquisitionLegacyMissionType({ type: 'prospect_discovery' }), true);
    assert.equal(isAcquisitionLegacyMissionType({ type: 'campaign_creation' }), true);
    assert.equal(isAcquisitionLegacyMissionType({ type: 'weekly_brief' }), false);
  });

  it('AmoOperatorApproval does not create shadow legacy missions', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });

    let legacyCreateCount = 0;
    const legacyEngine = createMissionEngine({
      registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    });
    legacyEngine.createFromObjective = async (...args) => {
      legacyCreateCount += 1;
      return legacyEngine.store.create({
        objectiveText: args[0].objective,
        tenantId: args[0].tenantId,
        type: 'acquisition_search',
        status: 'executing',
      });
    };

    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      missionEngine: legacyEngine,
      allowFixtureFallback: true,
    });

    assert.equal(legacyCreateCount, 0);
  });

  it('hydration loads persisted AMO missions (missionsLoaded > 0)', async () => {
    const source = amo.createAcquisitionMissionEngine();
    source.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const runtimeEngine = amo.createAcquisitionMissionEngine();
    const service = {
      getEngine: () => runtimeEngine,
      hydrateTenant: async (tenantId) => {
        for (const row of source.list(tenantId)) {
          runtimeEngine.store.putMission(row);
        }
        return runtimeEngine;
      },
    };

    const session = { id: 's-spec131', context: { tenantId: '10' } };
    const result = await ensureAmoTenantHydrated({
      session,
      context: session.context,
      acquisitionMissionService: service,
    });

    assert.equal(result.hydrated, true);
    assert.ok(result.missionsLoaded > 0);
  });
});
