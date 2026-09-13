'use strict';

/**
 * Regression: production AMO Scout execution injected the mission store
 * into Scout.discover as the SPEC-143 intelligence memory store.
 *
 * Production shape (Anchor tenant 10):
 *   APPROVE_DISCOVERY → CER → TME → runScoutForAmoMission
 *   → buildScoutDiscoverOpts → Scout.discover → store.loadForMarket()
 *
 * The AMO store does not implement loadForMarket. That threw and rolled
 * back before any DISCOVERY contribution could persist.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../packages/acquisition-mission');
const {
  createAcquisitionMissionEngine,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
} = amo;
const {
  buildScoutDiscoverOpts,
  runScoutDiscovery,
} = require('../packages/max/workspace/ScoutDiscoveryExecutor');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
} = require('../packages/max/workspace/AmoOperatorApproval');
const { prepareInvestigationWithMemory } = require('../packages/scout/memory');

const TENANT_ID = '10';
const OBJECTIVE =
  'Acquire recurring commercial cleaning customers from law firms in Greater Manchester, NH.';

function productionCandidates() {
  return [
    {
      id: 'co-harbor',
      name: 'Harbor Law Group',
      industry: 'law_firm',
      location: 'Manchester, NH',
      website: 'https://harborlaw.example',
      icpScore: 84,
      signals: [
        {
          type: 'hiring',
          label: 'Hiring operations manager',
          source: 'job_board',
          observedAt: '2026-09-01T00:00:00.000Z',
        },
      ],
      evidence: [
        {
          label: 'Operations manager job posting',
          source: 'job_board',
          snapshot: { source: 'job_board', companyName: 'Harbor Law Group' },
        },
      ],
      people: [{ name: 'Alex Morgan', jobTitle: 'Office Manager' }],
    },
    {
      id: 'co-granite',
      name: 'Granite Legal Partners',
      industry: 'law_firm',
      location: 'Bedford, NH',
      website: 'https://granitelegal.example',
      icpScore: 78,
      signals: [
        {
          type: 'hiring',
          label: 'Hiring office coordinator',
          source: 'linkedin',
          observedAt: '2026-09-02T00:00:00.000Z',
        },
      ],
      evidence: [
        {
          label: 'LinkedIn hiring post',
          source: 'linkedin',
          snapshot: { source: 'linkedin', companyName: 'Granite Legal Partners' },
        },
      ],
      people: [{ name: 'Jordan Hale', jobTitle: 'Office Manager' }],
    },
  ];
}

function createProductionMission(engine, overrides = {}) {
  return engine.create({
    tenantId: TENANT_ID,
    clientId: 10,
    objective: OBJECTIVE,
    targetSegment: 'Law Firms',
    ...overrides,
  });
}

describe('AMO Scout production store contract', () => {
  it('AMO mission store does not implement SPEC-143 loadForMarket', () => {
    const engine = createAcquisitionMissionEngine();
    assert.equal(typeof engine.store.loadForMarket, 'undefined');
    assert.equal(typeof engine.store.putMission, 'function');
    assert.equal(typeof engine.store.listOutcomeLearnings, 'function');
  });

  it('buildScoutDiscoverOpts does not inject engine.store as Scout memory store', () => {
    const engine = createAcquisitionMissionEngine();
    const mission = createProductionMission(engine, { planApproved: true });

    const scoutOpts = buildScoutDiscoverOpts(mission, { store: engine.store }, { engine });

    assert.notStrictEqual(scoutOpts.memoryStore, engine.store);
    assert.notStrictEqual(scoutOpts.store, engine.store);
    const injected = scoutOpts.memoryStore || scoutOpts.store;
    if (injected) {
      assert.equal(typeof injected.loadForMarket, 'function');
    }
  });

  it('production injection shape can load market memory without loadForMarket throw', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = createProductionMission(engine, { planApproved: true });
    const scoutOpts = buildScoutDiscoverOpts(mission, {}, { engine });

    const memory = await prepareInvestigationWithMemory({
      tenantId: TENANT_ID,
      mission,
      marketDefinition: {
        geography: 'Greater Manchester NH',
        segment: 'law_firm',
      },
      opts: { store: scoutOpts.memoryStore || scoutOpts.store },
    });

    assert.equal(memory.memory.loaded, true);
    assert.equal(memory.memory.tenantId, TENANT_ID);
  });

  it('canonical AMO Scout.discover path does not throw store.loadForMarket is not a function', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = createProductionMission(engine, { planApproved: true });
    const candidates = productionCandidates();

    const executionResult = await runScoutDiscovery(
      { mission },
      {
        engine,
        discover: async () => candidates,
        companies: candidates,
        enablePlaces: false,
        allowFixtureFallback: false,
      }
    );

    assert.notEqual(executionResult.status, 'FAILED');
    assert.ok(executionResult.contributions);
  });

  it('APPROVE_DISCOVERY persists a Scout DISCOVERY contribution on the production path', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = createProductionMission(engine);
    const candidates = productionCandidates();

    const planResult = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: TENANT_ID,
      question: 'Approved.',
    });

    const discoveryResult = await advanceDiscoveryAfterApproval({
      engine,
      mission: planResult.snapshot.mission,
      tenantId: TENANT_ID,
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: false,
      discover: async () => candidates,
      scoutCompanies: candidates,
      enablePlaces: false,
    });

    assert.equal(discoveryResult.executionOutcome, 'completed');
    assert.ok(discoveryResult.discovery);
    assert.equal(discoveryResult.discovery.specialist, SPECIALISTS.SCOUT);
    assert.equal(discoveryResult.discovery.kind, CONTRIBUTION_KINDS.DISCOVERY);

    const snapshot = engine.inspect(mission.id, { tenantId: TENANT_ID });
    const persisted = snapshot.contributions.filter(
      (row) => row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
    );
    assert.equal(persisted.length, 1);
    assert.ok(persisted[0].payload);
  });
});
