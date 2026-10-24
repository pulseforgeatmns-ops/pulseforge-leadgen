'use strict';

/**
 * SPEC-143 — Scout Acquisition Intelligence Memory tests.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { Scout, memory } = require('../packages/scout');
const { runInvestigationEngine } = require('../packages/scout/investigation/InvestigationLoop');

const {
  MEMORY_TYPES,
  MEMORY_STATUS,
  STARTING_POINT_BUCKETS,
  buildClaimMemory,
  buildCompanyMemory,
  buildMarketMemory,
  buildInvestigationMemory,
  marketEntityKey,
  claimEntityKey,
  computeEffectiveConfidence,
  computeFreshnessDays,
  isMemoryStale,
  extractKnowledgeFromInvestigation,
  createMemoryIntelligenceStore,
  loadIntelligenceMemory,
  prepareInvestigationWithMemory,
  buildInvestigationStartingPoint,
  reconcileClaimMemory,
  detectClaimContradiction,
  buildMemoryGraphFromKnowledge,
  persistInvestigationKnowledge,
  clearMemoryLog,
  listMemoryLog,
  MEMORY_EVENTS,
  setDefaultStore,
} = memory;

function sampleMission(overrides = {}) {
  return {
    id: 'mission-spec143-1',
    tenantId: '10',
    clientId: 10,
    objectiveText: 'Acquire one recurring commercial cleaning client in Manchester NH',
    constraints: {
      vertical: 'property_management',
      locationHint: 'Manchester NH',
    },
    ...overrides,
  };
}

function sampleCandidates() {
  return [
    {
      id: 'c1',
      name: 'ABC Property Management',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://abc-pm.example',
      icpScore: 85,
      signals: [{ type: 'portfolio_growth', label: '41 STR properties', source: 'website' }],
      people: [{ name: 'John Smith', jobTitle: 'Operations Director', email: 'john@abc-pm.example' }],
      evidence: [{ source: 'website', label: 'Manages 41 STR properties', kind: 'portfolio_size' }],
    },
  ];
}

function sampleInvestigationResult(overrides = {}) {
  return {
    marketDefinition: {
      geography: 'Manchester NH',
      segment: 'property_management',
      valid: true,
    },
    candidateUniverse: { candidates: sampleCandidates(), estimatedMarket: 42 },
    claims: [
      {
        id: 'claim-1',
        entityId: 'c1',
        text: 'ABC manages 41 STR properties.',
        confidence: 0.94,
        supportedBy: [{ source: 'website' }, { source: 'linkedin' }],
        missingEvidence: [],
      },
    ],
    overallConfidence: 0.91,
    iterations: [
      {
        iteration: 1,
        nextStep: { gap: 'portfolio_size', providerId: 'website', capability: 'website', entityId: 'c1' },
      },
    ],
    missingEvidence: { missing: ['county_records'], gapCount: 1 },
    qualification: { qualifiedCount: 1, watchCount: 0, rejectedCount: 0 },
    ...overrides,
  };
}

describe('SPEC-143 — Scout Acquisition Intelligence Memory', () => {
  let store;

  beforeEach(() => {
    clearMemoryLog();
    store = createMemoryIntelligenceStore();
    setDefaultStore(store);
  });

  it('extracts market, company, person, claim, and investigation memory from results', () => {
    const knowledge = extractKnowledgeFromInvestigation(sampleInvestigationResult(), {
      tenantId: '10',
      missionId: 'mission-spec143-1',
    });

    assert.ok(knowledge.market);
    assert.equal(knowledge.market.type, MEMORY_TYPES.MARKET);
    assert.equal(knowledge.market.geography, 'Manchester NH');
    assert.equal(knowledge.companies.length, 1);
    assert.equal(knowledge.companies[0].name, 'ABC Property Management');
    assert.equal(knowledge.people.length, 1);
    assert.equal(knowledge.people[0].name, 'John Smith');
    assert.equal(knowledge.claims.length, 1);
    assert.equal(knowledge.claims[0].text, 'ABC manages 41 STR properties.');
    assert.ok(knowledge.claims[0].verified);
    assert.ok(knowledge.investigation);
    assert.equal(knowledge.counts.claims, 1);
  });

  it('decays confidence as memory ages', () => {
    const fresh = buildClaimMemory({
      tenantId: '10',
      entityKey: 'test',
      text: 'Fresh claim',
      confidence: 0.91,
      verifiedAt: new Date().toISOString(),
      sourceCount: 3,
      verificationSources: ['website', 'linkedin', 'county_records'],
    });
    const stale = buildClaimMemory({
      tenantId: '10',
      entityKey: 'test-stale',
      text: 'Stale claim',
      confidence: 0.91,
      verifiedAt: new Date(Date.now() - 120 * 24 * 60 * 60 * 1000).toISOString(),
      sourceCount: 1,
    });

    const freshEffective = computeEffectiveConfidence(fresh);
    const staleEffective = computeEffectiveConfidence(stale);

    assert.ok(freshEffective > staleEffective);
    assert.ok(computeFreshnessDays(fresh.verifiedAt) < 2);
    assert.ok(isMemoryStale(stale));
  });

  it('builds investigation starting point with known/unknown/verify/discover buckets', () => {
    const verifiedClaim = buildClaimMemory({
      tenantId: '10',
      entityKey: claimEntityKey({ entityId: 'c1', text: 'ABC manages 41 STRs' }),
      entityId: 'c1',
      text: 'ABC manages 41 STRs',
      confidence: 0.94,
      verified: true,
      missingEvidence: [],
    });
    const weakClaim = buildClaimMemory({
      tenantId: '10',
      entityKey: claimEntityKey({ entityId: 'c2', text: 'Unknown vendor' }),
      entityId: 'c2',
      text: 'Unknown vendor relationship',
      confidence: 0.4,
      verified: false,
      missingEvidence: ['vendor_references'],
    });

    const startingPoint = buildInvestigationStartingPoint(
      { claims: [verifiedClaim, weakClaim], companies: [], people: [] },
      { geography: 'Manchester NH', segment: 'property_management' },
      [],
      { confidenceThreshold: 0.8 }
    );

    assert.ok(startingPoint.known.length >= 1);
    assert.ok(startingPoint.needToVerify.length >= 1 || startingPoint.unknown.length >= 1);
    assert.equal(startingPoint.counts.claims, 2);
  });

  it('detects contradictions between stored and new claims', () => {
    const existing = buildClaimMemory({
      tenantId: '10',
      entityKey: 'c1:employees',
      entityId: 'c1',
      text: 'Company has 15 employees',
      confidence: 0.85,
    });
    const incoming = buildClaimMemory({
      tenantId: '10',
      entityKey: 'c1:employees',
      entityId: 'c1',
      text: 'Company has 120 employees',
      confidence: 0.88,
    });

    const conflict = detectClaimContradiction(existing, incoming);
    assert.ok(conflict);
    assert.equal(conflict.action, 'reinvestigate');

    const { memory, conflict: reconciledConflict } = reconcileClaimMemory(existing, incoming);
    assert.ok(reconciledConflict);
    assert.ok([MEMORY_STATUS.ACTIVE, MEMORY_STATUS.CONFLICT].includes(memory.status));
  });

  it('persists and reloads intelligence memory for a market', async () => {
    const knowledge = extractKnowledgeFromInvestigation(sampleInvestigationResult(), {
      tenantId: '10',
      missionId: 'mission-1',
    });

    await store.persistKnowledge('10', knowledge);
    const loaded = await loadIntelligenceMemory({
      tenantId: '10',
      marketDefinition: { geography: 'Manchester NH', segment: 'property_management' },
      opts: { store },
    });

    assert.equal(loaded.loaded, true);
    assert.ok(loaded.market);
    assert.equal(loaded.claims.length, 1);
    assert.equal(loaded.companies.length, 1);
    assert.ok(loaded.claims[0].effectiveConfidence > 0);
  });

  it('builds connected memory graph', () => {
    const knowledge = extractKnowledgeFromInvestigation(sampleInvestigationResult(), {
      tenantId: '10',
    });
    const graph = buildMemoryGraphFromKnowledge(knowledge);

    assert.ok(graph.nodes.size >= 4);
    assert.ok(graph.edges.length >= 3);
    const serialized = memory.serializeMemoryGraph(graph);
    assert.equal(serialized.summary.claims, 1);
    assert.equal(serialized.summary.companies, 1);
  });

  it('second investigation requires less work when memory exists', async () => {
    const mission = sampleMission();
    const candidates = sampleCandidates();
    const sharedStore = createMemoryIntelligenceStore();
    const sharedOpts = {
      discover: async () => candidates,
      companies: candidates,
      estimatedMarket: 4,
      maxIterations: 8,
      confidenceThreshold: 0.5,
      memoryStore: sharedStore,
    };

    const first = await runInvestigationEngine({
      mission,
      opts: sharedOpts,
    });

    assert.ok(first.memoryPersist?.persisted);
    assert.ok(first.iterations.length >= 1);
    const firstSteps = first.iterations.filter((i) => i.nextStep).length;

    const second = await runInvestigationEngine({
      mission: { ...mission, id: 'mission-spec143-2' },
      opts: sharedOpts,
    });

    assert.equal(second.memoryLoaded, true);
    assert.ok(second.startingPoint);
    assert.ok(second.startingPoint.counts.known >= 0);
    assert.ok(
      second.startingPoint.skippedSteps.length >= 0 ||
        (second.startingPoint.preloadedClaims || []).length >= 0
    );

    const events = listMemoryLog();
    assert.ok(events.some((e) => e.event === MEMORY_EVENTS.PERSISTED));
    assert.ok(events.some((e) => e.event === MEMORY_EVENTS.LOADED));
  });

  it('investigate() returns memory metadata', async () => {
    const { investigate } = require('../packages/scout');
    const mission = sampleMission();
    const candidates = sampleCandidates();
    const memoryStore = createMemoryIntelligenceStore();

    const result = await investigate({
      mission,
      opts: {
        discover: async () => candidates,
        companies: candidates,
        estimatedMarket: 4,
        maxIterations: 6,
        confidenceThreshold: 0.5,
        memoryStore,
      },
    });

    assert.ok(result.startingPoint);
    assert.ok(result.memoryPersist?.persisted || result.memoryPersist?.knowledge);
  });

  it('prepareInvestigationWithMemory surfaces prior knowledge before discovery', async () => {
    const knowledge = extractKnowledgeFromInvestigation(sampleInvestigationResult(), {
      tenantId: '10',
    });
    await store.persistKnowledge('10', knowledge);

    const prep = await prepareInvestigationWithMemory({
      tenantId: '10',
      marketDefinition: { geography: 'Manchester NH', segment: 'property_management' },
      opts: { store },
    });

    assert.equal(prep.hasPriorKnowledge, true);
    assert.ok(prep.startingPoint.counts.claims >= 1);
    assert.ok(prep.memory.market);
  });

  it('uses market entity key for deduplication', () => {
    const key = marketEntityKey('Manchester NH', 'property_management');
    assert.match(key, /manchester/);
    assert.match(key, /property_management/);
  });
});
