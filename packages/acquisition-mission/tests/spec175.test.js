'use strict';

/**
 * SPEC-175 — External Discovery Capability.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildProviderRegistry,
  PROVIDER_CAPABILITY,
  EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE,
  hasOperationalEvidenceProvider,
} = require('../../scout/coverage/ExternalDiscoveryProviderRegistry');
const {
  evaluateDiscoveryCapability,
  CAPABILITY_BLOCKER_CODE,
} = require('../../scout/coverage/DiscoveryCapabilityGate');
const {
  validateCandidateMinimum,
  enforceCandidateMinimumContract,
} = require('../../scout/coverage/CandidateMinimumContract');
const {
  buildDiscoveryPlan,
  executeCoveragePlan,
  expandCitiesFromSearchDefinition,
} = require('../../scout/coverage/DiscoveryCoverageEngine');
const { createInjectedDiscoverAdapter } = require('../../max/scoutAcquisition/DiscoveryAdapters');
const { buildAcquisitionSearchDefinition } = require('../../max/scoutAcquisition/SearchDefinition');
const { runDiscoveryPipeline } = require('../../scout/DiscoveryPipeline');
const { constructCandidateUniverse } = require('../../max/scoutAcquisition/CandidateUniverse');
const { runScoutAcquisitionIntelligence } = require('../../max/scoutAcquisition/ScoutAdapter');
const { DISCOVERY_OUTCOMES } = require('../../scout/types');
const amo = require('../index');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  validateDiscoveryPreconditions,
} = require('../../max/workspace/AmoOperatorApproval');
const { resetEngine } = require('../../../services/acquisitionMission');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-175 — External Discovery Capability', () => {
  let savedPlacesKey;

  beforeEach(() => {
    savedPlacesKey = process.env.GOOGLE_PLACES_KEY;
    delete process.env.GOOGLE_PLACES_KEY;
    resetEngine();
  });

  afterEach(() => {
    if (savedPlacesKey === undefined) {
      delete process.env.GOOGLE_PLACES_KEY;
    } else {
      process.env.GOOGLE_PLACES_KEY = savedPlacesKey;
    }
  });

  it('Scenario 1: Google Places available — candidate universe populated, discovery continues', async () => {
    process.env.GOOGLE_PLACES_KEY = 'test-key';
    const adapter = createInjectedDiscoverAdapter(async ({ searchDefinition }) => {
      const city = searchDefinition.geography?.label || 'Manchester NH';
      return [{ id: 'pl-1', name: 'Harbor Law Group', location: city, source: 'public_business_places' }];
    });

    const registry = buildProviderRegistry({ adapters: [adapter], discover: adapter.discover });
    const places = registry.find((row) => row.id === 'google_places');
    assert.equal(places.capability, PROVIDER_CAPABILITY.AVAILABLE);

    const delegation = {
      tenantId: '10',
      authority: 'observe',
      targetContext: { geography: 'Manchester, NH', segments: ['law_firm'] },
      businessContext: { serviceGeography: 'Manchester, NH', commercialCapability: 'commercial_cleaning' },
    };
    const result = await runScoutAcquisitionIntelligence(delegation, {
      discoveryAdapters: [adapter],
      enablePlaces: false,
    });
    assert.notEqual(result.status, 'blocked');
    assert.ok(result.payload.candidateUniverse.length >= 1);
  });

  const lawFirmDefinition = () =>
    buildAcquisitionSearchDefinition({
      tenantId: '10',
      targetContext: { geography: 'Manchester, NH', segments: ['law_firm'] },
      businessContext: {
        serviceGeography: 'Manchester, NH',
        commercialCapability: 'commercial_cleaning',
        preferredSegments: ['law_firm'],
      },
    });

  const hooksettAuburnDefinition = () =>
    buildAcquisitionSearchDefinition({
      tenantId: '10',
      targetContext: { geography: 'Hooksett and Auburn', segments: ['law_firm'] },
      businessContext: {
        serviceGeography: 'Hooksett and Auburn',
        commercialCapability: 'commercial_cleaning',
        preferredSegments: ['law_firm'],
      },
    });

  it('Scenario 2: Google Places unavailable — mission pauses before discovery execution with capability blocker', async () => {
    const searchDefinition = lawFirmDefinition();
    const evaluation = evaluateDiscoveryCapability({
      adapters: [],
      coveragePlan: buildDiscoveryPlan(searchDefinition, { adapters: [] }),
      requireExternalDiscovery: true,
    });
    assert.equal(evaluation.canExecute, false);
    assert.equal(evaluation.blockReason, EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE);
    assert.equal(evaluation.blockerCode, CAPABILITY_BLOCKER_CODE);

    const pipeline = await runDiscoveryPipeline({
      mission: {
        id: 'm-175-2',
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      },
      delegation: {
        tenantId: '10',
        authority: 'observe',
        targetContext: {
          geography: 'Greater Manchester',
          segments: ['law_firm'],
        },
        businessContext: {
          serviceGeography: 'Greater Manchester',
          commercialCapability: 'commercial_cleaning',
          preferredSegments: ['law_firm'],
        },
      },
      opts: { enablePlaces: false, discoveryAdapters: [] },
    });
    assert.equal(pipeline.outcome, DISCOVERY_OUTCOMES.BLOCKED);
    assert.equal(pipeline.blockReason, EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE);
    assert.equal(pipeline.capabilityBlocked, true);
  });

  it('Scenario 3: two cities execute independently with all concepts', () => {
    const definition = hooksettAuburnDefinition();
    const cities = expandCitiesFromSearchDefinition(definition);
    assert.equal(cities.length, 2);
    assert.ok(cities.some((c) => /Hooksett/i.test(c)));
    assert.ok(cities.some((c) => /Auburn/i.test(c)));

    const adapter = createInjectedDiscoverAdapter(async () => []);
    const plan = buildDiscoveryPlan(definition, { adapters: [adapter] });
    const hooksettWorkloads = plan.workloads.filter((w) => /Hooksett/i.test(w.city));
    const auburnWorkloads = plan.workloads.filter((w) => /Auburn/i.test(w.city));
    assert.ok(hooksettWorkloads.length >= plan.concepts.length);
    assert.ok(auburnWorkloads.length >= plan.concepts.length);
    assert.equal(hooksettWorkloads.length, auburnWorkloads.length);
  });

  it('Scenario 4: one provider returns zero candidates — discovery completes legitimately', async () => {
    const searchDefinition = lawFirmDefinition();
    const adapter = createInjectedDiscoverAdapter(async () => []);
    const plan = buildDiscoveryPlan(searchDefinition, { adapters: [adapter] });
    const result = await executeCoveragePlan(plan, searchDefinition, [adapter]);
    assert.equal(result.candidates.length, 0);
    assert.ok(result.sourceTypesChecked.length >= 1);
    assert.equal(result.coverage.complete, true);
  });

  it('Scenario 5: no providers — discovery never reaches candidate qualification', async () => {
    const searchDefinition = lawFirmDefinition();

    assert.equal(hasOperationalEvidenceProvider({ adapters: [] }), false);

    const universe = await constructCandidateUniverse({
      searchDefinition,
      existing: { companies: [], people: [], criteria: {}, rejectedCandidates: [] },
      adapters: [],
      forceDiscover: true,
      useCoverageEngine: true,
    });
    assert.equal(universe.capabilityBlocked, true);
    assert.equal(universe.blockReason, EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE);
    assert.equal(universe.discoveryRan, false);
  });

  it('Scenario 6: TME precondition gate blocks discovery when no external provider is configured', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    const planResult = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Proceed with this plan.',
    });

    assert.throws(
      () =>
        validateDiscoveryPreconditions({
          mission: planResult.snapshot.mission,
          engine,
          tenantId: '10',
          enablePlaces: false,
        }),
      (err) =>
        err.code === 'external_discovery_capability_unavailable' &&
        err.message === EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE
    );

    await assert.rejects(
      () =>
        advanceDiscoveryAfterApproval({
          engine,
          mission: planResult.snapshot.mission,
          tenantId: '10',
          question: 'Approved. Begin Discovery.',
          enablePlaces: false,
        }),
      (err) => err.code === 'external_discovery_capability_unavailable'
    );
  });

  it('candidate minimum contract requires identity, location, source, and provenance', () => {
    const valid = validateCandidateMinimum(
      {
        id: 'co-1',
        name: 'Harbor Law Group',
        location: 'Manchester, NH',
        source: 'public_business_places',
      },
      { id: 'Manchester NH|law_firm|public_business_data', city: 'Manchester NH', concept: 'law_firm', source: 'public_business_data' }
    );
    assert.equal(valid.valid, true);
    assert.ok(valid.candidate.retrievalProvenance.provider);

    const invalid = validateCandidateMinimum({ name: 'Missing Fields Co' }, {});
    assert.equal(invalid.valid, false);
    assert.ok(invalid.missing.includes('identity') || invalid.missing.includes('location'));

    const filtered = enforceCandidateMinimumContract(
      [
        { id: 'ok', name: 'Good Co', location: 'Hooksett NH', source: 'places' },
        { description: 'No identity or location' },
      ],
      { id: 'w1', city: 'Hooksett NH', concept: 'law_firm', source: 'places' }
    );
    assert.equal(filtered.accepted.length, 1);
    assert.equal(filtered.rejected.length, 1);
  });

  it('provider registry declares STUB and NOT_IMPLEMENTED providers explicitly', () => {
    const registry = buildProviderRegistry({ adapters: [] });
    const linkedin = registry.find((row) => row.id === 'linkedin');
    const airbnb = registry.find((row) => row.id === 'airbnb');
    assert.equal(linkedin.capability, PROVIDER_CAPABILITY.STUB);
    assert.equal(airbnb.capability, PROVIDER_CAPABILITY.NOT_IMPLEMENTED);
  });
});
