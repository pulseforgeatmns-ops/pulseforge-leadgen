'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  createSpecialistDelegationService,
} = require('../services/specialistDelegation');
const {
  runAcquisitionIntelligenceLoop,
  runScoutAcquisitionIntelligence,
  createMemoryAcquisitionState,
  buildAcquisitionSearchDefinition,
  evaluateBasicFit,
  resolveCandidateUniverse,
  createMemoryDiscoveryStore,
  ANCHOR_TENANT_ID,
} = require('../services/scoutAcquisitionIntelligence');
const { OPPORTUNITY_CLASSES, INTENT_STATES, SOURCE_TYPES } = require('../packages/max/scoutAcquisition/Types');

const ANCHOR_QUESTION =
  'Max, where should we be looking for commercial cleaning opportunities right now?';

function loopOpts(store, aoStore, extras = {}) {
  const service = createSpecialistDelegationService({ store });
  return {
    delegationService: service,
    aoStore,
    companies: extras.companies !== undefined ? extras.companies : [],
    people: extras.people || [],
    discover: extras.discover,
    discoveryAdapters: extras.discoveryAdapters,
    discoveryStore: extras.discoveryStore,
    persistCompanies: extras.persistCompanies,
    enrichPeople: extras.enrichPeople,
    loadCompanies: extras.loadCompanies,
    now: extras.now,
  };
}

function anchorInput(overrides = {}) {
  return {
    authorizedTenantId: ANCHOR_TENANT_ID,
    tenantId: ANCHOR_TENANT_ID,
    question: ANCHOR_QUESTION,
    objective: 'Find commercial cleaning opportunities in Anchor\'s service area.',
    reason: 'Current pipeline intelligence is insufficient.',
    authority: 'observe',
    businessContext: {
      serviceGeography: 'Manchester, NH',
      commercialCapability: 'commercial_cleaning',
      preferredSegments: ['property_management'],
      exclusions: ['restaurant'],
    },
    targetContext: {
      geography: 'Manchester, NH',
      segments: ['property_management'],
      businessType: 'commercial_cleaning',
      desiredSignals: ['expansion', 'portfolio_growth', 'operational_change'],
    },
    applyPriority: false,
    ...overrides,
  };
}

function marketCandidates() {
  return [
    {
      id: 'disc-granite',
      name: 'Granite State Property Management',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://granitepm.example',
      signals: [
        {
          type: 'portfolio_growth',
          observedAt: '2026-07-12T00:00:00.000Z',
          source: 'company_website',
          label: 'Company website lists 37 managed properties.',
          observation: 'Company website lists 37 managed properties.',
        },
      ],
    },
    {
      id: 'disc-queen',
      name: 'Queen City Residences',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://queencity.example',
      signals: [
        {
          type: 'portfolio_growth',
          observedAt: '2026-07-20T00:00:00.000Z',
          label: 'Added three buildings to the downtown portfolio.',
        },
      ],
    },
    {
      id: 'disc-abc',
      name: 'ABC Property Management',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://abcpm.example',
    },
    {
      id: 'disc-abc-llc',
      name: 'ABC Property Mgmt LLC',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://www.abcpm.example',
    },
    {
      id: 'disc-abc-comma',
      name: 'ABC Property Management, LLC',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://abcpm.example/',
    },
    {
      id: 'disc-watch',
      name: 'Canal Street Property Partners',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://canalpm.example',
    },
    {
      id: 'disc-realtor',
      name: 'Lakeside Residential Realty',
      industry: 'property_management',
      location: 'Manchester, NH',
      snippet: 'residential realtor selling single-family homes',
    },
    {
      id: 'disc-nashua',
      name: 'Nashua Commercial Clean Buyers',
      industry: 'property_management',
      location: 'Nashua, NH',
    },
  ];
}

describe('SPEC-100A Scout acquisition discovery foundation', () => {
  let store;
  let aoStore;

  beforeEach(() => {
    store = createMemoryStore();
    aoStore = createMemoryAcquisitionState();
  });

  it('resolves a broad acquisition objective into an inspectable search definition', () => {
    const definition = buildAcquisitionSearchDefinition({
      tenantId: ANCHOR_TENANT_ID,
      targetContext: {
        geography: 'Manchester, NH',
        businessType: 'commercial_cleaning',
      },
      businessContext: {
        serviceGeography: 'Manchester, NH',
        commercialCapability: 'commercial_cleaning',
      },
    });
    assert.equal(definition.valid, true);
    assert.equal(definition.businessNeed, 'commercial_cleaning');
    assert.equal(definition.geography.label, 'Manchester, NH');
    assert.ok(definition.segments.length);
    assert.match(definition.populationStatement, /commercial organizations/i);
    assert.match(definition.populationStatement, /Manchester/i);
    assert.equal(definition.expansionRequiresAuthority, true);
    assert.ok(definition.geography.permittedNearby.includes('Bedford'));
  });

  it('produces an actual candidate universe for a valid acquisition definition', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discover: async () => marketCandidates(),
      })
    );
    const coverage = result.result.payload.investigation.coverage;
    assert.ok(coverage.candidatesDiscovered >= 5);
    assert.ok(coverage.candidatesResolved >= 4);
    assert.ok(coverage.candidatesEvaluated >= 4);
    assert.ok(coverage.basicFitCount >= 3);
    assert.equal(coverage.candidatesDiscovered, result.result.payload.investigation.coverage.candidatesDiscovered);
    assert.ok(result.result.payload.searchDefinition.populationStatement);
  });

  it('retrieves existing relevant companies before discovering the open market', async () => {
    const discoveryStore = createMemoryDiscoveryStore();
    const existing = [
      {
        id: 'co-granite',
        tenantId: ANCHOR_TENANT_ID,
        name: 'Granite State Property Management',
        industry: 'property_management',
        location: 'Manchester, NH',
        website: 'https://granitepm.example',
        lastEvaluatedAt: '2026-08-16T12:00:00.000Z',
        signals: marketCandidates()[0].signals,
      },
    ];
    let discoverCalls = 0;
    const first = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        companies: existing,
        discoveryStore,
        discover: async () => {
          discoverCalls += 1;
          return marketCandidates();
        },
      })
    );
    assert.equal(first.delegated, true);
    assert.ok(discoverCalls >= 1);
    assert.ok(first.result.payload.discoveryPlan);
    assert.equal(first.result.payload.discoveryPlan.totals.cities, 1);
    assert.equal(first.result.payload.retrievedBeforeInvestigate, true);
    const names = (first.result.payload.evaluatedCandidates || []).map((c) => c.name);
    assert.ok(names.some((n) => /Granite State/i.test(n)));
    const graniteHits = names.filter((n) => /Granite State/i.test(n));
    assert.equal(graniteHits.length, 1);
  });

  it('deduplicates ABC Property Management aliases to one entity', async () => {
    const resolved = resolveCandidateUniverse(
      [],
      marketCandidates().filter((c) => /ABC Property/i.test(c.name))
    );
    assert.equal(resolved.candidatesResolved, 1);
    assert.ok(resolved.duplicatesRemoved >= 2);

    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discover: async () => marketCandidates().filter((c) => /ABC Property/i.test(c.name)),
      })
    );
    const abc = (result.result.payload.evaluatedCandidates || []).filter((c) =>
      /ABC Property/i.test(c.name)
    );
    assert.equal(abc.length, 1);
  });

  it('keeps explainable basic-fit reasons and does not fabricate buying intent', async () => {
    const definition = buildAcquisitionSearchDefinition(anchorInput());
    const strong = evaluateBasicFit(
      {
        name: 'Granite State Property Management',
        industry: 'property_management',
        location: 'Manchester, NH',
        snippet: 'operating 14 multifamily properties',
        icpScore: 82,
      },
      definition
    );
    assert.equal(strong.basicFit, true);
    assert.ok(strong.reasons.some((r) => /property|Manchester|14 multifamily/i.test(r)));
    assert.equal(strong.intent, INTENT_STATES.UNKNOWN);

    const rejected = evaluateBasicFit(
      {
        name: 'Lakeside Residential Realty',
        industry: 'property_management',
        location: 'Manchester, NH',
        snippet: 'residential realtor with no managed facilities',
      },
      definition
    );
    assert.equal(rejected.basicFit, false);
    assert.match(rejected.reasons.join(' '), /residential/i);
    assert.equal(rejected.intent, INTENT_STATES.UNKNOWN);
  });

  it('preserves strong-fit candidates when timing is unknown', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discover: async () => [
          {
            id: 'disc-fit',
            name: 'Elm Plaza Property Management',
            industry: 'property_management',
            location: 'Manchester, NH',
            website: 'https://elmplaza.example',
            snippet: 'managing 14 multifamily properties downtown',
          },
        ],
      })
    );
    assert.equal(result.result.status, 'completed');
    assert.equal(result.result.artifactRefs.length, 0);
    assert.ok(result.result.payload.fitCandidates.length >= 1);
    assert.equal(result.result.payload.fitCandidates[0].classification, OPPORTUNITY_CLASSES.FIT);
    assert.equal(result.result.payload.fitCandidates[0].intent, INTENT_STATES.UNKNOWN);
    assert.match(result.result.summary, /meet the target profile|timing is unknown/i);
    assert.doesNotMatch(JSON.stringify(result.result.payload.fitCandidates), /ready to buy|currently wants/i);
  });

  it('keeps a valid company when person enrichment fails', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discover: async () => [
          {
            id: 'disc-fit',
            name: 'Elm Plaza Property Management',
            industry: 'property_management',
            location: 'Manchester, NH',
            website: 'https://elmplaza.example',
          },
        ],
        enrichPeople: async () => {
          throw new Error('Hunter unavailable');
        },
      })
    );
    assert.ok(result.result.payload.evaluatedCandidates.length >= 1);
    assert.ok(result.result.uncertainties.some((u) => /decision-maker unknown/i.test(u)));
    assert.ok(result.result.payload.fitCandidates.length + result.result.artifactRefs.length >= 1);
  });

  it('preserves useful discovery when one source fails', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        companies: [
          {
            id: 'co-existing',
            tenantId: ANCHOR_TENANT_ID,
            name: 'Existing PM Co',
            industry: 'property_management',
            location: 'Manchester, NH',
            website: 'https://existingpm.example',
          },
        ],
        discover: async () => {
          throw new Error('Places unavailable');
        },
      })
    );
    assert.ok(result.result.payload.evaluatedCandidates.some((c) => c.companyId === 'co-existing'));
    assert.ok(
      result.result.payload.investigation.sources.sourceTypesUnavailable.includes(
        SOURCE_TYPES.PUBLIC_BUSINESS_DATA
      ) || result.result.payload.investigation.limitations.some((l) => /LinkedIn|Facebook|Instagram/i.test(l))
    );
    assert.equal(result.result.payload.broadened, false);
    assert.deepEqual(result.outboundInvoked, []);
  });

  it('returns partial or blocked — not a market-negative conclusion — when discovery yields zero candidates', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, { companies: [], people: [] })
    );
    assert.ok(['blocked', 'partial'].includes(result.result.status));
    assert.equal(result.result.payload.investigation.coverage.candidatesEvaluated, 0);
    assert.equal(result.evaluation.marketAbsenceJustified, false);
    assert.match(result.prose, /discovery limitation|incomplete investigation|could not construct/i);
    assert.doesNotMatch(result.prose, /reasonably confident there isn't an obvious/i);
  });

  it('returns a valid complete result when discovery is strong but opportunities are zero', async () => {
    const companies = Array.from({ length: 20 }, (_, i) => ({
      id: `co-fit-${i}`,
      tenantId: ANCHOR_TENANT_ID,
      name: `Manchester PM ${i + 1}`,
      industry: 'property_management',
      location: 'Manchester, NH',
      website: `https://pm${i}.example`,
      icpScore: 76,
    }));
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, { companies })
    );
    assert.equal(result.result.status, 'completed');
    assert.equal(result.result.artifactRefs.length, 0);
    assert.ok(result.result.payload.investigation.coverage.basicFitCount >= 12);
    assert.ok(result.result.payload.fitCandidates.length >= 12);
    assert.match(result.result.summary, /meet the target profile|timing is unknown/i);
    assert.equal(result.evaluation.materialChange, false);
  });

  it('does not weaken criteria to manufacture an arbitrary lead quota', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discover: async () =>
          marketCandidates().filter((c) =>
            ['ABC Property Management', 'Canal Street Property Partners'].includes(c.name)
          ),
      })
    );
    assert.ok(result.result.payload.investigation.coverage.candidatesEvaluated <= 4);
    assert.ok(result.result.payload.investigation.coverage.basicFitCount <= 4);
    assert.doesNotMatch(result.result.summary, /at least (10|25|50) leads/i);
    assert.equal(result.result.payload.broadened, false);
  });

  it('does not silently broaden Manchester into all New Hampshire commercial companies', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discover: async () => [],
      })
    );
    const def = result.result.payload.searchDefinition;
    assert.equal(def.geography.label, 'Manchester, NH');
    assert.deepEqual(def.geography.cities, ['Manchester']);
    assert.equal(def.expansionRequiresAuthority, true);
    assert.ok(def.geography.permittedNearby.length);
    assert.equal(def.geography.cities.includes('Nashua'), false);
    assert.equal(result.result.payload.discoveryPlan.totals.cities, 1);
  });

  it('keeps discovery sources and evidence traceable', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discover: async () => [marketCandidates()[0]],
      })
    );
    assert.ok(result.result.payload.investigation.sources.sourceTypesChecked.length);
    const evidence = result.result.evidenceRefs;
    assert.ok(evidence.length);
    assert.ok(evidence.every((e) => e.id && (e.snapshot || e.sourceKind)));
    assert.ok(
      evidence.some((e) => e.snapshot && e.snapshot.source) ||
        result.result.payload.investigation.sources.sourceTypesChecked.includes(
          SOURCE_TYPES.PUBLIC_BUSINESS_DATA
        )
    );
  });

  it('populates the SPEC-099A funnel from actual execution', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discover: async () => marketCandidates(),
      })
    );
    const coverage = result.result.payload.investigation.coverage;
    assert.equal(typeof coverage.candidatesDiscovered, 'number');
    assert.equal(typeof coverage.candidatesResolved, 'number');
    assert.equal(typeof coverage.candidatesEvaluated, 'number');
    assert.equal(typeof coverage.basicFitCount, 'number');
    assert.equal(typeof coverage.signalBearingCount, 'number');
    assert.equal(typeof coverage.supportedOpportunityCount, 'number');
    assert.ok(coverage.candidatesDiscovered >= coverage.candidatesResolved);
    assert.ok(coverage.candidatesResolved >= coverage.candidatesEvaluated);
    assert.equal(coverage.supportedOpportunityCount, result.result.artifactRefs.length);
  });

  it('keeps candidate intelligence tenant-scoped', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        companies: [
          {
            id: 'co-aji',
            tenantId: '2',
            name: 'Aji Secret PM',
            industry: 'property_management',
            location: 'Manchester, NH',
            website: 'https://aji.example',
          },
        ],
        discover: async () => [
          {
            id: 'disc-aji',
            tenantId: '2',
            name: 'Aji Secret PM',
            industry: 'property_management',
            location: 'Manchester, NH',
          },
        ],
      })
    );
    assert.equal(
      result.result.payload.evaluatedCandidates.some((c) => /Aji Secret/i.test(c.name)),
      false
    );
  });

  it('invokes no outbound action while constructing the candidate universe', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discover: async () => marketCandidates(),
      })
    );
    assert.deepEqual(result.outboundInvoked, []);
    assert.equal(result.result.payload.outboundInvoked.length, 0);
    assert.equal(
      result.result.actionsTaken.some((a) => /send|sms|call|enroll|publish|outreach/i.test(a.text)),
      false
    );
  });

  it('persists discovered companies so the next run can retrieve before rediscovering', async () => {
    const discoveryStore = createMemoryDiscoveryStore();
    const persisted = [];
    await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        discoveryStore,
        persistCompanies: async ({ companies }) => {
          persisted.push(...companies);
        },
        discover: async () => [marketCandidates()[2]],
      })
    );
    assert.ok(persisted.length);
    assert.ok(persisted[0].discoveredAt);
    assert.ok(persisted[0].lastEvaluatedAt);
    const listed = await discoveryStore.list(ANCHOR_TENANT_ID);
    assert.ok(listed.some((c) => /ABC Property/i.test(c.name)));

    let discoverCalls = 0;
    const second = await runScoutAcquisitionIntelligence(
      {
        tenantId: ANCHOR_TENANT_ID,
        authority: 'observe',
        targetContext: anchorInput().targetContext,
        businessContext: anchorInput().businessContext,
      },
      {
        companies: [],
        discoveryStore,
        discover: async () => {
          discoverCalls += 1;
          return [marketCandidates()[2], marketCandidates()[5]];
        },
      }
    );
    assert.ok(discoverCalls >= 1);
    assert.ok(second.payload.discoveryPlan);
    const abc = (second.payload.evaluatedCandidates || []).filter((c) =>
      /ABC Property/i.test(c.name)
    );
    assert.equal(abc.length, 1);
    assert.ok((second.payload.evaluatedCandidates || []).some((c) => /Canal Street/i.test(c.name)));
  });
});
