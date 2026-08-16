'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  createSpecialistDelegationService,
  SpecialistDelegationError,
} = require('../services/specialistDelegation');
const {
  runAcquisitionIntelligenceLoop,
  createMemoryAcquisitionState,
  buildBoundedScoutContext,
  retrieveExistingIntelligence,
  evaluateScoutResult,
  looksLikeAcquisitionQuestion,
  ANCHOR_TENANT_ID,
} = require('../services/scoutAcquisitionIntelligence');
const {
  collectDomainSignals,
  buildDomainSummary,
  PRIORITY_STATES,
} = require('../packages/max/commandDeck/spatial/DomainPriority');
const { composeSpatialOverview } = require('../packages/max/commandDeck/sections/SpatialOverview');

const ANCHOR_QUESTION =
  'Max, where should we be looking for commercial cleaning opportunities right now?';

function anchorCompanies(tenantId = ANCHOR_TENANT_ID) {
  return [
    {
      id: 'co-granite',
      tenantId,
      name: 'Granite State Property Management',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://granitepm.example',
      icpScore: 82,
      updatedAt: '2026-08-01T12:00:00.000Z',
      signals: [
        {
          type: 'portfolio_growth',
          observedAt: '2026-07-12T00:00:00.000Z',
          source: 'company_website',
          label: 'Company website lists 37 managed properties.',
          observation: 'Company website lists 37 managed properties.',
          inference: 'Portfolio growth may increase cleaning-vendor demand.',
        },
        {
          type: 'expansion',
          observedAt: '2026-07-18T00:00:00.000Z',
          source: 'press',
          label: 'Company announced two new properties in July 2026.',
          observation: 'Company announced two new properties in July 2026.',
        },
      ],
    },
    {
      id: 'co-queen',
      tenantId,
      name: 'Queen City Residences',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://queencity.example',
      icpScore: 78,
      updatedAt: '2026-08-02T12:00:00.000Z',
      signals: [
        {
          type: 'portfolio_growth',
          observedAt: '2026-07-20T00:00:00.000Z',
          label: 'Added three buildings to the downtown portfolio.',
        },
        {
          type: 'hiring',
          observedAt: '2026-07-22T00:00:00.000Z',
          label: 'Hiring a property operations coordinator.',
        },
      ],
    },
    {
      id: 'co-merrimack',
      tenantId,
      name: 'Merrimack Valley PM',
      industry: 'property_management',
      location: 'Bedford, NH',
      icpScore: 74,
      signals: [
        {
          type: 'portfolio_growth',
          observedAt: '2026-07-08T00:00:00.000Z',
          label: 'Portfolio grew by four doors this summer.',
        },
      ],
    },
    {
      id: 'co-bedford',
      tenantId,
      name: 'Bedford Portfolio Partners',
      industry: 'property_management',
      location: 'Bedford, NH',
      icpScore: 71,
      signals: [
        {
          type: 'portfolio_growth',
          observedAt: '2026-07-05T00:00:00.000Z',
          label: 'Acquired a 40-unit building in July 2026.',
        },
      ],
    },
    {
      id: 'co-hooksett',
      tenantId,
      name: 'Hooksett Holdings',
      industry: 'property_management',
      location: 'Hooksett, NH',
      icpScore: 68,
    },
    {
      id: 'co-elm',
      tenantId,
      name: 'Elm Street Management',
      industry: 'property_management',
      location: 'Manchester, NH',
      icpScore: 66,
      signals: [
        {
          type: 'hiring',
          observedAt: '2026-07-28T00:00:00.000Z',
          label: 'Posted an operations assistant role.',
        },
      ],
    },
    {
      id: 'co-riverwalk',
      tenantId,
      name: 'Riverwalk PM',
      industry: 'property_management',
      location: 'Manchester, NH',
      icpScore: 64,
      signals: [
        {
          type: 'operational_change',
          observedAt: '2026-07-25T00:00:00.000Z',
          label: 'Opened a new operations desk downtown.',
        },
      ],
    },
    {
      id: 'co-stale',
      tenantId,
      name: 'Legacy Towers LLC',
      industry: 'property_management',
      location: 'Manchester, NH',
      icpScore: 60,
      signals: [
        {
          type: 'expansion',
          observedAt: '2023-03-01T00:00:00.000Z',
          label: 'Expanded three years ago.',
        },
      ],
    },
    {
      id: 'co-nashua-restaurant',
      tenantId,
      name: 'Nashua Diner Group',
      industry: 'restaurant',
      location: 'Nashua, NH',
      icpScore: 90,
    },
  ];
}

function anchorPeople(tenantId = ANCHOR_TENANT_ID) {
  return [
    {
      id: 'p-granite-ops',
      tenantId,
      companyId: 'co-granite',
      name: 'Pat Riley',
      jobTitle: 'Director of Operations',
      decisionMaker: true,
    },
    {
      id: 'p-queen-ops',
      tenantId,
      companyId: 'co-queen',
      name: 'Chris Hale',
      jobTitle: 'Office Manager',
    },
    {
      id: 'p-merrimack',
      tenantId,
      companyId: 'co-merrimack',
      name: 'Jordan Lee',
      jobTitle: 'Owner',
      decisionMaker: true,
    },
  ];
}

function loopOpts(store, aoStore, extras = {}) {
  const service = createSpecialistDelegationService({ store });
  return {
    delegationService: service,
    aoStore,
    companies: extras.companies || anchorCompanies(),
    people: extras.people || anchorPeople(),
    discover: extras.discover,
    loadCompanies: extras.loadCompanies,
    priorityApplier: extras.priorityApplier,
    freshnessMs: extras.freshnessMs,
    now: extras.now,
  };
}

function anchorInput(overrides = {}) {
  return {
    authorizedTenantId: ANCHOR_TENANT_ID,
    tenantId: ANCHOR_TENANT_ID,
    question: ANCHOR_QUESTION,
    objective:
      "Identify Manchester-area property management companies that fit Anchor Cleaning's commercial acquisition strategy and show evidence of near-term opportunity.",
    reason:
      'The operator has prioritized property managers and current pipeline intelligence in that segment is insufficient.',
    authority: 'observe',
    businessContext: {
      serviceGeography: 'Manchester, NH',
      commercialCapability: 'commercial_cleaning',
      preferredSegments: ['property_management'],
      acquisitionDirection: 'commercial offices and property managers in the Manchester ring',
      exclusions: ['restaurant'],
    },
    targetContext: {
      geography: 'Manchester, NH',
      segments: ['property_management'],
      businessType: 'commercial_cleaning',
      desiredSignals: ['expansion', 'portfolio_growth', 'hiring', 'decision_maker'],
    },
    applyPriority: true,
    ...overrides,
  };
}

describe('SPEC-100 Max ↔ Scout acquisition intelligence loop', () => {
  /** @type {ReturnType<typeof createMemoryStore>} */
  let store;
  /** @type {ReturnType<typeof createMemoryAcquisitionState>} */
  let aoStore;
  let priorityCalls;

  beforeEach(() => {
    store = createMemoryStore();
    aoStore = createMemoryAcquisitionState();
    priorityCalls = [];
  });

  it('creates a valid acquisition_intelligence Scout delegation', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        priorityApplier: async (payload) => {
          priorityCalls.push(payload);
          return { applied: true };
        },
      })
    );

    assert.equal(result.delegated, true);
    assert.equal(result.delegation.specialist, 'scout');
    assert.equal(result.delegation.capability, 'acquisition_intelligence');
    assert.equal(result.delegation.authority, 'observe');
    assert.equal(result.delegation.expectedReturn.type, 'acquisition_intelligence');
    assert.equal(result.result.status, 'completed');
    assert.ok(result.result.artifactRefs.length >= 4);
    assert.ok(result.result.confidence >= 0.55);
  });

  it('rejects draft, execute_after_approval, and execute authority', async () => {
    const service = createSpecialistDelegationService({ store });
    for (const authority of ['draft', 'execute_after_approval', 'execute']) {
      await assert.rejects(
        () =>
          service.createDelegation({
            authorizedTenantId: ANCHOR_TENANT_ID,
            specialist: 'scout',
            capability: 'acquisition_intelligence',
            objective: 'Identify Manchester property managers.',
            reason: 'Operator asked for more opportunities.',
            authority,
          }),
        (err) =>
          err instanceof SpecialistDelegationError &&
          err.code === 'unsupported_authority'
      );
    }
  });

  it('sends Scout only bounded tenant-specific business context', async () => {
    const bounded = buildBoundedScoutContext({
      authorizedTenantId: ANCHOR_TENANT_ID,
      businessContext: {
        serviceGeography: 'Manchester, NH',
        preferredSegments: ['property_management'],
        commercialCapability: 'commercial_cleaning',
      },
      approvedUnderstanding: {
        businessName: 'Anchor Cleaning',
        serviceGeography: 'Manchester, NH',
        secretPlaybook: 'should not be copied wholesale',
      },
      targetContext: {
        geography: 'Manchester, NH',
        segments: ['property_management'],
      },
    });

    assert.equal(bounded.bounded, true);
    assert.ok(bounded.omitted.includes('fullBlueprint'));
    assert.equal(bounded.businessContext.serviceGeography, 'Manchester, NH');
    assert.deepEqual(bounded.businessContext.preferredSegments, ['property_management']);
    assert.equal(bounded.businessContext.approvedUnderstanding.businessName, 'Anchor Cleaning');
    assert.equal(bounded.businessContext.approvedUnderstanding.secretPlaybook, undefined);
  });

  it('checks existing durable intelligence before a second identical investigation', async () => {
    const opts = loopOpts(store, aoStore, {
      priorityApplier: async () => ({ applied: true }),
    });
    const first = await runAcquisitionIntelligenceLoop(anchorInput(), opts);
    assert.equal(first.delegated, true);

    const second = await runAcquisitionIntelligenceLoop(anchorInput(), opts);
    assert.equal(second.delegated, false);
    assert.equal(second.kind, 'reuse');
    const listed = await store.listDelegations({ tenantId: ANCHOR_TENANT_ID });
    assert.equal(listed.length, 1);
  });

  it('attaches provenance to material opportunities', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore)
    );
    const opp = result.result.artifactRefs.find((a) => a.companyId === 'co-granite');
    assert.ok(opp);
    assert.ok(opp.evidenceRefs.length);
    assert.ok(opp.evidenceRefs.every((e) => e.id && e.sourceKind === 'observed_fact'));
    assert.ok(opp.observations.some((o) => /37 managed properties/i.test(o.text)));
  });

  it('keeps observed facts distinct from inferences and unknowns', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore)
    );
    const opp = result.result.artifactRefs.find((a) => a.companyId === 'co-granite');
    assert.ok(opp.observations.every((o) => o.kind === 'observation'));
    assert.ok(opp.inferences.every((i) => i.kind === 'inference'));
    assert.ok(opp.unknowns.every((u) => u.kind === 'unknown'));
    assert.ok(opp.inferences.some((i) => /may increase cleaning-vendor demand/i.test(i.text)));
    assert.ok(
      opp.unknowns.some((u) => /dissatisfaction|contract timing/i.test(u.text))
    );
    assert.equal(
      result.evaluation.acceptedClaims.some((c) => /may increase cleaning-vendor demand/i.test(c.text)),
      false
    );
  });

  it('does not fabricate vendor timing when it is unknown', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore)
    );
    const blob = JSON.stringify(result.result);
    assert.doesNotMatch(blob, /contract ends|renewal in (january|q1)/i);
    assert.ok(result.result.uncertainties.some((u) => /timing|vendor|dissatisfaction/i.test(u)));
  });

  it('returns a valid SPEC-098 structured result', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore)
    );
    assert.ok(['completed', 'partial', 'blocked', 'failed'].includes(result.result.status));
    assert.ok(result.result.summary);
    assert.ok(Array.isArray(result.result.observations));
    assert.ok(Array.isArray(result.result.evidenceRefs));
    assert.ok(result.result.confidence != null);
    assert.ok(Array.isArray(result.result.uncertainties));
    assert.ok(result.result.recommendedNextAction);
  });

  it('does not treat a Scout result as Max ground truth', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore)
    );
    assert.equal(result.evaluation.acceptedAsGroundTruth, false);
    assert.ok(result.evaluation.rejectedClaims.length);
  });

  it('does not let Scout mutate Acquisition priority directly', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({ applyPriority: false }),
      loopOpts(store, aoStore, {
        priorityApplier: async (payload) => {
          priorityCalls.push(payload);
          return { applied: true };
        },
      })
    );
    assert.equal(result.result.commandDeckPriority, undefined);
    assert.equal(result.evaluation.priorityApplied, false);
    assert.equal(priorityCalls.length, 0);
  });

  it('lets Max apply Command Deck priority only for material Scout intelligence', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({ applyPriority: true }),
      loopOpts(store, aoStore, {
        priorityApplier: async (payload) => {
          priorityCalls.push(payload);
          return { applied: true, domainId: payload.domainId };
        },
      })
    );
    assert.equal(result.evaluation.materialChange, true);
    assert.equal(result.evaluation.suggestedPriorityChange.domain, 'acquisition');
    assert.equal(result.evaluation.suggestedPriorityChange.to, 'elevated');
    assert.equal(priorityCalls.length, 1);
    assert.equal(priorityCalls[0].domainId, 'acquisition');
    assert.equal(result.state.summary.includes('timely'), true);
  });

  it('does not elevate Acquisition for a successful but non-material Scout run', async () => {
    const staleOnly = [
      {
        id: 'co-old',
        tenantId: ANCHOR_TENANT_ID,
        name: 'Old Portfolio Co',
        industry: 'property_management',
        location: 'Manchester, NH',
        signals: [
          {
            type: 'expansion',
            observedAt: '2022-01-01T00:00:00.000Z',
            label: 'Expanded in 2022.',
          },
        ],
      },
    ];
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({ applyPriority: true }),
      loopOpts(store, aoStore, {
        companies: staleOnly,
        people: [],
        priorityApplier: async (payload) => {
          priorityCalls.push(payload);
          return { applied: true };
        },
      })
    );
    assert.equal(result.delegated, true);
    assert.equal(result.result.status, 'completed');
    assert.equal(result.evaluation.materialChange, false);
    assert.equal(result.evaluation.suggestedPriorityChange, null);
    assert.equal(priorityCalls.length, 0);
    assert.match(
      result.prose,
      /incomplete investigation|don't consider that strong evidence|nothing material changed/i
    );
  });

  it('preserves collected intelligence on partial enrichment failure', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({ fixtureMode: 'enrichment_failure', applyPriority: false }),
      loopOpts(store, aoStore)
    );
    assert.equal(result.result.status, 'partial');
    assert.ok(result.result.artifactRefs.length);
    assert.ok(result.result.errors.some((e) => e.code === 'enrichment_unavailable'));
    assert.ok(result.result.uncertainties.some((u) => /decision-maker enrichment failed/i.test(u)));
  });

  it('treats zero supported opportunities as valid completed intelligence', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({ applyPriority: false }),
      loopOpts(store, aoStore, { companies: [], people: [] })
    );
    assert.equal(result.result.status, 'completed');
    assert.match(result.result.summary, /no sufficiently supported opportunities/i);
    assert.equal(result.result.artifactRefs.length, 0);
    assert.equal(result.evaluation.materialChange, false);
  });

  it('does not autonomously broaden target criteria when results are sparse', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({ applyPriority: false }),
      loopOpts(store, aoStore, {
        companies: [
          {
            id: 'co-nashua-restaurant',
            tenantId: ANCHOR_TENANT_ID,
            name: 'Nashua Diner Group',
            industry: 'restaurant',
            location: 'Nashua, NH',
          },
        ],
        people: [],
      })
    );
    assert.equal(result.result.payload.broadened, false);
    assert.equal(result.result.artifactRefs.length, 0);
    assert.match(result.result.summary, /no sufficiently supported opportunities/i);
    assert.match(
      JSON.stringify(result.result.observations),
      /expanding geography or segment/i
    );
  });

  it('isolates tenants — Aji cannot read Anchor Scout results', async () => {
    const ajiCompanies = [
      {
        id: 'co-aji-secret',
        tenantId: '2',
        name: 'Aji Private Target',
        industry: 'property_management',
        location: 'Manchester, NH',
        signals: [
          {
            type: 'portfolio_growth',
            observedAt: '2026-08-01T00:00:00.000Z',
            label: 'Private Aji intelligence',
          },
        ],
      },
    ];
    const mixed = [...anchorCompanies(), ...ajiCompanies];
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, { companies: mixed, people: anchorPeople() })
    );
    assert.equal(
      result.result.artifactRefs.some((a) => a.companyId === 'co-aji-secret'),
      false
    );

    const leaked = await store.getResult(result.result.id, '2');
    assert.equal(leaked, null);
    const leakedDelegation = await store.getDelegation(result.delegation.id, '2');
    assert.equal(leakedDelegation, null);
  });

  it('invokes no outbound capability', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore)
    );
    assert.deepEqual(result.outboundInvoked, []);
    assert.equal(
      result.result.actionsTaken.some((a) => /send|sms|call|enroll|publish/i.test(a.text)),
      false
    );
  });

  it('traces why Acquisition moved through evaluation → result → delegation → evidence', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({ applyPriority: true }),
      loopOpts(store, aoStore, {
        priorityApplier: async () => ({ applied: true }),
      })
    );
    assert.equal(result.evaluation.materialChange, true);
    const why = await runAcquisitionIntelligenceLoop(
      {
        authorizedTenantId: ANCHOR_TENANT_ID,
        question: 'Why did Acquisition move?',
        context: { domainId: 'acquisition' },
      },
      loopOpts(store, aoStore)
    );
    assert.equal(why.delegated, false);
    assert.equal(why.kind, 'explain');
    assert.match(why.prose, /elevated Acquisition/i);
    assert.match(why.prose, /Scout/i);
    assert.match(why.prose, /don't have direct evidence|dissatisfaction|timing/i);
    assert.ok(why.trail.chain.delegation);
    assert.ok(why.trail.chain.result);
    assert.ok(why.trail.chain.evaluation);
    assert.ok(why.trail.chain.evidence.length);
  });

  it('answers follow-ups from existing intelligence and can chain a find-more delegation', async () => {
    const opts = loopOpts(store, aoStore, {
      priorityApplier: async () => ({ applied: true }),
    });
    await runAcquisitionIntelligenceLoop(anchorInput(), opts);

    const which = await runAcquisitionIntelligenceLoop(
      {
        authorizedTenantId: ANCHOR_TENANT_ID,
        question: 'Which four?',
        context: { acquisitionLoop: true, domainId: 'acquisition' },
      },
      opts
    );
    assert.equal(which.delegated, false);
    assert.match(which.prose, /Granite State|Queen City|Merrimack|Bedford/i);

    const more = await runAcquisitionIntelligenceLoop(
      {
        authorizedTenantId: ANCHOR_TENANT_ID,
        question: 'Find more like number two.',
        context: { acquisitionLoop: true, domainId: 'acquisition' },
        businessContext: anchorInput().businessContext,
        targetContext: anchorInput().targetContext,
        applyPriority: false,
      },
      opts
    );
    assert.equal(more.delegated, true);
    assert.ok(more.bounded.targetContext.priorResultId);
    assert.ok(more.bounded.targetContext.seedCompanyId);
  });

  it('does not treat CIE-style strategy questions as Scout work', () => {
    assert.equal(
      looksLikeAcquisitionQuestion('What do you think our biggest opportunity is?'),
      false
    );
    assert.equal(
      looksLikeAcquisitionQuestion(
        'Which property managers in the GTA are showing buying signals right now?'
      ),
      false
    );
    assert.equal(looksLikeAcquisitionQuestion(ANCHOR_QUESTION), true);
  });

  it('filters existing intelligence to delegated geography and segment', () => {
    const retrieved = retrieveExistingIntelligence({
      authorizedTenantId: ANCHOR_TENANT_ID,
      tenantId: ANCHOR_TENANT_ID,
      targetContext: {
        geography: 'Manchester, NH',
        segments: ['property_management'],
      },
      businessContext: { exclusions: ['restaurant'] },
      companies: anchorCompanies(),
      people: anchorPeople(),
    });
    assert.ok(retrieved.companies.every((c) => c.industry === 'property_management'));
    assert.ok(!retrieved.companies.some((c) => c.id === 'co-nashua-restaurant'));
    assert.ok(retrieved.retrievedBeforeInvestigate);
  });

  it('surfaces accepted AO intelligence on the Command Deck without Scout writing priority', async () => {
    const loop = await runAcquisitionIntelligenceLoop(
      anchorInput({ applyPriority: true }),
      loopOpts(store, aoStore, {
        priorityApplier: async () => ({ applied: true }),
      })
    );
    const signals = collectDomainSignals({
      missions: [],
      priorityQueue: [],
      watchAlerts: [],
      acquisitionIntelligence: {
        summary: loop.state.summary,
        priorityImpact: loop.state.priorityImpact,
      },
    });
    assert.equal(signals.acquisition.priority, PRIORITY_STATES.ELEVATED);
    const summary = buildDomainSummary('acquisition', signals.acquisition);
    assert.match(summary.compressed, /timely opportunit/i);

    const overview = await composeSpatialOverview({
      model: {
        meta: { generatedAt: '2026-08-16T12:00:00.000Z' },
        operations: { missions: [] },
        priorityQueue: [],
        watchAlerts: [],
      },
      acquisitionIntelligence: {
        summary: loop.state.summary,
        priorityImpact: loop.state.priorityImpact,
        resultId: loop.result.id,
      },
    });
    const acquisition = overview.domains.find((d) => d.id === 'acquisition');
    assert.equal(acquisition.priority, PRIORITY_STATES.ELEVATED);
    assert.match(acquisition.summary.compressed, /timely/i);
  });

  it('keeps Acquisition normal when Scout succeeds but Max finds no material change', async () => {
    const signals = collectDomainSignals({
      missions: [],
      priorityQueue: [],
      watchAlerts: [],
      acquisitionIntelligence: {
        summary: 'Pipeline steady',
        priorityImpact: null,
      },
    });
    assert.equal(signals.acquisition.priority, PRIORITY_STATES.NORMAL);
  });

  it('returns blocked without fabricating opportunities when discovery fails', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({ applyPriority: false }),
      loopOpts(store, aoStore, {
        companies: [],
        people: [],
        discover: async () => {
          throw new Error('Places unavailable');
        },
      })
    );
    assert.equal(result.result.status, 'blocked');
    assert.equal(result.result.artifactRefs.length, 0);
    assert.ok(result.result.errors.some((e) => e.code === 'provider_error'));
  });

  it('does not recommend Scout for an unrelated question', async () => {
    assert.equal(looksLikeAcquisitionQuestion('Draft a LinkedIn post about reviews.'), false);
    const result = await runAcquisitionIntelligenceLoop(
      {
        authorizedTenantId: ANCHOR_TENANT_ID,
        question: 'Draft a LinkedIn post about reviews.',
      },
      loopOpts(store, aoStore)
    );
    assert.equal(result.delegated, false);
    assert.equal(result.kind, 'unrelated');
  });
});
