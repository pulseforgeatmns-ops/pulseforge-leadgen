'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  createSpecialistDelegationService,
} = require('../services/specialistDelegation');
const {
  runAcquisitionIntelligenceLoop,
  createMemoryAcquisitionState,
  looksLikeInvestigationInspection,
  ANCHOR_TENANT_ID,
} = require('../services/scoutAcquisitionIntelligence');
const {
  scoreCoverage,
  coverageBand,
  classifyInspectionPresentation,
  isSystemProvenanceId,
  toBusinessEvidenceRefs,
} = require('../packages/max/scoutAcquisition/InvestigationProvenance');
const { SOURCE_TYPES: TYPE_SOURCE_TYPES } = require('../packages/max/scoutAcquisition/Types');
const {
  maybeHandleScoutAcquisitionTurn,
} = require('../packages/max/workspace/ScoutAcquisitionContext');

const ANCHOR_QUESTION =
  'Max, where should we be looking for commercial cleaning opportunities right now?';

function manyCompanies(count, extras = {}) {
  return Array.from({ length: count }, (_, i) => ({
    id: extras.idPrefix ? `${extras.idPrefix}${i}` : `co-eval-${i}`,
    tenantId: extras.tenantId || ANCHOR_TENANT_ID,
    name: extras.namePrefix ? `${extras.namePrefix} ${i + 1}` : `Manchester PM ${i + 1}`,
    industry: extras.industry || 'property_management',
    location: extras.location || 'Manchester, NH',
    website: extras.website === false ? null : `https://pm${i}.example`,
    icpScore: extras.icpScore != null ? extras.icpScore : 72,
    signals: extras.signals || extras.signalFor?.(i) || [],
  }));
}

function timelyGrowth(i) {
  return [
    {
      type: 'portfolio_growth',
      observedAt: '2026-07-12T00:00:00.000Z',
      source: 'company_website',
      label: `Portfolio grew by ${i + 2} doors this summer.`,
      observation: `Portfolio grew by ${i + 2} doors this summer.`,
    },
  ];
}

function loopOpts(store, aoStore, extras = {}) {
  const service = createSpecialistDelegationService({ store });
  return {
    delegationService: service,
    aoStore,
    companies: extras.companies || [],
    people: extras.people || [],
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
      desiredSignals: ['expansion', 'portfolio_growth', 'operational_change', 'decision_maker', 'vendor_need'],
    },
    applyPriority: false,
    ...overrides,
  };
}

describe('SPEC-099A Scout investigation provenance', () => {
  let store;
  let aoStore;

  beforeEach(() => {
    store = createMemoryStore();
    aoStore = createMemoryAcquisitionState();
  });

  it('does not treat zero + weak coverage as market absence', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        companies: manyCompanies(2, { website: false, icpScore: 60 }),
      })
    );
    const investigation = result.result.payload.investigation;
    assert.equal(investigation.coverage.supportedOpportunityCount, 0);
    assert.equal(investigation.coverageBand, 'weak');
    assert.equal(result.evaluation.marketAbsenceJustified, false);
    assert.equal(result.evaluation.materialChange, false);
    assert.match(result.prose, /incomplete investigation|don't consider that strong evidence/i);
    assert.doesNotMatch(result.prose, /reasonably confident there isn't an obvious/i);
  });

  it('can treat zero + strong coverage as meaningful negative intelligence', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        companies: manyCompanies(28, {
          icpScore: 74,
          signals: [
            {
              type: 'expansion',
              observedAt: '2023-03-01T00:00:00.000Z',
              label: 'Expanded three years ago.',
            },
          ],
        }),
      })
    );
    const investigation = result.result.payload.investigation;
    assert.equal(investigation.coverage.supportedOpportunityCount, 0);
    assert.ok(investigation.coverage.candidatesEvaluated >= 24);
    assert.equal(investigation.coverageBand, 'strong');
    assert.equal(result.evaluation.marketAbsenceJustified, true);
    assert.equal(result.evaluation.materialChange, false);
    assert.match(result.prose, /thoroughly|reasonably confident/i);
    assert.doesNotMatch(result.prose, /incomplete investigation/i);
  });

  it('recommends positive findings without claiming complete-market coverage when coverage is weak', async () => {
    const companies = manyCompanies(5, {
      website: false,
      signalFor: (i) => (i < 3 ? timelyGrowth(i) : []),
    });
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({
        fixtureMode: 'enrichment_failure',
        targetContext: {
          geography: 'Manchester, NH and Bedford, NH',
          segments: ['property_management'],
          desiredSignals: ['portfolio_growth'],
        },
        applyPriority: true,
      }),
      loopOpts(store, aoStore, {
        companies,
        priorityApplier: async () => ({ applied: true }),
      })
    );
    assert.ok(result.result.artifactRefs.length >= 3);
    assert.equal(result.result.payload.investigation.coverageBand, 'weak');
    assert.equal(result.evaluation.comparativeClaimJustified, false);
    if (result.evaluation.materialChange) {
      assert.match(result.prose, /not broad enough|best or only/i);
    }
    assert.doesNotMatch(result.prose, /the three best opportunities in Manchester/i);
  });

  it('records actual investigated geography, not merely requested scope', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({
        targetContext: {
          geography: 'Manchester, NH and Bedford, NH',
          segments: ['property_management'],
          desiredSignals: ['expansion', 'portfolio_growth'],
        },
      }),
      loopOpts(store, aoStore, {
        companies: manyCompanies(6, { location: 'Manchester, NH' }),
      })
    );
    const scope = result.result.payload.investigation.scope;
    assert.match(scope.requestedGeography, /Bedford/i);
    assert.match(scope.geography, /Manchester/i);
    assert.equal(/Bedford/i.test(scope.geography || ''), false);
    assert.ok(scope.investigatedGeography.every((g) => /manchester/i.test(g)));
  });

  it('persists the candidate funnel on the specialist result', async () => {
    const mixed = [
      ...manyCompanies(10, { location: 'Manchester, NH', icpScore: 80 }),
      {
        id: 'co-nashua-out',
        tenantId: ANCHOR_TENANT_ID,
        name: 'Nashua Offices',
        industry: 'property_management',
        location: 'Nashua, NH',
        icpScore: 90,
      },
    ];
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, { companies: mixed })
    );
    const coverage = result.result.payload.investigation.coverage;
    assert.equal(coverage.candidatesDiscovered, 11);
    assert.equal(coverage.candidatesEvaluated, 10);
    assert.ok(coverage.basicFitCount >= 1);
    assert.equal(coverage.supportedOpportunityCount, result.result.artifactRefs.length);
    const persisted = await store.getResult(result.result.id, ANCHOR_TENANT_ID);
    assert.deepEqual(persisted.payload.investigation.coverage, coverage);
  });

  it('keeps rejection reasons inspectable, including near-threshold entities', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        companies: [
          {
            id: 'co-stale-near',
            tenantId: ANCHOR_TENANT_ID,
            name: 'ABC Property Management',
            industry: 'property_management',
            location: 'Manchester, NH',
            icpScore: 84,
            signals: [
              {
                type: 'expansion',
                observedAt: '2024-01-01T00:00:00.000Z',
                label: 'Announced portfolio expansion.',
              },
            ],
          },
          {
            id: 'co-fit-only',
            tenantId: ANCHOR_TENANT_ID,
            name: 'Fit Only LLC',
            industry: 'property_management',
            location: 'Manchester, NH',
            icpScore: 70,
          },
        ],
      })
    );
    const investigation = result.result.payload.investigation;
    const reasons = investigation.rejectionSummary.map((row) => row.reason);
    assert.ok(reasons.includes('stale_evidence') || reasons.includes('no_timing_signal'));
    assert.ok(investigation.nearThreshold.some((row) => /ABC Property Management/i.test(row.company)));
    assert.match(
      investigation.nearThreshold[0].rejectedBecause,
      /month|timing|operational signal/i
    );
  });

  it('represents unavailable source classes explicitly, including future social perception', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, { companies: manyCompanies(3) })
    );
    const sources = result.result.payload.investigation.sources;
    assert.ok(sources.sourceTypesChecked.includes(TYPE_SOURCE_TYPES.EXISTING_PF));
    assert.ok(sources.sourceTypesUnavailable.includes(TYPE_SOURCE_TYPES.LINKEDIN));
    assert.ok(sources.sourceTypesUnavailable.includes(TYPE_SOURCE_TYPES.FACEBOOK));
    assert.ok(sources.sourceTypesUnavailable.includes(TYPE_SOURCE_TYPES.INSTAGRAM));
    assert.equal(sources.perception.linkedin, 'unavailable');
    assert.equal(sources.perception.facebook, 'unavailable');
    assert.equal(sources.perception.instagram, 'unavailable');
    assert.ok(
      result.result.payload.investigation.limitations.some((line) => /LinkedIn/i.test(line))
    );
  });

  it('preserves successful investigation work across provider failure instead of collapsing to a bare status', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput({ fixtureMode: 'enrichment_failure' }),
      loopOpts(store, aoStore, {
        companies: manyCompanies(8, { signalFor: (i) => (i < 3 ? timelyGrowth(i) : []) }),
      })
    );
    assert.equal(result.result.status, 'partial');
    const coverage = result.result.payload.investigation.coverage;
    assert.equal(coverage.candidatesDiscovered, 8);
    assert.equal(coverage.candidatesEvaluated, 8);
    assert.equal(coverage.unresolvedCount, 8);
    assert.ok(result.result.artifactRefs.length >= 3);
    assert.ok(result.result.errors.some((e) => e.code === 'enrichment_unavailable'));
    assert.ok(result.result.payload.investigation.limitations.some((line) => /enrichment failed/i.test(line)));
    assert.notEqual(JSON.stringify(result.result.payload.investigation), '"partial"');
  });

  it('keeps result confidence distinct from coverageConfidence', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        companies: manyCompanies(28, {
          signals: [
            {
              type: 'expansion',
              observedAt: '2023-03-01T00:00:00.000Z',
              label: 'Expanded three years ago.',
            },
          ],
        }),
      })
    );
    assert.ok(result.result.confidence != null);
    assert.ok(result.result.payload.coverageConfidence != null);
    assert.equal(
      result.result.payload.coverageConfidence,
      result.result.payload.investigation.coverageConfidence
    );
    assert.equal(result.evaluation.coverageConfidence, result.result.payload.coverageConfidence);
    assert.notEqual(result.result.confidence, result.result.payload.coverageConfidence);
  });

  it('does not label system provenance as business evidence', async () => {
    const presented = classifyInspectionPresentation({
      supportingEvidence: [],
      confidenceContributors: ['scout_acquisition', 'spec_100'],
      provenance: [
        { id: 'scout_acquisition', kind: 'capability', label: 'Scout acquisition intelligence' },
        { id: 'spec_100', kind: 'spec', label: 'SPEC-100' },
      ],
      investigation: { coverageBand: 'weak', coverageConfidence: 0.3, coverage: {} },
    });
    assert.equal(presented.evidenceCount, 0);
    assert.match(presented.summary, /Investigation/);
    assert.match(presented.summary, /Provenance/);
    assert.doesNotMatch(presented.summary, /Evidence · 2/);
    assert.equal(isSystemProvenanceId('scout_acquisition'), true);
    assert.equal(isSystemProvenanceId('spec_100'), true);
    assert.deepEqual(
      toBusinessEvidenceRefs([
        { id: 'scout_acquisition', label: 'scout_acquisition' },
        {
          id: 'ev-1',
          label: 'Company website lists 37 managed properties.',
          sourceKind: 'observed_fact',
        },
      ]).map((e) => e.id),
      ['ev-1']
    );
  });

  it('answers investigation questions from durable data rather than speculation', async () => {
    const first = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, {
        companies: manyCompanies(4, { website: false, icpScore: 68 }),
      })
    );
    assert.equal(first.delegated, true);
    assert.ok(first.state.investigation);

    const questions = [
      'What did Scout actually investigate?',
      'How thorough was the search?',
      'Why did he find nothing?',
      'How many companies were considered?',
      'What eliminated them?',
      'Where was Scout\'s coverage weak?',
      'Do you trust the conclusion?',
      'What would you investigate next?',
    ];
    for (const question of questions) {
      assert.equal(looksLikeInvestigationInspection(question), true);
      const answer = await runAcquisitionIntelligenceLoop(
        {
          authorizedTenantId: ANCHOR_TENANT_ID,
          question,
          context: { acquisitionLoop: true, domainId: 'acquisition' },
        },
        loopOpts(store, aoStore)
      );
      assert.equal(answer.delegated, false);
      assert.ok(answer.prose);
      assert.doesNotMatch(answer.prose, /I do not yet have durable investigation provenance/i);
    }

    const thorough = await runAcquisitionIntelligenceLoop(
      {
        authorizedTenantId: ANCHOR_TENANT_ID,
        question: 'How thorough was the search?',
        context: { acquisitionLoop: true },
      },
      loopOpts(store, aoStore)
    );
    assert.match(thorough.prose, /evaluated 4|discovered 4/i);

    const trust = await runAcquisitionIntelligenceLoop(
      {
        authorizedTenantId: ANCHOR_TENANT_ID,
        question: 'Do you trust the conclusion?',
        context: { acquisitionLoop: true },
      },
      loopOpts(store, aoStore)
    );
    assert.match(trust.prose, /incomplete investigation|don't treat Scout's conclusion/i);
  });

  it('keeps investigation provenance tenant-scoped', async () => {
    const mixed = [
      ...manyCompanies(4, { tenantId: ANCHOR_TENANT_ID }),
      ...manyCompanies(3, {
        tenantId: '2',
        idPrefix: 'co-aji-',
        namePrefix: 'Aji Secret',
        location: 'Manchester, NH',
        signalFor: () => timelyGrowth(1),
      }),
    ];
    const result = await runAcquisitionIntelligenceLoop(
      anchorInput(),
      loopOpts(store, aoStore, { companies: mixed })
    );
    const names = JSON.stringify(result.result.payload.investigation);
    assert.doesNotMatch(names, /Aji Secret/);
    assert.equal(result.result.payload.investigation.coverage.candidatesDiscovered, 4);
    assert.equal(await store.getResult(result.result.id, '2'), null);
    assert.equal(await aoStore.get('2'), null);
    assert.ok(await aoStore.get(ANCHOR_TENANT_ID));
  });

  it('scores coverage from evidence contributors instead of an LLM self-score', () => {
    const strong = scoreCoverage({
      requestedGeography: 'Manchester, NH',
      investigatedGeography: ['Manchester, NH'],
      requestedSegments: ['property_management'],
      investigatedSegments: ['property_management'],
      sourceTypesChecked: [
        TYPE_SOURCE_TYPES.EXISTING_PF,
        TYPE_SOURCE_TYPES.COMPANY_WEBSITES,
      ],
      sourceTypesUnavailable: [
        TYPE_SOURCE_TYPES.LINKEDIN,
        TYPE_SOURCE_TYPES.FACEBOOK,
        TYPE_SOURCE_TYPES.INSTAGRAM,
      ],
      candidatesEvaluated: 28,
      candidatesDiscovered: 28,
      timelyEvidenceCount: 0,
      unresolvedCount: 0,
    });
    const weak = scoreCoverage({
      requestedGeography: 'Manchester, NH and Bedford, NH',
      investigatedGeography: ['Manchester, NH'],
      requestedSegments: ['property_management'],
      investigatedSegments: ['property_management'],
      sourceTypesChecked: [TYPE_SOURCE_TYPES.EXISTING_PF],
      sourceTypesUnavailable: [
        TYPE_SOURCE_TYPES.ENRICHMENT_PROVIDER,
        TYPE_SOURCE_TYPES.LINKEDIN,
        TYPE_SOURCE_TYPES.FACEBOOK,
        TYPE_SOURCE_TYPES.INSTAGRAM,
      ],
      candidatesEvaluated: 2,
      candidatesDiscovered: 2,
      enrichmentAttempted: true,
      enrichmentFailureRate: 1,
      timelyEvidenceCount: 0,
      unresolvedCount: 2,
    });
    assert.equal(coverageBand(strong), 'strong');
    assert.equal(coverageBand(weak), 'weak');
    assert.ok(strong > weak);
  });
});

describe('SPEC-099A Max workspace presentation', () => {
  it('puts business observations in evidence and system lineage in provenance', async () => {
    const store = createMemoryStore();
    const service = createSpecialistDelegationService({ store });
    const aoStore = createMemoryAcquisitionState();
    const session = { id: 'sess-anchor', context: { tenantId: ANCHOR_TENANT_ID } };
    const turn = await maybeHandleScoutAcquisitionTurn({
      question: ANCHOR_QUESTION,
      session,
      context: {
        tenantId: ANCHOR_TENANT_ID,
        businessContext: {
          serviceGeography: 'Manchester, NH',
          preferredSegments: ['property_management'],
        },
        targetContext: {
          geography: 'Manchester, NH',
          segments: ['property_management'],
        },
      },
      delegationService: service,
      aoStore,
      companies: manyCompanies(3, { signalFor: timelyGrowth }),
      people: [],
    });
    assert.ok(turn);
    assert.ok(turn.structured.investigation);
    assert.ok(turn.structured.provenance.some((p) => p.id === 'scout_acquisition'));
    assert.ok(turn.structured.provenance.some((p) => p.id === 'spec_099a'));
    assert.equal(turn.structured.confidenceContributors.includes('scout_acquisition'), false);
    assert.equal(turn.structured.confidenceContributors.includes('spec_100'), false);
    for (const ev of turn.structured.supportingEvidence) {
      assert.equal(isSystemProvenanceId(ev.id), false);
      assert.equal(isSystemProvenanceId(ev.summary), false);
    }
    assert.equal(turn.structured.metadata.evidenceCount, turn.structured.supportingEvidence.length);
    assert.ok(session.context.lastScoutInvestigation);
  });
});
