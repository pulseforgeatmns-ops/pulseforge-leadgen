'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  createSpecialistDelegationService,
  composeCognitiveTrace,
  classifyOperatorIntent,
  resolveRecentReferent,
  looksLikeInterrogation,
  looksLikeNewInvestigation,
  answerFromTrace,
  INTENT,
  FAILURE_BOUNDARIES,
} = require('../services/specialistDelegation');
const {
  runAcquisitionIntelligenceLoop,
  createMemoryAcquisitionState,
  ANCHOR_TENANT_ID,
} = require('../services/scoutAcquisitionIntelligence');

const GEO_QUESTION =
  "Why couldn't Scout resolve the geography? What geographic information did you give him for this investigation?";

function loopOpts(store, aoStore, extras = {}) {
  const service = createSpecialistDelegationService({ store });
  return {
    delegationService: service,
    aoStore,
    companies: extras.companies || [],
    people: extras.people || [],
    discover: extras.discover,
    freshnessMs: extras.freshnessMs,
  };
}

function geographyBlockedDelegation(overrides = {}) {
  return {
    id: 'del-geo-1',
    tenantId: ANCHOR_TENANT_ID,
    specialist: 'scout',
    capability: 'acquisition_intelligence',
    objective: 'Find current commercial-cleaning opportunities.',
    reason: 'Pipeline intelligence was insufficient.',
    createdAt: '2026-08-17T12:00:00.000Z',
    businessContext: {
      commercialCapability: 'commercial_cleaning',
      preferredSegments: ['property_management', 'office', 'daycare'],
    },
    targetContext: {
      geography: null,
      segments: ['property_management', 'office', 'daycare'],
      businessType: 'commercial_cleaning',
    },
    constraints: {},
    ...overrides,
  };
}

function geographyBlockedResult(overrides = {}) {
  return {
    id: 'res-geo-1',
    delegationId: 'del-geo-1',
    tenantId: ANCHOR_TENANT_ID,
    specialist: 'scout',
    capability: 'acquisition_intelligence',
    status: 'blocked',
    summary: 'Geography could not be resolved.',
    observations: [],
    actionsTaken: [{ text: 'Attempted to resolve the acquisition search definition.' }],
    evidenceRefs: [],
    uncertainties: ['Geography could not be resolved.'],
    errors: [{ code: 'invalid_target', message: 'Geography could not be resolved.' }],
    payload: {
      searchDefinition: {
        valid: false,
        invalidReason: 'Geography could not be resolved.',
        geography: null,
        segments: ['property_management'],
      },
      consumedContext: {
        geography: null,
        geographyResolved: false,
        segments: ['property_management'],
        valid: false,
        invalidReason: 'Geography could not be resolved.',
      },
      investigation: {
        scope: { geography: null, requestedGeography: null, segments: [] },
        coverage: {
          candidatesDiscovered: 0,
          candidatesEvaluated: 0,
          basicFitCount: 0,
          signalBearingCount: 0,
          supportedOpportunityCount: 0,
        },
        coverageBand: 'weak',
        coverageConfidence: 0.16,
        limitations: ['Geography could not be resolved.'],
        sources: { sourceTypesChecked: [], sourceTypesUnavailable: [] },
      },
    },
    ...overrides,
  };
}

function maxEvaluation(overrides = {}) {
  return {
    id: 'eval-geo-1',
    tenantId: ANCHOR_TENANT_ID,
    delegationId: 'del-geo-1',
    resultId: 'res-geo-1',
    materialChange: false,
    materiality: 'immaterial',
    acceptedAsGroundTruth: false,
    conclusionTrust: 'low',
    coverageBand: 'weak',
    coverageConfidence: 0.16,
    acceptedClaims: [],
    rejectedClaims: [],
    explanation:
      'Scout could not construct a candidate universe for this investigation. Geography could not be resolved. Zero companies evaluated is a discovery limitation, not evidence that no market opportunity exists. I am not elevating Acquisition.',
    createdAt: '2026-08-17T12:01:00.000Z',
    ...overrides,
  };
}

describe('SPEC-101 specialist cognitive trace', () => {
  describe('intent recognition', () => {
    it('recognizes interrogation vs a new investigation', () => {
      assert.equal(looksLikeInterrogation(GEO_QUESTION), true);
      assert.equal(looksLikeNewInvestigation(GEO_QUESTION), false);
      assert.equal(
        classifyOperatorIntent(GEO_QUESTION, [{ specialist: 'scout' }]).kind,
        INTENT.INTERROGATE
      );
      assert.equal(
        classifyOperatorIntent('Find commercial cleaning opportunities.', []).kind,
        INTENT.NEW_INVESTIGATION
      );
    });
  });

  describe('referent resolution', () => {
    it('resolves Scout / that investigation to the recent Scout trace', () => {
      const traces = [
        composeCognitiveTrace({
          delegation: geographyBlockedDelegation(),
          result: geographyBlockedResult(),
          evaluation: maxEvaluation(),
        }),
      ];
      const resolved = resolveRecentReferent({
        traces,
        question: "Why didn't Scout find anything?",
        specialist: 'scout',
      });
      assert.equal(resolved.status, 'resolved');
      assert.equal(resolved.trace.specialist, 'scout');
    });

    it('disambiguates multiple Scout investigations instead of inspecting the wrong one', () => {
      const commercial = composeCognitiveTrace({
        delegation: geographyBlockedDelegation({
          id: 'del-a',
          objective: 'commercial-cleaning market investigation',
        }),
        result: geographyBlockedResult({ id: 'res-a', delegationId: 'del-a' }),
      });
      const property = composeCognitiveTrace({
        delegation: geographyBlockedDelegation({
          id: 'del-b',
          objective: 'property-manager investigation in Nashua',
          createdAt: '2026-08-17T12:05:00.000Z',
        }),
        result: geographyBlockedResult({ id: 'res-b', delegationId: 'del-b' }),
      });
      const resolved = resolveRecentReferent({
        traces: [commercial, property],
        question: 'What did Scout actually investigate?',
        specialist: 'scout',
      });
      assert.equal(resolved.status, 'ambiguous');
      assert.equal(resolved.candidates.length, 2);
    });
  });

  describe('available vs supplied vs consumed', () => {
    it('classifies a delegation failure when Max knew geography but did not send it', () => {
      const trace = composeCognitiveTrace({
        availableContext: {
          recorded: true,
          business: 'Anchor Cleaning',
          serviceArea: 'Manchester, NH',
          segments: ['property_management'],
        },
        delegation: geographyBlockedDelegation(),
        result: geographyBlockedResult(),
        evaluation: maxEvaluation(),
      });
      assert.equal(trace.availableContext.serviceArea, 'Manchester, NH');
      assert.equal(trace.suppliedContext.geography, null);
      assert.equal(trace.consumedContext.geographyResolved, false);
      assert.equal(trace.failure.boundary, FAILURE_BOUNDARIES.DELEGATION);

      const answered = answerFromTrace({
        trace,
        question: GEO_QUESTION,
        intent: { kind: INTENT.INTERROGATE, topic: 'geography' },
      });
      assert.match(answered.prose, /delegation failure|wasn't included/i);
      assert.match(answered.prose, /Manchester/i);
      assert.doesNotMatch(
        answered.prose,
        /couldn't construct a candidate universe because geography couldn't be resolved/i
      );
      assert.equal(answered.rerun, false);
    });

    it('classifies a specialist interpretation failure when Max supplied geography and Scout did not consume it', () => {
      const trace = composeCognitiveTrace({
        availableContext: {
          recorded: true,
          serviceArea: 'Manchester, NH',
        },
        delegation: geographyBlockedDelegation({
          targetContext: {
            geography: 'Manchester, NH',
            segments: ['property_management'],
          },
          businessContext: { serviceGeography: 'Manchester, NH' },
        }),
        result: geographyBlockedResult(),
        evaluation: maxEvaluation(),
      });
      assert.equal(trace.suppliedContext.geography, 'Manchester, NH');
      assert.equal(trace.consumedContext.geographyResolved, false);
      assert.equal(trace.failure.boundary, FAILURE_BOUNDARIES.SPECIALIST_INTERPRETATION);

      const answered = answerFromTrace({
        trace,
        question: GEO_QUESTION,
        intent: { kind: INTENT.INTERROGATE, topic: 'geography' },
      });
      assert.match(answered.prose, /supplied Manchester/i);
      assert.match(answered.prose, /inside Scout|resolver|search definition/i);
    });

    it('keeps unknown cause unknown when the available layer was never recorded', () => {
      const trace = composeCognitiveTrace({
        delegation: geographyBlockedDelegation(),
        result: geographyBlockedResult(),
        evaluation: maxEvaluation(),
      });
      assert.equal(trace.availableContext.recorded, false);
      assert.equal(trace.failure.boundary, FAILURE_BOUNDARIES.UNKNOWN);

      const answered = answerFromTrace({
        trace,
        question: GEO_QUESTION,
        intent: { kind: INTENT.INTERROGATE, topic: 'geography' },
      });
      assert.match(answered.prose, /can't tell|don't have enough evidence|doesn't preserve/i);
      assert.doesNotMatch(answered.prose, /I omitted Manchester/i);
      assert.doesNotMatch(
        answered.prose,
        /couldn't construct a candidate universe because geography couldn't be resolved/i
      );
    });
  });

  describe('Max evaluation and evidence layers', () => {
    it('explains why Acquisition was not elevated from the persisted judgment', () => {
      const trace = composeCognitiveTrace({
        availableContext: { recorded: true, serviceArea: 'Manchester, NH' },
        delegation: geographyBlockedDelegation(),
        result: geographyBlockedResult(),
        evaluation: maxEvaluation(),
      });
      const answered = answerFromTrace({
        trace,
        question: 'Why did you decide not to elevate Acquisition?',
        intent: { kind: INTENT.INTERROGATE, topic: 'max_judgment' },
      });
      assert.match(answered.prose, /didn't change Acquisition's priority/i);
      assert.match(answered.prose, /zero|never constructed/i);
      assert.doesNotMatch(answered.prose, /materiality: immaterial/i);
    });

    it('keeps business evidence distinct from investigation and system provenance', () => {
      const trace = composeCognitiveTrace({
        delegation: geographyBlockedDelegation(),
        result: geographyBlockedResult({
          evidenceRefs: [
            { id: 'spec_100', kind: 'spec', label: 'SPEC-100', sourceKind: 'system' },
          ],
        }),
        evaluation: maxEvaluation(),
      });
      const answered = answerFromTrace({
        trace,
        question: 'What evidence did Scout use?',
        intent: { kind: INTENT.INTERROGATE, topic: 'evidence' },
      });
      assert.match(answered.prose, /no business evidence/i);
      assert.match(answered.prose, /investigation provenance/i);
      assert.match(answered.prose, /system provenance/i);
    });

    it('does not invent a relationship between a prospect list and Scout\'s investigation', () => {
      const trace = composeCognitiveTrace({
        delegation: geographyBlockedDelegation(),
        result: geographyBlockedResult(),
        evaluation: maxEvaluation(),
      });
      const answered = answerFromTrace({
        trace,
        question: 'You detected two companies in the prospect list. Why weren\'t those evaluated by Scout?',
        intent: { kind: INTENT.INTERROGATE, topic: 'prospect_list' },
        detectedCompanyCount: 2,
      });
      assert.match(answered.prose, /separate surface|won't invent|won't fabricate/i);
      assert.match(answered.prose, /zero companies|candidate universe/i);
    });
  });

  describe('loop integration', () => {
    let store;
    let aoStore;

    beforeEach(() => {
      store = createMemoryStore();
      aoStore = createMemoryAcquisitionState();
    });

    it('does not rerun Scout when the operator interrogates a completed investigation', async () => {
      const first = await runAcquisitionIntelligenceLoop(
        {
          authorizedTenantId: ANCHOR_TENANT_ID,
          question: 'Max, where should we be looking for commercial cleaning opportunities right now?',
          objective: 'Find commercial cleaning opportunities.',
          reason: 'Need current market intelligence.',
          businessContext: {
            serviceGeography: 'Manchester, NH',
            commercialCapability: 'commercial_cleaning',
            preferredSegments: ['property_management'],
          },
          targetContext: {
            geography: 'Manchester, NH',
            segments: ['property_management'],
            businessType: 'commercial_cleaning',
          },
        },
        loopOpts(store, aoStore, {
          companies: [
            {
              id: 'co-1',
              tenantId: ANCHOR_TENANT_ID,
              name: 'Granite PM',
              industry: 'property_management',
              location: 'Manchester, NH',
              icpScore: 70,
            },
          ],
        })
      );
      assert.equal(first.delegated, true);
      const before = (await store.listDelegations({ tenantId: ANCHOR_TENANT_ID })).length;

      const follow = await runAcquisitionIntelligenceLoop(
        {
          authorizedTenantId: ANCHOR_TENANT_ID,
          question: GEO_QUESTION,
          context: { acquisitionLoop: true, domainId: 'acquisition' },
        },
        loopOpts(store, aoStore)
      );
      assert.equal(follow.delegated, false);
      assert.equal(follow.kind, 'interrogate');
      assert.equal(follow.outboundInvoked.length, 0);
      const after = (await store.listDelegations({ tenantId: ANCHOR_TENANT_ID })).length;
      assert.equal(after, before);
      assert.doesNotMatch(
        follow.prose,
        /couldn't construct a candidate universe because geography couldn't be resolved/i
      );
    });

    it('survives a store snapshot the way a refresh/restart would', async () => {
      await runAcquisitionIntelligenceLoop(
        {
          authorizedTenantId: ANCHOR_TENANT_ID,
          question: 'Max, where should we be looking for commercial cleaning opportunities right now?',
          objective: 'Find commercial cleaning opportunities.',
          reason: 'Need current market intelligence.',
          businessContext: {
            serviceGeography: 'Manchester, NH',
            commercialCapability: 'commercial_cleaning',
          },
          targetContext: {
            geography: 'Manchester, NH',
            segments: ['property_management'],
            businessType: 'commercial_cleaning',
          },
        },
        loopOpts(store, aoStore)
      );
      const snapshot = store.serialize();
      const revived = createMemoryStore(snapshot);
      const follow = await runAcquisitionIntelligenceLoop(
        {
          authorizedTenantId: ANCHOR_TENANT_ID,
          question: 'What geographic information did you give him for this investigation?',
          context: { acquisitionLoop: true },
        },
        loopOpts(revived, createMemoryAcquisitionState())
      );
      assert.equal(follow.delegated, false);
      assert.equal(follow.kind, 'interrogate');
      assert.match(follow.prose, /Manchester|did not include|gave/i);
    });

    it('does not leak another tenant\'s specialist trace', async () => {
      await runAcquisitionIntelligenceLoop(
        {
          authorizedTenantId: ANCHOR_TENANT_ID,
          question: 'Max, where should we be looking for commercial cleaning opportunities right now?',
          objective: 'Find commercial cleaning opportunities.',
          reason: 'Need current market intelligence.',
          businessContext: { serviceGeography: 'Manchester, NH' },
          targetContext: { geography: 'Manchester, NH', segments: ['property_management'] },
        },
        loopOpts(store, aoStore)
      );
      const leaked = await runAcquisitionIntelligenceLoop(
        {
          authorizedTenantId: '2',
          question: GEO_QUESTION,
          context: { acquisitionLoop: true },
        },
        loopOpts(store, aoStore)
      );
      assert.equal(leaked.delegated, false);
      assert.match(leaked.prose, /don't have an inspectable trace|won't invent/i);
      assert.doesNotMatch(leaked.prose, /Manchester/i);
    });

    it('persists available vs supplied context on a live Scout delegation', async () => {
      const result = await runAcquisitionIntelligenceLoop(
        {
          authorizedTenantId: ANCHOR_TENANT_ID,
          question: 'Max, where should we be looking for commercial cleaning opportunities right now?',
          objective: 'Find commercial cleaning opportunities.',
          reason: 'Need current market intelligence.',
          businessContext: {
            serviceGeography: 'Manchester, NH',
            commercialCapability: 'commercial_cleaning',
          },
          targetContext: {
            geography: 'Manchester, NH',
            segments: ['property_management'],
            businessType: 'commercial_cleaning',
          },
          context: {
            clientIntelligence: {
              businessName: 'Anchor Cleaning',
              geography: 'Manchester, NH',
            },
          },
        },
        loopOpts(store, aoStore)
      );
      assert.equal(result.delegated, true);
      assert.equal(result.availableContext.serviceArea, 'Manchester, NH');
      assert.equal(result.delegation.availableContext.serviceArea, 'Manchester, NH');
      assert.equal(result.delegation.targetContext.geography, 'Manchester, NH');
      assert.ok(result.result.payload.consumedContext);
      assert.equal(result.result.payload.consumedContext.geography, 'Manchester, NH');
    });

    it('lets the generic test_intelligence specialist satisfy the same trace contract', async () => {
      const service = createSpecialistDelegationService({ store });
      const { delegation, result } = await service.delegateAndExecute({
        authorizedTenantId: '1',
        tenantId: '1',
        specialist: 'test_intelligence',
        capability: 'acquisition_assessment',
        objective: 'Assess whether Acquisition currently has meaningful opportunity.',
        reason: 'Contract fixture.',
        authority: 'observe',
        targetContext: { geography: 'Manchester, NH', segments: ['property_management'] },
        availableContext: {
          recorded: true,
          serviceArea: 'Manchester, NH',
          business: 'Anchor Cleaning',
        },
      });
      const evaluation = await service.evaluateResult({
        authorizedTenantId: '1',
        resultId: result.id,
      });
      const trace = composeCognitiveTrace({ delegation, result, evaluation });
      assert.equal(trace.specialist, 'test_intelligence');
      assert.equal(trace.suppliedContext.geography, 'Manchester, NH');
      assert.equal(trace.consumedContext.geography, 'Manchester, NH');
      const answered = answerFromTrace({
        trace,
        question: 'Why did you trust that conclusion?',
        intent: { kind: INTENT.INTERROGATE, topic: 'trust' },
      });
      assert.match(answered.prose, /trust|ground truth|evidence/i);
      assert.equal(answered.rerun, false);
    });
  });
});
