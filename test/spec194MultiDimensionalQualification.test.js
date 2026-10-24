'use strict';

/**
 * SPEC-194 — Multi-dimensional prospect qualification acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { qualifyCandidate } = require('../packages/max/scoutAcquisition/InvestigationProvenance');
const {
  READINESS_STATES,
  EVIDENCE_KINDS,
  QUALIFICATION_STATUSES,
  PROSPECT_BUCKETS,
} = require('../packages/max/scoutAcquisition/Types');
const {
  buildProspectEvaluation,
  detectNegativeSegmentEvidence,
  businessFitQualifiedCount,
} = require('../packages/max/scoutAcquisition/ProspectEvaluation');
const { attachFitToClassified, evaluateBasicFit } = require('../packages/max/scoutAcquisition/FitEvaluation');
const {
  normalizeScoutDiscoveryPayload,
  hasSufficientEvidenceForPrioritization,
} = require('../packages/acquisition-mission/DiscoveryPayload');
const { evaluatePrioritizationReadiness } = require('../packages/acquisition-mission/DecisionReadiness');
const { buildAcquisitionSearchDefinition } = require('../services/scoutAcquisitionIntelligence');

const NOW = new Date('2026-08-27T12:00:00.000Z').getTime();

function propertyManagementSearchDefinition(includeStr = false) {
  const segments = includeStr ? ['property_management', 'short_term_rental'] : ['property_management'];
  return buildAcquisitionSearchDefinition({
    tenantId: '10',
    targetContext: {
      geography: 'Manchester, NH',
      segments,
      businessType: 'commercial_cleaning',
    },
    businessContext: {
      serviceGeography: 'Manchester, NH',
      commercialCapability: 'commercial_cleaning',
      preferredSegments: ['property_management'],
    },
  });
}

describe('SPEC-194 — multi-dimensional prospect qualification', () => {
  it('Lot 202: qualified with unknown readiness when timing is absent', () => {
    const classified = {
      name: 'Lot 202 Property Management',
      fit: 0.81,
      signals: [],
      observations: [{ text: 'Property management operator in Manchester.' }],
      evidenceRefs: [{ id: 'ev-lot', label: 'Google Places listing', sourceKind: 'observed_fact' }],
    };
    const company = {
      id: 'co-lot-202',
      name: 'Lot 202 Property Management',
      industry: 'property_management',
      location: 'Manchester, NH',
      website: 'https://lot202.example',
      icpScore: 74,
    };
    const searchDefinition = propertyManagementSearchDefinition();
    const attached = attachFitToClassified(classified, company, searchDefinition, NOW);
    const evaluation = attached.evaluation;

    assert.ok(
      [QUALIFICATION_STATUSES.QUALIFIED, QUALIFICATION_STATUSES.UNCERTAIN].includes(
        evaluation.qualification.status
      )
    );
    assert.equal(evaluation.readiness.status, READINESS_STATES.UNKNOWN);
    assert.ok(
      [PROSPECT_BUCKETS.INVESTIGATION_REQUIRED, PROSPECT_BUCKETS.FIT_INVESTIGATION].includes(
        evaluation.bucket
      )
    );
    assert.ok(evaluation.investigation.unresolvedHypotheses.length > 0);
    assert.equal(attached.qualification.supported, false);
    assert.equal(attached.qualification.reason, null);
  });

  it('21 qualified / 0 ready / 21 unknown readiness reporting shape', () => {
    const fitCandidates = Array.from({ length: 21 }, (_, i) => ({
      companyId: `co-${i + 1}`,
      name: `Qualified PM ${i + 1}`,
      fit: 0.78,
      qualified: true,
      qualificationStatus: QUALIFICATION_STATUSES.QUALIFIED,
      readinessState: READINESS_STATES.UNKNOWN,
      signals: [],
      evidenceRefs: [
        {
          id: `ev-${i + 1}`,
          label: 'Google Places listing',
          snapshot: { source: 'google_places', companyName: `Qualified PM ${i + 1}` },
        },
      ],
    }));

    const scoutResult = {
      status: 'completed',
      summary: '21 qualified prospects; timing unknown.',
      payload: {
        opportunities: [],
        fitCandidates,
        qualifiedCount: 21,
        readinessReadyCount: 0,
        readinessUnknownCount: 21,
        readinessNotReadyCount: 0,
      },
    };

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assert.equal(payload.qualifiedCount, 21);
    assert.equal(payload.readinessReadyCount, 0);
    assert.equal(payload.readinessUnknownCount, 21);
    assert.equal(payload.rankedProspects.length, 21);
    assert.ok(payload.rankedProspects.every((row) => row.readinessState === READINESS_STATES.UNKNOWN));
    assert.ok(hasSufficientEvidenceForPrioritization({ ...payload, blocked: false, discoveryStatus: 'complete' }));
  });

  it('negative segment evidence disqualifies; missing STR evidence stays uncertain', () => {
    assert.ok(detectNegativeSegmentEvidence('No vacation rentals / residential management only'));

    const negativeCompany = {
      id: 'co-negative',
      name: 'Commercial PM Residential Only',
      industry: 'property_management',
      location: 'Manchester, NH',
      snippet: 'No vacation rentals / residential management',
      icpScore: 68,
    };
    const negativeEval = buildProspectEvaluation({
      candidate: negativeCompany,
      classified: { name: negativeCompany.name, companyId: negativeCompany.id, signals: [], observations: [] },
      fit: evaluateBasicFit(negativeCompany, propertyManagementSearchDefinition()),
      qualification: qualifyCandidate({ name: negativeCompany.name, fit: 0.68, signals: [] }, negativeCompany, NOW),
      searchDefinition: propertyManagementSearchDefinition(),
    });
    assert.equal(negativeEval.qualification.status, QUALIFICATION_STATUSES.NOT_QUALIFIED);
    assert.equal(negativeEval.bucket, PROSPECT_BUCKETS.EXCLUDED);

    const incompleteCompany = {
      id: 'co-incomplete',
      name: 'Portfolio PM Co',
      industry: 'property_management',
      location: 'Manchester, NH',
      snippet: 'Manages multifamily portfolio in Manchester',
      icpScore: 72,
    };
    const incompleteEval = buildProspectEvaluation({
      candidate: incompleteCompany,
      classified: {
        name: incompleteCompany.name,
        companyId: incompleteCompany.id,
        fit: 0.72,
        signals: [],
        observations: [{ text: incompleteCompany.snippet }],
      },
      fit: evaluateBasicFit(incompleteCompany, propertyManagementSearchDefinition(true)),
      qualification: qualifyCandidate(
        { name: incompleteCompany.name, fit: 0.72, signals: [], observations: [{ text: incompleteCompany.snippet }] },
        incompleteCompany,
        NOW
      ),
      searchDefinition: propertyManagementSearchDefinition(true),
    });
    assert.equal(incompleteEval.qualification.status, QUALIFICATION_STATUSES.UNCERTAIN);
    assert.notEqual(incompleteEval.qualification.status, QUALIFICATION_STATUSES.NOT_QUALIFIED);
  });

  it('stale timing is not_ready nurture, not disqualification', () => {
    const classified = {
      name: 'Stale Signal PM',
      fit: 0.8,
      signals: [
        {
          type: 'expansion',
          observedAt: '2023-01-01T00:00:00.000Z',
          label: 'Expanded portfolio.',
        },
      ],
      observations: [{ text: 'Portfolio operator.' }],
      evidenceRefs: [{ id: 'ev-2', label: 'Website', sourceKind: 'observed_fact' }],
    };
    const company = {
      id: 'co-stale',
      name: 'Stale Signal PM',
      industry: 'property_management',
      location: 'Manchester, NH',
      icpScore: 76,
    };
    const attached = attachFitToClassified(classified, company, propertyManagementSearchDefinition(), NOW);

    assert.equal(attached.evaluation.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);
    assert.equal(attached.evaluation.readiness.status, READINESS_STATES.NOT_READY);
    assert.equal(attached.evaluation.bucket, PROSPECT_BUCKETS.NURTURE);
    assert.equal(attached.qualification.qualified, true);
    assert.equal(attached.qualification.supported, false);
    assert.equal(attached.qualification.evidenceKind, EVIDENCE_KINDS.NEGATIVE_EVIDENCE);
  });

  it('insufficient evidence never becomes negative evidence for unknown readiness', () => {
    const result = qualifyCandidate(
      { name: 'Unknown Timing Co', fit: 0.77, signals: [], observations: [{ text: 'Good fit.' }] },
      { name: 'Unknown Timing Co', icpScore: 72 },
      NOW
    );
    assert.equal(result.qualified, true);
    assert.equal(result.readinessState, READINESS_STATES.UNKNOWN);
    assert.equal(result.evidenceKind, EVIDENCE_KINDS.INSUFFICIENT_EVIDENCE);
    assert.notEqual(result.evidenceKind, EVIDENCE_KINDS.NEGATIVE_EVIDENCE);
  });

  it('qualifiedCount reflects business fit, not timing signals', () => {
    const evaluations = [
      {
        qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
        readiness: { status: READINESS_STATES.UNKNOWN },
      },
      {
        qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
        readiness: { status: READINESS_STATES.NOT_READY },
      },
      {
        qualification: { status: QUALIFICATION_STATUSES.NOT_QUALIFIED },
        readiness: { status: READINESS_STATES.UNKNOWN },
      },
    ];
    assert.equal(businessFitQualifiedCount(evaluations), 2);
  });

  it('ranks ready prospects ahead of unknown-readiness qualified prospects', () => {
    const payload = normalizeScoutDiscoveryPayload({
      status: 'completed',
      payload: {
        opportunities: [
          {
            companyId: 'co-ready',
            name: 'Ready PM',
            fit: 0.7,
            signals: [{ type: 'hiring', label: 'Hiring facilities coordinator' }],
            evidenceRefs: [{ id: 'ev-r', label: 'Job posting', snapshot: { source: 'job_board' } }],
          },
        ],
        fitCandidates: [
          {
            companyId: 'co-fit',
            name: 'Strong Fit PM',
            fit: 0.9,
            qualified: true,
            readinessState: READINESS_STATES.UNKNOWN,
            signals: [],
            evidenceRefs: [{ id: 'ev-f', label: 'Website', snapshot: { source: 'website' } }],
          },
        ],
        qualifiedCount: 2,
        readinessReadyCount: 1,
        readinessUnknownCount: 1,
      },
    });

    assert.equal(payload.rankedProspects[0].readinessState, READINESS_STATES.READY);
    assert.equal(payload.rankedProspects[1].readinessState, READINESS_STATES.UNKNOWN);
  });

  it('prioritization blocker cites insufficient comparative evidence when qualified but unrankable', () => {
    const readiness = evaluatePrioritizationReadiness({
      blocked: false,
      discoveryStatus: 'complete',
      qualifiedCount: 3,
      summary: 'Three qualified prospects.',
      rankedProspects: [],
      evidence: [{ source: 'google_places', label: 'Listing' }],
      fitCandidates: [],
    });
    assert.ok(readiness.blockers.some((b) => b.code === 'no_ranked_prospects'));
    assert.match(readiness.blockers[0].reason, /insufficient comparative evidence/i);
  });
});
