'use strict';

/**
 * SPEC-199 — Durable Candidate Belief State (AUDIT-074).
 * Candidate intelligence must survive investigation continuations.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  hydrateCandidateBelief,
  mergeCandidateBeliefs,
  collectCandidateBeliefsFromPayload,
  beliefsToPreservedCandidates,
  mergeDiscoveredIntelligence,
  partitionDiscoveredCandidates,
  projectBeliefToUniverseRecord,
  checkBeliefRegressionIntegrity,
  reconcilePreservedEvaluation,
  mergeEvidenceArrays,
} = require('../packages/scout/investigation/CandidateBeliefState');
const {
  extractPreservedCandidatesFromPayload,
  extractInvestigationCandidatesFromPayload,
} = require('../packages/scout/investigation/EntityInvestigationContinuation');
const { QUALIFICATION_STATUSES, READINESS_STATES } = require('../packages/max/scoutAcquisition/Types');
const { attachFitToClassified } = require('../packages/max/scoutAcquisition/FitEvaluation');
const { buildAcquisitionSearchDefinition } = require('../services/scoutAcquisitionIntelligence');

function blueDoorQualifiedPayload() {
  const evaluation = {
    qualified: true,
    bucket: 'investigation_required',
    qualification: {
      status: QUALIFICATION_STATUSES.QUALIFIED,
      reason: 'STR property manager in target geography',
    },
    readiness: { status: READINESS_STATES.UNKNOWN },
    businessFit: { score: 0.72, basicFit: true, reasons: ['property management'] },
  };

  return {
    qualifiedCount: 15,
    candidateUniverseCount: 24,
    rankedProspects: [],
    fitCandidates: [],
    candidateUniverse: [
      {
        candidateId: 'candidate-blue-door',
        candidate_id: 'candidate-blue-door',
        canonicalIdentity: 'candidate-blue-door',
        name: 'Blue Door STR',
        address: 'Manchester, NH',
        cities: ['Manchester, NH'],
        dedupeStatus: 'primary',
        qualification: evaluation.qualification,
        readiness: evaluation.readiness,
        evaluation,
        businessFit: evaluation.businessFit,
        evidenceRefs: [
          { id: 'ev-a', label: 'Google Places listing' },
          { id: 'ev-b', label: 'STR portfolio page' },
          { id: 'ev-c', label: 'Manchester location confirmed' },
        ],
      },
      ...Array.from({ length: 14 }, (_, i) => ({
        candidateId: `candidate-qualified-${i + 1}`,
        candidate_id: `candidate-qualified-${i + 1}`,
        name: `Qualified Co ${i + 1}`,
        dedupeStatus: 'primary',
        qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
        readiness: { status: READINESS_STATES.UNKNOWN },
        evaluation: {
          qualified: true,
          qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
          readiness: { status: READINESS_STATES.UNKNOWN },
        },
      })),
      ...Array.from({ length: 9 }, (_, i) => ({
        candidateId: `candidate-uncertain-${i + 1}`,
        candidate_id: `candidate-uncertain-${i + 1}`,
        name: `Uncertain Co ${i + 1}`,
        dedupeStatus: 'primary',
        qualification: { status: QUALIFICATION_STATUSES.UNCERTAIN },
      })),
    ],
    prospectEvaluations: [],
  };
}

describe('SPEC-199 — Durable Candidate Belief State', () => {
  it('hydrateCandidateBelief preserves qualification, location, and evidenceRefs', () => {
    const payload = blueDoorQualifiedPayload();
    const beliefs = collectCandidateBeliefsFromPayload(payload);
    const blueDoor = [...beliefs.values()].find((row) => row.candidateId === 'candidate-blue-door');

    assert.ok(blueDoor);
    assert.equal(blueDoor.identity.location, 'Manchester, NH');
    assert.equal(blueDoor.businessFit.basicFit, true);
    assert.equal(blueDoor.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);
    assert.equal(blueDoor.evidenceRefs.length, 3);
  });

  it('extractPreservedCandidatesFromPayload hydrates full belief, not identity shell', () => {
    const preserved = extractPreservedCandidatesFromPayload(blueDoorQualifiedPayload());
    assert.equal(preserved.length, 24);

    const blueDoor = preserved.find((row) => row.id === 'candidate-blue-door');
    assert.ok(blueDoor);
    assert.equal(blueDoor.location, 'Manchester, NH');
    assert.equal(blueDoor.basicFit, true);
    assert.equal(blueDoor.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);
    assert.equal(blueDoor.evidenceRefs.length, 3);
    assert.ok(blueDoor._preservedBelief);
    assert.equal(blueDoor._preservedBelief.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);
  });

  it('continuation 2 hydrates 15 qualified before any provider executes', () => {
    const run1 = blueDoorQualifiedPayload();
    const preserved = extractPreservedCandidatesFromPayload(run1);
    assert.equal(preserved.length, 24);

    const preInvestigationQualified = preserved.filter(
      (row) => row.qualification && row.qualification.status === QUALIFICATION_STATUSES.QUALIFIED
    );
    assert.equal(preInvestigationQualified.length, 15);
  });

  it('Blue Door regression — hydrated candidate retains belief after preservation', () => {
    const preserved = extractPreservedCandidatesFromPayload(blueDoorQualifiedPayload());
    const blueDoor = preserved.find((row) => row.id === 'candidate-blue-door');

    assert.equal(blueDoor.location, 'Manchester, NH');
    assert.equal(blueDoor.basicFit, true);
    assert.equal(blueDoor.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);
    assert.deepEqual(
      blueDoor.evidenceRefs.map((ev) => ev.id),
      ['ev-a', 'ev-b', 'ev-c']
    );
  });

  it('known identity merges provider intelligence instead of discarding', () => {
    const existing = {
      id: 'candidate-blue-door',
      name: 'Blue Door STR',
      location: 'Manchester, NH',
      qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
      signals: [{ id: 'sig-prior', type: 'location', label: 'Manchester' }],
    };
    const discovered = {
      id: 'place-blue-door',
      name: 'Blue Door STR',
      address: 'Manchester, NH',
      phone: '603-555-0100',
      signals: [{ id: 'sig-new', type: 'phone', label: '603-555-0100' }],
    };

    const merged = mergeDiscoveredIntelligence(existing, discovered);
    assert.equal(merged.phone, '603-555-0100');
    assert.equal(merged.signals.length, 2);
    assert.equal(merged.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);

    const partitioned = partitionDiscoveredCandidates([existing], [discovered]);
    assert.equal(partitioned.discoveredRaw.length, 0);
    assert.equal(partitioned.existingCompanies[0].phone, '603-555-0100');
  });

  it('evidence merge is additive with deduplication', () => {
    const merged = mergeEvidenceArrays(
      [{ id: 'ev-a', label: 'prior' }, { id: 'ev-b', label: 'prior b' }],
      [{ id: 'ev-b', label: 'duplicate' }, { id: 'ev-c', label: 'new' }]
    );
    assert.equal(merged.length, 3);
    assert.deepEqual(merged.map((ev) => ev.id), ['ev-a', 'ev-b', 'ev-c']);
  });

  it('reconcilePreservedEvaluation prevents qualification regression without contradictory evidence', () => {
    const searchDefinition = buildAcquisitionSearchDefinition({
      tenantId: '10',
      targetContext: { geography: 'Manchester, NH', segments: ['property_management'] },
      businessContext: { serviceGeography: 'Manchester, NH', preferredSegments: ['property_management'] },
    });

    const company = {
      id: 'candidate-blue-door',
      name: 'Blue Door STR',
      _preservedBelief: hydrateCandidateBelief({
        candidateId: 'candidate-blue-door',
        name: 'Blue Door STR',
        location: 'Manchester, NH',
        evaluation: {
          qualified: true,
          qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
          readiness: { status: READINESS_STATES.UNKNOWN },
          businessFit: { basicFit: true, score: 0.72 },
        },
        qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
        businessFit: { basicFit: true, score: 0.72 },
      }),
    };

    const classified = {
      companyId: company.id,
      name: company.name,
      signals: [],
      unknowns: [],
      evidenceRefs: [],
      observations: [],
    };

    const attached = attachFitToClassified(classified, { id: company.id, name: company.name }, searchDefinition, Date.now());
    const reconciled = reconcilePreservedEvaluation(attached, company);

    assert.equal(
      reconciled.evaluation.qualification.status,
      QUALIFICATION_STATUSES.QUALIFIED,
      'qualification must not regress when prior belief was qualified and no contradictory evidence exists'
    );
  });

  it('negative evidence allows qualification to change', () => {
    const prior = hydrateCandidateBelief({
      candidateId: 'candidate-rental-only',
      name: 'Rental Only PM',
      qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
      evaluation: {
        qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
        readiness: { status: READINESS_STATES.UNKNOWN },
      },
    });

    const incoming = hydrateCandidateBelief({
      candidateId: 'candidate-rental-only',
      name: 'Rental Only PM',
      evaluation: {
        qualification: {
          status: QUALIFICATION_STATUSES.NOT_QUALIFIED,
          reason: 'Company exclusively manages long-term rentals',
          reasonCode: 'segment_mismatch',
        },
        readiness: { status: READINESS_STATES.NOT_READY },
      },
    });

    const merged = mergeCandidateBeliefs(prior, incoming);
    assert.equal(merged.qualification.status, QUALIFICATION_STATUSES.NOT_QUALIFIED);
    assert.match(merged.qualification.reason, /long-term rentals/i);
  });

  it('projectBeliefToUniverseRecord commits qualification to candidateUniverse', () => {
    const record = projectBeliefToUniverseRecord({
      company: { id: 'candidate-blue-door', name: 'Blue Door STR', location: 'Manchester, NH' },
      classified: {
        evaluation: {
          qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
          readiness: { status: READINESS_STATES.UNKNOWN },
          businessFit: { basicFit: true, score: 0.72 },
        },
        evidenceRefs: [{ id: 'ev-a' }, { id: 'ev-b' }, { id: 'ev-c' }],
      },
    });

    assert.equal(record.candidateId, 'candidate-blue-door');
    assert.equal(record.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);
    assert.equal(record.businessFit.basicFit, true);
    assert.equal(record.evidenceRefs.length, 3);
  });

  it('checkBeliefRegressionIntegrity flags destructive 15 → 0 collapse', () => {
    const prior = blueDoorQualifiedPayload();
    const next = {
      candidateUniverse: prior.candidateUniverse.map((row) => ({
        ...row,
        qualification: { status: QUALIFICATION_STATUSES.NOT_QUALIFIED },
        evaluation: {
          qualification: { status: QUALIFICATION_STATUSES.NOT_QUALIFIED },
        },
      })),
    };

    const integrity = checkBeliefRegressionIntegrity({ priorPayload: prior, nextPayload: next });
    assert.equal(integrity.violation, true);
    assert.equal(integrity.priorQualified, 15);
    assert.equal(integrity.nextQualified, 0);
  });

  it('checkBeliefRegressionIntegrity allows explainable single-candidate loss', () => {
    const prior = blueDoorQualifiedPayload();
    const nextBeliefs = collectCandidateBeliefsFromPayload(prior);
    const target = [...nextBeliefs.values()].find((row) => row.candidateId === 'candidate-qualified-1');
    target.qualification = { status: QUALIFICATION_STATUSES.NOT_QUALIFIED, reason: 'segment mismatch' };
    if (target.evaluation) {
      target.evaluation.qualification = target.qualification;
    }

    const nextPayload = {
      candidateUniverse: beliefsToPreservedCandidates(nextBeliefs).map((row) => ({
        candidateId: row.id,
        qualification: row.qualification,
        evaluation: row.evaluation,
      })),
    };

    const integrity = checkBeliefRegressionIntegrity({ priorPayload: prior, nextPayload });
    assert.equal(integrity.violation, false);
    assert.equal(integrity.priorQualified, 15);
    assert.equal(integrity.nextQualified, 14);
  });

  it('extractInvestigationCandidatesFromPayload still works with belief-enriched universe', () => {
    const payload = blueDoorQualifiedPayload();
    const candidates = extractInvestigationCandidatesFromPayload(payload);
    assert.ok(candidates.length > 0);

    const preserved = extractPreservedCandidatesFromPayload(payload);
    const qualifiedPreserved = preserved.filter(
      (row) => row.qualification && row.qualification.status === QUALIFICATION_STATUSES.QUALIFIED
    );
    assert.equal(qualifiedPreserved.length, 15);

    const blueDoorPreserved = preserved.find((row) => row.id === 'candidate-blue-door');
    assert.ok(blueDoorPreserved);
    assert.equal(blueDoorPreserved.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);
  });
});
