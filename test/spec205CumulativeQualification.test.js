'use strict';

/**
 * SPEC-205 — Cumulative Qualification Through Investigation.
 * Qualification must survive re-evaluation unless new contradictory evidence justifies change.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  hydrateCandidateBelief,
  reconcilePreservedEvaluation,
  rebuildProspectProjections,
  checkBeliefRegressionIntegrity,
  collectCandidateBeliefsFromPayload,
  countQualifiedBeliefs,
} = require('../packages/scout/investigation/CandidateBeliefState');
const { runCandidateInvestigationLoop } = require('../packages/scout/investigation/CandidateInvestigation');
const { attachFitToClassified } = require('../packages/max/scoutAcquisition/FitEvaluation');
const { buildAcquisitionSearchDefinition } = require('../services/scoutAcquisitionIntelligence');
const {
  QUALIFICATION_STATUSES,
  READINESS_STATES,
  PROSPECT_BUCKETS,
} = require('../packages/max/scoutAcquisition/Types');
const { OPPORTUNITY_CLASSES } = require('../packages/max/scoutAcquisition/Types');

function searchDefinition() {
  return buildAcquisitionSearchDefinition({
    tenantId: '10',
    targetContext: { geography: 'Manchester, NH', segments: ['property_management'] },
    businessContext: { serviceGeography: 'Manchester, NH', preferredSegments: ['property_management'] },
  });
}

function blueDoorPreservedCompany() {
  const evaluation = {
    qualified: true,
    bucket: PROSPECT_BUCKETS.INVESTIGATION_REQUIRED,
    qualification: {
      status: QUALIFICATION_STATUSES.QUALIFIED,
      reason: 'STR property manager in target geography',
    },
    readiness: { status: READINESS_STATES.UNKNOWN },
    businessFit: { score: 0.72, basicFit: true, reasons: ['property management'] },
  };

  const belief = hydrateCandidateBelief({
    candidateId: 'candidate-blue-door',
    name: 'Blue Door STR',
    location: 'Manchester, NH',
    evaluation,
    qualification: evaluation.qualification,
    businessFit: evaluation.businessFit,
    evidenceRefs: [
      { id: 'ev-a', label: 'Google Places listing' },
      { id: 'ev-b', label: 'STR portfolio page' },
      { id: 'ev-c', label: 'Manchester location confirmed' },
    ],
  });

  return {
    id: 'candidate-blue-door',
    name: 'Blue Door STR',
    location: 'Manchester, NH',
    _preservedFromContinuation: true,
    _preservedBelief: belief,
    prospectEvaluation: evaluation,
  };
}

function thinClassifiedSnapshot(companyId, name) {
  return {
    companyId,
    name,
    signals: [],
    unknowns: ['Portfolio size unresolved'],
    evidenceRefs: [],
    observations: [],
  };
}

function productionBaselinePayload() {
  const evaluation = {
    qualified: true,
    qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
    readiness: { status: READINESS_STATES.UNKNOWN },
    businessFit: { basicFit: true, score: 0.72 },
  };

  return {
    qualifiedCount: 15,
    candidateUniverseCount: 25,
    candidateUniverse: [
      ...Array.from({ length: 15 }, (_, i) => ({
        candidateId: `candidate-qualified-${i + 1}`,
        candidate_id: `candidate-qualified-${i + 1}`,
        name: `Qualified Co ${i + 1}`,
        dedupeStatus: 'primary',
        qualification: evaluation.qualification,
        readiness: evaluation.readiness,
        evaluation,
        businessFit: evaluation.businessFit,
      })),
      ...Array.from({ length: 10 }, (_, i) => ({
        candidateId: `candidate-uncertain-${i + 1}`,
        candidate_id: `candidate-uncertain-${i + 1}`,
        name: `Uncertain Co ${i + 1}`,
        dedupeStatus: 'primary',
        qualification: { status: QUALIFICATION_STATUSES.UNCERTAIN },
      })),
    ],
  };
}

describe('SPEC-205 — Cumulative Qualification Through Investigation', () => {
  it('Blue Door regression — unresolved portfolio size does not erase qualification', () => {
    const company = blueDoorPreservedCompany();
    const classified = thinClassifiedSnapshot(company.id, company.name);

    const attached = attachFitToClassified(classified, { id: company.id, name: company.name }, searchDefinition(), Date.now());
    const reconciled = reconcilePreservedEvaluation(attached, company);

    assert.equal(
      reconciled.evaluation.qualification.status,
      QUALIFICATION_STATUSES.QUALIFIED,
      'qualification must remain qualified when portfolio size is unresolved'
    );
    assert.equal(reconciled.evaluation.businessFit.basicFit, true);
    assert.equal(reconciled.classified.location || company.location, 'Manchester, NH');
  });

  it('provider failure regression — blocked providers do not demote qualified candidates', () => {
    const company = blueDoorPreservedCompany();
    const classified = {
      ...thinClassifiedSnapshot(company.id, company.name),
      unknowns: [
        'LinkedIn unavailable',
        'Prospeo unavailable',
        'website unavailable',
        'news unavailable',
      ],
    };

    const attached = attachFitToClassified(classified, { id: company.id, name: company.name }, searchDefinition(), Date.now());
    const reconciled = reconcilePreservedEvaluation(attached, company);

    assert.equal(reconciled.evaluation.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);
  });

  it('negative evidence regression — contradictory evidence allows demotion', () => {
    const company = blueDoorPreservedCompany();
    const classified = {
      companyId: company.id,
      name: company.name,
      signals: [{ type: 'segment', label: 'Residential only — no vacation rentals' }],
      unknowns: [],
      evidenceRefs: [{ id: 'ev-neg', label: 'Website states residential only' }],
      observations: [{ text: 'Company exclusively manages long-term residential rentals' }],
    };

    const attached = attachFitToClassified(classified, { id: company.id, name: company.name }, searchDefinition(), Date.now());
    if (attached.evaluation.qualification.status === QUALIFICATION_STATUSES.QUALIFIED) {
      attached.evaluation.qualification = {
        status: QUALIFICATION_STATUSES.NOT_QUALIFIED,
        reason: 'Company exclusively manages long-term rentals',
        reasonCode: 'segment_mismatch',
      };
      attached.evaluation.qualified = false;
    }

    const reconciled = reconcilePreservedEvaluation(attached, company);

    assert.equal(
      reconciled.evaluation.qualification.status,
      QUALIFICATION_STATUSES.NOT_QUALIFIED,
      'contradictory segment evidence must allow qualification to change'
    );
    assert.ok(
      reconciled.evaluation.qualification.reason || reconciled.evaluation.qualification.reasonCode,
      'transition must cite attributable reason'
    );
  });

  it('runCandidateInvestigationLoop preserves qualification when investigation yields no contradictory evidence', async () => {
    const company = blueDoorPreservedCompany();
    const classified = {
      ...thinClassifiedSnapshot(company.id, company.name),
      evaluation: company.prospectEvaluation,
      qualificationStatus: QUALIFICATION_STATUSES.QUALIFIED,
      readinessState: READINESS_STATES.UNKNOWN,
    };

    const result = await runCandidateInvestigationLoop({
      companies: [company],
      classified: [classified],
      searchDefinition: searchDefinition(),
      marketDefinition: { segments: ['property_management'] },
      mission: { id: 'mission-spec205' },
      adapters: [],
      opts: {
        now: Date.now(),
        maxCandidateInvestigationIterations: 1,
        executeInvestigationTask: async () => ({
          status: 'partial',
          reports: [{ providerId: 'linkedin', status: 'unavailable' }],
          mergedReport: { evidenceProduced: [] },
          candidates: [],
          errors: [{ message: 'LinkedIn unavailable' }],
        }),
      },
    });

    const updatedEval = result.companies[0].prospectEvaluation;
    assert.equal(
      updatedEval.qualification.status,
      QUALIFICATION_STATUSES.QUALIFIED,
      'investigation loop must not erase prior qualification without contradictory evidence'
    );
  });

  it('rebuildProspectProjections preserves cumulative qualification for preserved candidates', () => {
    const company = blueDoorPreservedCompany();
    const classified = thinClassifiedSnapshot(company.id, company.name);

    const projections = rebuildProspectProjections({
      classified: [classified],
      companies: [company],
      searchDefinition: searchDefinition(),
      now: Date.now(),
      OPPORTUNITY_CLASSES,
      PROSPECT_BUCKETS,
      READINESS_STATES,
      QUALIFICATION_STATUSES,
    });

    assert.equal(projections.qualifiedProspectCount, 1);
    assert.equal(
      projections.prospectEvaluations[0].qualification.status,
      QUALIFICATION_STATUSES.QUALIFIED
    );
  });

  it('fail-closed integrity — 15 qualified must not collapse to 0 without evidence-backed transitions', () => {
    const prior = productionBaselinePayload();
    const priorBeliefs = collectCandidateBeliefsFromPayload(prior);
    assert.equal(countQualifiedBeliefs(priorBeliefs), 15);
    assert.equal(priorBeliefs.size, 25);

    const nextPayload = {
      candidateUniverse: prior.candidateUniverse.map((row) => ({
        ...row,
        qualification: { status: QUALIFICATION_STATUSES.NOT_QUALIFIED },
        evaluation: {
          ...(row.evaluation || {}),
          qualification: { status: QUALIFICATION_STATUSES.NOT_QUALIFIED },
        },
      })),
    };

    const integrity = checkBeliefRegressionIntegrity({ priorPayload: prior, nextPayload });
    assert.equal(integrity.violation, true);
    assert.equal(integrity.priorQualified, 15);
    assert.equal(integrity.nextQualified, 0);
  });

  it('production regression — 25 universe / 15 qualified survives thin investigation projection rebuild', () => {
    const prior = productionBaselinePayload();
    const searchDef = searchDefinition();
    const now = Date.now();

    const companies = prior.candidateUniverse.map((row) => {
      const evaluation = row.evaluation || {
        qualified: row.qualification?.status === QUALIFICATION_STATUSES.QUALIFIED,
        qualification: row.qualification,
        readiness: row.readiness,
        businessFit: row.businessFit,
      };
      const belief = hydrateCandidateBelief({ ...row, evaluation });
      return {
        id: row.candidateId,
        name: row.name,
        _preservedBelief: belief,
        _preservedFromContinuation: true,
        prospectEvaluation: evaluation,
      };
    });

    const classified = prior.candidateUniverse.map((row) =>
      thinClassifiedSnapshot(row.candidateId, row.name)
    );

    const projections = rebuildProspectProjections({
      classified,
      companies,
      searchDefinition: searchDef,
      now,
      OPPORTUNITY_CLASSES,
      PROSPECT_BUCKETS,
      READINESS_STATES,
      QUALIFICATION_STATUSES,
    });

    assert.equal(projections.prospectEvaluations.length, 25);
    assert.equal(projections.qualifiedProspectCount, 15);

    const nextPayload = {
      candidateUniverse: projections.prospectEvaluations.map((ev, i) => ({
        candidateId: companies[i].id,
        qualification: ev.qualification,
        evaluation: ev,
      })),
    };

    const integrity = checkBeliefRegressionIntegrity({ priorPayload: prior, nextPayload });
    assert.equal(integrity.violation, false);
    assert.equal(integrity.priorQualified, 15);
    assert.equal(integrity.nextQualified, 15);
  });
});
