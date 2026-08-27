'use strict';

/**
 * SPEC-195 — Candidate-Scoped Investigative Continuation acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildCandidateHypothesisState,
  deriveCanonicalGaps,
  buildCandidateInvestigationTasks,
  buildCandidateInvestigationQueue,
  selectNextInvestigation,
  shouldStopInvestigation,
  runCandidateInvestigationLoop,
  assertNoProseOnlyInvestigation,
  TASK_STATUS,
  STOP_REASONS,
  CANDIDATE_HYPOTHESIS_IDS,
} = require('../packages/scout/investigation/CandidateInvestigation');
const { evidenceRequirementSatisfied } = require('../packages/scout/coverage/EvidenceRequirements');
const { buildEvidenceRequest } = require('../packages/scout/coverage/EvidenceRequest');
const { INVESTIGATIVE_EVIDENCE } = require('../packages/scout/coverage/EvidenceRequirements');
const {
  buildProspectEvaluation,
  deriveInvestigationNeeds,
} = require('../packages/max/scoutAcquisition/ProspectEvaluation');
const { attachFitToClassified } = require('../packages/max/scoutAcquisition/FitEvaluation');
const { buildAcquisitionSearchDefinition } = require('../services/scoutAcquisitionIntelligence');
const {
  READINESS_STATES,
  QUALIFICATION_STATUSES,
  PROSPECT_BUCKETS,
} = require('../packages/max/scoutAcquisition/Types');

function lot202Candidate() {
  const classified = {
    name: 'Lot 202 Property Management',
    fit: 0.91,
    signals: [],
    unknowns: ['No identifiable operations decision-maker'],
    observations: [{ text: 'Property management operator in Manchester.' }],
    evidenceRefs: [{ id: 'ev-lot', label: 'Google Places listing', sourceKind: 'observed_fact' }],
  };
  const company = {
    id: 'co-lot-202',
    name: 'Lot 202 Property Management',
    industry: 'property_management',
    location: 'Manchester, NH',
    website: 'https://lot202.example',
    icpScore: 91,
  };
  const searchDefinition = buildAcquisitionSearchDefinition({
    tenantId: '10',
    targetContext: {
      geography: 'Manchester, NH',
      segments: ['property_management'],
      businessType: 'commercial_cleaning',
    },
    businessContext: {
      serviceGeography: 'Manchester, NH',
      commercialCapability: 'commercial_cleaning',
      preferredSegments: ['property_management'],
    },
  });
  const attached = attachFitToClassified(classified, company, searchDefinition, Date.now());
  return {
    company,
    classified: attached.classified,
    evaluation: attached.evaluation,
    searchDefinition,
  };
}

describe('SPEC-195 — Candidate-Scoped Investigative Continuation', () => {
  it('deriveInvestigationNeeds produces canonical gaps alongside presentation strings', () => {
    const { evaluation } = lot202Candidate();
    const investigation = evaluation.investigation;
    assert.ok(Array.isArray(investigation.missingEvidence));
    assert.ok(Array.isArray(investigation.canonicalGaps));
    assert.ok(investigation.canonicalGaps.length > 0);
    assert.ok(investigation.canonicalGaps.every((gap) => gap.gap && gap.evidenceType));
  });

  it('Lot 202 retains candidate-specific hypothesis state', () => {
    const { evaluation, company } = lot202Candidate();
    const state = buildCandidateHypothesisState(
      { id: company.id, name: company.name, unknowns: [{ text: 'No identifiable operations decision-maker' }] },
      evaluation
    );
    assert.ok(state[CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER]);
    assert.equal(state[CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER].status, 'unresolved');
    assert.ok(state[CANDIDATE_HYPOTHESIS_IDS.BUYING_READINESS]);
  });

  it('high-value unknowns become executable tasks with candidateId', () => {
    const { evaluation, company } = lot202Candidate();
    const candidate = {
      id: company.id,
      name: company.name,
      website: company.website,
      evaluation,
      unknowns: [{ text: 'No identifiable operations decision-maker' }],
      qualificationStatus: QUALIFICATION_STATUSES.QUALIFIED,
      readinessState: READINESS_STATES.UNKNOWN,
      prospectBucket: PROSPECT_BUCKETS.INVESTIGATION_REQUIRED,
    };

    const tasks = buildCandidateInvestigationTasks(candidate);
    assert.ok(tasks.length > 0);
    assert.ok(tasks.every((task) => task.candidateId === 'co-lot-202'));
    assert.ok(tasks.every((task) => task.scope === 'entity'));
    assert.ok(tasks.some((task) => task.evidenceType === INVESTIGATIVE_EVIDENCE.DECISION_MAKERS));

    const audit = assertNoProseOnlyInvestigation(candidate, tasks);
    assert.equal(audit.ok, true, audit.message || 'expected executable or blocked tasks');
  });

  it('evidence request carries candidate website and hypothesis context', () => {
    const { evaluation, company } = lot202Candidate();
    const tasks = buildCandidateInvestigationTasks({
      id: company.id,
      name: company.name,
      website: company.website,
      address: company.location,
      evaluation,
    });
    const task = tasks[0];
    const request = buildEvidenceRequest(task, { segments: ['property_management'] }, {});
    assert.equal(request.candidateId, 'co-lot-202');
    assert.equal(request.businessName, 'Lot 202 Property Management');
    assert.equal(request.website, 'https://lot202.example');
    assert.ok(request.hypothesisId);
    assert.equal(request.scope, 'entity');
  });

  it('market-level evidence cannot satisfy candidate-level gaps', () => {
    const requirement = {
      evidenceType: INVESTIGATIVE_EVIDENCE.DECISION_MAKERS,
      entityId: 'co-lot-202',
      hypothesisId: CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER,
    };
    const marketOnly = [{ evidenceType: INVESTIGATIVE_EVIDENCE.DECISION_MAKERS, evidenceProduced: ['people'] }];
    const candidateScoped = [
      {
        evidenceType: INVESTIGATIVE_EVIDENCE.DECISION_MAKERS,
        entityId: 'co-lot-202',
        hypothesisId: CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER,
        evidenceProduced: ['people'],
      },
    ];

    assert.equal(evidenceRequirementSatisfied(requirement, marketOnly), false);
    assert.equal(evidenceRequirementSatisfied(requirement, candidateScoped), true);
  });

  it('investigation queue prioritizes by expected information gain', () => {
    const { evaluation, company } = lot202Candidate();
    const candidates = [
      {
        id: company.id,
        name: company.name,
        website: company.website,
        rank: 1,
        fit: 0.91,
        evaluation,
        qualificationStatus: QUALIFICATION_STATUSES.QUALIFIED,
        readinessState: READINESS_STATES.UNKNOWN,
        prospectBucket: PROSPECT_BUCKETS.INVESTIGATION_REQUIRED,
      },
      {
        id: 'co-low-fit',
        name: 'Low Fit Co',
        rank: 10,
        fit: 0.4,
        evaluation: buildProspectEvaluation({
          candidate: { id: 'co-low-fit', name: 'Low Fit Co' },
          classified: { fit: 0.4, unknowns: [], signals: [] },
          fit: { score: 0.4, basicFit: false },
          qualification: { qualified: false },
          searchDefinition: {},
        }),
        qualificationStatus: QUALIFICATION_STATUSES.UNCERTAIN,
        readinessState: READINESS_STATES.UNKNOWN,
        prospectBucket: PROSPECT_BUCKETS.FIT_INVESTIGATION,
      },
    ];

    const queue = buildCandidateInvestigationQueue(candidates);
    assert.ok(queue.length > 0);
    assert.equal(queue[0].candidateId, 'co-lot-202');
    assert.ok(queue[0].expectedInformationGain >= (queue[queue.length - 1].expectedInformationGain || 0));
  });

  it('runCandidateInvestigationLoop executes tasks and updates hypotheses', async () => {
    const { evaluation, company, classified, searchDefinition } = lot202Candidate();

    const result = await runCandidateInvestigationLoop({
      companies: [company],
      classified: [classified],
      searchDefinition,
      marketDefinition: { segments: ['property_management'] },
      mission: { id: 'mission-anchor' },
      adapters: [],
      opts: {
        maxCandidateInvestigationIterations: 2,
        executeInvestigationTask: async (task) => ({
          taskId: task.id,
          evidenceType: task.evidenceType,
          status: 'completed',
          reports: [{ providerId: 'website', evidenceType: task.evidenceType, evidenceProduced: ['decision_maker_name'] }],
          mergedReport: { evidenceProduced: ['decision_maker_name'] },
          candidates: [
            {
              id: company.id,
              name: company.name,
              people: [{ name: 'Alex Manager', jobTitle: 'Operations Manager', decisionMaker: true }],
              signals: [{ type: 'decision_maker', label: 'Alex Manager' }],
            },
          ],
          errors: [],
        }),
      },
    });

    assert.ok(result.executedTasks.length > 0);
    assert.ok(result.hypothesisStates['co-lot-202']);
    assert.ok(result.stop);
    const next = selectNextInvestigation(result.queue, { completedTaskIds: result.executedTasks.map((t) => t.taskId) });
    assert.equal(next, null);
  });

  it('stops on information exhausted when providers are blocked', () => {
    const tasks = [
      {
        id: 'task:1:decision_makers',
        status: TASK_STATUS.BLOCKED,
        providers: [],
        expectedInformationGain: 0.5,
      },
    ];
    const stop = shouldStopInvestigation({}, tasks, {});
    assert.equal(stop.stop, true);
    assert.equal(stop.reason, STOP_REASONS.INFORMATION_EXHAUSTED);
  });

  it('AUDIT-070 regression — recommendedNextInvestigation cannot be prose-only', () => {
    const { evaluation, company } = lot202Candidate();
    const candidate = {
      id: company.id,
      name: company.name,
      website: company.website,
      evaluation,
      unknowns: [{ text: 'No identifiable operations decision-maker' }],
      qualificationStatus: QUALIFICATION_STATUSES.QUALIFIED,
      readinessState: READINESS_STATES.UNKNOWN,
      prospectBucket: PROSPECT_BUCKETS.INVESTIGATION_REQUIRED,
      recommendedNextInvestigation: {
        action: 'Identify operations decision-maker via website and LinkedIn',
        impact: 'high',
      },
    };

    const tasks = buildCandidateInvestigationTasks(candidate);
    const audit = assertNoProseOnlyInvestigation(candidate, tasks);
    assert.equal(audit.ok, true, audit.message);
    assert.ok(tasks.length > 0);
  });
});
