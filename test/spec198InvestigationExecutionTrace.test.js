'use strict';

/**
 * SPEC-198 — Investigation Execution Trace acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildInvestigationExecutionTrace,
  readTraceMetricsFromExecutionState,
  resolveProviderFromResult,
} = require('../packages/scout/investigation/InvestigationExecutionTrace');
const {
  runCandidateInvestigationLoop,
  buildCandidateHypothesisState,
  CANDIDATE_HYPOTHESIS_IDS,
} = require('../packages/scout/investigation/CandidateInvestigation');
const { attachFitToClassified } = require('../packages/max/scoutAcquisition/FitEvaluation');
const { buildAcquisitionSearchDefinition } = require('../services/scoutAcquisitionIntelligence');
const {
  READINESS_STATES,
  QUALIFICATION_STATUSES,
} = require('../packages/max/scoutAcquisition/Types');

const TRACE_FIELDS = Object.freeze([
  'candidateId',
  'candidateName',
  'hypothesisId',
  'gap',
  'provider',
  'startedAt',
  'completedAt',
  'evidenceProduced',
  'qualificationBefore',
  'qualificationAfter',
  'readinessBefore',
  'readinessAfter',
  'confidenceBefore',
  'confidenceAfter',
  'rankBefore',
  'rankAfter',
]);

function lot202Fixture() {
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

function investigationLoopInput(overrides = {}) {
  const { evaluation, company, classified, searchDefinition } = lot202Fixture();
  return {
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
      ...overrides.opts,
    },
    ...overrides,
  };
}

function semanticLoopSnapshot(result) {
  return {
    executedTaskIds: result.executedTasks.map((row) => row.taskId || row.task && row.task.id),
    queueTaskIds: result.queue.map((task) => task.id),
    stop: result.stop,
    candidateEvaluations: result.candidates.map((row) => ({
      id: row.id,
      qualification: row.evaluation && row.evaluation.qualification && row.evaluation.qualification.status,
      readiness: row.evaluation && row.evaluation.readiness && row.evaluation.readiness.status,
      rank: row.rank,
    })),
    classifiedEvaluations: result.classified.map((row) => row.evaluation && row.evaluation.qualification && row.evaluation.qualification.status),
  };
}

describe('SPEC-198 — Investigation Execution Trace', () => {
  it('buildInvestigationExecutionTrace includes all canonical fields', () => {
    const trace = buildInvestigationExecutionTrace({
      task: {
        candidateId: 'co-lot-202',
        entityName: 'Lot 202 Property Management',
        hypothesisId: CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER,
        gap: 'decision_maker',
        providers: [{ providerId: 'website' }],
        completedAt: '2026-08-28T12:00:05.000Z',
      },
      result: {
        reports: [{ providerId: 'website' }],
        mergedReport: { evidenceProduced: ['decision_maker_name'] },
        status: 'completed',
      },
      before: {
        qualification: QUALIFICATION_STATUSES.QUALIFIED,
        readiness: READINESS_STATES.UNKNOWN,
        confidence: 0,
        rank: 1,
      },
      after: {
        qualification: QUALIFICATION_STATUSES.QUALIFIED,
        readiness: READINESS_STATES.UNKNOWN,
        confidence: 0.7,
        rank: 1,
      },
      startedAt: '2026-08-28T12:00:00.000Z',
      completedAt: '2026-08-28T12:00:05.000Z',
      evidenceProduced: ['decision_maker_name'],
    });

    for (const field of TRACE_FIELDS) {
      assert.ok(Object.prototype.hasOwnProperty.call(trace, field), `missing field: ${field}`);
    }

    assert.equal(trace.candidateId, 'co-lot-202');
    assert.equal(trace.candidateName, 'Lot 202 Property Management');
    assert.equal(trace.hypothesisId, CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER);
    assert.equal(trace.gap, 'decision_maker');
    assert.equal(trace.provider, 'website');
    assert.deepEqual(trace.evidenceProduced, ['decision_maker_name']);
    assert.equal(trace.confidenceAfter, 0.7);
  });

  it('readTraceMetricsFromExecutionState reads canonical objects without re-evaluation', () => {
    const { evaluation, company } = lot202Fixture();
    const hypothesisState = buildCandidateHypothesisState(
      { id: company.id, name: company.name, unknowns: [{ text: 'No identifiable operations decision-maker' }] },
      evaluation
    );
    hypothesisState[CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER].confidence = 0.42;

    const metrics = readTraceMetricsFromExecutionState(
      evaluation,
      hypothesisState[CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER],
      2
    );

    assert.equal(metrics.qualification, QUALIFICATION_STATUSES.QUALIFIED);
    assert.equal(metrics.readiness, READINESS_STATES.UNKNOWN);
    assert.equal(metrics.confidence, 0.42);
    assert.equal(metrics.rank, 2);
  });

  it('resolveProviderFromResult prefers report provider over task assignment', () => {
    assert.equal(
      resolveProviderFromResult({ reports: [{ providerId: 'prospeo' }] }, { providers: [{ providerId: 'website' }] }),
      'prospeo'
    );
    assert.equal(resolveProviderFromResult({}, { providers: [{ providerId: 'website' }] }), 'website');
    assert.equal(resolveProviderFromResult({}, {}), null);
  });

  it('runCandidateInvestigationLoop persists one trace per executed task from execution state', async () => {
    const result = await runCandidateInvestigationLoop(investigationLoopInput());

    assert.ok(Array.isArray(result.executionTraces));
    assert.equal(result.executionTraces.length, result.executedTasks.length);
    assert.ok(result.executionTraces.length > 0);

    const candidate = result.candidates.find((row) => row.id === 'co-lot-202');
    const hypothesisRow =
      candidate &&
      candidate.hypothesisState &&
      candidate.hypothesisState[CANDIDATE_HYPOTHESIS_IDS.DECISION_MAKER];

    for (const trace of result.executionTraces) {
      for (const field of TRACE_FIELDS) {
        assert.ok(Object.prototype.hasOwnProperty.call(trace, field), `missing field: ${field}`);
      }
      assert.equal(trace.candidateId, 'co-lot-202');
      assert.equal(trace.provider, 'website');
      assert.equal(trace.confidenceAfter, hypothesisRow && hypothesisRow.confidence);
    }
  });

  it('observability-only — traces do not alter investigation progression outputs', async () => {
    const first = await runCandidateInvestigationLoop(investigationLoopInput());
    const second = await runCandidateInvestigationLoop(investigationLoopInput());

    assert.deepEqual(semanticLoopSnapshot(first), semanticLoopSnapshot(second));
    assert.ok(first.executionTraces.length > 0);
    assert.equal(first.executionTraces.length, second.executionTraces.length);
  });

  it('returns empty executionTraces when no investigable candidates exist', async () => {
    const result = await runCandidateInvestigationLoop({
      companies: [],
      classified: [],
      searchDefinition: {},
      adapters: [],
      opts: {},
    });

    assert.deepEqual(result.executionTraces, []);
  });
});
