'use strict';

/**
 * SPEC-102F — canonical Max competency catalog.
 * Implementation specs encode behavior; this registry tracks maturation.
 */

const { STAGES } = require('./CompetencyLifecycle');

const CATEGORIES = Object.freeze({
  CORE_MANAGEMENT: 'core_management',
  ARBITRATION: 'arbitration',
  PLANNING: 'planning',
  ECONOMICS: 'economics',
});

/** @type {import('./TrainingExercise').CompetencyDefinition[]} */
const COMPETENCIES = Object.freeze([
  {
    id: 'retrieve_before_delegation',
    label: 'Retrieve Before Delegation',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-16',
    specRefs: ['SPEC-102', 'SPEC-103'],
    regressionTests: [
      'test/retrievalBeforeDelegation.test.js',
      'packages/max/workspace/tests/retrievalBeforeDelegation.test.js',
      'packages/max/workspace/tests/businessUnderstandingRetrieval.test.js',
    ],
    exercises: [{
      id: 'service_area_retrieval',
      assignment: 'What do you currently understand about our service area?',
      observedBehavior: 'Delegated to Scout instead of retrieving durable knowledge.',
      expectedBehavior: 'Answer from Blueprint / Playbook / KG / prior investigations, or say unknown.',
      failureMode: 'Question → Scout without retrieval attempt.',
      generalLesson: 'Do not delegate work Max can already answer.',
      retest: 'What do you currently understand about our service area?',
      transferTest: 'What do you know about Kumho Tire?',
      graduationDecision: 'graduated',
      graduatedAt: '2026-08-16',
      specRef: 'SPEC-102',
    }],
  },
  {
    id: 'delegation_discipline',
    label: 'Delegation Discipline',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-16',
    specRefs: ['SPEC-098'],
    regressionTests: [
      'packages/max/workspace/tests/specialistDelegation.test.js',
    ],
    exercises: [],
  },
  {
    id: 'specialist_evaluation',
    label: 'Specialist Evaluation',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-16',
    specRefs: ['SPEC-100', 'SPEC-098'],
    regressionTests: [
      'packages/max/workspace/tests/scoutAcquisition.test.js',
    ],
    exercises: [],
  },
  {
    id: 'investigation_coverage_reasoning',
    label: 'Investigation Coverage Reasoning',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-16',
    specRefs: ['SPEC-099A'],
    regressionTests: [
      'test/scoutInvestigationProvenance.test.js',
    ],
    exercises: [{
      id: 'coverage_vs_conclusion',
      assignment: 'Where should we look for commercial cleaning opportunities?',
      observedBehavior: 'Delegated correctly; coverage reasoning missing.',
      expectedBehavior: 'Evaluate investigation quality separately from conclusions.',
      failureMode: 'Treat zero results and weak coverage as equivalent.',
      generalLesson: 'A specialist conclusion and the quality of the investigation are separate.',
      retest: 'Where should we look for commercial cleaning opportunities?',
      transferTest: 'Scout found zero — was the investigation thorough?',
      graduationDecision: 'graduated',
      graduatedAt: '2026-08-16',
      specRef: 'SPEC-099A',
    }],
  },
  {
    id: 'specialist_trace_interrogation',
    label: 'Specialist Trace Interrogation',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-16',
    specRefs: ['SPEC-101'],
    regressionTests: [
      'packages/max/workspace/tests/specialistInterrogation.test.js',
    ],
    exercises: [],
  },
  {
    id: 'explain_uncertainty',
    label: 'Explain Uncertainty',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-16',
    specRefs: ['SPEC-101', 'SPEC-099A'],
    regressionTests: [
      'packages/max/workspace/tests/specialistInterrogation.test.js',
    ],
    exercises: [],
  },
  {
    id: 'evidence_separation',
    label: 'Evidence Separation',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-16',
    specRefs: ['SPEC-101', 'SPEC-099A'],
    regressionTests: [
      'packages/max/workspace/tests/specialistInterrogation.test.js',
    ],
    exercises: [],
  },
  {
    id: 'durable_business_understanding',
    label: 'Durable Business Understanding',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-17',
    specRefs: ['SPEC-103', 'SPEC-083', 'SPEC-084'],
    regressionTests: [
      'packages/max/workspace/tests/businessUnderstandingRetrieval.test.js',
      'packages/max/workspace/tests/clientIntelligenceContinuity.test.js',
    ],
    exercises: [{
      id: 'anchor_blueprint_recall',
      assignment: 'What do you currently understand about Anchor Cleaning?',
      observedBehavior: 'Answered "I don\'t currently know" despite approved Blueprint in store.',
      expectedBehavior: 'Rich business summary from approved Blueprint without delegating.',
      failureMode: 'Session-only retrieval — durable Blueprint never loaded.',
      generalLesson: 'Business understanding must be retrieved from persistent stores, not session stubs.',
      retest: 'What do you currently understand about Anchor Cleaning?',
      transferTest: 'What is our service area?',
      graduationDecision: 'graduated',
      graduatedAt: '2026-08-17',
      specRef: 'SPEC-103',
    }],
  },
  {
    id: 'multi_specialist_arbitration',
    label: 'Multi-specialist Arbitration',
    category: CATEGORIES.ARBITRATION,
    stage: STAGES.TRAINING,
    specRefs: [],
    regressionTests: [],
    exercises: [],
  },
  {
    id: 'conflict_resolution',
    label: 'Conflict Resolution',
    category: CATEGORIES.ARBITRATION,
    stage: STAGES.NOT_STARTED,
    specRefs: [],
    regressionTests: [],
    exercises: [],
  },
  {
    id: 'long_horizon_planning',
    label: 'Long-horizon Planning',
    category: CATEGORIES.PLANNING,
    stage: STAGES.NOT_STARTED,
    specRefs: [],
    regressionTests: [],
    exercises: [],
  },
  {
    id: 'economic_tradeoff_reasoning',
    label: 'Economic Tradeoff Reasoning',
    category: CATEGORIES.ECONOMICS,
    stage: STAGES.NOT_STARTED,
    specRefs: [],
    regressionTests: [],
    exercises: [],
  },
  {
    id: 'opportunity_cost_analysis',
    label: 'Opportunity Cost Analysis',
    category: CATEGORIES.ECONOMICS,
    stage: STAGES.NOT_STARTED,
    specRefs: [],
    regressionTests: [],
    exercises: [],
  },
]);

function listCompetencies() {
  return COMPETENCIES.map(c => ({ ...c, exercises: c.exercises.map(e => ({ ...e })) }));
}

function getCompetency(id) {
  const found = COMPETENCIES.find(c => c.id === id);
  return found ? { ...found, exercises: found.exercises.map(e => ({ ...e })) } : null;
}

function listByCategory(category) {
  return listCompetencies().filter(c => c.category === category);
}

function listByStage(stage) {
  return listCompetencies().filter(c => c.stage === stage);
}

module.exports = {
  CATEGORIES,
  COMPETENCIES,
  listCompetencies,
  getCompetency,
  listByCategory,
  listByStage,
};
