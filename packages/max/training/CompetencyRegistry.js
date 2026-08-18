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
    id: 'intent_bound_response_selection',
    label: 'Intent-Bound Response Selection',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-18',
    specRefs: ['SPEC-109', 'SPEC-107', 'SPEC-102'],
    regressionTests: [
      'test/intentBoundResponseSelection.test.js',
      'packages/max/workspace/tests/intentBoundResponseSelection.test.js',
    ],
    exercises: [{
      id: 'completed_recently_is_retrieval',
      assignment: 'What have we completed recently?',
      observedBehavior: 'Returned a Blueprint acquisition recommendation instead of completed operating state.',
      expectedBehavior: 'Select the Retrieval contract before reasoning. Answer verified state and unknowns first. Do not lead with unsolicited strategy.',
      failureMode: 'Advice as a universal response type — retrieve then recommend regardless of intent.',
      generalLesson: 'Operator intent determines response structure. Evidence determines content. Reasoning determines recommendations. Those are separate stages.',
      retest: 'What have we completed recently?',
      transferTest: 'What outreach has already been sent? How is Anchor Cleaning doing? What should we do next? That\'s incorrect. Investigate commercial prospects.',
      graduationDecision: 'graduated',
      graduatedAt: '2026-08-18',
      specRef: 'SPEC-109',
    }],
  },
  {
    id: 'claim_grounding',
    label: 'Claim Grounding',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-18',
    specRefs: ['SPEC-108', 'SPEC-107A', 'SPEC-107'],
    regressionTests: [
      'test/recommendationClaimGrounding.test.js',
      'test/claimGrounding.test.js',
      'packages/max/workspace/tests/recommendationClaimGrounding.test.js',
      'packages/max/workspace/tests/claimGrounding.test.js',
    ],
    exercises: [{
      id: 'unsupported_operating_state_retraction',
      assignment: 'Given what is already in motion, what should I focus on next? Then: You said outbound email is already active. What evidence tells you that?',
      observedBehavior: 'Asserted an active email motion from inventory, then restated inventory when challenged.',
      expectedBehavior: 'Evaluate operating-state claims against evidence before recommending. Retract unsupported claims and revise the recommendation.',
      failureMode: 'Treat planned work, inventory, or objectives as observed execution.',
      generalLesson: 'Recommendations may only depend on supported operating-state claims. Planned is not completed; inventory is not execution; goals are not operating state.',
      retest: 'You said outbound email is already active. What evidence tells you that?',
      transferTest: 'Follow-up is scheduled tomorrow — did it occur? 67 prospects exist — has outreach begun? Blueprint says acquire twenty commercial clients — are you expanding?',
      graduationDecision: 'graduated',
      graduatedAt: '2026-08-18',
      specRef: 'SPEC-108',
    }],
  },
  {
    id: 'business_intelligence_synthesis',
    label: 'Business Intelligence Synthesis',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-18',
    specRefs: ['SPEC-110', 'SPEC-109', 'SPEC-108', 'SPEC-107'],
    regressionTests: [
      'test/businessIntelligenceSynthesis.test.js',
      'packages/max/workspace/tests/businessIntelligenceSynthesis.test.js',
    ],
    exercises: [{
      id: 'anchor_inventory_is_not_intelligence',
      assignment: 'How is Anchor Cleaning doing?',
      observedBehavior: 'Returned a raw inventory (72 prospects, 69 Scout companies, 20 AO leads) and left the operator to reason.',
      expectedBehavior: 'Synthesize first-class business intelligence objects from grounded claims, present conclusions first, then supporting evidence. Never speculate when evidence is insufficient.',
      failureMode: 'Evidence presentation as the response — retrieve and list counts without identifying bottlenecks, progress, readiness, risk, or unknowns.',
      generalLesson: 'Evidence exists to support intelligence. Intelligence exists to support operator decisions. Max should communicate conclusions, not merely inventories.',
      retest: 'How is Anchor Cleaning doing?',
      transferTest: 'What outreach has already been sent? What should we do next? Are Yelp Ads working? Where should we focus next?',
      graduationDecision: 'graduated',
      graduatedAt: '2026-08-18',
      specRef: 'SPEC-110',
    }],
  },
  {
    id: 'operator_intent_taxonomy',
    label: 'Operator Intent Taxonomy',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-18',
    specRefs: ['SPEC-111', 'SPEC-110', 'SPEC-109'],
    regressionTests: [
      'test/operatorIntentTaxonomy.test.js',
      'packages/max/workspace/tests/operatorIntentTaxonomy.test.js',
    ],
    exercises: [{
      id: 'diagnosis_is_not_recommendation',
      assignment: "What's preventing us from growing faster?",
      observedBehavior: 'Collapsed diagnosis into a Blueprint acquisition recommendation.',
      expectedBehavior: 'Classify Diagnosis before reasoning. Identify the current bottleneck from business intelligence. Do not lead with generic commercial acquisition advice.',
      failureMode: 'Intent routing treats diagnostic questions as Recommendation or Unknown Analysis as a Scout shortcut.',
      generalLesson: 'Intent determines analysis. Analysis determines reasoning. Reasoning determines response. Operators ask for different forms of intelligence.',
      retest: "What's preventing us from growing faster?",
      transferTest: "What don't we know yet that matters? What's our biggest operational risk? How much progress have we made? What should we do next?",
      graduationDecision: 'graduated',
      graduatedAt: '2026-08-18',
      specRef: 'SPEC-111',
    }],
  },
  {
    id: 'reasoning_pipeline_conformance',
    label: 'Reasoning Pipeline Conformance',
    category: CATEGORIES.CORE_MANAGEMENT,
    stage: STAGES.GRADUATED,
    graduatedAt: '2026-08-18',
    specRefs: ['SPEC-112', 'SPEC-111', 'SPEC-109'],
    regressionTests: [
      'test/reasoningPipelineConformance.test.js',
      'packages/max/workspace/tests/reasoningPipelineConformance.test.js',
    ],
    exercises: [{
      id: 'no_blueprint_advisory_bypass',
      assignment: 'Based on what you know about my business, what should we focus on first?',
      observedBehavior: 'Answered with Blueprint Advisory ("I\'d start by proving a repeatable commercial acquisition motion") instead of the governed pipeline.',
      expectedBehavior: 'Enter the single governed pipeline. Classify Recommendation. Retrieve and ground evidence. Compose via ResponseContract. Blueprint is evidence, not a responder.',
      failureMode: 'CIE / Blueprint advisory remains an operator-facing reasoning engine alongside the governed stack.',
      generalLesson: 'There is one operator reasoning pipeline. Legacy advisors become providers. Unknown intent fails toward Retrieval, never Blueprint Advisory.',
      retest: 'What should I do next?',
      transferTest: 'How is Anchor doing? What\'s preventing growth? What don\'t we know? What\'s risky? What outreach has been sent? Should Scout investigate?',
      graduationDecision: 'graduated',
      graduatedAt: '2026-08-18',
      specRef: 'SPEC-112',
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
