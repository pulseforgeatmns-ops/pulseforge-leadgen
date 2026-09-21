'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  SESSION_STATUSES,
  ALLOWED_TRANSITIONS,
  BLUEPRINT_SECTIONS,
  QUESTION_BANK,
  ANSWER_KINDS,
  MESSAGE_TYPES,
  ClientIntelligenceError,
  createMemoryStore,
  scoreEvidenceConfidence,
  summarizeSection,
  computeProgress,
  buildUnderstandingProgress,
  buildExecutiveSummary,
  buildReflection,
  hasSpecificitySignals,
  looksAmbiguous,
  assertTransition,
  startClientInterview,
  postInterviewMessage,
  resumeInterview,
  getInterview,
  reviseBlueprint,
  approveBlueprint,
  answerLooksEmpty,
  classifyUserResponse,
  classifyInterviewMessage,
  looksLikeRefinementFeedback,
  containsMetaInstructionLanguage,
  containsRawPromptFragment,
  partitionUserResponse,
  sanitizeSummaryForBrief,
  synthesizeNormalizedFact,
  stripInterviewQuestionEcho,
  reviewCorrectionOperations,
  projectWorkingSemanticOperations,
} = require('../services/clientIntelligenceInterview');

const {
  SECTION_PROVENANCE,
} = require('../services/clientIntelligencePlaybookHandoff');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store, useMemoryPlaybookStore: true } };
}

const AJI_ANSWERS = [
  'Aji Home Services — premium residential cleaning.',
  'Recurring cleans, deep cleans, and move-out cleans.',
  'Busy homeowners and property managers.',
  'Bargain hunters and warehouses.',
  'Coastal South Carolina.',
  'Reliable crews and clear communication.',
  'Friendly professional voice.',
  'Book qualified cleaning appointments in 90 days.',
  'Appointments booked and close rate.',
];

async function completeInterview(opts) {
  const started = await startClientInterview({ clientId: 42 }, opts);
  let turn = started;
  for (const answer of AJI_ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  return { started, turn };
}

describe('clientIntelligenceInterview lifecycle', () => {
  it('allows only sequential status transitions', () => {
    assert.deepEqual(ALLOWED_TRANSITIONS.NEW, ['DISCOVERY']);
    assert.deepEqual(ALLOWED_TRANSITIONS.DISCOVERY, ['CLARIFICATION']);
    assert.deepEqual(ALLOWED_TRANSITIONS.CLIENT_REVIEW, ['APPROVED', 'DISCOVERY']);
    assert.throws(() => assertTransition('NEW', 'APPROVED'), ClientIntelligenceError);
    assert.throws(() => assertTransition('DISCOVERY', 'APPROVED'), ClientIntelligenceError);
    assert.equal(SESSION_STATUSES.length, 7);
  });

  it('starts interactive interview with first question and understanding progress', async () => {
    const { opts } = withStore();
    const started = await startClientInterview({ clientId: 7 }, opts);
    assert.ok(started.interviewId);
    assert.equal(started.status, 'DISCOVERY');
    assert.equal(started.nextAction, 'ASK');
    assert.equal(started.question.id, 'identity');
    assert.ok(started.question.goal);
    assert.ok(started.question.askedBecause);
    assert.match(started.message, /tell me about the business/i);
    assert.equal(started.progress.percent, 0);
    assert.equal(started.progress.label, 'Business Understanding');
    assert.ok(started.understanding);
    assert.equal(started.understanding.sections.length, BLUEPRINT_SECTIONS.length);
    for (const row of started.understanding.sections) {
      assert.equal('summary' in row, false);
      assert.ok(row.title);
      assert.ok(row.status);
    }
  });

  it('extracts evidence after every answer and advances stages', async () => {
    const { opts } = withStore();
    const started = await startClientInterview({ clientId: 8 }, opts);
    const turn = await postInterviewMessage(
      started.interviewId,
      'Aji Home Services provides residential cleaning.',
      opts
    );
    assert.ok(turn.evidence);
    assert.equal(turn.evidence.type, 'EXPLICIT');
    assert.equal(turn.evidence.category, 'identity');
    assert.ok(turn.evidence.confidence >= 0.7);
    assert.equal(turn.question.id, 'services');

    const detail = await getInterview(started.interviewId, opts);
    assert.equal(detail.evidence.length, 1);
    assert.ok(detail.turns.some((t) => t.speaker === 'client' && t.goal));
    assert.ok(detail.turns.some((t) => t.askedBecause));
  });

  it('completes discovery through full lifecycle to CLIENT_REVIEW blueprint', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    assert.equal(turn.status, 'CLIENT_REVIEW');
    assert.ok(turn.blueprint);
    assert.equal(turn.blueprint.generatedBy, 'CIE-v1');
    assert.equal(turn.blueprint.status, 'in_review');
    assert.ok(turn.executiveSummary);
    assert.equal(turn.executiveSummary.title, 'Executive Business Brief');
    assert.equal(turn.executiveSummary.subtitle, 'Prepared by Max');
    assert.equal(turn.executiveSummary.tagline, 'A working picture for leadership review');
    assert.equal(turn.executiveSummary.sections.length, 11);
    for (const key of BLUEPRINT_SECTIONS) {
      assert.ok(turn.blueprint.sections[key]);
      assert.ok(Array.isArray(turn.blueprint.sections[key].evidenceIds));
      assert.ok(Array.isArray(turn.blueprint.sections[key].unknowns));
      assert.ok('confidence' in turn.blueprint.sections[key]);
      assert.ok(turn.blueprint.sections[key].summary);
      // Consultant notes — not raw transcript mirroring
      assert.notEqual(turn.blueprint.sections[key].summary, AJI_ANSWERS[BLUEPRINT_SECTIONS.indexOf(key)]);
    }
    assert.ok(turn.understanding);
    for (const row of turn.understanding.sections) {
      assert.equal('summary' in row, false);
    }
    assert.equal(QUESTION_BANK.length, 9);
    assert.equal(turn.progress.percent, 100);
  });

  it('inserts conversational reflections every 3 answers without adding questions', async () => {
    const { opts } = withStore();
    const started = await startClientInterview({ clientId: 11 }, opts);
    let turn = started;
    for (let i = 0; i < 3; i += 1) {
      turn = await postInterviewMessage(started.interviewId, AJI_ANSWERS[i], opts);
    }
    assert.ok(turn.reflection);
    assert.match(
      turn.reflection,
      /here's what i'm hearing so far|let me make sure i understand|taking away so far/i
    );
    assert.equal(turn.question.id, 'avoid_customers');
    assert.equal(QUESTION_BANK.length, 9);

    const detail = await getInterview(started.interviewId, opts);
    const reflections = detail.turns.filter(
      (t) =>
        t.speaker === 'assistant' &&
        /here's what i'm hearing so far|let me make sure i understand|taking away so far/i.test(
          t.message
        )
    );
    assert.equal(reflections.length, 1);
    assert.equal(turn.progress.completed, 3);
  });

  it('resumes CLIENT_REVIEW into conversational refinement across multiple turns', async () => {
    const { opts, store } = withStore();
    const { turn } = await completeInterview(opts);
    assert.equal(turn.status, 'CLIENT_REVIEW');
    await assert.rejects(
      () => postInterviewMessage(turn.interviewId, 'More detail', opts),
      (err) => err instanceof ClientIntelligenceError && err.code === 'awaiting_review'
    );

    const resumed = await resumeInterview(turn.interviewId, opts);
    assert.equal(resumed.status, 'DISCOVERY');
    assert.equal(resumed.resumed, true);
    assert.match(resumed.message, /refine/i);

    const refined = await postInterviewMessage(
      turn.interviewId,
      'Our ideal customers are boutique hotel owners along the coast.',
      opts
    );
    assert.equal(refined.status, 'DISCOVERY');
    assert.equal(refined.nextAction, 'ASK');
    assert.equal(refined.question, null);
    assert.equal(refined.blueprint, null);
    assert.match(refined.message, /updated my working understanding/i);

    let session = await store.getSession(turn.interviewId);
    assert.equal(session.interview_state.refinementPass, true);
    assert.match(
      session.interview_state.sectionState.idealCustomers.summary,
      /boutique hotel|ideal customers/i
    );

    const secondRefinement = await postInterviewMessage(
      turn.interviewId,
      'Differentiation is a hypothesis: practical, transformation-focused 12-week approach.',
      opts
    );
    assert.equal(secondRefinement.status, 'DISCOVERY');
    assert.equal(secondRefinement.nextAction, 'ASK');
    assert.equal(secondRefinement.blueprint, null);

    session = await store.getSession(turn.interviewId);
    assert.equal(session.interview_state.refinementPass, true);
    assert.match(
      session.interview_state.sectionState.competitiveAdvantages.summary,
      /hypothesis|transformation-focused|12-week/i
    );
  });

  it('explicit refinement regeneration transitions to Blueprint review without approval', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    await resumeInterview(turn.interviewId, opts);

    const review = await postInterviewMessage(
      turn.interviewId,
      'Show me the updated Blueprint.',
      opts
    );
    assert.equal(review.status, 'CLIENT_REVIEW');
    assert.equal(review.nextAction, 'GENERATE_BLUEPRINT');
    assert.ok(review.blueprint);
    assert.equal(review.blueprint.status, 'in_review');
  });

  it('mixed refinement update plus explicit regeneration persists update first', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    await resumeInterview(turn.interviewId, opts);

    const review = await postInterviewMessage(
      turn.interviewId,
      'Our ideal customers are boutique hotel owners along the coast. Regenerate the brief.',
      opts
    );
    assert.equal(review.status, 'CLIENT_REVIEW');
    assert.equal(review.nextAction, 'GENERATE_BLUEPRINT');
    assert.ok(review.blueprint);
    assert.equal(review.blueprint.status, 'in_review');
    assert.match(
      review.blueprint.sections.idealCustomers.summary,
      /boutique hotel|ideal customers/i
    );
  });

  it('notes mode skips Q&A and generates blueprint', async () => {
    const { opts } = withStore();
    const started = await startClientInterview(
      {
        clientId: 9,
        notes:
          'We are Aji Home Services. We sell residential cleaning. Ideal customers are homeowners. Avoid warehouses. Market is Myrtle Beach. Advantage is reliable crews. Voice is friendly. Goal is more appointments. Success metric is booked jobs.',
      },
      opts
    );
    assert.equal(started.status, 'CLIENT_REVIEW');
    assert.ok(started.blueprint);
    assert.ok(started.blueprint.sections.identity.summary);
    assert.match(started.blueprint.sections.identity.summary, /Aji Home Services/i);
    assert.equal(
      /^We are Aji Home Services\.?$/i.test(started.blueprint.sections.identity.summary),
      false
    );
  });
});

describe('SPEC-226 Blueprint refinement semantic corrections', () => {
  it('persists inspectable correction history through the resumed Blueprint review path', async () => {
    const { opts, store } = withStore();
    const { started } = await completeInterview(opts);
    await resumeInterview(started.interviewId, opts);
    await postInterviewMessage(
      started.interviewId,
      'One primary offer = 12-week transformation program. Geography is not currently constrained. Lead volume is not a success metric.',
      opts
    );

    const session = await store.getSession(started.interviewId);
    const history = session.interview_state.workingSemanticCorrections;
    assert.ok(history.length >= 3);
    assert.ok(history.every((item) => item.evidence_ref && item.created_at));
    assert.deepEqual(session.interview_state.normalizedFacts.services, ['12-week transformation program']);
    assert.equal(session.interview_state.normalizedFacts.geography.length, 0);
    assert.equal(session.interview_state.normalizedFacts.success_metrics.some((item) => /lead volume/i.test(item)), false);
  });

  it('projects the Babrun correction turn into active beliefs without stale instruction text', () => {
    const prior = {
      business_name: 'Babrun',
      business_description: 'coaching programs for founders',
      services: ['delegation', 'premium positioning', '12-week coaching'],
      ideal_customers: ['any small business'],
      geography: ['United States'],
      differentiation: 'premium positioning',
      success_metrics: ['raw lead volume', 'founder dependence'],
      epistemic_states: { differentiation: 'KNOWN', geography: 'KNOWN' },
    };
    const correction = [
      'One primary offer = 12-week transformation program.',
      'Delegation is an outcome, not a separate service.',
      'ICP is an existing operating small business, generally fewer than 10 employees, with a founder operational bottleneck; initial segments are cleaning/home services, e-commerce, and fitness.',
      'Geography is not currently constrained.',
      'Differentiation is practical transformation-focused 12-week approach and remains a hypothesis, unvalidated.',
      'Qualified founder conversations, ICP-qualified conversations, serious program conversations, paid enrollments, and discovery to enrollment conversion are metrics.',
      'Pain-pattern frequency and segment-response patterns are learning signals.',
      'Employee problems, lack of owner time, founder dependence, and revenue pressure are pains or learning signals.',
      'Lead volume is not a success metric.',
      'We did not establish premium positioning.',
    ].join('\n');

    const operations = reviewCorrectionOperations(correction, { normalizedFacts: prior }, 'turn-babrun');
    const active = projectWorkingSemanticOperations(prior, operations);

    assert.ok(operations.length >= 12);
    assert.ok(operations.some((item) => item.operation === 'CORRECT'));
    assert.ok(operations.some((item) => item.operation === 'RETRACT'));
    assert.ok(operations.some((item) => item.operation === 'RECLASSIFY'));
    assert.deepEqual(active.services, ['12-week transformation program']);
    assert.ok(active.transformation_areas.some((item) => /delegation/i.test(item)));
    assert.ok(active.ideal_customers.some((item) => /existing operating small business/i.test(item)));
    assert.ok(active.ideal_customers.some((item) => /e-commerce/i.test(item)));
    assert.equal(active.geography.length, 0);
    // SPEC-228: an explicit "geography isn't a meaningful constraint" retraction
    // is NOT_APPLICABLE (deliberately unconstrained), not UNKNOWN (unresolved).
    assert.equal(active.epistemic_states.geography, 'NOT_APPLICABLE');
    assert.equal(active.epistemic_states.differentiation, 'HYPOTHESIS');
    assert.match(active.differentiation, /practical transformation-focused/i);
    assert.equal(active.success_metrics.some((item) => /lead volume|founder dependence/i.test(item)), false);
    assert.equal(active.excluded_metrics.some((item) => /lead volume/i.test(item)), true);
    assert.ok(active.pains.some((item) => /founder dependence/i.test(item)));
    assert.equal(/Do not interpret|not a success metric|did not establish premium positioning/i.test(JSON.stringify(active)), false);

    const brief = buildExecutiveSummary({
      identity: { summary: 'Babrun is a coaching business.', confidence: 0.7 },
      services: { summary: '', confidence: 0.7 },
      idealCustomers: { summary: '', confidence: 0.7 },
      targetMarkets: { summary: '', confidence: 0.7 },
      competitiveAdvantages: { summary: '', confidence: 0.7 },
      successMetrics: { summary: '', confidence: 0.7 },
    }, { normalizedFacts: active, clientId: 1 });
    const rendered = JSON.stringify(brief);
    assert.match(rendered, /12-week transformation program/i);
    assert.match(rendered, /qualified founder conversations/i);
    assert.equal(/lead volume|premium positioning|not a success metric|did not establish/i.test(rendered), false);
  });

  it('fails closed for an unresolved correction-like proposition', () => {
    const operations = reviewCorrectionOperations(
      'Correction: move that ambiguous thing to the right category.',
      { normalizedFacts: {} },
      'turn-ambiguous'
    );
    assert.deepEqual(operations.map((item) => item.operation), ['CLARIFY']);
    assert.equal(projectWorkingSemanticOperations({ success_metrics: ['existing metric'] }, operations).success_metrics[0], 'existing metric');
  });
});

describe('SPEC-228 complete pre-approval correction projection', () => {
  const { sectionsFromNormalizedFacts } = require('../services/clientIntelligenceInterview');

  // Production-equivalent contamination: identity accumulation, stale
  // services/metric confusion, literal correction prose stored as data, and
  // a prior Blueprint section state capable of stale fallback.
  const CONTAMINATED_PRIOR = {
    business_name: 'Babrun',
    business_description: 'Babrun is a coaching programs for founders',
    services: ['delegation', 'premium positioning', '12-week coaching'],
    ideal_customers: ['any small business', 'ICP-qualified conversations'],
    ideal_customer_traits: [],
    disqualified_customers: [],
    geography: ["Do not interpret geography isn't the primary constraint as a geographic market"],
    vertical_focus: null,
    differentiation: 'premium positioning',
    brand_voice: null,
    ninety_day_outcomes: null,
    success_metrics: [
      'raw lead volume',
      'founder dependence',
      'lead volume is explicitly not a success metric',
    ],
    epistemic_states: {
      business_description: 'KNOWN',
      services: 'KNOWN',
      ideal_customers: 'KNOWN',
      geography: 'KNOWN',
      differentiation: 'KNOWN',
    },
    hypotheses: {},
    evidence_statements: {},
    business_facts: {},
    transformation_areas: [],
    pains: [],
    learning_signals: [],
    excluded_metrics: [],
  };

  const PRIOR_SECTIONS = {
    identity: {
      summary: 'Babrun is a Babrun is a coaching programs for founders. This identity framing is how the operator describes the business today.',
      confidence: 0.7,
      evidenceIds: [],
      unknowns: [],
    },
    successMetrics: {
      summary: 'Success will be judged by raw lead volume, lead volume is explicitly not a success metric.',
      confidence: 0.7,
      evidenceIds: [],
      unknowns: [],
    },
  };

  const PRIOR_CORRECTIONS = [
    {
      operation: 'ASSERT',
      slot: 'services',
      value: '12-week coaching',
      evidence_ref: 'turn-prior',
      created_at: '2026-08-01T00:00:00.000Z',
      source_text: 'Add 12-week coaching as a service.',
    },
  ];

  const PRODUCTION_CORRECTION_TEXT = [
    'Before I approve this, I need to correct a few parts of your understanding.',
    '',
    'Babrun has one primary offer: the 12-week transformation program. Managing employees, delegation, reducing founder dependence, and reducing the owner\'s day-to-day operational burden are transformation areas or outcomes, not separate services.',
    '',
    'Geography isn\'t currently a meaningful targeting constraint. We\'re primarily targeting by business stage and characteristics: operating small businesses, generally fewer than 10 employees, where the founder is still too central to operations. Cleaning and other home services, e-commerce, and fitness are initial segments to test.',
    '',
    'Our differentiation is still a hypothesis: the practical, transformation-focused 12-week approach may be more compelling than generic business education or open-ended advice because it changes how the owner actually operates. We have not validated that as a consistent buying reason yet.',
    '',
    'The core measures I actually want to watch are qualified founder conversations, ICP-qualified conversations, serious program conversations, paid enrollments, and discovery-to-enrollment conversion.',
    '',
    'Pain patterns and segment response patterns are learning signals. Lack of owner time, founder dependence, employee problems, and revenue pressure are pain categories, not standalone metrics. Raw lead volume is explicitly not a success metric.',
    '',
    'We have not established premium positioning for Babrun.',
    '',
    'Please update your understanding from these corrections and regenerate the Executive Business Brief for my review. Do not approve the Blueprint yet.',
  ].join('\n');

  function runCorrectionRound(priorFacts, priorSections) {
    const operations = reviewCorrectionOperations(
      PRODUCTION_CORRECTION_TEXT,
      { normalizedFacts: priorFacts },
      'turn-babrun-228'
    );
    const active = projectWorkingSemanticOperations(priorFacts, operations);
    const sections = sectionsFromNormalizedFacts(active, priorSections);
    return { operations, active, sections };
  }

  it('eliminates business_description identity accumulation from a contaminated prior', () => {
    const { active } = runCorrectionRound(CONTAMINATED_PRIOR, PRIOR_SECTIONS);
    assert.equal(active.business_name, 'Babrun');
    assert.equal(/babrun is a babrun/i.test(active.business_description || ''), false);
    assert.equal(/^babrun is a/i.test(active.business_description || ''), false);
  });

  it('passes the SPEC-228 hard normalizedFacts state gate', () => {
    const { operations, active } = runCorrectionRound(CONTAMINATED_PRIOR, PRIOR_SECTIONS);

    // OFFER
    assert.deepEqual(active.services, ['12-week transformation program']);
    for (const term of ['managing employees', 'delegation', 'founder dependence', 'operational burden']) {
      assert.ok(
        active.transformation_areas.some((item) => new RegExp(term, 'i').test(item)),
        `expected transformation_areas to contain "${term}"`
      );
    }
    assert.equal(active.services.some((item) => /delegation|premium positioning|employee/i.test(item)), false);

    // ICP
    assert.ok(active.ideal_customers.some((item) => /existing operating small business/i.test(item)));
    assert.ok(active.ideal_customers.some((item) => /cleaning/i.test(item)));
    assert.ok(active.ideal_customers.some((item) => /e-commerce/i.test(item)));
    assert.ok(active.ideal_customers.some((item) => /fitness/i.test(item)));
    assert.ok(active.ideal_customer_traits.some((item) => /fewer than 10 employees/i.test(item)));
    assert.ok(active.ideal_customer_traits.some((item) => /founder operational bottleneck/i.test(item)));
    assert.equal(active.ideal_customers.some((item) => /conversations|enrollments/i.test(item)), false);

    // GEOGRAPHY
    assert.equal(active.geography.length, 0);
    assert.equal(active.epistemic_states.geography, 'NOT_APPLICABLE');
    assert.equal(active.geography.some((item) => /do not interpret/i.test(item)), false);

    // DIFFERENTIATION
    assert.match(active.differentiation, /practical.*transformation-focused 12-week approach/i);
    assert.equal(active.epistemic_states.differentiation, 'HYPOTHESIS');
    assert.equal(/premium positioning/i.test(active.differentiation || ''), false);

    // PAINS
    for (const pain of ['lack of owner time', 'founder dependence', 'employee problems', 'revenue pressure']) {
      assert.ok(active.pains.some((item) => new RegExp(pain, 'i').test(item)), `expected pains to contain "${pain}"`);
    }

    // METRICS
    for (const metric of [
      'qualified founder conversations',
      'icp-qualified conversations',
      'serious program conversations',
      'paid enrollments',
      'discovery-to-enrollment conversion',
    ]) {
      assert.ok(
        active.success_metrics.some((item) => new RegExp(metric, 'i').test(item)),
        `expected success_metrics to contain "${metric}"`
      );
    }
    for (const excluded of ['raw lead volume', 'lack of owner time', 'founder dependence', 'employee problems', 'revenue pressure']) {
      assert.equal(
        active.success_metrics.some((item) => new RegExp(excluded, 'i').test(item)),
        false,
        `expected success_metrics to NOT contain "${excluded}"`
      );
    }
    assert.ok(active.excluded_metrics.some((item) => /raw lead volume/i.test(item)));

    // CORRECTION INSTRUCTION LEAKAGE
    assert.equal(
      /do not interpret|not a success metric|not a standalone metric|did not establish|not established/i.test(JSON.stringify(active)),
      false
    );

    assert.ok(operations.some((item) => item.operation === 'CORRECT'));
    assert.ok(operations.some((item) => item.operation === 'RETRACT'));
    assert.ok(operations.some((item) => item.operation === 'RECLASSIFY'));
  });

  it('passes the SPEC-228 Blueprint regeneration gate', () => {
    const { active, sections } = runCorrectionRound(CONTAMINATED_PRIOR, PRIOR_SECTIONS);
    const brief = buildExecutiveSummary(sections, { normalizedFacts: active, clientId: 1 });
    const rendered = JSON.stringify(brief);

    assert.equal(/babrun is a babrun/i.test(rendered), false);
    assert.match(rendered, /12-week transformation program/i);
    assert.match(rendered, /qualified founder conversations/i);
    assert.equal(
      /premium positioning|lead volume is explicitly not a success metric|do not interpret|did not establish|not a standalone metric/i.test(rendered),
      false
    );
  });

  it('does not reintroduce stale identity/metric prose from prior sections when active state is clean', () => {
    const { sections } = runCorrectionRound(CONTAMINATED_PRIOR, PRIOR_SECTIONS);
    assert.equal(/babrun is a babrun/i.test(sections.identity.summary), false);
    assert.equal(/lead volume is explicitly not a success metric/i.test(sections.successMetrics.summary), false);
  });

  it('runs stably across three refinement cycles without re-accumulating stale beliefs', () => {
    let facts = CONTAMINATED_PRIOR;
    let sections = PRIOR_SECTIONS;
    let corrections = [...PRIOR_CORRECTIONS];
    for (let round = 0; round < 3; round += 1) {
      const operations = reviewCorrectionOperations(
        PRODUCTION_CORRECTION_TEXT,
        { normalizedFacts: facts },
        `turn-babrun-228-round-${round}`
      );
      corrections = [...corrections, ...operations];
      facts = projectWorkingSemanticOperations(facts, operations);
      sections = sectionsFromNormalizedFacts(facts, sections);
      assert.equal(/babrun is a babrun/i.test(facts.business_description || ''), false);
      assert.equal(/babrun is a babrun/i.test(sections.identity.summary || ''), false);
      assert.deepEqual(facts.services, ['12-week transformation program']);
      assert.equal(facts.geography.length, 0);
    }
    assert.ok(corrections.length > PRIOR_CORRECTIONS.length);
  });

  it('fails closed (CLARIFY, no mutation) for an ambiguous negation target', () => {
    const priorMetrics = ['existing metric'];
    const operations = reviewCorrectionOperations(
      'Correction: that is not a metric, remove it.',
      { normalizedFacts: { success_metrics: priorMetrics } },
      'turn-ambiguous-negation'
    );
    assert.ok(operations.some((item) => item.operation === 'CLARIFY'));
    const active = projectWorkingSemanticOperations({ success_metrics: priorMetrics }, operations);
    assert.deepEqual(active.success_metrics, priorMetrics);
  });

  it('fails closed (CLARIFY, no mutation) when a reclassification target is missing', () => {
    const priorServices = ['existing service'];
    const operations = reviewCorrectionOperations(
      'Reclassify that into the right bucket.',
      { normalizedFacts: { services: priorServices } },
      'turn-ambiguous-reclassify'
    );
    assert.ok(operations.some((item) => item.operation === 'CLARIFY'));
    const active = projectWorkingSemanticOperations({ services: priorServices }, operations);
    assert.deepEqual(active.services, priorServices);
  });

  it('keeps structured Save Edits and conversational refinement producing one coherent state', () => {
    const { applyCorrectionToNormalizedFacts } = require('../services/clientIntelligenceInterview');

    // A structured Save Edit on the identity field, typed the way an operator
    // naturally would (including the business name).
    let facts = applyCorrectionToNormalizedFacts(CONTAMINATED_PRIOR, {
      section: 'identity',
      substance: 'Babrun is a coaching company for small business founders',
    });
    assert.equal(/babrun is a babrun/i.test(facts.business_description || ''), false);

    // Structured edits must not be routed through reviewCorrectionOperations —
    // that free-form path stays a separate boundary.
    const structuredOps = reviewCorrectionOperations('', { normalizedFacts: facts }, 'turn-structured');
    assert.deepEqual(structuredOps, []);

    // Conversational refinement afterward must still produce one coherent state.
    const { operations, active } = runCorrectionRound(facts, PRIOR_SECTIONS);
    assert.equal(/babrun is a babrun/i.test(active.business_description || ''), false);
    assert.deepEqual(active.services, ['12-week transformation program']);
    assert.ok(operations.length > 0);
  });
});

describe('consultant summaries', () => {
  it('rewrites identity into understanding notes', () => {
    const summary = summarizeSection('identity', [
      'Anchor Cleaning—we are a commercial focused cleaning company serving Greater Manchester',
    ]);
    assert.match(summary, /Anchor Cleaning is a commercial/i);
    assert.ok(summary.split(/(?<=[.!?])\s+/).length <= 4);
  });
});

describe('confidence rules', () => {
  it('does not use response length — short explicit can score high', () => {
    const short = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'Property managers.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    const long = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement:
        'Property managers are our preferred ICP and we love working with them across many buildings and portfolios every single week of the year.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    // Length alone must not change score; both share the property-managers specificity signal.
    assert.equal(short, long);
  });

  it('differs naturally across specificity, ambiguity, confirmation, and contradiction', () => {
    const vague = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'Maybe various customers, not sure.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    const specific = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'Commercial property managers in Manchester.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    assert.ok(specific > vague);
    assert.equal(looksAmbiguous('Maybe various customers, not sure.'), true);
    assert.equal(hasSpecificitySignals('Commercial property managers in Manchester.'), true);

    const base = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'We serve property managers.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    const confirmed = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'Yes, property managers are correct.',
      priorStatements: ['We serve property managers.'],
      isConfirmation: true,
      hasCorroboration: true,
    });
    assert.ok(confirmed > base);

    const contradicted = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'We never serve property managers.',
      priorStatements: ['We serve property managers.'],
      isConfirmation: false,
      hasCorroboration: true,
    });
    assert.ok(contradicted < base);
  });

  it('spreads section confidence across a completed interview', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    const scores = BLUEPRINT_SECTIONS.map((k) => turn.blueprint.sections[k].confidence);
    const unique = new Set(scores.map((n) => Number(n.toFixed(3))));
    assert.ok(unique.size >= 2, 'expected confidence values to differ across sections');
  });

  it('treats empty answers as unknowns without inventing facts', () => {
    assert.equal(answerLooksEmpty('n/a'), true);
    assert.equal(answerLooksEmpty('Property managers'), false);
  });

  it('progress is driven by completed blueprint sections', () => {
    const progress = computeProgress({
      identity: { summary: 'Acme is a cleaning company.' },
      services: { summary: '' },
      idealCustomers: { summary: 'Busy homeowners.' },
    });
    assert.equal(progress.completed, 2);
    assert.equal(progress.total, BLUEPRINT_SECTIONS.length);
    assert.equal(progress.percent, Math.round((2 / BLUEPRINT_SECTIONS.length) * 100));
  });

  it('buildReflection summarizes current understanding only', () => {
    const text = buildReflection(
      {
        identity: { summary: 'Aji Home Services is a premium residential cleaning company.' },
        services: { summary: 'Today the business delivers recurring and deep cleans.' },
      },
      3
    );
    assert.match(text, /here's what i'm hearing so far/i);
    assert.match(text, /Identity:/);
    assert.match(text, /Services:/);
  });

  it('buildUnderstandingProgress never includes summaries', () => {
    const progress = buildUnderstandingProgress({
      identity: {
        summary: 'Secret narrative that clients must not see yet.',
        confidence: 0.9,
        evidenceIds: ['e1'],
        unknowns: [],
      },
      services: {
        summary: '',
        confidence: 0.2,
        evidenceIds: [],
        unknowns: ['Missing clear answer for services'],
      },
    });
    assert.equal(progress.sections.length, BLUEPRINT_SECTIONS.length);
    const identity = progress.sections.find((s) => s.key === 'identity');
    assert.equal(identity.status, 'ready');
    assert.equal('summary' in identity, false);
    assert.ok(!JSON.stringify(progress).includes('Secret narrative'));
  });

  it('buildExecutiveSummary synthesizes CEO-facing Executive Business Brief', () => {
    const summary = buildExecutiveSummary({
      identity: {
        summary:
          'Aji is a cleaning company. This identity framing is how the operator describes the business today, and it anchors every other Blueprint section.',
        confidence: 0.9,
        unknowns: [],
      },
      services: {
        summary:
          'Today the business delivers recurring cleans. Service understanding reflects what is actually sold now, not aspirational packaging.',
        confidence: 0.8,
        unknowns: [],
      },
      idealCustomers: {
        summary: 'Ideal customers are busy homeowners. This ICP picture prioritizes fit over volume.',
        confidence: 0.8,
        unknowns: [],
      },
      avoidCustomers: {
        summary:
          'The business prefers to avoid bargain hunters. These constraints protect targeting quality and should stay visible in the Blueprint.',
        confidence: 0.7,
        unknowns: [],
      },
      targetMarkets: {
        summary:
          'Priority markets center on Coastal SC. Geography and vertical focus here bound where discovery should concentrate first.',
        confidence: 0.7,
        unknowns: [],
      },
      competitiveAdvantages: {
        summary:
          'Competitive edge is described as reliable crews. This is operator-stated differentiation — useful for messaging, not an invented strategy claim.',
        confidence: 0.8,
        unknowns: [],
      },
      brandVoice: {
        summary:
          'Brand voice should read as friendly professional. Tone guidance constrains later language without choosing channels or campaigns.',
        confidence: 0.7,
        unknowns: [],
      },
      campaignGoals: {
        summary:
          'Near-term growth goals focus on book appointments. These are desired business outcomes for the next phase of work, not execution tactics.',
        confidence: 0.8,
        unknowns: [],
      },
      successMetrics: {
        summary:
          'Success will be judged by close rate. These signals define whether the engagement is working from the client\'s perspective.',
        confidence: 0.6,
        unknowns: ['Pricing philosophy', 'Missing clear answer for capacity'],
      },
    });
    assert.equal(summary.title, 'Executive Business Brief');
    assert.equal(summary.subtitle, 'Prepared by Max');
    assert.equal(summary.tagline, 'A working picture for leadership review');
    assert.equal(summary.sections.length, 11);
    const byId = Object.fromEntries(summary.sections.map((s) => [s.id, s]));
    assert.equal(byId.whoYouAre.title, 'Who You Are');
    assert.equal(byId.whoYouServe.title, 'Who You Serve');
    assert.equal(byId.whyChooseYou.title, 'Why Customers Choose You');
    assert.equal(byId.whereHeaded.title, "Where You're Headed");
    assert.equal(byId.recommendedScorecard.title, 'Recommended Operator Scorecard');
    assert.equal(byId.approvedScorecard.title, 'Operator Approved Scorecard');
    assert.equal(byId.metricsUnderReview.title, 'Metrics Under Review');
    assert.equal(byId.observations.title, 'Initial Observations');
    assert.equal(byId.assessment.title, "Max's Initial Assessment");
    assert.equal(byId.learnMore.title, "Areas I'd Like To Learn More");
    assert.equal(byId.conversations.title, "Conversations I'd Recommend Next");

    const banned =
      /\n|•|Blueprint|operator-stated|ICP|Unknown:|Missing clear answer|Generated from|evidenceId|sectionKey|CIE-v|prompt artifact/i;
    for (const section of [byId.whoYouAre, byId.whoYouServe, byId.whyChooseYou, byId.whereHeaded]) {
      const sentences = section.body.split(/(?<=[.!?])\s+/).filter(Boolean);
      assert.ok(sentences.length >= 2 && sentences.length <= 4, `${section.id}: ${sentences.length}`);
      assert.equal(banned.test(section.body), false, section.body);
    }
    assert.match(byId.whoYouAre.body, /Aji/);
    assert.match(byId.whoYouAre.body, /recurring cleans/i);
    assert.match(byId.whoYouAre.body, /center of gravity|growth advice/i);
    assert.equal(
      /Service understanding reflects|anchors every other|deciding factors tend to be|creates value through recurring|concentrate first in |This is a business built around|The relationships worth winning are with|Near-term commercial attention belongs in |Customers choose this business for /i.test(
        byId.whoYouAre.body + byId.whyChooseYou.body + byId.whoYouServe.body
      ),
      false
    );
    assert.match(byId.whoYouServe.body, /busy homeowners/i);
    assert.match(byId.whoYouServe.body, /Ideal customers include/i);
    assert.match(byId.whyChooseYou.body, /reliable crews/i);
    assert.match(byId.whyChooseYou.body, /Customers choose (?:Aji|this business)/i);
    assert.match(byId.whereHeaded.body, /booking appointments/i);
    assert.match(byId.whereHeaded.body, /(?:near-term priority|growth priorities)/i);

    assert.ok(byId.observations.items.length >= 1);
    assert.ok(byId.observations.items.length <= 5);
    assert.match(byId.observations.items.join(' '), /reliable crews|commercial focus|positioning|differentiation|geography|Coastal SC/i);
    assert.equal(/should launch|recommend that you|campaign/i.test(byId.observations.items.join(' ')), false);

    assert.equal(byId.assessment.ratings.length, 4);
    for (const rating of byId.assessment.ratings) {
      assert.ok(rating.stars >= 1 && rating.stars <= 5);
      assert.ok(String(rating.explanation || '').length > 10);
    }
    assert.ok(byId.assessment.confidencePercent >= 1 && byId.assessment.confidencePercent <= 100);

    assert.ok(byId.learnMore.items.length >= 1);
    assert.match(byId.learnMore.items.join(' '), /Pricing philosophy/i);
    assert.equal(/nothing outstanding|no major gaps/i.test(byId.learnMore.body + byId.learnMore.items.join(' ')), false);

    assert.ok(byId.conversations.items.length >= 1);
    assert.match(byId.conversations.body, /I'd enjoy exploring/i);
  });

  it('buildExecutiveSummary always identifies learn-more areas even when unknowns are empty', () => {
    const filled = {
      summary: 'A clear statement about the business.',
      confidence: 0.85,
      unknowns: [],
    };
    const summary = buildExecutiveSummary({
      identity: filled,
      services: filled,
      idealCustomers: filled,
      avoidCustomers: filled,
      targetMarkets: filled,
      competitiveAdvantages: filled,
      brandVoice: filled,
      campaignGoals: filled,
      successMetrics: filled,
    });
    const learnMore = summary.sections.find((s) => s.id === 'learnMore');
    assert.ok(learnMore.items.length >= 3);
    assert.equal(/nothing outstanding/i.test(JSON.stringify(learnMore)), false);
  });
});

describe('Executive Brief refinement / evidence separation', () => {
  const BANNED_META =
    /This revision introduced a problem|turn the raw interview answers|the brief is treating|instructions to Max|do not include|please regenerate|clean business language|business facts only|the substance is mostly right/i;

  const ANCHOR_SECTIONS = {
    identity: {
      summary:
        'Anchor Cleaning is a cleaning company. This identity framing is how the operator describes the business today, and it anchors every other Blueprint section.',
      confidence: 0.9,
      unknowns: [],
    },
    services: {
      summary:
        'Today the business delivers residential cleaning, office cleaning, deep cleans, move-in/move-out cleans, short-term rental turnovers, and recurring cleaning. Service understanding reflects what is actually sold now, not aspirational packaging.',
      confidence: 0.88,
      unknowns: [],
    },
    idealCustomers: {
      summary:
        'Ideal customers are property managers, short-term rental companies, facilities managers, daycares, schools, and high-traffic buildings — plus residential clients who value reliability. This ICP picture prioritizes fit over volume.',
      confidence: 0.86,
      unknowns: [],
    },
    avoidCustomers: {
      summary:
        'The business prefers to avoid customers who only value the lowest price. Better-fit customers value reliability, responsiveness, professionalism, accountability, and peace of mind. These constraints protect targeting quality and should stay visible in the Blueprint.',
      confidence: 0.84,
      unknowns: [],
    },
    targetMarkets: {
      summary:
        'Priority markets center on Greater Manchester, including Bedford, Londonderry, Auburn, Goffstown, and Hooksett, with a near-term growth focus on commercial cleaning. Geography and vertical focus here bound where discovery should concentrate first.',
      confidence: 0.85,
      unknowns: [],
    },
    competitiveAdvantages: {
      summary:
        'Competitive edge is described as customers trust the team to show up consistently, communicate clearly, solve problems quickly, and make facilities feel taken care of. This is operator-stated differentiation — useful for messaging, not an invented strategy claim.',
      confidence: 0.87,
      unknowns: [],
    },
    brandVoice: {
      summary:
        'Brand voice should read as calm, professional, reliable, direct, and easy to work with. Tone guidance constrains later language without choosing channels or campaigns.',
      confidence: 0.82,
      unknowns: [],
    },
    campaignGoals: {
      summary:
        'Near-term growth goals focus on commercial cleaning growth in Greater Manchester. These are desired business outcomes for the next phase of work, not execution tactics.',
      confidence: 0.83,
      unknowns: [],
    },
    successMetrics: {
      summary:
        'Success will be judged by more qualified conversations, more walkthroughs or estimate requests, clearer market learning, and a small but real pipeline of commercial opportunities. These signals define whether the engagement is working from the client\'s perspective.',
      confidence: 0.8,
      unknowns: [],
    },
  };

  it('classifies refinement phrases as refinement_feedback, not business_fact', () => {
    const samples = [
      'Please refine the Executive Business Brief.',
      'This revision introduced a problem in Who You Are.',
      'The brief is treating instructions as facts.',
      'Max is treating refinement feedback as business evidence.',
      'Do not include instructions to Max in the brief.',
      'Please regenerate with clean business language.',
      'Turn the raw interview answers into clean business language.',
      'These are instructions to Max, not facts about Anchor.',
    ];
    for (const sample of samples) {
      assert.equal(
        classifyUserResponse(sample),
        ANSWER_KINDS.REFINEMENT_FEEDBACK,
        sample
      );
      assert.equal(looksLikeRefinementFeedback(sample), true, sample);
      assert.equal(containsMetaInstructionLanguage(sample), true, sample);
    }
    assert.equal(
      classifyUserResponse('Anchor is a cleaning company serving Greater Manchester.'),
      ANSWER_KINDS.BUSINESS_FACT
    );
    assert.equal(
      classifyUserResponse('ignore this', { speaker: 'system' }),
      ANSWER_KINDS.SYSTEM_GUIDANCE
    );
    assert.equal(
      classifyUserResponse('Executive Business Brief body', { context: 'generated_brief' }),
      ANSWER_KINDS.GENERATED_BRIEF
    );
  });

  it('partitions mixed messages so only business facts remain', () => {
    const partitioned = partitionUserResponse(
      'This revision introduced a problem. Anchor is a cleaning company serving Greater Manchester. Please regenerate and do not include instructions to Max.'
    );
    assert.ok(partitioned.facts.some((f) => /Anchor is a cleaning company/i.test(f)));
    assert.ok(partitioned.guidance.length >= 1);
    assert.equal(
      partitioned.facts.some((f) => /This revision introduced/i.test(f)),
      false
    );
  });

  it('sanitizeSummaryForBrief strips meta-instruction language from evidence', () => {
    const dirty =
      'Anchor Cleaning is a cleaning company. This revision introduced a problem. Please regenerate with clean business language.';
    const clean = sanitizeSummaryForBrief(dirty);
    assert.match(clean, /Anchor Cleaning is a cleaning company/i);
    assert.equal(BANNED_META.test(clean), false, clean);
  });

  it('does not include refinement feedback in Executive Business Brief sections', () => {
    const contaminated = {
      ...ANCHOR_SECTIONS,
      identity: {
        summary:
          'The business is understood as This revision introduced a problem — turn the raw interview answers into clean business language. This identity framing is how the operator describes the business today, and it anchors every other Blueprint section.',
        confidence: 0.95,
        unknowns: [],
      },
      idealCustomers: {
        summary:
          'Ideal customers are The brief is treating instructions to Max as facts about Anchor. Please regenerate. This ICP picture prioritizes fit over volume.',
        confidence: 0.95,
        unknowns: [],
      },
    };
    const brief = buildExecutiveSummary(contaminated);
    const blob = JSON.stringify(brief);
    assert.equal(BANNED_META.test(blob), false, blob);
    for (const section of brief.sections) {
      const text = `${section.body || ''} ${(section.items || []).join(' ')} ${(section.ratings || [])
        .map((r) => r.explanation)
        .join(' ')}`;
      assert.equal(BANNED_META.test(text), false, `${section.id}: ${text}`);
    }
  });

  it('preserves valid Anchor facts in polished synthesized form', () => {
    const brief = buildExecutiveSummary(ANCHOR_SECTIONS);
    const byId = Object.fromEntries(brief.sections.map((s) => [s.id, s]));
    const blob = JSON.stringify(brief);

    assert.equal(BANNED_META.test(blob), false, blob);
    assert.match(byId.whoYouAre.body, /Anchor/i);
    assert.match(byId.whoYouAre.body, /cleaning/i);
    assert.match(
      byId.whoYouAre.body,
      /residential cleaning|office cleaning|deep cleans|move-in|recurring cleaning/i
    );
    assert.match(byId.whoYouServe.body, /property managers|Greater Manchester|Bedford|Hooksett/i);
    assert.match(byId.whoYouServe.body, /lowest price|reliability|peace of mind/i);
    assert.match(
      byId.whyChooseYou.body,
      /show up consistently|communicate clearly|solve problems|taken care of/i
    );
    assert.match(byId.whyChooseYou.body, /calm|professional|reliable|direct/i);
    assert.match(byId.whereHeaded.body, /commercial cleaning|Greater Manchester/i);
    assert.match(
      JSON.stringify(byId.recommendedScorecard),
      /qualified conversations|walkthroughs|estimate|commercial opportunities|qualified prospects/i
    );

    // Polished synthesis — not Mad-Lib raw concatenation templates
    assert.equal(
      /This is a business built around|The relationships worth winning are with|Near-term commercial attention belongs in |Customers choose this business for /i.test(
        blob
      ),
      false
    );
    assert.match(byId.whoYouServe.body, /Ideal customers include/i);
    assert.match(byId.whyChooseYou.body, /Customers choose (?:Anchor(?: Cleaning)?|this business)/i);
    assert.match(byId.whoYouAre.body, /Services include/i);
  });

  it('ratings do not depend on refinement instructions as evidence', () => {
    const clean = buildExecutiveSummary(ANCHOR_SECTIONS);
    const contaminated = buildExecutiveSummary({
      ...ANCHOR_SECTIONS,
      identity: {
        summary:
          'This revision introduced a problem. Please regenerate. Instructions to Max are not facts about Anchor.',
        confidence: 0.99,
        unknowns: [],
      },
      services: {
        summary: 'Do not include raw interview answers. The substance is mostly right.',
        confidence: 0.99,
        unknowns: [],
      },
    });
    const cleanAssessment = clean.sections.find((s) => s.id === 'assessment');
    const dirtyAssessment = contaminated.sections.find((s) => s.id === 'assessment');
    assert.ok(dirtyAssessment.confidencePercent < cleanAssessment.confidencePercent);
    const clarity = dirtyAssessment.ratings.find((r) => r.label === 'Business Clarity');
    assert.ok(clarity.stars <= 2);
    assert.equal(BANNED_META.test(JSON.stringify(dirtyAssessment)), false);
  });

  it('refinement resume stores guidance without contaminating blueprint commercial fields', async () => {
    const { opts, store } = withStore();
    const { turn } = await completeInterview(opts);
    await resumeInterview(turn.interviewId, opts);
    const refined = await postInterviewMessage(
      turn.interviewId,
      'This revision introduced a problem. The brief is treating instructions to Max as facts. Please regenerate and turn the raw interview answers into clean business language. Do not include refinement feedback.',
      opts
    );
    assert.equal(refined.status, 'CLIENT_REVIEW');
    assert.ok(refined.executiveSummary);
    const blob = JSON.stringify(refined.executiveSummary);
    assert.equal(BANNED_META.test(blob), false, blob);
    assert.equal(BANNED_META.test(JSON.stringify(refined.blueprint.sections)), false);

    const session = await store.getSession(turn.interviewId);
    assert.ok((session.interview_state.revisionGuidance || []).length >= 1);
    assert.equal(
      session.interview_state.revisionGuidance.some((g) =>
        /This revision introduced/i.test(g.message)
      ),
      true
    );
  });
});

describe('blueprint revise + approve + playbook handoff', () => {
  it('revises in_review blueprint and records CLIENT_EDITED evidence', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    const revised = await reviseBlueprint(
      turn.blueprint.id,
      { identity: { summary: 'Aji Home Services LLC' } },
      opts
    );
    assert.equal(revised.sections.identity.summary, 'Aji Home Services LLC');
    assert.equal(revised.version, '1.0');
    const detail = await getInterview(turn.interviewId, opts);
    assert.ok(detail.evidence.some((e) => e.type === 'CLIENT_EDITED'));
  });

  it('approves immutably and creates pending_review playbook with provenance', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    const result = await approveBlueprint(turn.blueprint.id, opts);
    assert.equal(result.blueprint.status, 'approved');
    assert.equal(result.ok, true);
    assert.equal(result.status, 'APPROVED');
    assert.equal(result.message, 'approved');
    assert.equal(result.alreadyApproved, false);
    assert.ok(result.playbook);
    assert.equal(result.playbook.status, 'pending_review');
    assert.deepEqual(result.playbook.preferredChannels, []);
    assert.deepEqual(result.playbook.offers, []);
    assert.deepEqual(result.playbook.outreachSequence, []);
    assert.ok(result.initialGrowthDirection);
    assert.equal(result.initialGrowthDirection.kind, 'initial_growth_direction');
    assert.ok(result.initialGrowthDirection.paragraphs.length >= 3);
    assert.ok(result.sectionProvenance);
    for (const [field, sources] of Object.entries(SECTION_PROVENANCE)) {
      if (['preferredChannels', 'outreachSequence', 'offers', 'constraints'].includes(field)) {
        continue;
      }
      assert.ok(sources.length >= 1, `${field} must trace to blueprint sections`);
    }

    const again = await approveBlueprint(turn.blueprint.id, opts);
    assert.equal(again.alreadyApproved, true);
    assert.equal(again.ok, true);
    assert.equal(again.status, 'APPROVED');
    assert.equal(again.message, 'already_approved');
    assert.equal(again.blueprint.status, 'approved');
  });

  it('treats APPROVED session retries as already_approved (no red error)', async () => {
    const { opts, store } = withStore();
    const { turn } = await completeInterview(opts);
    const first = await approveBlueprint(turn.blueprint.id, opts);
    assert.equal(first.alreadyApproved, false);

    // Simulate stale UI race: session APPROVED but caller still holding in_review snapshot id.
    await store.updateBlueprint(turn.blueprint.id, turn.blueprint.version, {
      status: 'in_review',
    });
    await store.updateSession(turn.interviewId, { status: 'APPROVED' });

    const again = await approveBlueprint(turn.blueprint.id, opts);
    assert.equal(again.ok, true);
    assert.equal(again.status, 'APPROVED');
    assert.equal(again.message, 'already_approved');
    assert.equal(again.alreadyApproved, true);
  });

  it('never overwrites an approved blueprint — revise creates new version', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    await approveBlueprint(turn.blueprint.id, opts);
    const next = await reviseBlueprint(
      turn.blueprint.id,
      { services: { summary: 'Only recurring residential cleans' } },
      opts
    );
    assert.notEqual(next.version, '1.0');
    assert.equal(next.status, 'in_review');
    assert.equal(next.sections.services.summary, 'Only recurring residential cleans');
  });
});

describe('interview message classification + supplemental memory', () => {
  it('classifies message types before attaching to the active question', () => {
    assert.equal(
      classifyInterviewMessage('Also, we only serve Greater Manchester.'),
      MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT
    );
    assert.equal(
      classifyInterviewMessage('I forgot to mention we avoid bargain hunters.'),
      MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT
    );
    assert.equal(
      classifyInterviewMessage('I also forgot to mention property managers for ICP', {
        activeQuestion: QUESTION_BANK.find((q) => q.id === 'avoid_customers'),
      }),
      MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT
    );
    assert.equal(
      classifyInterviewMessage('Actually, focus on Bedford and Hooksett first.'),
      MESSAGE_TYPES.CORRECTION
    );
    assert.equal(
      classifyInterviewMessage('This still sounds weird — Max isn’t understanding.'),
      MESSAGE_TYPES.REFINEMENT_FEEDBACK
    );
    assert.equal(
      classifyInterviewMessage('Can you explain what you mean by ideal customer?'),
      MESSAGE_TYPES.QUESTION_TO_MAX
    );
    assert.equal(
      classifyInterviewMessage('Anchor Cleaning is a commercial cleaning company.'),
      MESSAGE_TYPES.DIRECT_ANSWER
    );
  });

  it('stores supplemental context separately without advancing the active question', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 99 }, opts);
    assert.equal(started.question.id, 'identity');

    const supplement = await postInterviewMessage(
      started.interviewId,
      'Also, for context — we only take commercial accounts in Greater Manchester.',
      opts
    );
    assert.equal(supplement.messageType, MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT);
    assert.equal(supplement.question.id, 'identity');
    assert.match(supplement.message, /business context|ideal customer|geography|remember/i);
    assert.ok((supplement.supplementalContext || []).length >= 1);

    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 0);
    assert.equal(session.interview_state.answers.identity, undefined);
    assert.ok((session.interview_state.supplementalContext || []).length >= 1);
    assert.equal(
      session.interview_state.supplementalContext[0].domain,
      'geography'
    );
    assert.equal(
      session.interview_state.supplementalContext[0].confirmed,
      true
    );

    const realAnswer = await postInterviewMessage(
      started.interviewId,
      'Anchor Cleaning — commercial cleaning for professional offices.',
      opts
    );
    assert.equal(realAnswer.question.id, 'services');
    const after = await store.getSession(started.interviewId);
    assert.equal(after.interview_state.stepIndex, 1);
    assert.match(after.interview_state.answers.identity, /Anchor Cleaning/i);
  });

  it('refinement feedback does not become business evidence', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 100 }, opts);
    const turn = await postInterviewMessage(
      started.interviewId,
      'The brief should be more conversational — this needs to be fixed.',
      opts
    );
    assert.equal(turn.messageType, MESSAGE_TYPES.REFINEMENT_FEEDBACK);
    assert.equal(turn.question.id, 'identity');
    assert.equal(turn.evidence, null);

    const session = await store.getSession(started.interviewId);
    assert.ok((session.interview_state.revisionGuidance || []).length >= 1);
    assert.equal(session.interview_state.stepIndex, 0);
    assert.equal(
      String(session.interview_state.sectionState.identity.summary || ''),
      ''
    );
  });

  it('correction messages update or supersede relevant facts', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 101 }, opts);
    await postInterviewMessage(
      started.interviewId,
      'Anchor Cleaning — commercial cleaning.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Office cleaning and recurring commercial cleans.',
      opts
    );
    // On ideal_customers question — correction about geography should not become the ICP answer.
    const before = await store.getSession(started.interviewId);
    assert.equal(
      QUESTION_BANK[before.interview_state.stepIndex].id,
      'ideal_customers'
    );

    const corrected = await postInterviewMessage(
      started.interviewId,
      'Actually, replace that — geography should be Greater Manchester including Bedford and Hooksett.',
      opts
    );
    assert.equal(corrected.messageType, MESSAGE_TYPES.CORRECTION);
    assert.equal(corrected.question.id, 'ideal_customers');
    assert.match(corrected.message, /update|geography|correction|replaced/i);

    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, before.interview_state.stepIndex);
    assert.equal(session.interview_state.answers.ideal_customers, undefined);
    assert.match(
      String(session.interview_state.sectionState.targetMarkets.summary || ''),
      /Greater Manchester|Bedford|Hooksett/i
    );
  });

  it('last-message correction without domain language targets the prior answer', async () => {
    const {
      resolveCorrectionTarget,
      looksLikeCorrection,
    } = require('../services/clientIntelligenceInterview');

    assert.equal(
      looksLikeCorrection('disregard last message, please replace with the following; calm and reliable crews'),
      true
    );

    const target = resolveCorrectionTarget(
      'disregard last message, please replace with the following; calm and reliable crews',
      {
        activeQuestion: QUESTION_BANK.find((q) => q.id === 'brand_voice'),
        state: {
          stepIndex: 6,
          answers: {
            identity: 'Anchor Cleaning',
            services: 'office cleaning',
            ideal_customers: 'property managers',
            avoid_customers: 'lowest price',
            target_markets: 'Greater Manchester',
            advantages: 'Pulseforge software automation',
          },
        },
      }
    );
    assert.equal(target.reason, 'last_answered');
    assert.equal(target.section, 'competitiveAdvantages');
    assert.equal(target.questionId, 'advantages');
  });

  it('ICP forgot-to-mention add-on updates ideal customers without answering avoid question', async () => {
    const {
      looksLikeSupplementalContext,
      classifyInterviewMessage,
      parseSupplementalMessage,
    } = require('../services/clientIntelligenceInterview');

    const msg = 'I also forgot to mention property managers for ICP';
    const avoidQ = QUESTION_BANK.find((q) => q.id === 'avoid_customers');
    assert.equal(looksLikeSupplementalContext(msg, { activeQuestion: avoidQ }), true);
    assert.equal(
      classifyInterviewMessage(msg, { activeQuestion: avoidQ }),
      MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT
    );
    const parsed = parseSupplementalMessage(msg);
    assert.equal(parsed.domain, 'ideal_customer');
    assert.equal(parsed.section, 'idealCustomers');
    assert.equal(parsed.substance, 'property managers');
    assert.equal(/forgot to mention|for ICP/i.test(parsed.substance), false);

    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 10 }, opts);
    await postInterviewMessage(
      started.interviewId,
      'Anchor Cleaning we are a commercial-focused cleaning company.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'standard office, recurring cleans, deep cleans',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'facility managers, professional offices, daycares',
      opts
    );

    const before = await store.getSession(started.interviewId);
    assert.equal(QUESTION_BANK[before.interview_state.stepIndex].id, 'avoid_customers');
    assert.equal(
      (before.interview_state.normalizedFacts.ideal_customers || []).some((s) =>
        /property managers/i.test(s)
      ),
      false
    );
    assert.deepEqual(before.interview_state.normalizedFacts.disqualified_customers || [], []);

    const supplement = await postInterviewMessage(started.interviewId, msg, opts);
    assert.equal(supplement.messageType, MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT);
    assert.equal(supplement.question.id, 'avoid_customers');
    assert.match(
      supplement.message,
      /add property managers to your ideal customer profile/i
    );
    assert.match(
      supplement.message,
      /are there any customers or segments you'?d rather not take on/i
    );
    assert.equal(/Where should we focus first/i.test(supplement.message), false);

    const after = await store.getSession(started.interviewId);
    assert.equal(after.interview_state.stepIndex, before.interview_state.stepIndex);
    assert.equal(QUESTION_BANK[after.interview_state.stepIndex].id, 'avoid_customers');
    assert.equal(after.interview_state.answers.avoid_customers, undefined);
    assert.ok(
      (after.interview_state.normalizedFacts.ideal_customers || []).some((s) =>
        /property managers/i.test(s)
      )
    );
    assert.deepEqual(after.interview_state.normalizedFacts.disqualified_customers || [], []);
    assert.equal(
      /forgot to mention|I also forgot/i.test(
        JSON.stringify(after.interview_state.normalizedFacts)
      ),
      false
    );

    // Finish interview and assert brief treats property managers as ICP, not declined.
    await postInterviewMessage(
      started.interviewId,
      "I don't want to work with customers whose main priority is the lowest price",
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Greater Manchester area includes Bedford, Hooksett, Londonderry, Auburn, Goffstown',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'trust — responsive, consistent, and accountable without needing to chase the work',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      "anchor's brand voice should sound calm, professional, reliable, and easy to work with",
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'would feel successful if Anchor has a clearer path to commercial opportunities over the next 90 days',
      opts
    );
    const done = await postInterviewMessage(
      started.interviewId,
      'we will know by watching qualified replies, booked conversations, and walkthroughs',
      opts
    );
    assert.ok(done.executiveSummary);
    const blob = JSON.stringify(done.executiveSummary);
    assert.match(blob, /property managers/i);
    assert.equal(/forgot to mention/i.test(blob), false, blob);
    assert.equal(
      /declines? property managers|avoid(?:s|ing)? property managers|does not want .*property managers/i.test(
        blob
      ),
      false,
      blob
    );
    const byId = Object.fromEntries(done.executiveSummary.sections.map((s) => [s.id, s]));
    assert.match(byId.whoYouServe.body, /property managers/i);
    assert.equal(
      /declines? property managers|avoid(?:s|ing)? property managers/i.test(
        byId.whoYouServe.body
      ),
      false
    );
  });
});

describe('Executive Brief synthesis — no raw answer bleed', () => {
  const BANNED_BLEED =
    /when a great-fit customer chooses|anchor'?s brand voice should sound|over the next 90 days,\s*this growth work|we will know the growth work is working|i don't want to work with|Customers choose this business because of when|Brand voice should feel .*\bshould\s+(?:sound|feel)|Near-term growth priorities center on over the next|Success will be judged by we will know|The business declines i don't want/i;

  it('normalizes raw answers before brief rendering', () => {
    assert.match(
      synthesizeNormalizedFact(
        'avoid',
        "i don't want to work with customers who's main priority is the lowest price",
        { businessName: 'Anchor' }
      ),
      /Anchor deliberately avoids customers who prioritize the lowest price/i
    );
    assert.match(
      synthesizeNormalizedFact(
        'markets',
        'both- greater Manchester area includes Bedford, hooksett, Londonderry, auburn, goffstown',
        { businessName: 'Anchor' }
      ),
      /Greater Manchester area.*Bedford.*Hooksett.*Londonderry.*Auburn.*Goffstown/i
    );
    assert.match(
      synthesizeNormalizedFact(
        'advantages',
        'when a great-fit customer chooses Anchor over someone else, what usually tips the decision is trust — responsive, consistent, and accountable without needing to chase the work',
        { businessName: 'Anchor' }
      ),
      /Customers choose Anchor because they trust/i
    );
    assert.equal(
      containsRawPromptFragment(
        synthesizeNormalizedFact(
          'advantages',
          'when a great-fit customer chooses Anchor over someone else, what usually tips the decision is trust',
          { businessName: 'Anchor' }
        )
      ),
      false
    );
    assert.match(
      synthesizeNormalizedFact(
        'metrics',
        'we will know the growth work is working by watching both activity quality and real opportunity movement',
        { businessName: 'Anchor' }
      ),
      /Success should be measured by (?:watching )?both activity quality and real opportunity movement|activity quality and real opportunity movement/i
    );
  });

  it('strips interview question echoes from raw answers', () => {
    const cleaned = stripInterviewQuestionEcho(
      "when a great-fit customer chooses Anchor over someone else, what usually tips the decision is trust and consistency"
    );
    assert.equal(/when a great-fit customer chooses/i.test(cleaned), false);
    assert.match(cleaned, /trust and consistency/i);
  });

  it('Anchor brief renders polished sentences with no raw prompt fragments', () => {
    const sections = {
      identity: {
        summary:
          'Anchor Cleaning is a cleaning company. This identity framing is how the operator describes the business today, and it anchors every other Blueprint section.',
        confidence: 0.9,
        unknowns: [],
      },
      services: {
        summary:
          'Today the business delivers commercial cleaning for professional offices. Service understanding reflects what is actually sold now, not aspirational packaging.',
        confidence: 0.88,
        unknowns: [],
      },
      idealCustomers: {
        summary:
          'Ideal customers are law firms and accounting practices that value reliability. This ICP picture prioritizes fit over volume.',
        confidence: 0.86,
        unknowns: [],
      },
      avoidCustomers: {
        summary:
          "The business prefers to avoid i don't want to work with customers who's main priority is the lowest price. These constraints protect targeting quality and should stay visible in the Blueprint.",
        confidence: 0.84,
        unknowns: [],
      },
      targetMarkets: {
        summary:
          'Priority markets center on both- greater Manchester area includes Bedford, hooksett, Londonderry, auburn, goffstown. Geography and vertical focus here bound where discovery should concentrate first.',
        confidence: 0.85,
        unknowns: [],
      },
      competitiveAdvantages: {
        summary:
          'Competitive edge is described as when a great-fit customer chooses Anchor over someone else, what usually tips the decision is trust — responsive, consistent, and accountable without needing to chase the work. This is operator-stated differentiation — useful for messaging, not an invented strategy claim.',
        confidence: 0.87,
        unknowns: [],
      },
      brandVoice: {
        summary:
          "Brand voice should read as anchor's brand voice should sound calm, professional, reliable, and direct. Tone guidance constrains later language without choosing channels or campaigns.",
        confidence: 0.82,
        unknowns: [],
      },
      campaignGoals: {
        summary:
          'Near-term growth goals focus on over the next 90 days, this growth work should grow commercial cleaning in Greater Manchester. These are desired business outcomes for the next phase of work, not execution tactics.',
        confidence: 0.83,
        unknowns: [],
      },
      successMetrics: {
        summary:
          'Success will be judged by we will know the growth work is working by watching both activity quality and real opportunity movement. These signals define whether the engagement is working from the client\'s perspective.',
        confidence: 0.8,
        unknowns: [],
      },
    };

    const brief = buildExecutiveSummary(sections);
    const byId = Object.fromEntries(brief.sections.map((s) => [s.id, s]));
    const blob = JSON.stringify(brief);

    assert.equal(BANNED_BLEED.test(blob), false, blob);
    assert.equal(containsRawPromptFragment(blob), false, blob);

    assert.match(byId.whoYouServe.body, /Ideal customers include|law firms|accounting/i);
    assert.match(
      byId.whoYouServe.body,
      /deliberately avoids|lowest price|reliability|professionalism|accountability/i
    );
    assert.match(
      byId.whoYouServe.body,
      /Greater Manchester|Bedford|Hooksett|Londonderry|Auburn|Goffstown/i
    );
    assert.match(byId.whyChooseYou.body, /Customers choose Anchor/i);
    assert.match(byId.whyChooseYou.body, /trust/i);
    assert.match(byId.whyChooseYou.body, /brand voice should feel/i);
    assert.equal(/Brand voice should feel .*\bshould\s+(?:sound|feel)/i.test(byId.whyChooseYou.body), false);
    assert.equal(/anchor'?s brand voice should sound/i.test(byId.whyChooseYou.body), false);
    assert.match(byId.whereHeaded.body, /commercial cleaning|Greater Manchester|near-term priority/i);
    assert.equal(/Near-term growth priorities center on over the next/i.test(byId.whereHeaded.body), false);
    assert.match(
      JSON.stringify(byId.recommendedScorecard),
      /Success should be measured by|qualified replies|walkthroughs|estimate|Qualified Prospects/i
    );
    assert.equal(/Success will be judged by we will know/i.test(JSON.stringify(byId.recommendedScorecard)), false);
    assert.equal(/i don't want to work with/i.test(byId.whoYouServe.body), false);
  });
});

describe('Anchor transcript — correction routing + normalized brief', () => {
  const ANCHOR_TRANSCRIPT = [
    {
      type: 'answer',
      text:
        'Anchor Cleaning we are a commercial-focused cleaning company. Happy to take residential work too.',
    },
    {
      type: 'answer',
      text:
        'standard home, standard office, move in/out cleans, deep cleans, recurring cleans, short term rental companies',
    },
    {
      type: 'correction',
      text: 'I meant to say short term rental turnovers for services',
    },
    {
      type: 'answer',
      text:
        'property managers, STR companies, facility managers, professional offices, daycares, rec centers, high traffic buildings',
    },
    {
      type: 'answer',
      text:
        "I don't want to work with customers whose main priority is the lowest price — better-fit customers value reliability, professionalism, and accountability.",
    },
    {
      type: 'answer',
      text:
        'both — Greater Manchester area includes Bedford, Hooksett, Londonderry, Auburn, Goffstown',
    },
    {
      type: 'answer',
      text:
        'trust — They are usually not just choosing the cheapest cleaning company. The decision usually tips toward Anchor when a great-fit customer chooses confidence that the work will be done right without needing to chase the team. A great-fit customer chooses Anchor for responsiveness and accountability.',
    },
    {
      type: 'answer',
      text: "anchor's brand voice should sound calm, professional, reliable, and easy to work with",
    },
    {
      type: 'answer',
      text:
        'would feel successful if Anchor has a clearer, repeatable path to commercial cleaning opportunities in Greater Manchester over the next 90 days',
    },
    {
      type: 'answer',
      text:
        'we will know the growth work is working by watching qualified replies, booked conversations, walkthroughs, estimate requests, and real commercial pipeline movement',
    },
  ];

  const BANNED =
    /would feel successful if|we will know the growth work|both geography is|anchor'?s brand voice should sound|when a great-fit customer chooses|to say short term|Ideal customers include to say/i;

  async function runAnchorTranscript(opts) {
    const started = await startClientInterview({ clientId: 10 }, opts);
    let turn = started;
    for (const step of ANCHOR_TRANSCRIPT) {
      turn = await postInterviewMessage(started.interviewId, step.text, opts);
    }
    return { started, turn };
  }

  it('routes services correction into services, not the active ideal-customer question', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 10 }, opts);
    await postInterviewMessage(
      started.interviewId,
      ANCHOR_TRANSCRIPT[0].text,
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      ANCHOR_TRANSCRIPT[1].text,
      opts
    );

    const before = await store.getSession(started.interviewId);
    assert.equal(
      require('../services/clientIntelligenceInterview').QUESTION_BANK[
        before.interview_state.stepIndex
      ].id,
      'ideal_customers'
    );

    const corrected = await postInterviewMessage(
      started.interviewId,
      'I meant to say short term rental turnovers for services',
      opts
    );
    assert.equal(corrected.messageType, MESSAGE_TYPES.CORRECTION);
    assert.equal(corrected.question.id, 'ideal_customers');
    assert.match(corrected.message, /services|update/i);

    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, before.interview_state.stepIndex);
    assert.equal(session.interview_state.answers.ideal_customers, undefined);
    assert.ok(
      (session.interview_state.normalizedFacts.services || []).some((s) =>
        /short-term rental turnovers/i.test(s)
      )
    );
    assert.equal(
      (session.interview_state.normalizedFacts.ideal_customers || []).some((s) =>
        /to say|turnovers for services/i.test(s)
      ),
      false
    );
    assert.match(
      String(session.interview_state.sectionState.services.summary || ''),
      /short-term rental turnovers/i
    );
    assert.equal(
      /to say short term rental turnovers for services/i.test(
        String(session.interview_state.sectionState.idealCustomers.summary || '')
      ),
      false
    );
  });

  it('routes disregard-last-message correction to prior differentiation, not brand voice', async () => {
    const {
      looksLikeCorrection,
      classifyInterviewMessage,
      parseCorrectionMessage,
      resolveCorrectionTarget,
      stripCorrectionPreamble,
    } = require('../services/clientIntelligenceInterview');

    const wrongDiff =
      'Pulseforge wins on software automation and lead generation for marketing agencies.';
    const correctionMsg =
      'disregard last message, please replace with the following; When a great-fit customer chooses Anchor over someone else, what usually tips the decision is trust — responsive, consistent, and accountable without needing to chase the work.';
    const brandVoiceAnswer =
      "anchor's brand voice should sound calm, professional, reliable, and easy to work with";

    assert.equal(looksLikeCorrection(correctionMsg), true);
    assert.equal(classifyInterviewMessage(correctionMsg), MESSAGE_TYPES.CORRECTION);
    assert.equal(
      /disregard last message|please replace with the following/i.test(
        stripCorrectionPreamble(correctionMsg)
      ),
      false
    );

    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 10 }, opts);

    await postInterviewMessage(
      started.interviewId,
      'Anchor Cleaning we are a commercial-focused cleaning company.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'standard office, recurring cleans, deep cleans',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'property managers, facility managers, professional offices',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      "I don't want to work with customers whose main priority is the lowest price",
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Greater Manchester area includes Bedford, Hooksett, Londonderry, Auburn, Goffstown',
      opts
    );
    await postInterviewMessage(started.interviewId, wrongDiff, opts);

    const beforeCorrection = await store.getSession(started.interviewId);
    assert.equal(
      QUESTION_BANK[beforeCorrection.interview_state.stepIndex].id,
      'brand_voice'
    );
    assert.match(
      String(beforeCorrection.interview_state.normalizedFacts.differentiation || ''),
      /software|lead generation|Pulseforge/i
    );
    assert.equal(beforeCorrection.interview_state.normalizedFacts.brand_voice, null);

    const resolved = resolveCorrectionTarget(correctionMsg, {
      activeQuestion: QUESTION_BANK.find((q) => q.id === 'brand_voice'),
      state: beforeCorrection.interview_state,
    });
    assert.equal(resolved.domain, 'differentiation');
    assert.equal(resolved.section, 'competitiveAdvantages');
    assert.equal(resolved.reason, 'explicit_domain');

    const parsed = parseCorrectionMessage(correctionMsg, null, {
      activeQuestion: QUESTION_BANK.find((q) => q.id === 'brand_voice'),
      state: beforeCorrection.interview_state,
    });
    assert.equal(parsed.domain, 'differentiation');
    assert.equal(parsed.section, 'competitiveAdvantages');
    assert.equal(/disregard|please replace/i.test(parsed.substance), false);
    assert.match(parsed.substance, /trust|responsive|accountable/i);

    const corrected = await postInterviewMessage(
      started.interviewId,
      correctionMsg,
      opts
    );
    assert.equal(corrected.messageType, MESSAGE_TYPES.CORRECTION);
    assert.equal(corrected.question.id, 'brand_voice');
    assert.match(
      corrected.message,
      /replaced your previous answer about why customers choose Anchor/i
    );
    assert.match(corrected.message, /keep the current question open/i);
    assert.match(corrected.message, /how should Anchor sound/i);

    const afterCorrection = await store.getSession(started.interviewId);
    assert.equal(
      afterCorrection.interview_state.stepIndex,
      beforeCorrection.interview_state.stepIndex
    );
    assert.equal(
      QUESTION_BANK[afterCorrection.interview_state.stepIndex].id,
      'brand_voice'
    );

    const facts = afterCorrection.interview_state.normalizedFacts;
    assert.match(String(facts.differentiation || ''), /trust|responsive|accountab/i);
    assert.equal(/Pulseforge|software|lead generation/i.test(String(facts.differentiation || '')), false);
    assert.equal(facts.brand_voice, null);
    assert.equal(
      /disregard last message/i.test(String(facts.brand_voice || '')),
      false
    );
    assert.equal(
      /disregard last message/i.test(
        String(afterCorrection.interview_state.answers.brand_voice || '')
      ),
      false
    );
    assert.equal(afterCorrection.interview_state.answers.brand_voice, undefined);
    assert.match(
      String(afterCorrection.interview_state.answers.advantages || ''),
      /trust|responsive|accountab/i
    );
    assert.equal(
      /disregard last message|please replace with the following/i.test(
        String(afterCorrection.interview_state.answers.advantages || '')
      ),
      false
    );

    const brandTurn = await postInterviewMessage(
      started.interviewId,
      brandVoiceAnswer,
      opts
    );
    // Active question advances only after a real brand-voice answer.
    assert.equal(brandTurn.question.id, 'campaign_goals');
    assert.notEqual(brandTurn.messageType, MESSAGE_TYPES.CORRECTION);

    const afterBrand = await store.getSession(started.interviewId);
    assert.match(
      String(afterBrand.interview_state.normalizedFacts.brand_voice || ''),
      /calm, professional, reliable/i
    );
    assert.equal(
      /disregard last message/i.test(
        String(afterBrand.interview_state.normalizedFacts.brand_voice || '')
      ),
      false
    );
    assert.match(
      String(afterBrand.interview_state.normalizedFacts.differentiation || ''),
      /trust|responsive|accountab/i
    );
    assert.equal(
      /Pulseforge|software|lead generation/i.test(
        String(afterBrand.interview_state.normalizedFacts.differentiation || '')
      ),
      false
    );

    // Finish remaining questions so we can assert on the generated brief.
    await postInterviewMessage(
      started.interviewId,
      'would feel successful if Anchor has a clearer path to commercial cleaning opportunities in Greater Manchester over the next 90 days',
      opts
    );
    const done = await postInterviewMessage(
      started.interviewId,
      'we will know the growth work is working by watching qualified replies, booked conversations, and walkthroughs',
      opts
    );
    assert.ok(done.executiveSummary);
    const briefBlob = JSON.stringify(done.executiveSummary);
    assert.equal(/disregard last message/i.test(briefBlob), false, briefBlob);
    assert.equal(/please replace with the following/i.test(briefBlob), false, briefBlob);
    assert.equal(/Pulseforge|software automation|lead generation for marketing/i.test(briefBlob), false, briefBlob);

    const byId = Object.fromEntries(done.executiveSummary.sections.map((s) => [s.id, s]));
    assert.match(byId.whyChooseYou.body, /trust|responsive|accountab/i);
    assert.equal(
      /disregard last message/i.test(byId.whyChooseYou.body),
      false
    );
    // Differentiation content must not be used as brand voice.
    assert.equal(
      /Brand voice should feel .*disregard|brand voice should feel trust — responsive/i.test(
        byId.whyChooseYou.body
      ),
      false
    );
    assert.match(
      byId.whyChooseYou.body,
      /brand voice should feel calm, professional, reliable/i
    );
  });

  it('normalizes business phrases used in Anchor services and segments', () => {
    const {
      normalizeBusinessPhrase,
      splitListItems,
      parseCorrectionMessage,
    } = require('../services/clientIntelligenceInterview');
    assert.equal(normalizeBusinessPhrase('standard home'), 'standard home cleaning');
    assert.equal(normalizeBusinessPhrase('standard office'), 'standard office cleaning');
    assert.equal(
      normalizeBusinessPhrase('move in/out cleans'),
      'move-in/move-out cleaning'
    );
    assert.equal(normalizeBusinessPhrase('recurring cleans'), 'recurring cleaning');
    assert.equal(
      normalizeBusinessPhrase('STR companies'),
      'short-term rental companies'
    );
    assert.equal(
      normalizeBusinessPhrase('short term rental turnovers'),
      'short-term rental turnovers'
    );
    assert.equal(normalizeBusinessPhrase('auburn'), 'Auburn');
    assert.equal(normalizeBusinessPhrase('hooksett'), 'Hooksett');
    assert.ok(
      splitListItems('property managers, STR companies, high traffic buildings').includes(
        'short-term rental companies'
      )
    );
    const parsed = parseCorrectionMessage(
      'I meant to say short term rental turnovers for services'
    );
    assert.equal(parsed.domain, 'services');
    assert.equal(parsed.section, 'services');
    assert.equal(parsed.substance, 'short-term rental turnovers');
  });

  it('Anchor end-to-end brief uses normalized evidence with no raw bleed', async () => {
    const { opts, store } = withStore();
    const { turn } = await runAnchorTranscript(opts);
    assert.equal(turn.status, 'CLIENT_REVIEW');
    assert.ok(turn.executiveSummary);

    const session = await store.getSession(turn.interviewId);
    const facts = session.interview_state.normalizedFacts;
    assert.ok(facts.services.some((s) => /short-term rental turnovers/i.test(s)));
    assert.equal(
      facts.ideal_customers.some((s) => /to say|turnovers for services/i.test(s)),
      false
    );

    const brief = turn.executiveSummary;
    const byId = Object.fromEntries(brief.sections.map((s) => [s.id, s]));
    const blob = JSON.stringify(brief);

    assert.equal(BANNED.test(blob), false, blob);
    assert.equal(containsRawPromptFragment(blob), false, blob);

    assert.match(byId.whoYouAre.body, /short-term rental turnovers/i);
    assert.match(
      byId.whoYouAre.body,
      /standard home cleaning|standard office cleaning|move-in\/move-out cleaning|recurring cleaning|deep cleans/i
    );

    assert.match(byId.whoYouServe.body, /property managers/i);
    assert.match(byId.whoYouServe.body, /short-term rental companies/i);
    assert.match(byId.whoYouServe.body, /facility managers/i);
    assert.match(byId.whoYouServe.body, /professional offices/i);
    assert.match(byId.whoYouServe.body, /daycares/i);
    assert.match(byId.whoYouServe.body, /rec centers/i);
    assert.match(byId.whoYouServe.body, /high-traffic buildings/i);
    assert.equal(/to say short term rental turnovers for services/i.test(byId.whoYouServe.body), false);

    assert.match(byId.whoYouServe.body, /Greater Manchester/i);
    assert.match(byId.whoYouServe.body, /Bedford/i);
    assert.match(byId.whoYouServe.body, /Londonderry/i);
    assert.match(byId.whoYouServe.body, /Auburn/i);
    assert.match(byId.whoYouServe.body, /Goffstown/i);
    assert.match(byId.whoYouServe.body, /Hooksett/i);

    assert.match(byId.whyChooseYou.body, /Customers choose Anchor/i);
    assert.match(
      byId.whyChooseYou.body,
      /Anchor(?: Cleaning)?'?s brand voice should feel calm, professional, reliable(?:,)? and easy to work with/i
    );
    assert.equal(/should sound anchor/i.test(byId.whyChooseYou.body), false);
    assert.match(byId.whyChooseYou.body, /calm, professional, reliable(?:,)? and easy to work with/i);
    assert.equal(/anchor'?s calm/i.test(byId.whyChooseYou.body), false);

    assert.equal(/would feel successful if/i.test(byId.whereHeaded.body), false);
    assert.equal(/both geography is/i.test(byId.whoYouServe.body), false);

    // Business name + grammar hygiene
    assert.equal(/Anchor Cleaning we/i.test(blob), false, blob);
    assert.equal(/\ba Anchor\b/i.test(blob), false, blob);
    assert.equal(/low — price|great — fit/i.test(blob), false, blob);
    assert.match(byId.whoYouAre.body, /Anchor Cleaning\b/);
    assert.equal(/Anchor Cleaning we\b/i.test(byId.whoYouAre.body), false);

    const observationItems = byId.observations.items || [];
    assert.ok(observationItems.length >= 1);
    assert.ok(observationItems.length <= 5);
    for (const item of observationItems) {
      const sentences = item.split(/(?<=[.!?])\s+/).filter(Boolean);
      assert.ok(sentences.length === 1, `observation should be one sentence: ${item}`);
      assert.ok(item.split(/\s+/).length <= 45, `observation too long: ${item}`);
      assert.equal(/They are usually not just choosing|The decision usually tips|A great-fit customer chooses/i.test(item), false);
    }
    const observations = observationItems.join(' ');
    assert.equal(BANNED.test(observations), false, observations);
    assert.equal(/Anchor Cleaning we|a Anchor|anchor'?s calm|low — price|great — fit/i.test(observations), false);
    assert.match(observations, /differentiation centers on trust|brand voice reinforces|Geographic attention|near-term growth goal|Commercial focus/i);
  });

  it('sanitizes business names and brand-voice lead-ins', () => {
    const {
      sanitizeBusinessName,
      normalizeBrandVoiceTone,
      synthesizeDifferentiationSnippet,
      ingestAnswerIntoNormalizedFacts,
      emptyNormalizedFacts,
    } = require('../services/clientIntelligenceInterview');

    assert.equal(sanitizeBusinessName('Anchor Cleaning we'), 'Anchor Cleaning');
    assert.equal(sanitizeBusinessName('Anchor Cleaning we are'), 'Anchor Cleaning');
    assert.equal(sanitizeBusinessName('We are Anchor Cleaning'), 'Anchor Cleaning');

    assert.equal(
      normalizeBrandVoiceTone("anchor's calm, professional, reliable, and easy to work with"),
      'calm, professional, reliable, and easy to work with'
    );
    assert.equal(
      normalizeBrandVoiceTone("anchor's brand voice should sound calm, professional, reliable, and easy to work with"),
      'calm, professional, reliable, and easy to work with'
    );

    const longDiff =
      'trust — they are usually not just choosing the cheapest cleaning company. The decision usually tips toward Anchor when a great-fit customer chooses confidence that the work will be done right without needing to chase the team.';
    const snippet = synthesizeDifferentiationSnippet(longDiff);
    assert.match(snippet, /trust/i);
    assert.equal(/They are usually not just choosing/i.test(snippet), false);
    assert.ok(snippet.split(/\s+/).length <= 30);

    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(
      facts,
      'identity',
      'Anchor Cleaning we are a commercial-focused cleaning company'
    );
    assert.equal(facts.business_name, 'Anchor Cleaning');
    assert.match(facts.business_description || '', /commercial-focused cleaning company/i);
    assert.equal(/commercial — focused/i.test(facts.business_description || ''), false);
  });
});

describe('Executive Brief field normalization — clean entity lists', () => {
  const SERVICES_PROSE =
    'Anchor Cleaning provides standard home cleaning, standard office cleaning, deep cleans, move-in/move-out cleaning, recurring cleaning, and short-term rental turnovers. The strongest growth focus is recurring commercial cleaning for customers who need weekly or multiple-times-per-week service.';
  const ICP_PROSE =
    'Anchor Cleaning most wants to work with commercial customers who value reliability, consistency, and clear communication. The ideal customers are property managers, short-term rental companies, facility managers, professional offices, rec centers, and high-traffic buildings that need dependable recurring cleaning.';
  const SUPP_DAYCARES = 'I forgot to mention daycares as part of my ideal customer profile';

  it('extracts clean service and ICP entities from prose answers', () => {
    const {
      extractServiceList,
      extractCustomerSegments,
      extractValueTraits,
      extractGrowthFocusItems,
      stripBusinessNameLeadIn,
      parseSupplementalMessage,
      ingestAnswerIntoNormalizedFacts,
      emptyNormalizedFacts,
      applyCorrectionToNormalizedFacts,
    } = require('../services/clientIntelligenceInterview');

    assert.equal(
      /Anchor Cleaning provides/i.test(stripBusinessNameLeadIn(SERVICES_PROSE)),
      false
    );
    assert.deepEqual(extractServiceList(SERVICES_PROSE), [
      'standard home cleaning',
      'standard office cleaning',
      'deep cleans',
      'move-in/move-out cleaning',
      'recurring cleaning',
      'short-term rental turnovers',
    ]);
    assert.ok(
      extractGrowthFocusItems(SERVICES_PROSE).some((g) => /recurring commercial cleaning/i.test(g))
    );
    assert.ok(
      extractGrowthFocusItems(SERVICES_PROSE).some((g) =>
        /weekly or multiple-times-per-week/i.test(g)
      )
    );

    assert.deepEqual(extractCustomerSegments(ICP_PROSE), [
      'property managers',
      'facility managers',
      'short-term rental companies',
      'professional offices',
      'rec centers',
      'high-traffic buildings',
    ]);
    assert.deepEqual(extractValueTraits(ICP_PROSE), [
      'reliability',
      'consistency',
      'clear communication',
    ]);

    const parsed = parseSupplementalMessage(SUPP_DAYCARES);
    assert.equal(parsed.substance, 'daycares');
    assert.equal(/as part of my ideal customer profile/i.test(parsed.substance), false);
    assert.equal(/forgot to mention/i.test(parsed.substance), false);

    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(
      facts,
      'identity',
      'Anchor Cleaning we are a commercial-focused cleaning company. Happy to take residential work too.'
    );
    facts = ingestAnswerIntoNormalizedFacts(facts, 'services', SERVICES_PROSE);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'idealCustomers', ICP_PROSE);
    facts = applyCorrectionToNormalizedFacts(facts, {
      section: 'idealCustomers',
      substance: parsed.substance,
      domain: 'ideal_customer',
    });

    assert.equal(
      facts.services.some((s) => /Anchor Cleaning provides/i.test(s)),
      false
    );
    assert.equal(
      facts.ideal_customers.some((s) => /Anchor Cleaning most wants/i.test(s)),
      false
    );
    assert.ok(facts.ideal_customers.includes('daycares'));
    assert.equal(
      facts.ideal_customers.some((s) =>
        /daycares as part of my ideal customer profile/i.test(s)
      ),
      false
    );
    assert.ok(facts.ideal_customer_traits.includes('reliability'));
    assert.match(String(facts.growth_focus || ''), /recurring commercial cleaning/i);
  });

  it('renders WHO YOU ARE / WHO YOU SERVE from clean entity lists', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 10 }, opts);

    await postInterviewMessage(
      started.interviewId,
      'Anchor Cleaning we are a commercial-focused cleaning company. Happy to take residential work too.',
      opts
    );
    await postInterviewMessage(started.interviewId, SERVICES_PROSE, opts);
    await postInterviewMessage(started.interviewId, ICP_PROSE, opts);

    const beforeSupp = await store.getSession(started.interviewId);
    assert.equal(
      QUESTION_BANK[beforeSupp.interview_state.stepIndex].id,
      'avoid_customers'
    );

    const supplement = await postInterviewMessage(started.interviewId, SUPP_DAYCARES, opts);
    assert.equal(supplement.messageType, MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT);
    assert.equal(supplement.question.id, 'avoid_customers');

    const afterSupp = await store.getSession(started.interviewId);
    assert.ok(afterSupp.interview_state.normalizedFacts.ideal_customers.includes('daycares'));
    assert.equal(
      afterSupp.interview_state.normalizedFacts.ideal_customers.some((s) =>
        /as part of my ideal customer profile/i.test(s)
      ),
      false
    );
    assert.deepEqual(
      afterSupp.interview_state.normalizedFacts.disqualified_customers || [],
      []
    );

    await postInterviewMessage(
      started.interviewId,
      "I don't want to work with customers whose main priority is the lowest price",
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Greater Manchester area includes Bedford, Hooksett, Londonderry, Auburn, Goffstown',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'trust — responsive, consistent, and accountable',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      "anchor's brand voice should sound calm, professional, reliable, and easy to work with",
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'would feel successful if Anchor has a clearer path to commercial opportunities over the next 90 days',
      opts
    );
    const done = await postInterviewMessage(
      started.interviewId,
      'we will know by watching qualified replies, booked conversations, and walkthroughs',
      opts
    );

    assert.ok(done.executiveSummary);
    const byId = Object.fromEntries(done.executiveSummary.sections.map((s) => [s.id, s]));
    const whoYouAre = byId.whoYouAre.body;
    const whoYouServe = byId.whoYouServe.body;

    assert.equal(/Services include Anchor Cleaning provides/i.test(whoYouAre), false);
    assert.equal(/Ideal customers include Anchor Cleaning most wants/i.test(whoYouServe), false);
    assert.equal(/forgot to mention/i.test(whoYouServe), false);
    assert.equal(/as part of my ideal customer profile/i.test(whoYouServe), false);

    assert.match(whoYouAre, /Services include standard home cleaning/i);
    assert.match(whoYouAre, /standard office cleaning/i);
    assert.match(whoYouAre, /move-in\/move-out cleaning/i);
    assert.match(whoYouAre, /short-term rental turnovers/i);

    assert.match(whoYouServe, /(?:ideal customers include|current acquisition focus is)/i);
    assert.match(whoYouServe, /property managers/i);
    assert.match(whoYouServe, /short-term rental companies/i);
    assert.match(whoYouServe, /daycares/i);
    assert.match(whoYouServe, /rec centers/i);
    assert.match(whoYouServe, /high-traffic buildings/i);

    const session = await store.getSession(started.interviewId);
    const facts = session.interview_state.normalizedFacts;
    assert.equal(facts.services.some((s) => /Anchor Cleaning provides/i.test(s)), false);
    assert.equal(
      facts.ideal_customers.some((s) => /Anchor Cleaning most wants/i.test(s)),
      false
    );
    assert.ok(facts.ideal_customers.includes('daycares'));
  });
});
