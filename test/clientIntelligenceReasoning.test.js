'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  MESSAGE_CLASSES,
  ARTIFACT_KINDS,
  classifyReasoningMessage,
  assessAnswerSufficiency,
  buildProbingFollowUp,
  inferCrossSectionTarget,
  emptyReasoningMemory,
  ensureReasoningMemory,
  recordAcceptedFact,
  addQuestionDebt,
  clearQuestionDebt,
  markArtifactGenerated,
  resolveNextArtifact,
  checkArtifactReadiness,
  synthesizeBusinessLanguage,
  planReasoningTurn,
  looksLikeVagueAnswer,
} = require('../services/clientIntelligenceReasoning');

const {
  MESSAGE_TYPES,
  QUESTION_BANK,
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  classifyInterviewMessage,
  synthesizeBusinessLanguage: synthFromInterview,
} = require('../services/clientIntelligenceInterview');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store } };
}

describe('SPEC-090 conversational reasoning — classification', () => {
  it('classifies the eight core message types plus refinement', () => {
    assert.equal(
      classifyReasoningMessage('Anchor Cleaning is a commercial cleaning company.'),
      MESSAGE_CLASSES.DIRECT_ANSWER
    );
    assert.equal(
      classifyReasoningMessage('Actually, focus on Bedford first.'),
      MESSAGE_CLASSES.DIRECT_ANSWER // without CIE correction detector
    );
    assert.equal(
      classifyReasoningMessage('Actually, focus on Bedford first.', {
        looksLikeCorrection: () => true,
      }),
      MESSAGE_CLASSES.CORRECTION
    );
    assert.equal(
      classifyReasoningMessage('I also forgot to mention property managers.', {
        looksLikeAddOn: () => true,
      }),
      MESSAGE_CLASSES.ADD_ON
    );
    assert.equal(classifyReasoningMessage('looks good'), MESSAGE_CLASSES.APPROVAL);
    assert.equal(classifyReasoningMessage('approve'), MESSAGE_CLASSES.APPROVAL);
    assert.equal(
      classifyReasoningMessage('Can you explain what you mean by ideal customer?'),
      MESSAGE_CLASSES.CLARIFICATION_REQUEST
    );
    assert.equal(classifyReasoningMessage('various things'), MESSAGE_CLASSES.INSUFFICIENT_ANSWER);
    assert.equal(classifyReasoningMessage('idk'), MESSAGE_CLASSES.INSUFFICIENT_ANSWER);
    assert.equal(classifyReasoningMessage('skip'), MESSAGE_CLASSES.SKIP);
    assert.equal(classifyReasoningMessage('pass'), MESSAGE_CLASSES.SKIP);
    assert.equal(classifyReasoningMessage('hello'), MESSAGE_CLASSES.OFF_TOPIC);
    assert.equal(
      classifyReasoningMessage('Please regenerate the brief — not facts about the business.', {
        looksLikeRefinement: () => true,
      }),
      MESSAGE_CLASSES.REFINEMENT_FEEDBACK
    );
  });

  it('CIE MESSAGE_TYPES aliases map add_on and clarification_request', () => {
    assert.equal(MESSAGE_TYPES.ADD_ON, 'add_on');
    assert.equal(MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT, 'add_on');
    assert.equal(MESSAGE_TYPES.CLARIFICATION_REQUEST, 'clarification_request');
    assert.equal(MESSAGE_TYPES.QUESTION_TO_MAX, 'clarification_request');
    assert.equal(
      classifyInterviewMessage('Also, we only serve Greater Manchester.'),
      MESSAGE_TYPES.ADD_ON
    );
    assert.equal(
      classifyInterviewMessage('Can you explain what you mean by ideal customer?'),
      MESSAGE_TYPES.CLARIFICATION_REQUEST
    );
  });
});

describe('SPEC-090 conversational reasoning — probing', () => {
  it('flags vague answers as insufficient and builds a focused probe', () => {
    assert.equal(looksLikeVagueAnswer('various things'), true);
    const assessment = assessAnswerSufficiency('maybe stuff', {
      id: 'ideal_customers',
      section: 'idealCustomers',
    });
    assert.equal(assessment.sufficient, false);
    assert.equal(assessment.shouldProbe, true);

    const probe = buildProbingFollowUp(
      { id: 'ideal_customers', section: 'idealCustomers' },
      assessment,
      'Anchor Cleaning'
    );
    assert.match(probe, /strongest-fit customer|specific/i);
    assert.equal(/various things/i.test(probe), false);
  });

  it('asks a probing follow-up instead of advancing on a vague answer', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 901 }, opts);
    assert.equal(started.question.id, 'identity');

    const vague = await postInterviewMessage(started.interviewId, 'various things', opts);
    assert.equal(vague.messageType, MESSAGE_TYPES.INSUFFICIENT_ANSWER);
    assert.equal(vague.nextAction, 'PROBE');
    assert.equal(vague.question.id, 'identity');
    assert.match(vague.message, /business name|one-sentence|specific/i);

    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 0);
    assert.equal(session.interview_state.answers.identity, undefined);
    assert.ok(session.interview_state.reasoningMemory.activeProbe);
    assert.ok(
      (session.interview_state.reasoningMemory.questionDebt || []).some(
        (d) => d.questionId === 'identity'
      )
    );
  });
});

describe('SPEC-090 conversational reasoning — cross-section add-on', () => {
  it('routes ICP add-on away from avoid_customers into idealCustomers', () => {
    const avoidQ = QUESTION_BANK.find((q) => q.id === 'avoid_customers');
    const cross = inferCrossSectionTarget('I forgot property managers', avoidQ, {
      inferDomain: () => 'ideal_customer',
      tagDomain: () => 'ideal_customer',
      domainToSection: { ideal_customer: 'idealCustomers' },
    });
    assert.equal(cross.section, 'idealCustomers');
    assert.ok(cross.confidence >= 0.8);

    const planned = planReasoningTurn('I forgot property managers', {
      activeQuestion: avoidQ,
      looksLikeAddOn: () => true,
      crossSectionHelpers: {
        inferDomain: () => 'ideal_customer',
        tagDomain: () => 'ideal_customer',
        domainToSection: { ideal_customer: 'idealCustomers' },
      },
    });
    assert.equal(planned.messageClass, MESSAGE_CLASSES.ADD_ON);
    assert.equal(planned.targetSection, 'idealCustomers');
  });

  it('updates ideal customers from an add-on while on avoid question', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 902 }, opts);
    await postInterviewMessage(
      started.interviewId,
      'Anchor Cleaning — commercial cleaning for professional offices.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Office cleaning, recurring commercial cleans, and deep cleans.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Facility managers, professional offices, and daycares.',
      opts
    );

    const before = await store.getSession(started.interviewId);
    assert.equal(QUESTION_BANK[before.interview_state.stepIndex].id, 'avoid_customers');

    const addOn = await postInterviewMessage(
      started.interviewId,
      'I forgot property managers',
      opts
    );
    assert.equal(addOn.messageType, MESSAGE_TYPES.ADD_ON);
    assert.equal(addOn.question.id, 'avoid_customers');
    assert.match(addOn.message, /property managers|ideal customer/i);

    const after = await store.getSession(started.interviewId);
    assert.equal(after.interview_state.stepIndex, before.interview_state.stepIndex);
    assert.equal(after.interview_state.answers.avoid_customers, undefined);
    assert.ok(
      (after.interview_state.normalizedFacts.ideal_customers || []).some((s) =>
        /property managers/i.test(s)
      )
    );
    assert.ok(
      (after.interview_state.reasoningMemory.acceptedFacts || []).some(
        (f) => f.section === 'idealCustomers' && /property managers/i.test(f.substance)
      )
    );
  });
});

describe('SPEC-090 conversational reasoning — session memory', () => {
  it('tracks accepted facts, debt, and confidence', () => {
    let mem = emptyReasoningMemory();
    mem = recordAcceptedFact(mem, {
      section: 'services',
      substance: 'recurring office cleans',
      source: 'direct_answer',
    });
    mem = addQuestionDebt(mem, {
      questionId: 'brand_voice',
      section: 'brandVoice',
      reason: 'skipped',
    });
    assert.equal(mem.acceptedFacts.length, 1);
    assert.equal(mem.questionDebt.length, 1);
    mem = clearQuestionDebt(mem, 'brand_voice');
    assert.equal(mem.questionDebt.length, 0);
    assert.equal(ensureReasoningMemory({}).acceptedFacts.length, 0);
  });
});

describe('SPEC-090 conversational reasoning — artifact readiness + non-repeat', () => {
  it('blocks readiness when required sections are missing', () => {
    const result = checkArtifactReadiness(ARTIFACT_KINDS.BLUEPRINT, {
      sectionState: {
        identity: { summary: 'Anchor Cleaning', confidence: 0.7 },
        services: { summary: '', confidence: 0 },
      },
      normalizedFacts: { business_name: 'Anchor Cleaning' },
    });
    assert.equal(result.ready, false);
    assert.ok(result.missing.includes('services'));
    assert.ok(result.followUp);
    assert.match(result.confidenceNote, /services/i);
  });

  it('marks weak evidence with a confidence note when present but thin', () => {
    const result = checkArtifactReadiness(ARTIFACT_KINDS.CAMPAIGN_PREVIEW, {
      sectionState: {
        idealCustomers: { summary: 'offices', confidence: 0.2 },
        targetMarkets: { summary: 'Manchester', confidence: 0.7 },
        campaignGoals: { summary: 'more walkthroughs', confidence: 0.7 },
        avoidCustomers: { summary: 'lowest price', confidence: 0.7 },
      },
    });
    assert.equal(result.ready, true);
    assert.ok(result.weak.includes('idealCustomers'));
    assert.match(result.confidenceNote, /thin|directional/i);
  });

  it('does not repeat the same artifact when the next one is requested', () => {
    let mem = emptyReasoningMemory();
    mem = markArtifactGenerated(mem, ARTIFACT_KINDS.BLUEPRINT);
    mem = markArtifactGenerated(mem, ARTIFACT_KINDS.GROWTH_DIRECTION);

    const again = resolveNextArtifact(mem, ARTIFACT_KINDS.GROWTH_DIRECTION);
    assert.equal(again.hold, ARTIFACT_KINDS.GROWTH_DIRECTION);
    assert.equal(again.emit, ARTIFACT_KINDS.CAMPAIGN_PREVIEW);
    assert.match(again.message, /already covered|Growth Direction|Campaign Preview/i);

    const fresh = resolveNextArtifact(mem, ARTIFACT_KINDS.CAMPAIGN_PREVIEW);
    assert.equal(fresh.emit, ARTIFACT_KINDS.CAMPAIGN_PREVIEW);
    assert.equal(fresh.hold, null);
  });
});

describe('SPEC-090 conversational reasoning — synthesis + guardrails', () => {
  it('rewrites stitched prompt text into clean business language', () => {
    const clean = synthesizeBusinessLanguage(
      "I also forgot to mention property managers for ICP",
      { section: 'idealCustomers', businessName: 'Anchor Cleaning' }
    );
    assert.match(clean, /property managers/i);
    assert.equal(/forgot to mention|for ICP/i.test(clean), false);
    assert.match(clean, /Anchor Cleaning/i);

    const meta = synthesizeBusinessLanguage(
      'Please regenerate the brief — turn raw interview answers into clean business language',
      { section: 'services' }
    );
    assert.equal(meta, '');
  });

  it('does not treat operator instructions as business facts during interview', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 903 }, opts);
    const turn = await postInterviewMessage(
      started.interviewId,
      'Please regenerate the brief — this revision introduced weird sentences. Not facts about the business.',
      opts
    );
    assert.equal(turn.messageType, MESSAGE_TYPES.REFINEMENT_FEEDBACK);
    assert.equal(turn.question.id, 'identity');

    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 0);
    assert.ok((session.interview_state.revisionGuidance || []).length >= 1);
    assert.equal(String(session.interview_state.sectionState.identity.summary || ''), '');
    assert.equal(
      (session.interview_state.reasoningMemory.acceptedFacts || []).length,
      0
    );
  });

  it('interview export re-exports synthesizeBusinessLanguage', () => {
    assert.equal(typeof synthFromInterview, 'function');
    assert.match(
      synthFromInterview('office cleaning and deep cleans', {
        section: 'services',
        businessName: 'Anchor Cleaning',
      }),
      /Anchor Cleaning provides/i
    );
  });
});
