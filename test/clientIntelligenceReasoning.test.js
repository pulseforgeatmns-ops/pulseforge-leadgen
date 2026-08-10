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
  markArtifactApproved,
  resolveNextArtifact,
  resolveCampaignArtifactAction,
  checkArtifactReadiness,
  synthesizeBusinessLanguage,
  planReasoningTurn,
  looksLikeVagueAnswer,
  looksLikeApprovalPlusNextRequest,
  looksLikeProspectListDraftRequest,
  looksLikeScoutHandoffBriefRequest,
  looksLikeHandBriefToScoutRequest,
  looksLikeExecuteExistingScoutWorkRequest,
  extractWorkRequestIdFromMessage,
  looksLikeLiveSourcingApproval,
  classifyProspectAcquisitionIntent,
  PROSPECT_ACQUISITION_INTENTS,
  looksLikeReviseCriteriaRequest,
  shouldBlockCriteriaQuestionReplay,
  isBannedCriteriaReplayQuestion,
  shouldForceProspectListDraft,
  isLiveSourcingApproved,
  markLiveSourcingApproved,
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

describe('SPEC-091 artifact progression — approval + next request', () => {
  const APPROVAL_PLUS =
    'Approved. Before we build anything, tell me how you would approach building the first prospect list for this test...';

  it('classifies approval_plus_next_request and artifact_request', () => {
    assert.equal(looksLikeApprovalPlusNextRequest(APPROVAL_PLUS), true);
    assert.equal(
      classifyReasoningMessage(APPROVAL_PLUS),
      MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST
    );
    assert.equal(
      classifyReasoningMessage('Approved'),
      MESSAGE_CLASSES.APPROVAL
    );
    assert.equal(
      classifyReasoningMessage('How would you approach building the first prospect list?'),
      MESSAGE_CLASSES.ARTIFACT_REQUEST
    );
  });

  it('does not replay an approved criteria artifact — advances to build proposal', () => {
    let mem = emptyReasoningMemory();
    mem = markArtifactGenerated(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA, 'draft');
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    assert.equal(mem.lastArtifactStatus, 'approved');
    assert.equal(
      mem.nextRecommendedArtifact,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );

    const resolved = resolveNextArtifact(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    assert.equal(resolved.hold, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    assert.equal(resolved.emit, ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL);
    assert.match(resolved.message, /already approved|Build Proposal/i);
  });

  it('resolveCampaignArtifactAction advances on approval_plus_next_request', () => {
    const priorCriteria = {
      kind: 'prospect_list_criteria_preview',
      status: 'draft',
      title: 'Prospect List Criteria Preview',
    };
    const action = resolveCampaignArtifactAction({
      userMessage: APPROVAL_PLUS,
      priorCriteriaPreview: priorCriteria,
      step: 'prospect_list_criteria_preview',
    });
    assert.equal(action.messageClass, MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST);
    assert.equal(action.action, 'emit_build_proposal');
    assert.equal(action.emitKind, ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL);
    assert.ok(
      action.memory.approvedArtifacts.includes(ARTIFACT_KINDS.PROSPECT_CRITERIA)
    );
  });

  it('approval-only after criteria shown advances without replaying criteria', () => {
    const action = resolveCampaignArtifactAction({
      userMessage: 'Approved',
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'draft',
      },
      step: 'prospect_list_criteria_preview',
    });
    assert.equal(action.messageClass, MESSAGE_CLASSES.APPROVAL);
    assert.equal(action.action, 'emit_build_proposal');
    assert.notEqual(action.action, 'replay_criteria');
  });
});

describe('Prospect list draft progression after build proposal approval', () => {
  const DRAFT_REQ =
    'Now generate the first reviewable prospect list batch. This is a reviewable list draft only. No outreach copy, sends, CRM writes, or account changes.';

  it('classifies reviewable list draft requests', () => {
    assert.equal(looksLikeProspectListDraftRequest(DRAFT_REQ), true);
    assert.equal(
      classifyReasoningMessage(DRAFT_REQ),
      MESSAGE_CLASSES.ARTIFACT_REQUEST
    );
  });

  it('routes draft request after approved build proposal to emit_prospect_list_draft', () => {
    let mem = emptyReasoningMemory();
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
    );
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );

    const action = resolveCampaignArtifactAction({
      userMessage: DRAFT_REQ,
      memory: mem,
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
      },
      priorBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      step: 'prospect_list_build_proposal_approved',
    });

    assert.equal(action.action, 'emit_prospect_list_draft');
    assert.equal(
      action.emitKind,
      ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT
    );
    assert.equal(action.planningState, 'prospect_list_draft_requested');
    assert.equal(
      shouldBlockCriteriaQuestionReplay(action.memory),
      true
    );
  });

  it('blocks criteria question replay when criteria + build proposal are approved', () => {
    let mem = emptyReasoningMemory();
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
    );
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );
    assert.equal(shouldBlockCriteriaQuestionReplay(mem), true);
    assert.equal(
      isBannedCriteriaReplayQuestion(
        'Before building a prospect list, define what should qualify or disqualify a property manager for this first test.'
      ),
      true
    );
  });

  it('does not treat reviewable draft wording as revise-criteria', () => {
    const confusion =
      'We already completed validation metrics and approved the Prospect List Build Proposal.\n\n' +
      'Current state:\n' +
      '- Prospect List Criteria Preview approved\n' +
      '- Prospect List Build Proposal approved\n' +
      '- Next step is a reviewable prospect list draft\n\n' +
      'Now generate the first reviewable prospect list batch...';
    assert.equal(looksLikeReviseCriteriaRequest(confusion), false);
    assert.equal(looksLikeProspectListDraftRequest(confusion), true);
    assert.equal(looksLikeReviseCriteriaRequest('revise criteria'), true);
    assert.equal(looksLikeReviseCriteriaRequest('change criteria'), true);
    assert.equal(
      looksLikeReviseCriteriaRequest('update inclusion/exclusion'),
      true
    );
  });

  it('hard guard forces draft when message declares approvals + draft request', () => {
    const confusion =
      'Current state:\n' +
      '- Prospect List Criteria Preview approved\n' +
      '- Prospect List Build Proposal approved\n' +
      '- Next step is a reviewable prospect list draft\n\n' +
      'Now generate the first reviewable prospect list batch...';
    const mem = emptyReasoningMemory();
    assert.equal(shouldForceProspectListDraft(confusion, mem), true);
    const action = resolveCampaignArtifactAction({
      userMessage: confusion,
      memory: mem,
      step: 'opening',
    });
    assert.equal(action.action, 'emit_prospect_list_draft');
    assert.equal(action.planningState, 'prospect_list_draft_requested');
  });
});

describe('Live sourcing approval after prospect list draft', () => {
  const LIVE_APPROVAL =
    'Approved. Use only public sources. Build 15–25 real prospects. Include source URLs. No outreach/copy/CRM/account changes.';

  const SCOUT_HANDOFF =
    'Do not build the prospect list directly as Max. Create a Scout Handoff Brief using the approved campaign/list criteria. Scout’s job is to inspect public sources and gather prospects with evidence.';

  const HAND_TO_SCOUT = 'Hand this brief to Scout';

  it('detects live_sourcing_approved from explicit approval + public sources + real prospects', () => {
    assert.equal(looksLikeLiveSourcingApproval(LIVE_APPROVAL), true);
    assert.equal(looksLikeProspectListDraftRequest(LIVE_APPROVAL), false);
    const mem = markLiveSourcingApproved(emptyReasoningMemory());
    assert.equal(isLiveSourcingApproved(mem, ''), true);
    assert.equal(isLiveSourcingApproved(emptyReasoningMemory(), LIVE_APPROVAL), true);
  });

  it('does not treat Scout Handoff Brief as live sourcing', () => {
    assert.equal(looksLikeScoutHandoffBriefRequest(SCOUT_HANDOFF), true);
    assert.equal(looksLikeLiveSourcingApproval(SCOUT_HANDOFF), false);
    assert.equal(
      classifyProspectAcquisitionIntent(SCOUT_HANDOFF),
      PROSPECT_ACQUISITION_INTENTS.CREATE_SCOUT_HANDOFF_BRIEF
    );
    assert.equal(
      classifyProspectAcquisitionIntent(LIVE_APPROVAL),
      PROSPECT_ACQUISITION_INTENTS.PERFORM_LIVE_SOURCING
    );
  });

  it('routes Hand this brief to Scout separately from create brief', () => {
    assert.equal(looksLikeHandBriefToScoutRequest(HAND_TO_SCOUT), true);
    assert.equal(looksLikeScoutHandoffBriefRequest(HAND_TO_SCOUT), false);
    assert.equal(looksLikeLiveSourcingApproval(HAND_TO_SCOUT), false);
    assert.equal(
      classifyProspectAcquisitionIntent(HAND_TO_SCOUT),
      PROSPECT_ACQUISITION_INTENTS.HAND_BRIEF_TO_SCOUT
    );

    let mem = emptyReasoningMemory();
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
    );
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );

    const action = resolveCampaignArtifactAction({
      userMessage: HAND_TO_SCOUT,
      memory: mem,
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
      },
      priorBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      step: 'scout_handoff_brief',
    });

    assert.equal(action.action, 'hand_brief_to_scout');
    assert.notEqual(action.action, 'emit_scout_handoff_brief');
    assert.notEqual(action.action, 'emit_live_sourcing');
  });

  it('routes Scout Handoff Brief to emit_scout_handoff_brief, not live sourcing', () => {
    let mem = emptyReasoningMemory();
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
    );
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );

    const action = resolveCampaignArtifactAction({
      userMessage: SCOUT_HANDOFF,
      memory: mem,
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
      },
      priorBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      step: 'prospect_list_build_proposal_approved',
    });

    assert.equal(action.action, 'emit_scout_handoff_brief');
    assert.equal(action.emitKind, ARTIFACT_KINDS.SCOUT_HANDOFF_BRIEF);
    assert.notEqual(action.action, 'emit_live_sourcing');
  });

  it('routes live sourcing approval to emit_live_sourcing, never draft placeholders', () => {
    let mem = emptyReasoningMemory();
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
    );
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );
    mem = markArtifactGenerated(
      mem,
      ARTIFACT_KINDS.REVIEWABLE_PROSPECT_LIST_DRAFT,
      'draft'
    );

    const action = resolveCampaignArtifactAction({
      userMessage: LIVE_APPROVAL,
      memory: mem,
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
      },
      priorBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      priorProspectListDraft: {
        kind: 'reviewable_prospect_list_draft',
        status: 'draft',
      },
      step: 'prospect_list_draft_generated',
    });

    assert.equal(action.action, 'emit_live_sourcing');
    assert.equal(action.liveSourcingApproved, true);
    assert.equal(action.memory.liveSourcingApproved, true);
    assert.notEqual(action.action, 'emit_prospect_list_draft');
  });

  it('routes execute existing Scout work request by ID — never ask-to-generate-batch fallback', () => {
    const EXECUTE_MSG =
      'Execute the existing Scout work request now.\nworkRequestId: f0ac74ac-16a6-4dba-b024-d3727b285a86';

    assert.equal(
      extractWorkRequestIdFromMessage(EXECUTE_MSG),
      'f0ac74ac-16a6-4dba-b024-d3727b285a86'
    );
    assert.equal(looksLikeExecuteExistingScoutWorkRequest(EXECUTE_MSG), true);
    assert.equal(looksLikeHandBriefToScoutRequest(EXECUTE_MSG), false);
    assert.equal(looksLikeScoutHandoffBriefRequest(EXECUTE_MSG), false);
    assert.equal(looksLikeLiveSourcingApproval(EXECUTE_MSG), false);
    assert.equal(looksLikeProspectListDraftRequest(EXECUTE_MSG), false);
    assert.equal(
      classifyProspectAcquisitionIntent(EXECUTE_MSG),
      PROSPECT_ACQUISITION_INTENTS.EXECUTE_EXISTING_SCOUT_WORK_REQUEST
    );

    let mem = emptyReasoningMemory();
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
    );
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );

    const action = resolveCampaignArtifactAction({
      userMessage: EXECUTE_MSG,
      memory: mem,
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
      },
      priorBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      step: 'prospect_list_build_proposal_approved',
    });

    assert.equal(action.action, 'execute_existing_scout_work_request');
    assert.equal(
      action.workRequestId,
      'f0ac74ac-16a6-4dba-b024-d3727b285a86'
    );
    assert.notEqual(action.action, 'ack_build_approval');
    assert.notEqual(action.action, 'hand_brief_to_scout');
    assert.notEqual(action.action, 'emit_prospect_list_draft');
    assert.doesNotMatch(
      String(action.note || ''),
      /Ask me to generate the first reviewable prospect list batch/i
    );
  });

  it('routes retry failed Scout work request by ID', () => {
    const RETRY_MSG =
      'Retry the failed Scout work request.\nworkRequestId: a1b2c3d4-e5f6-7890-abcd-ef1234567890';
    assert.equal(looksLikeExecuteExistingScoutWorkRequest(RETRY_MSG), true);
    assert.equal(
      classifyProspectAcquisitionIntent(RETRY_MSG),
      PROSPECT_ACQUISITION_INTENTS.EXECUTE_EXISTING_SCOUT_WORK_REQUEST
    );
    const action = resolveCampaignArtifactAction({
      userMessage: RETRY_MSG,
      memory: emptyReasoningMemory(),
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
      },
      priorBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      step: 'scout_handoff_failed',
    });
    assert.equal(action.action, 'execute_existing_scout_work_request');
    assert.equal(
      action.workRequestId,
      'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
    );
  });
});
