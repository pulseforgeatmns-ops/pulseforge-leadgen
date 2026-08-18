'use strict';

/**
 * SPEC-102 / SPEC-103 / SPEC-105 — retrieve durable knowledge before any
 * specialist routing.
 *
 * Memory → reasoning → specialist → execution.
 * Retrieval / explanation / reflection never invoke a specialist.
 * Unknown is a successful epistemic response.
 *
 * SPEC-103 adds the canonical durable load path: Blueprint → Playbook →
 * Knowledge Graph → Mission/Objectives → Campaign Context → Workspace.
 * SPEC-105 adds operating-evidence retrieval (AO, prospects, Scout state,
 * missions, activity, outcomes) before Blueprint advisory reasoning.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  classifyCognitiveMode,
  COGNITIVE_MODES,
  forbidsSpecialistDelegation,
} = require('../specialistDelegation/CognitiveMode');
const { UNKNOWN_ANSWER } = require('../specialistDelegation/RetrievalGate');
const {
  KNOWLEDGE_STATES,
  loadDurableBusinessUnderstanding,
  composeDurableRetrievalAnswer,
  bundleToLegacySources,
  sourceOrderUsed,
  formatUnknownAnswer,
  SERVICE_AREA_RE,
  ENTITY_KNOW_RE,
} = require('./BusinessUnderstandingRetrieval');
const {
  isOperatingGroundedRecommendation,
  isBareCurrentStateRecommendation,
  hasOperatingGrounding,
  shouldRetrieveOperatingEvidence,
  isInventoryOnlyRequest,
  loadOperatingEvidence,
  composeOperatingEvidenceAnswer,
  operatingStructured,
  bundleHasUsableOperatingSignal,
} = require('./OperatingEvidenceRetrieval');
const {
  composeEvidenceGroundedRecommendation,
  assembleOperatingState,
} = require('./OperatingStateRecommendation');
const {
  isClaimChallenge,
  isOperatorClaimCorrection,
  lastRecommendationFrom,
  retractedIdsFrom,
  handleClaimChallenge,
  recordWorkingModel,
  recommendationRecord,
} = require('./RecommendationClaimChallenge');
const {
  CONTRACT_IDS,
  selectResponseContract,
  composeAccordingToContract,
  attachContractMetadata,
  looksLikeSummary,
  looksLikeCompletedRetrieval,
} = require('./ResponseContract');
const { attachPipelineLog } = require('./ReasoningPipeline');

const ADVISORY_ESSAY_RE =
  /I'd start by proving a repeatable (?:commercial )?acquisition motion/i;

const INDUSTRY_RE =
  /\b(what industries|which industries|who do we (?:target|serve)|ideal customers)\b/i;

const HISTORY_RE =
  /\bwhat happened (?:yesterday|today|last (?:night|week)|in (?:that|the) (?:conversation|investigation|search))\b/i;

const ELEVATION_RE =
  /\bwhy (?:didn'?t|did) you elevate\b/i;

const DESK_WORKFLOW_RE =
  /\b(canary|fillable\s+(?:verification\s+)?table|verification\s+work\s+order|preparation[-\s]*only|packet\s+review|call\s+script)\b/i;

function isOperatorDeskWorkflowQuestion(question) {
  const q = String(question || '');
  if (!q.trim()) return false;
  try {
    const active = require('./ActiveWorkContext');
    if (active.isCanarySummaryJudgmentRequest(q)) return true;
    if (active.isFillableTableRequest(q)) return true;
    if (active.isPacketReviewRequest(q)) return true;
    if (active.isCallScriptReviewRequest(q)) return true;
    if (active.isFocusedCanaryWorkOrderRequest(q)) return true;
  } catch (_) {
    /* ActiveWorkContext unavailable — fall through to lexical cues */
  }
  return DESK_WORKFLOW_RE.test(q);
}

function isHardRetrievalQuestion(question, mode) {
  if (isOperatorDeskWorkflowQuestion(question)) return false;
  if (SERVICE_AREA_RE.test(question)) return true;
  if (ENTITY_KNOW_RE.test(question)) return true;
  if (INDUSTRY_RE.test(question)) return true;
  if (HISTORY_RE.test(question)) return true;
  if (ELEVATION_RE.test(question)) return true;
  if (isClaimChallenge(question) || isOperatorClaimCorrection(question)) return true;
  if (shouldRetrieveOperatingEvidence(question)) return true;
  if (looksLikeSummary(question) || looksLikeCompletedRetrieval(question)) return true;
  if (mode && mode.kind === COGNITIVE_MODES.REFLECTION) return true;
  if (mode && mode.kind === COGNITIVE_MODES.EXPLANATION) return true;
  if (
    mode &&
    (mode.kind === COGNITIVE_MODES.DIAGNOSIS ||
      mode.kind === COGNITIVE_MODES.UNKNOWN_ANALYSIS ||
      mode.kind === COGNITIVE_MODES.RISK ||
      mode.kind === COGNITIVE_MODES.PROGRESS)
  ) {
    return true;
  }
  if (mode && mode.via === 'unknown_intent_fallback') return true;
  if (mode && mode.via === 'planning_to_retrieval') return true;
  if (mode && mode.via === 'recommendation_follow_up') return true;
  if (mode && mode.via === 'retrieval') return true;
  if (mode && mode.via === 'summary') return true;
  if (mode && mode.via === 'operating_evidence') return true;
  return false;
}

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function namesMatch(asked, known) {
  const a = present(asked).toLowerCase();
  const b = present(known).toLowerCase();
  if (!a || !b) return false;
  if (a === b) return true;
  const aTokens = a.split(/\s+/).filter((t) => t.length > 2);
  if (!aTokens.length) return false;
  return aTokens.every((t) => b.includes(t));
}

/**
 * Inspect durable retrieval sources — loads from persistent stores when
 * session context is empty (SPEC-103).
 *
 * @param {object} input
 * @returns {Promise<object>}
 */
async function inspectRetrievalSources(input = {}) {
  const bundle = await loadDurableBusinessUnderstanding(input);
  const legacy = bundleToLegacySources(bundle);

  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};

  legacy.lastSpecialist =
    (sessionCtx.lastSpecialistEvaluation &&
      sessionCtx.lastSpecialistEvaluation.specialist) ||
    (sessionCtx.lastScoutEvaluation ? 'scout' : null);

  legacy.knowledgeState = bundle.knowledgeState;
  legacy.blueprintSource = bundle.blueprintSource;
  legacy.contract = bundle.contract;
  legacy._bundle = bundle;

  return legacy;
}

function composeInvestigationRetrieval(question, mode, sources) {
  const used = [];

  if (HISTORY_RE.test(question) && sources.investigation) {
    used.push('previousInvestigations');
    const coverage =
      sources.investigation.coverageBand ||
      (sources.investigation.coverage && sources.investigation.coverageBand) ||
      null;
    return {
      prose: coverage
        ? `The most recent investigation coverage was ${coverage}. I can inspect that work — I won't rerun a specialist to narrate it.`
        : 'I have a prior investigation on file. I can inspect that work rather than starting a new one.',
      used,
      knowledgeState: sources.knowledgeState,
    };
  }

  if (ELEVATION_RE.test(question) && sources.evaluation) {
    used.push('previousInvestigations');
    if (sources.evaluation.materialChange === true) {
      return {
        prose:
          'I elevated Acquisition because the last evaluation found a material change in opportunity.',
        used,
        knowledgeState: sources.knowledgeState,
      };
    }
    return {
      prose:
        "I didn't elevate Acquisition because the last evaluation was not material enough to change Command Deck priority.",
      used,
      knowledgeState: sources.knowledgeState,
    };
  }

  if (mode.kind === COGNITIVE_MODES.EXPLANATION && !sources.evaluation) {
    return {
      prose: formatUnknownAnswer(sources.knowledgeState || KNOWLEDGE_STATES.NEVER_LEARNED),
      used,
      knowledgeState: sources.knowledgeState,
    };
  }

  return null;
}

function entityNameFromWhoIs(question) {
  const m = String(question || '').match(/\bwho is\s+(.+?)\??\s*$/i);
  return m ? present(m[1]).replace(/[.]+$/, '') : '';
}

function isUnrelatedEntityQuestion(question, sources) {
  if (!/\bwho is\b/i.test(String(question || ''))) return false;
  const asked = entityNameFromWhoIs(question);
  if (!asked) return false;
  if (sources.businessName && namesMatch(asked, sources.businessName)) return false;
  if (sources.identity && namesMatch(asked, sources.identity)) return false;
  return true;
}

function composeRetrievalAnswer(question, mode, sources) {
  const investigationAnswer = composeInvestigationRetrieval(question, mode, sources);
  if (investigationAnswer) return investigationAnswer;

  const bundle = sources._bundle || {};
  const durable = composeDurableRetrievalAnswer(question, mode, bundle);
  if (durable.prose !== formatUnknownAnswer(bundle.knowledgeState)) {
    return durable;
  }

  if (isUnrelatedEntityQuestion(question, sources)) {
    return {
      prose: UNKNOWN_ANSWER,
      used: [],
      knowledgeState: sources.knowledgeState,
    };
  }

  if (mode.kind === COGNITIVE_MODES.REFLECTION) {
    return durable;
  }

  return {
    prose: formatUnknownAnswer(sources.knowledgeState || KNOWLEDGE_STATES.NEVER_LEARNED),
    used: durable.used || [],
    knowledgeState: sources.knowledgeState || KNOWLEDGE_STATES.NEVER_LEARNED,
  };
}

function workspaceStructured(answer, extras = {}) {
  return buildStructuredResponse({
    answer,
    reasoning: extras.reasoning || [
      'Answered from durable knowledge before considering a specialist.',
    ],
    supportingEvidence: extras.supportingEvidence || [],
    contradictingEvidence: [],
    confidence: extras.confidence != null ? extras.confidence : 0.84,
    nextInvestigations: [],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: ['retrieval_before_delegation', 'spec_102', 'spec_103'],
    timelineReferences: [],
    relatedEntities: extras.relatedEntities || [],
    metadata: {
      sourcesUsed: {
        briefing: extras.used && extras.used.includes('briefing'),
        reasoning: true,
        memory: true,
        policy: true,
        knowledge: extras.used && extras.used.includes('blueprint'),
      },
      evidenceCount: extras.evidenceCount || 0,
      asOf: new Date().toISOString(),
      unavailable: extras.unavailable || [],
      cognitiveMode: extras.cognitiveMode || null,
      retrievalBeforeDelegation: true,
      businessUnderstandingRetrieval: true,
      knowledgeState: extras.knowledgeState || null,
      blueprintSource: extras.blueprintSource || null,
      specialistDelegated: false,
      scoutDelegated: false,
      responseContract: extras.responseContract || null,
      intentBoundResponse: Boolean(extras.responseContract),
    },
  });
}

const FIRST_CLASS_ANALYSIS = new Set([
  COGNITIVE_MODES.DIAGNOSIS,
  COGNITIVE_MODES.UNKNOWN_ANALYSIS,
  COGNITIVE_MODES.RISK,
  COGNITIVE_MODES.PROGRESS,
  COGNITIVE_MODES.INVESTIGATION,
]);

const PLAN_PROVIDER_KINDS = new Set([
  'decompose',
  'follow_up',
  'challenge',
  'targeting',
  'approach',
  'unknowns',
  'plan_select',
  'plan_advance',
  'plan_deepen',
  'plan_recover',
  'plan_critique',
  'plan_capability',
  'plan_operator',
  'plan_continuity',
]);

function isFirstClassAnalysisMode(mode) {
  if (!mode) return false;
  if (FIRST_CLASS_ANALYSIS.has(mode.kind)) return true;
  if (mode.via === 'summary') return true;
  return false;
}

/**
 * Plan continuity and specificity use CIE/ActiveClientReasoning as providers.
 * They do not compose Blueprint Advisory essays or bypass ResponseContract.
 */
async function maybeHandlePlanProviderTurn(input, question, mode, contract) {
  if (isFirstClassAnalysisMode(mode)) return null;
  const session = input.session;
  if (!session || !session.context || typeof session.context !== 'object') {
    return null;
  }

  let cie;
  try {
    cie = require('./ClientIntelligenceContext');
  } catch (_) {
    return null;
  }

  if (cie.isClientContextExecutionRequest(question)) return null;
  if (cie.shouldUseExecutionClarification(question, session)) return null;
  try {
    const { classifyAdvisoryHandoffIntent } = require('./ActiveClientReasoning');
    if (classifyAdvisoryHandoffIntent(question, session)) return null;
  } catch (_) {
    /* handoff classifier unavailable */
  }

  const { getActiveClientReasoning, classifyActiveThoughtFollowUp } = require(
    './ActiveClientReasoning'
  );
  const active = getActiveClientReasoning(session);
  const lastTurn = session.context.lastClientIntelligenceTurn;
  const lastRec =
    session.context.lastRecommendation ||
    (session.context.workingModel && session.context.workingModel.lastRecommendation);
  if (!active && !lastTurn && !lastRec) return null;

  const follow = classifyActiveThoughtFollowUp(question, session);
  const specificity = cie.looksLikeSpecificityAsk(question, session);
  const scored = cie.scoreClientBusinessSemantics(question, session);
  const ownerFollow = Boolean(scored && scored.features && scored.features.ownerPerspective);
  const planFollow = Boolean(follow && follow.op && follow.op !== 'subject_change');
  if (!specificity && !planFollow && !ownerFollow) return null;

  const sources = await inspectRetrievalSources(input);
  const summary =
    (sources && sources.blueprint) ||
    (session.context && session.context.clientIntelligence) ||
    null;
  if (!summary || !summary.approved) return null;

  const composeSession = ownerFollow
    ? {
        context: {
          lastClientIntelligenceTurn: session.context.lastClientIntelligenceTurn,
          clientIntelligence: session.context.clientIntelligence,
        },
      }
    : session;
  const composed = cie.composeClientContextReasoning(summary, question, {
    session: composeSession,
    mode: ownerFollow ? 'approach' : undefined,
  });
  if (!composed || !composed.prose) return null;
  if (ADVISORY_ESSAY_RE.test(composed.prose)) return null;
  if (composed.kind && !PLAN_PROVIDER_KINDS.has(composed.kind)) return null;

  cie.syncGovernedReasoningTurn(
    session,
    {
      kind: composed.kind,
      reason: `governed_plan_${composed.kind}`,
      recommendationFocus: composed.recommendationFocus || summary.idealCustomers,
      question,
      planSteps: composed.planSteps,
      conversationalFocusIndex: composed.conversationalFocusIndex,
    },
    summary,
    composed.prose
  );

  const structured = attachPipelineLog(
    attachContractMetadata(
      workspaceStructured(composed.prose, {
        used: ['blueprint', 'active_reasoning'],
        cognitiveMode: mode && mode.kind,
        responseContract: contract && contract.id,
        reasoning: [
          `Classified operator intent as ${mode && mode.kind}.`,
          'Used approved Blueprint and active reasoning as evidence. CIE did not compose Blueprint Advisory.',
        ],
        evidenceCount: 2,
      }),
      contract
    ),
    { analysis: mode, contract }
  );

  return {
    reason: `governed_plan_${composed.kind}`,
    structured,
    prose: composed.prose,
    mode,
    sourcesUsed: ['blueprint', 'active_reasoning'],
    knowledgeState: 'available',
    delegated: false,
    launchedScout: false,
    responseContract: contract,
    turnKind: composed.kind,
    recommendationFocus: composed.recommendationFocus || summary.idealCustomers,
  };
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleOperatingEvidenceTurn(input, question, mode) {
  const contract =
    input.responseContract ||
    selectResponseContract(question, mode);
  const inventoryOnly = isInventoryOnlyRequest(question);
  const recommend =
    !inventoryOnly &&
    ((contract && contract.id === CONTRACT_IDS.RECOMMENDATION) ||
      (!contract &&
        (isOperatingGroundedRecommendation(question) ||
          (mode && mode.kind === COGNITIVE_MODES.RECOMMENDATION))));
  const bundle = await loadOperatingEvidence(input);
  const understanding = await inspectRetrievalSources(input);
  if (
    recommend &&
    !bundleHasUsableOperatingSignal(bundle) &&
    (!understanding.contract ||
      understanding.knowledgeState === KNOWLEDGE_STATES.NEVER_LEARNED)
  ) {
    const missing = formatUnknownAnswer(
      understanding.knowledgeState || KNOWLEDGE_STATES.NEVER_LEARNED
    );
    const sessionCtxMissing =
      input.session && input.session.context && typeof input.session.context === 'object'
        ? input.session.context
        : null;
    if (sessionCtxMissing) {
      sessionCtxMissing.lastCognitiveMode = mode.kind;
      sessionCtxMissing.lastResponseContract = contract && contract.id;
    }
    const structured = attachPipelineLog(
      attachContractMetadata(
        operatingStructured(missing, {
          used: [],
          items: [],
          cognitiveMode: mode.kind,
          recommend: false,
          reasoning: [
            `Classified operator intent as ${mode.kind}.`,
            'No approved Blueprint and no recorded operating evidence. Unknown is acceptable — I will not invent a recommendation.',
          ],
        }),
        contract
      ),
      { analysis: mode, contract }
    );
    return {
      reason: 'governed_missing_blueprint',
      structured,
      prose: missing,
      mode,
      sourcesUsed: [],
      knowledgeState: understanding.knowledgeState || KNOWLEDGE_STATES.NEVER_LEARNED,
      delegated: false,
      launchedScout: false,
      responseContract: contract,
    };
  }
  const sessionCtx =
    input.session && input.session.context && typeof input.session.context === 'object'
      ? input.session.context
      : null;
  const composed = composeOperatingEvidenceAnswer(question, bundle, {
    inventoryOnly,
    recommend,
    businessUnderstanding: understanding,
    now: input.now,
    capability: bundle.capability,
    retractedPremises: retractedIdsFrom(input),
    operatorDeniedEmailActive: Boolean(sessionCtx && sessionCtx.operatorDeniedEmailActive),
    contract,
  });

  if (sessionCtx) {
    sessionCtx.lastCognitiveMode = mode.kind;
    sessionCtx.lastRetrievalSources = composed.used || [];
    sessionCtx.lastResponseContract = contract && contract.id;
    sessionCtx.lastOperatingEvidence = {
      tenantId: bundle.tenantId || null,
      itemCount: Array.isArray(bundle.items) ? bundle.items.length : 0,
      launchedScout: false,
    };
    if (composed.recommend) {
      recordWorkingModel(sessionCtx, {
        lastRecommendation: recommendationRecord(composed),
      });
    }
  }

  const structured = attachPipelineLog(
    attachContractMetadata(
    operatingStructured(composed.prose, {
      used: composed.used,
      items: composed.items || bundle.items || [],
      cognitiveMode: mode.kind,
      recommend: composed.recommend,
      mailExecuted: Boolean(bundle.campaign && bundle.campaign.mailExecuted),
      unavailable: itemsByUnavailable(bundle.items),
      businessIntelligence: composed.businessIntelligence || null,
      reasoning: [
        `Classified operator intent as ${mode.kind}.`,
        contract ? `Selected ${contract.label} response contract before retrieval.` : null,
        'Synthesized business intelligence from grounded operating evidence before presenting inventory.',
        composed.recommend
          ? 'Retrieved operating evidence as a prerequisite, then reasoned to a recommendation from synthesized findings.'
          : 'Retrieved existing PulseForge operating evidence. Blueprint facts are evidence, not a reasoning engine.',
        composed.recommend
          ? 'Recommendation is grounded in synthesized findings and retrieved operating evidence, capability state, and policy — not Blueprint-only advice. No action was executed.'
          : contract && contract.id === CONTRACT_IDS.SUMMARY
            ? 'Summary requested — business intelligence first, then observed state, goals, unknowns, and evidence. Any recommendation is optional and last.'
            : contract && contract.id === CONTRACT_IDS.DIAGNOSIS
              ? 'Diagnosis requested — identify the limiting constraint from bottlenecks, readiness, and momentum. Explain why, not what to do.'
              : contract && contract.id === CONTRACT_IDS.UNKNOWN_ANALYSIS
                ? 'Unknown analysis requested — surface evidence gaps without speculation or acquisition rumors.'
                : contract && contract.id === CONTRACT_IDS.RISK
                  ? 'Risk assessment requested — grounded operational risks only.'
                  : contract && contract.id === CONTRACT_IDS.PROGRESS
                    ? 'Progress review requested — measure movement against stated goals.'
            : 'Inventory requested — business intelligence summarizes verified state before evidence. No unsolicited strategy.',
      ].filter(Boolean),
    }),
    contract,
    { businessIntelligence: composed.businessIntelligence || null }
  ),
    {
      analysis: mode,
      contract,
    }
  );

  return {
    reason:
      contract && contract.id === CONTRACT_IDS.SUMMARY
        ? 'intent_bound_summary'
        : contract && contract.id === CONTRACT_IDS.DIAGNOSIS
          ? 'intent_bound_diagnosis'
          : contract && contract.id === CONTRACT_IDS.UNKNOWN_ANALYSIS
            ? 'intent_bound_unknown_analysis'
            : contract && contract.id === CONTRACT_IDS.RISK
              ? 'intent_bound_risk'
              : contract && contract.id === CONTRACT_IDS.PROGRESS
                ? 'intent_bound_progress'
        : 'operating_evidence_retrieval',
    structured,
    prose: composed.prose,
    mode,
    sourcesUsed: composed.used || [],
    knowledgeState: composed.knowledgeState,
    delegated: false,
    launchedScout: false,
    operatingEvidence: bundle,
    responseContract: contract,
  };
}

async function maybeHandleClaimChallengeTurn(input, question, mode) {
  const correction = isOperatorClaimCorrection(question);
  const bundle = await loadOperatingEvidence(input);
  const understanding = await inspectRetrievalSources(input);
  const sessionCtx =
    input.session && input.session.context && typeof input.session.context === 'object'
      ? input.session.context
      : null;
  const lastRecommendation = lastRecommendationFrom(input);
  const retracted = retractedIdsFrom(input);
  const extras = {
    businessUnderstanding: understanding,
    now: input.now,
    capability: bundle.capability,
    retractedPremises: retracted,
    operatorDeniedEmailActive: Boolean(sessionCtx && sessionCtx.operatorDeniedEmailActive) || correction,
  };

  if (correction && !retracted.includes('email_motion')) {
    extras.retractedPremises = [...retracted, 'email_motion'];
  }

  const state = assembleOperatingState(bundle, extras);
  const preview = handleClaimChallenge({
    question,
    state,
    lastRecommendation,
    correction,
    revised: null,
  });
  if (
    preview.evaluation.verdict === 'retract' &&
    preview.claim &&
    preview.claim.id &&
    !extras.retractedPremises.includes(preview.claim.id)
  ) {
    extras.retractedPremises = [...extras.retractedPremises, preview.claim.id];
  }
  const shouldRevise = preview.evaluation.verdict === 'retract' || correction;
  const revised = shouldRevise
    ? composeEvidenceGroundedRecommendation(bundle, extras)
    : null;
  const handled = handleClaimChallenge({
    question,
    state,
    lastRecommendation,
    correction,
    revised,
  });

  const contract =
    input.responseContract ||
    selectResponseContract(question, mode);
  const claimText =
    (handled.claim && (handled.claim.text || handled.claim.claim)) ||
    'the prior operating-state statement';
  const challengeProse = composeAccordingToContract(contract, {
    claimIdentified: `The challenged claim was: ${claimText}`,
    evidenceReviewed:
      (handled.evaluation && handled.evaluation.detail) ||
      'Retrieved claim-relevant operating evidence rather than restating the full inventory.',
    revision: handled.prose,
    updatedRecommendation: revised
      ? `REVISED RECOMMENDATION\n${revised.prose}`
      : handled.evaluation && handled.evaluation.verdict === 'confirm'
        ? 'No updated recommendation. The challenged claim remains supported by retrieved evidence.'
        : 'No updated recommendation was required after this challenge.',
  });

  if (sessionCtx) {
    const nextRetracted = [...extras.retractedPremises];
    if (handled.evaluation.verdict === 'retract' && handled.claim && handled.claim.id) {
      if (!nextRetracted.includes(handled.claim.id)) nextRetracted.push(handled.claim.id);
    }
    recordWorkingModel(sessionCtx, {
      lastRecommendation: revised ? recommendationRecord(revised) : lastRecommendation,
      retractedPremises: nextRetracted,
      operatorDeniedEmailActive: extras.operatorDeniedEmailActive,
    });
    sessionCtx.lastCognitiveMode = mode.kind;
    sessionCtx.lastRetrievalSources = ['operatingEvidence'];
    sessionCtx.lastResponseContract = contract && contract.id;
  }

  const structured = attachContractMetadata(
    operatingStructured(challengeProse, {
      used: ['operatingEvidence'],
      items: relevantChallengeItems(bundle, handled.claim),
      cognitiveMode: mode.kind,
      recommend: false,
      claimChallenge: true,
      claimVerdict: handled.evaluation.verdict,
      mailExecuted: Boolean(bundle.campaign && bundle.campaign.mailExecuted),
      unavailable: itemsByUnavailable(bundle.items),
      reasoning: [
        `Classified operator intent as claim challenge (${mode.kind}).`,
        'Selected Challenge response contract before retrieval.',
        'Retrieved evidence relevant to the challenged claim instead of restating the full operating inventory.',
        handled.evaluation.verdict === 'retract'
          ? 'The challenged operating-state premise was unsupported. It was retracted, the recommendation was rebuilt from supported claims only, and nothing was persisted as operating fact.'
          : handled.evaluation.verdict === 'qualified'
            ? 'The challenged claim was qualified: planned, inventory, or objectives are not observed execution.'
            : 'The challenged claim was confirmed from retrieved evidence, not from the prior wording alone.',
        'No action was executed.',
      ],
    }),
    contract
  );

  return {
    reason: 'recommendation_claim_challenge',
    structured,
    prose: challengeProse,
    mode,
    sourcesUsed: ['operatingEvidence'],
    knowledgeState: 'operating_evidence',
    delegated: false,
    launchedScout: false,
    operatingEvidence: bundle,
    claimChallenge: true,
    claimVerdict: handled.evaluation.verdict,
    executed: false,
    responseContract: contract,
  };
}

function relevantChallengeItems(bundle, claim) {
  const items = Array.isArray(bundle.items) ? bundle.items : [];
  const topic = claim && claim.topic;
  if (topic === 'mail' || topic === 'campaign_completed') {
    return items.filter((item) => /mail|operator|deliver/i.test(String((item && (item.claim || item.provenance)) || '')));
  }
  if (topic === 'follow_up') {
    return items.filter((item) => /follow/i.test(String((item && item.claim) || '')));
  }
  if (topic === 'email_motion') {
    return items.filter((item) =>
      /email|emmett|touchpoint|activity|mission/i.test(String((item && (item.claim || item.provenance || item.sourceKind)) || ''))
    );
  }
  if (topic === 'outreach_begun' || topic === 'inventory') {
    return items.filter((item) =>
      /prospect|scout|inventory|outreach/i.test(String((item && (item.claim || item.provenance || item.sourceKind)) || ''))
    );
  }
  if (topic === 'commercial_expansion' || topic === 'objective') {
    return items.filter((item) =>
      /objective|goal|blueprint|commercial/i.test(String((item && (item.claim || item.provenance || item.sourceKind)) || ''))
    );
  }
  return items.slice(0, 3);
}

function itemsByUnavailable(items) {
  return (items || [])
    .filter((item) => item && item.epistemic === 'unavailable')
    .map((item) => item.sourceKind || item.provenance)
    .filter(Boolean);
}

async function maybeHandleRecommendationFollowUpTurn(input, question, mode) {
  const last = lastRecommendationFrom(input);
  if (!last) return null;
  const contract =
    input.responseContract ||
    selectResponseContract(question, mode);
  const focus =
    (last.decision && (last.decision.focus || last.decision.recommendation)) ||
    last.recommendation ||
    'the prior next-action recommendation';
  const inference =
    (last.decision && last.decision.inference) ||
    'it follows retrieved operating evidence and approved Blueprint goals rather than a generic acquisition essay.';
  const prose = composeAccordingToContract(contract, {
    businessIntelligence:
      'The prior recommendation remains the working next-action until new operating evidence changes the constraint.',
    verifiedState: `The last governed recommendation was: ${focus}`,
    unknowns:
      'I will not restate a Blueprint advisory essay. New evidence would change this; assumption will not.',
    evidence:
      `Because ${inference} Approved Blueprint goals are desired state, not proof that the motion is already working.`,
  });

  const sessionCtx =
    input.session && input.session.context && typeof input.session.context === 'object'
      ? input.session.context
      : null;
  if (sessionCtx) {
    sessionCtx.lastCognitiveMode = mode.kind;
    sessionCtx.lastResponseContract = contract && contract.id;
  }

  const structured = attachPipelineLog(
    attachContractMetadata(
      workspaceStructured(prose, {
        used: ['operatingEvidence', 'blueprint'],
        cognitiveMode: mode.kind,
        responseContract: contract && contract.id,
        reasoning: [
          `Classified operator intent as ${mode.kind} (recommendation follow-up).`,
          'Selected Retrieval response contract. Explained the prior governed recommendation instead of re-running Blueprint Advisory.',
        ],
        evidenceCount: 1,
      }),
      contract
    ),
    { analysis: mode, contract }
  );

  return {
    reason: 'governed_recommendation_follow_up',
    structured,
    prose,
    mode,
    sourcesUsed: ['operatingEvidence', 'blueprint'],
    knowledgeState: 'operating_evidence',
    delegated: false,
    launchedScout: false,
    responseContract: contract,
  };
}

async function maybeHandleRetrievalBeforeDelegationTurn(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;

  const mode =
    input.cognitive ||
    classifyCognitiveMode(question, {
      session: input.session,
      context: input.context,
    });
  const contract =
    input.responseContract === undefined
      ? selectResponseContract(question, mode)
      : input.responseContract;

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.lastResponseContract = contract && contract.id;
    input.session.context.responseContract = contract || input.session.context.responseContract;
  }

  try {
    const cie = require('./ClientIntelligenceContext');
    if (cie.isClearlyNonBusinessUtterance(question, input.session)) {
      return null;
    }
  } catch (_) {
    /* classifier unavailable */
  }

  if (isOperatorDeskWorkflowQuestion(question)) {
    return null;
  }

  try {
    const cie = require('./ClientIntelligenceContext');
    if (cie.shouldUseExecutionClarification(question, input.session)) {
      return null;
    }
    if (cie.isClientContextExecutionRequest(question)) {
      return null;
    }
    if (cie.isOperationalDeskOrMissionRequest(question)) {
      return null;
    }
  } catch (_) {
    /* CIE workflow detectors unavailable */
  }
  try {
    const { classifyAdvisoryHandoffIntent } = require('./ActiveClientReasoning');
    if (classifyAdvisoryHandoffIntent(question, input.session)) {
      return null;
    }
  } catch (_) {
    /* handoff classifier unavailable */
  }

  if (mode && (mode.kind === COGNITIVE_MODES.EXECUTION || mode.kind === COGNITIVE_MODES.PLANNING)) {
    return null;
  }

  try {
    const { isOperatorOperatingUpdate } = require('./OperatorOperatingUpdate');
    if (isOperatorOperatingUpdate(question)) {
      return null;
    }
  } catch (_) {
    /* operating-update detector unavailable */
  }

  // SPEC-095 — objective recovery and Paige content routing happen after
  // retrieval. Yield so recovered launch/campaign context can attach.
  // Do not compose a governed recommendation for specialist content asks.
  try {
    const objectives = require('../../../services/operatorObjectives');
    if (
      typeof objectives.looksLikeObjectiveContentRequest === 'function' &&
      objectives.looksLikeObjectiveContentRequest(question)
    ) {
      return null;
    }
  } catch (_) {
    /* objective content detector unavailable */
  }
  try {
    const delegation = require('../../../services/maxPaigeCampaignDelegation');
    if (
      typeof delegation.shouldDelegateToPaige === 'function' &&
      delegation.shouldDelegateToPaige(question, input.context || {})
    ) {
      return null;
    }
  } catch (_) {
    /* Paige detector unavailable */
  }

  if (contract && contract.id === CONTRACT_IDS.INVESTIGATION) {
    const sources = await inspectRetrievalSources(input);
    let bundle = null;
    try {
      bundle = await loadOperatingEvidence(input);
    } catch (_) {
      bundle = null;
    }
    const knownBits = [];
    if (sources.businessName || sources.identity) {
      knownBits.push(sources.businessName || sources.identity);
    }
    if (bundle && Array.isArray(bundle.items)) {
      for (const item of bundle.items.slice(0, 4)) {
        if (item && item.claim) knownBits.push(item.claim);
      }
    }
    if (input.session && input.session.context && typeof input.session.context === 'object') {
      input.session.context.investigationKnown =
        knownBits.filter(Boolean).join('\n') ||
        'No durable investigation result is on file for this question.';
      input.session.context.lastCognitiveMode = mode.kind;
    }
    return null;
  }

  if (isClaimChallenge(question) || isOperatorClaimCorrection(question)) {
    const turn = await maybeHandleClaimChallengeTurn(
      { ...input, responseContract: contract },
      question,
      mode
    );
    if (turn && turn.structured) {
      turn.structured = attachPipelineLog(turn.structured, { analysis: mode, contract });
    }
    return turn;
  }

  if (mode && mode.via === 'recommendation_follow_up') {
    const follow = await maybeHandleRecommendationFollowUpTurn(
      { ...input, responseContract: contract },
      question,
      mode
    );
    if (follow) return follow;
  }

  const planTurn = await maybeHandlePlanProviderTurn(
    { ...input, responseContract: contract },
    question,
    mode,
    contract
  );
  if (planTurn) return planTurn;

  try {
    const cie = require('./ClientIntelligenceContext');
    if (
      cie.isEvidenceDependentClientRequest(question) &&
      !shouldRetrieveOperatingEvidence(question)
    ) {
      const sources = await inspectRetrievalSources(input);
      const summary = (sources && sources.blueprint) || {};
      const prose = cie.formatEvidenceDependentGapAnswer(summary);
      const structured = attachPipelineLog(
        attachContractMetadata(
          workspaceStructured(prose, {
            used: summary && summary.approved ? ['blueprint'] : [],
            cognitiveMode: mode && mode.kind,
            responseContract: contract && contract.id,
            reasoning: [
              `Classified operator intent as ${mode && mode.kind}.`,
              'Evidence-dependent question — Blueprint names the desired audience. Live market or prospect evidence is missing. I will not invent companies or buying signals.',
            ],
            evidenceCount: summary && summary.approved ? 1 : 0,
            unavailable: ['live_buying_signals', 'market_intelligence'],
          }),
          contract
        ),
        { analysis: mode, contract }
      );
      return {
        reason: 'governed_evidence_gap',
        structured,
        prose,
        mode,
        sourcesUsed: summary && summary.approved ? ['blueprint'] : [],
        knowledgeState: sources && sources.knowledgeState,
        delegated: false,
        launchedScout: false,
        responseContract: contract,
      };
    }
  } catch (_) {
    /* evidence-gap provider unavailable */
  }

  const analytical =
    mode &&
    (mode.kind === COGNITIVE_MODES.DIAGNOSIS ||
      mode.kind === COGNITIVE_MODES.UNKNOWN_ANALYSIS ||
      mode.kind === COGNITIVE_MODES.RISK ||
      mode.kind === COGNITIVE_MODES.PROGRESS);
  const recommendation = Boolean(mode && mode.kind === COGNITIVE_MODES.RECOMMENDATION);
  const summaryMode = Boolean(
    (mode && mode.via === 'summary') ||
      (contract && contract.id === CONTRACT_IDS.SUMMARY)
  );

  if (
    shouldRetrieveOperatingEvidence(question) ||
    looksLikeSummary(question) ||
    looksLikeCompletedRetrieval(question) ||
    recommendation ||
    analytical ||
    summaryMode
  ) {
    return maybeHandleOperatingEvidenceTurn(
      { ...input, responseContract: contract },
      question,
      mode
    );
  }

  const hardRetrieval = isHardRetrievalQuestion(question, mode);
  const governedRetrieval =
    contract &&
    (contract.id === CONTRACT_IDS.RETRIEVAL ||
      contract.forbidsSpecialistDelegation === true);
  if (!forbidsSpecialistDelegation(mode) && !hardRetrieval && !governedRetrieval) {
    return null;
  }

  const sources = await inspectRetrievalSources(input);
  const composed = composeRetrievalAnswer(question, mode, sources);
  const unknownProse = formatUnknownAnswer(
    sources.knowledgeState || KNOWLEDGE_STATES.NEVER_LEARNED
  );

  if (
    composed.prose === unknownProse &&
    composed.prose !== UNKNOWN_ANSWER &&
    !isHardRetrievalQuestion(question, mode) &&
    !governedRetrieval
  ) {
    return null;
  }

  if (
    composed.prose === UNKNOWN_ANSWER &&
    !isHardRetrievalQuestion(question, mode) &&
    !governedRetrieval
  ) {
    return null;
  }

  const used = sourceOrderUsed(composed.used || []);

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.lastCognitiveMode = mode.kind;
    input.session.context.lastRetrievalSources = used;
    input.session.context.lastKnowledgeState = composed.knowledgeState || sources.knowledgeState;
    input.session.context.lastResponseContract = contract && contract.id;
  }

  const isUnknown =
    composed.prose === unknownProse || composed.prose === UNKNOWN_ANSWER;

  const structured = attachPipelineLog(
    attachContractMetadata(
      workspaceStructured(composed.prose, {
        used,
        cognitiveMode: mode.kind,
        knowledgeState: composed.knowledgeState || sources.knowledgeState,
        blueprintSource: sources.blueprintSource,
        responseContract: contract && contract.id,
        reasoning: [
          `Classified operator intent as ${mode.kind}.`,
          contract ? `Selected ${contract.label} response contract before retrieval.` : null,
          isUnknown
            ? sources.knowledgeState === KNOWLEDGE_STATES.RETRIEVAL_FAILURE
              ? 'Approved understanding exists but retrieval failed — architectural failure, not a knowledge gap.'
              : sources.knowledgeState === KNOWLEDGE_STATES.NEVER_LEARNED
                ? 'No approved business understanding on file. Unknown is acceptable — I will not invent work for a specialist.'
                : 'Durable knowledge did not answer this. Unknown is acceptable — I will not invent work for a specialist.'
            : 'Retrieved from durable business understanding. Blueprint is evidence, not a reasoning engine.',
        ].filter(Boolean),
        evidenceCount: used.length,
        unavailable: isUnknown ? ['durable_knowledge'] : [],
      }),
      contract
    ),
    { analysis: mode, contract }
  );

  return {
    reason: 'retrieval_before_delegation',
    structured,
    prose: composed.prose,
    mode,
    sourcesUsed: used,
    knowledgeState: composed.knowledgeState || sources.knowledgeState,
    delegated: false,
    responseContract: contract,
  };
}

module.exports = {
  maybeHandleRetrievalBeforeDelegationTurn,
  inspectRetrievalSources,
  composeRetrievalAnswer,
  isHardRetrievalQuestion,
  isOperatorDeskWorkflowQuestion,
  SERVICE_AREA_RE,
  ENTITY_KNOW_RE,
};
