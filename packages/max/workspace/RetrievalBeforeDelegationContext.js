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
  const bareOnly =
    recommend &&
    isBareCurrentStateRecommendation(question) &&
    !hasOperatingGrounding(question);
  const bundle = await loadOperatingEvidence(input);
  if (bareOnly && !bundleHasUsableOperatingSignal(bundle)) {
    return null;
  }
  const understanding = await inspectRetrievalSources(input);
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

  const structured = attachContractMetadata(
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
          : 'Retrieved existing PulseForge operating evidence before Blueprint advisory reasoning.',
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
    input.responseContract ||
    selectResponseContract(question, mode);

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.lastResponseContract = contract && contract.id;
    input.session.context.responseContract = contract || input.session.context.responseContract;
  }

  if (isOperatorDeskWorkflowQuestion(question)) {
    return null;
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
    return maybeHandleClaimChallengeTurn({ ...input, responseContract: contract }, question, mode);
  }

  if (
    shouldRetrieveOperatingEvidence(question) ||
    looksLikeSummary(question) ||
    looksLikeCompletedRetrieval(question) ||
    (mode.kind === COGNITIVE_MODES.RECOMMENDATION &&
      isBareCurrentStateRecommendation(question))
  ) {
    return maybeHandleOperatingEvidenceTurn(
      { ...input, responseContract: contract },
      question,
      mode
    );
  }

  const hardRetrieval = isHardRetrievalQuestion(question, mode);
  if (!forbidsSpecialistDelegation(mode) && !hardRetrieval) return null;

  const sources = await inspectRetrievalSources(input);
  const composed = composeRetrievalAnswer(question, mode, sources);
  const unknownProse = formatUnknownAnswer(
    sources.knowledgeState || KNOWLEDGE_STATES.NEVER_LEARNED
  );

  if (
    composed.prose === unknownProse &&
    composed.prose !== UNKNOWN_ANSWER &&
    !isHardRetrievalQuestion(question, mode)
  ) {
    return null;
  }

  if (
    composed.prose === UNKNOWN_ANSWER &&
    !isHardRetrievalQuestion(question, mode)
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

  const structured = attachContractMetadata(
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
          : 'Retrieved from durable business understanding instead of delegating.',
      ].filter(Boolean),
      evidenceCount: used.length,
      unavailable: isUnknown ? ['durable_knowledge'] : [],
    }),
    contract
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
