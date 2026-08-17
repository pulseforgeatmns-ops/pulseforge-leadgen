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

const INDUSTRY_RE =
  /\b(what industries|which industries|who do we (?:target|serve)|ideal customers)\b/i;

const HISTORY_RE =
  /\bwhat happened (?:yesterday|today|last (?:night|week)|in (?:that|the) (?:conversation|investigation|search))\b/i;

const ELEVATION_RE =
  /\bwhy (?:didn'?t|did) you elevate\b/i;

function isHardRetrievalQuestion(question, mode) {
  if (SERVICE_AREA_RE.test(question)) return true;
  if (ENTITY_KNOW_RE.test(question)) return true;
  if (INDUSTRY_RE.test(question)) return true;
  if (HISTORY_RE.test(question)) return true;
  if (ELEVATION_RE.test(question)) return true;
  if (shouldRetrieveOperatingEvidence(question)) return true;
  if (mode && mode.kind === COGNITIVE_MODES.REFLECTION) return true;
  if (mode && mode.kind === COGNITIVE_MODES.EXPLANATION) return true;
  if (mode && mode.via === 'retrieval') return true;
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
    },
  });
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleOperatingEvidenceTurn(input, question, mode) {
  const inventoryOnly = isInventoryOnlyRequest(question);
  const recommend =
    !inventoryOnly &&
    (isOperatingGroundedRecommendation(question) ||
      (mode && mode.kind === COGNITIVE_MODES.RECOMMENDATION));
  const bareOnly =
    recommend &&
    isBareCurrentStateRecommendation(question) &&
    !hasOperatingGrounding(question);
  const bundle = await loadOperatingEvidence(input);
  if (bareOnly && !bundleHasUsableOperatingSignal(bundle)) {
    return null;
  }
  const understanding = await inspectRetrievalSources(input);
  const composed = composeOperatingEvidenceAnswer(question, bundle, {
    inventoryOnly,
    recommend,
    businessUnderstanding: understanding,
    now: input.now,
    capability: bundle.capability,
  });

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.lastCognitiveMode = mode.kind;
    input.session.context.lastRetrievalSources = composed.used || [];
    input.session.context.lastOperatingEvidence = {
      tenantId: bundle.tenantId || null,
      itemCount: Array.isArray(bundle.items) ? bundle.items.length : 0,
      launchedScout: false,
    };
  }

  const structured = operatingStructured(composed.prose, {
    used: composed.used,
    items: composed.items || bundle.items || [],
    cognitiveMode: mode.kind,
    recommend: composed.recommend,
    mailExecuted: Boolean(bundle.campaign && bundle.campaign.mailExecuted),
    unavailable: itemsByUnavailable(bundle.items),
    reasoning: [
      `Classified operator intent as ${mode.kind}.`,
      composed.recommend
        ? 'Retrieved operating evidence as a prerequisite, then reasoned to a recommendation.'
        : 'Retrieved existing PulseForge operating evidence before Blueprint advisory reasoning.',
      composed.recommend
        ? 'Recommendation is grounded in retrieved operating evidence, capability state, and policy — not Blueprint-only advice. No action was executed.'
        : 'Inventory requested — no new acquisition recommendation before evidence review.',
    ],
  });

  return {
    reason: 'operating_evidence_retrieval',
    structured,
    prose: composed.prose,
    mode,
    sourcesUsed: composed.used || [],
    knowledgeState: composed.knowledgeState,
    delegated: false,
    launchedScout: false,
    operatingEvidence: bundle,
  };
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

  if (
    shouldRetrieveOperatingEvidence(question) ||
    (mode.kind === COGNITIVE_MODES.RECOMMENDATION &&
      isBareCurrentStateRecommendation(question))
  ) {
    return maybeHandleOperatingEvidenceTurn(input, question, mode);
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
  }

  const isUnknown =
    composed.prose === unknownProse || composed.prose === UNKNOWN_ANSWER;

  const structured = workspaceStructured(composed.prose, {
    used,
    cognitiveMode: mode.kind,
    knowledgeState: composed.knowledgeState || sources.knowledgeState,
    blueprintSource: sources.blueprintSource,
    reasoning: [
      `Classified operator intent as ${mode.kind}.`,
      isUnknown
        ? sources.knowledgeState === KNOWLEDGE_STATES.RETRIEVAL_FAILURE
          ? 'Approved understanding exists but retrieval failed — architectural failure, not a knowledge gap.'
          : sources.knowledgeState === KNOWLEDGE_STATES.NEVER_LEARNED
            ? 'No approved business understanding on file. Unknown is acceptable — I will not invent work for a specialist.'
            : 'Durable knowledge did not answer this. Unknown is acceptable — I will not invent work for a specialist.'
        : 'Retrieved from durable business understanding instead of delegating.',
    ],
    evidenceCount: used.length,
    unavailable: isUnknown ? ['durable_knowledge'] : [],
  });

  return {
    reason: 'retrieval_before_delegation',
    structured,
    prose: composed.prose,
    mode,
    sourcesUsed: used,
    knowledgeState: composed.knowledgeState || sources.knowledgeState,
    delegated: false,
  };
}

module.exports = {
  maybeHandleRetrievalBeforeDelegationTurn,
  inspectRetrievalSources,
  composeRetrievalAnswer,
  isHardRetrievalQuestion,
  SERVICE_AREA_RE,
  ENTITY_KNOW_RE,
};
