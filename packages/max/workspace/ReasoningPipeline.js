'use strict';

/**
 * SPEC-113 / PILOT-0 AUDIT-001 — single governed operator reasoning pipeline.
 *
 * Operator Request
 *   → Intent Classification
 *   → Analysis Mode Selection
 *   → Response Contract
 *   → Evidence Retrieval
 *   → Claim Grounding
 *   → Business Intelligence Synthesis
 *   → Reasoning
 *   → Response Composition
 *
 * Blueprints are evidence. Specialists produce intelligence.
 * Only ResponseContract composes operator-facing reasoning.
 * Unknown intent fails toward Retrieval, never Recommendation or
 * Blueprint Advisory.
 */

const {
  COGNITIVE_MODES,
  classifyCognitiveMode,
} = require('../specialistDelegation/CognitiveMode');
const {
  OPERATOR_INTENTS,
  INTENT_LABELS,
  intentFromCognitiveMode,
} = require('./OperatorIntentRegistry');
const {
  CONTRACT_IDS,
  RetrievalContract,
  selectResponseContract,
  attachContractMetadata,
} = require('./ResponseContract');

const COMPOSER_ID = 'ResponseContract';
const PIPELINE_ID = 'governed_reasoning_pipeline';

const REASONING_KINDS = Object.freeze([
  COGNITIVE_MODES.RETRIEVAL,
  COGNITIVE_MODES.EXPLANATION,
  COGNITIVE_MODES.REFLECTION,
  COGNITIVE_MODES.INVESTIGATION,
  COGNITIVE_MODES.RECOMMENDATION,
  COGNITIVE_MODES.PLANNING,
  COGNITIVE_MODES.DIAGNOSIS,
  COGNITIVE_MODES.UNKNOWN_ANALYSIS,
  COGNITIVE_MODES.RISK,
  COGNITIVE_MODES.PROGRESS,
  COGNITIVE_MODES.UNCLASSIFIED,
]);

function present(value) {
  return String(value || '').trim();
}

function isExecutionKind(kind) {
  return kind === COGNITIVE_MODES.EXECUTION;
}

/**
 * Unknown client-business intent fails toward Retrieval.
 * Planning, execution, mission, and non-business chatter are not reasoning
 * contracts — they must not be captured by the retrieval fallback.
 */
function resolveAnalysisMode(cognitive, question, session) {
  const raw =
    cognitive && typeof cognitive === 'object'
      ? cognitive
      : classifyCognitiveMode(question);
  const kind = raw.kind || COGNITIVE_MODES.UNCLASSIFIED;

  if (isExecutionKind(kind) || kind === COGNITIVE_MODES.PLANNING) {
    return {
      ...raw,
      kind,
      intent: raw.intent || null,
      analysisMode: raw.analysisMode || null,
      fallbackUsed: false,
      governed: false,
    };
  }

  if (kind === COGNITIVE_MODES.UNCLASSIFIED) {
    let clientReasoning = false;
    let workspaceWorkflow = false;
    try {
      const cie = require('./ClientIntelligenceContext');
      clientReasoning = cie.isClientContextReasoningRequest(question, session);
      workspaceWorkflow = cie.isOperationalDeskOrMissionRequest(question);
    } catch (_) {
      clientReasoning = false;
    }
    try {
      const objectives = require('../../../services/operatorObjectives');
      if (
        objectives.detectObjectiveEstablishment(question) ||
        objectives.looksLikeObjectiveStatusRequest(question) ||
        objectives.looksLikeObjectiveContentRequest(question)
      ) {
        workspaceWorkflow = true;
      }
    } catch (_) {
      /* objective detectors unavailable */
    }
    if (!clientReasoning || workspaceWorkflow) {
      return {
        ...raw,
        kind,
        intent: raw.intent || null,
        analysisMode: raw.analysisMode || null,
        fallbackUsed: false,
        governed: false,
      };
    }
    const intent = OPERATOR_INTENTS.RETRIEVAL;
    return {
      ...raw,
      kind: COGNITIVE_MODES.RETRIEVAL,
      via: 'unknown_intent_fallback',
      intent,
      analysisMode: intent,
      requiresOperatingRetrieval: true,
      fallbackUsed: true,
      originalKind: kind,
      governed: true,
    };
  }

  const intent =
    raw.intent ||
    intentFromCognitiveMode(raw) ||
    (kind === COGNITIVE_MODES.RECOMMENDATION
      ? OPERATOR_INTENTS.RECOMMENDATION
      : kind === COGNITIVE_MODES.INVESTIGATION
        ? OPERATOR_INTENTS.INVESTIGATION
        : OPERATOR_INTENTS.RETRIEVAL);

  return {
    ...raw,
    kind,
    intent,
    analysisMode: raw.analysisMode || intent,
    fallbackUsed: false,
    governed: true,
  };
}

function resolveResponseContract(question, analysis) {
  if (!analysis || !analysis.governed) return null;
  const selected = selectResponseContract(question, analysis);
  if (selected) return selected;
  return RetrievalContract;
}

/**
 * Bind intent, analysis mode, and response contract before retrieval.
 * Always selects a contract for governed turns. Never defaults to
 * Blueprint Advisory or Recommendation for unknown intent.
 */
function bindGovernedReasoning(question, input = {}) {
  const cognitive =
    input.cognitive ||
    classifyCognitiveMode(question, {
      session: input.session,
      context: input.context,
    });
  const analysis = resolveAnalysisMode(cognitive, question, input.session);
  const contract = resolveResponseContract(question, analysis);
  const session = input.session || null;
  if (session && session.context && typeof session.context === 'object' && contract) {
    session.context.responseContract = contract;
    session.context.lastResponseContract = contract.id;
    session.context.lastAnalysisMode = analysis.analysisMode || analysis.intent;
    session.context.lastOperatorIntent = analysis.intent;
    session.context.reasoningPipeline = PIPELINE_ID;
  }
  return {
    cognitive,
    analysis,
    contract,
    pipelineId: PIPELINE_ID,
    composer: COMPOSER_ID,
    governed: Boolean(analysis && analysis.governed && contract),
  };
}

function countGroundedClaims(structured) {
  const meta = (structured && structured.metadata) || {};
  const bi = meta.businessIntelligence || {};
  const objects = Array.isArray(bi.objects) ? bi.objects : [];
  let n = 0;
  for (const obj of objects) {
    n += Array.isArray(obj && obj.supporting_claims) ? obj.supporting_claims.length : 0;
  }
  if (n) return n;
  const evidence = Array.isArray(structured && structured.supportingEvidence)
    ? structured.supportingEvidence
    : [];
  return evidence.length;
}

function reasoningComponentsFrom(structured, extras = {}) {
  const parts = [];
  const meta = (structured && structured.metadata) || {};
  if (meta.cognitiveMode) parts.push(meta.cognitiveMode);
  if (meta.responseContract) parts.push(`contract:${meta.responseContract}`);
  if (meta.businessIntelligence) parts.push('business_intelligence');
  if (meta.retrievalBeforeDelegation) parts.push('retrieval');
  if (meta.claimChallenge) parts.push('claim_grounding');
  if (extras.analysis && extras.analysis.fallbackUsed) parts.push('unknown_intent_fallback');
  const reasoning = Array.isArray(structured && structured.reasoning)
    ? structured.reasoning
    : [];
  if (reasoning.length) parts.push('reasoning');
  parts.push(COMPOSER_ID);
  return [...new Set(parts)];
}

function buildPipelineLog(input = {}) {
  const analysis = input.analysis || {};
  const contract = input.contract || null;
  const structured = input.structured || {};
  const meta = structured.metadata || {};
  const bi = meta.businessIntelligence || {};
  const objects = Array.isArray(bi.objects) ? bi.objects : [];
  const intent = analysis.intent || meta.operatorIntent || null;
  return Object.freeze({
    pipelineId: PIPELINE_ID,
    intent,
    intentLabel: INTENT_LABELS[intent] || intent,
    analysisMode: analysis.analysisMode || intent,
    analysisModeLabel:
      INTENT_LABELS[analysis.analysisMode || intent] || analysis.analysisMode || intent,
    responseContract: contract && contract.id,
    responseContractLabel: contract && contract.label,
    evidenceCount:
      meta.evidenceCount != null
        ? meta.evidenceCount
        : Array.isArray(structured.supportingEvidence)
          ? structured.supportingEvidence.length
          : 0,
    groundedClaims: countGroundedClaims(structured),
    businessIntelligenceObjects: objects.length,
    reasoningComponents: reasoningComponentsFrom(structured, input),
    composer: COMPOSER_ID,
    fallbackUsed: Boolean(analysis.fallbackUsed),
  });
}

function attachPipelineLog(structured, bind, extras = {}) {
  if (!structured) return structured;
  const log = buildPipelineLog({
    analysis: bind && bind.analysis,
    contract: bind && bind.contract,
    structured,
    ...extras,
  });
  const withContract = bind && bind.contract
    ? attachContractMetadata(structured, bind.contract, {
        analysisMode: bind.analysis && bind.analysis.analysisMode,
        intent: bind.analysis && bind.analysis.intent,
        businessIntelligence:
          structured.metadata && structured.metadata.businessIntelligence,
      })
    : structured;
  const metadata =
    withContract.metadata && typeof withContract.metadata === 'object'
      ? withContract.metadata
      : {};
  metadata.pipelineLog = log;
  metadata.composer = COMPOSER_ID;
  metadata.reasoningPipeline = PIPELINE_ID;
  metadata.analysisMode = log.analysisMode;
  metadata.operatorIntent = log.intent;
  withContract.metadata = metadata;
  return withContract;
}

function isGovernedReasoningKind(kind) {
  return REASONING_KINDS.includes(kind) && kind !== COGNITIVE_MODES.EXECUTION;
}

module.exports = {
  COMPOSER_ID,
  PIPELINE_ID,
  REASONING_KINDS,
  resolveAnalysisMode,
  resolveResponseContract,
  bindGovernedReasoning,
  buildPipelineLog,
  attachPipelineLog,
  isGovernedReasoningKind,
  isExecutionKind,
};
