'use strict';

/**
 * SPEC-102 / SPEC-111 — operator cognitive-mode classification.
 *
 * Every question belongs primarily to one mode. Classification happens
 * before any specialist routing. Investigation verbs are not inferred
 * from topic words like "cleaning" or a prior Scout turn.
 *
 * SPEC-111 expands analytical modes: diagnosis, unknown analysis, risk,
 * and progress are classified before recommendation or specialist paths.
 */

const {
  OPERATOR_INTENTS,
  classifyNewAnalysisMode,
  intentFromCognitiveMode,
  looksLikeDiagnosis,
  looksLikeUnknownAnalysis,
  looksLikeRisk,
  looksLikeProgress,
} = require('../workspace/OperatorIntentRegistry');

const COGNITIVE_MODES = Object.freeze({
  RETRIEVAL: 'retrieval',
  EXPLANATION: 'explanation',
  REFLECTION: 'reflection',
  INVESTIGATION: 'investigation',
  RECOMMENDATION: 'recommendation',
  PLANNING: 'planning',
  EXECUTION: 'execution',
  DIAGNOSIS: 'diagnosis',
  UNKNOWN_ANALYSIS: 'unknown_analysis',
  RISK: 'risk',
  PROGRESS: 'progress',
  UNCLASSIFIED: 'unclassified',
});

const NEVER_DELEGATE_MODES = Object.freeze([
  COGNITIVE_MODES.RETRIEVAL,
  COGNITIVE_MODES.EXPLANATION,
  COGNITIVE_MODES.REFLECTION,
  COGNITIVE_MODES.DIAGNOSIS,
  COGNITIVE_MODES.UNKNOWN_ANALYSIS,
  COGNITIVE_MODES.RISK,
  COGNITIVE_MODES.PROGRESS,
]);

const SPECIALIST_NAMES =
  'scout|paige|penny|emmett|cal|link|faye|ivy|riley|sam';

const EXECUTION_RE =
  /\b((?:send|approve|launch|publish|execute|mail)\s+(?:this|that|the|it|them|those)\b|(?:send|approve|launch|publish|execute)\s+(?:the\s+)?(?:email|recommendation|campaign|post|outreach)|launch campaign|approve this recommendation)\b/i;

const INVESTIGATION_RE = new RegExp(
  [
    String.raw`\bfind (?:\d+\s+)?(?:additional|more)\s+(?:property managers|prospects|leads|companies|opportunit)`,
    String.raw`\bfind (?:more )?(?:commercial |current )?(?:cleaning )?(?:\w+\s+){0,4}opportunit`,
    String.raw`\bfind more like\b`,
    String.raw`\blook(?:ing)? for (?:commercial|more|expansion|dentists?|property|competitors?|prospects?|leads?|signals?)`,
    String.raw`\blook for expansion\b`,
    String.raw`(?:^|[.!?]\s+)(?:(?:max|please),?\s+)?investigate\b`,
    String.raw`\bresearch (?:competitors?|the market|expansion|property|dentists?|prospects?)`,
    String.raw`\bsearch (?:for|again)\b`,
    String.raw`\brun (?:another|a new) (?:search|investigation)\b`,
    String.raw`\bwhere should we (?:be )?look`,
  ].join('|'),
  'i'
);

const PLANNING_RE =
  /\b(help me build|create a (?:rollout )?plan|build campaign|rollout plan|create a campaign)\b/i;

const RECOMMENDATION_RE =
  /\b(what should (?:we|i)(?: do| focus| prioritize| target| pursue)?|should (?:we|i) (?:target|pursue|go after|look at|focus|do next)|what(?:'s| is) next|what do you recommend|where is the (?:highest[- ]leverage|highest leverage)|what is (?:our|the|my) next (?:move|focus|step|priority))\b/i;

const REFLECTION_RE =
  /\b(do you trust|what worries you|where are you uncertain|what are you (?:uncertain|unsure|worried)|what don'?t you (?:currently )?know|where are you unsure)\b/i;

const EXPLANATION_RE =
  /\b(why (?:did|didn'?t|is|are|was|weren'?t|couldn'?t)|explain why|explain that|why that)\b/i;

const RETRIEVAL_RE = new RegExp(
  [
    String.raw`\bwhat do (?:you|we) (?:currently )?(?:understand|know|remember)`,
    String.raw`\bwhat (?:is|are|was|were) (?:our|the|my|your)\b`,
    String.raw`\bwho is\b`,
    String.raw`\bwho are (?:our|the|my|your)\b`,
    String.raw`\bideal customers?\b`,
    String.raw`\btarget customers?\b`,
    String.raw`\bbusiness priorities\b`,
    String.raw`\bbusiness goals?\b`,
    String.raw`\bwhat industries\b`,
    String.raw`\bwhat happened\b`,
    String.raw`\bwhat did (?:you|${SPECIALIST_NAMES})\b`,
    String.raw`\bsummarize\b`,
    String.raw`\bcompare\b`,
    String.raw`\bwhen (?:did|was|were)\b`,
    String.raw`\bwhat campaigns have we run\b`,
    String.raw`\bwhat evidence do we already have\b`,
    String.raw`\bwhat acquisition activity\b`,
    String.raw`\bwhat (?:prospects|leads) do we already have\b`,
    String.raw`\bwhat has happened so far\b`,
    String.raw`\bwhat have we already tried\b`,
    String.raw`\bwhat have we (?:completed|done|finished|accomplished)(?: recently)?\b`,
    String.raw`\brecently completed\b`,
    String.raw`\bwhat outreach has (?:already )?(?:been )?sent\b`,
    String.raw`\bwhat (?:emails?|mail|outreach) (?:has|have) (?:already )?(?:been )?(?:sent|mailed)\b`,
  ].join('|'),
  'i'
);

const SUMMARY_RE = new RegExp(
  [
    String.raw`\bhow(?:'s| is)\b.{0,80}\bdoing\b`,
    String.raw`\bhow are we doing\b`,
    String.raw`\bhow is (?:the )?(?:business|company|client|pipeline)\b`,
    String.raw`\bstatus (?:update|of)\b.{0,40}\b(?:anchor|campaign|pipeline|business)\b`,
  ].join('|'),
  'i'
);

const COMPLETED_RETRIEVAL_RE = new RegExp(
  [
    String.raw`\bwhat have we (?:completed|done|finished|accomplished)(?: recently)?\b`,
    String.raw`\bwhat(?:'s| is) (?:already )?(?:been )?(?:completed|done|finished) recently\b`,
    String.raw`\brecently completed\b`,
    String.raw`\bwhat outreach has (?:already )?(?:been )?sent\b`,
    String.raw`\bwhat (?:has|have) already been sent\b`,
    String.raw`\bwhat (?:emails?|mail|outreach) (?:has|have) (?:already )?(?:been )?(?:sent|mailed)\b`,
  ].join('|'),
  'i'
);

const WH_RETRIEVAL_PREFIX_RE =
  /^(?:(?:max|please),?\s+)?(?:what|who|when|where|how|explain|summarize|compare|reflect)\b/i;

function hasRecentSpecialistWork(input = {}) {
  const session = input.session || null;
  const ctx =
    (session && session.context && typeof session.context === 'object'
      ? session.context
      : {}) || {};
  const envelope = input.context && typeof input.context === 'object' ? input.context : {};
  return Boolean(
    ctx.lastScoutEvaluation ||
      ctx.lastSpecialistEvaluation ||
      ctx.lastCognitiveTraceId ||
      ctx.lastScoutInvestigation ||
      ctx.acquisitionLoop ||
      envelope.lastScoutEvaluation ||
      envelope.lastSpecialistEvaluation ||
      envelope.acquisitionLoop
  );
}

function looksLikeClaimChallenge(question) {
  try {
    const challenge = require('../workspace/RecommendationClaimChallenge');
    return (
      challenge.isClaimChallenge(question) || challenge.isOperatorClaimCorrection(question)
    );
  } catch (_) {
    return false;
  }
}

function looksLikeExistingEvidenceRetrieval(question) {
  try {
    const operating = require('../workspace/OperatingEvidenceRetrieval');
    return (
      operating.isExistingKnowledgeInvestigate(question) ||
      operating.isOperatingEvidenceQuestion(question)
    );
  } catch (_) {
    return false;
  }
}

function looksLikeInvestigation(question) {
  const q = String(question || '');
  if (looksLikeExistingEvidenceRetrieval(q)) return false;
  return INVESTIGATION_RE.test(q);
}

function looksLikeExecution(question) {
  return EXECUTION_RE.test(String(question || ''));
}

function looksLikePlanning(question) {
  return PLANNING_RE.test(String(question || ''));
}

function looksLikeRecommendation(question) {
  return RECOMMENDATION_RE.test(String(question || ''));
}

function looksLikeReflection(question) {
  return REFLECTION_RE.test(String(question || ''));
}

function looksLikeExplanation(question) {
  return EXPLANATION_RE.test(String(question || ''));
}

function looksLikeRetrieval(question) {
  const q = String(question || '');
  return RETRIEVAL_RE.test(q) || COMPLETED_RETRIEVAL_RE.test(q);
}

function looksLikeCompletedRetrieval(question) {
  return COMPLETED_RETRIEVAL_RE.test(String(question || ''));
}

function looksLikeSummary(question) {
  const q = String(question || '');
  if (!q.trim()) return false;
  if (looksLikeRecommendation(q)) return false;
  if (looksLikeClaimChallenge(q)) return false;
  if (looksLikeInvestigation(q)) return false;
  return SUMMARY_RE.test(q);
}

function modeResult(kind, via, extras = {}) {
  const result = {
    kind,
    via,
    explicitInvestigation: extras.explicitInvestigation === true,
    requiresOperatingRetrieval: extras.requiresOperatingRetrieval === true,
  };
  const intent =
    extras.intent ||
    intentFromCognitiveMode(result) ||
    (kind === COGNITIVE_MODES.DIAGNOSIS
      ? OPERATOR_INTENTS.DIAGNOSIS
      : kind === COGNITIVE_MODES.UNKNOWN_ANALYSIS
        ? OPERATOR_INTENTS.UNKNOWN_ANALYSIS
        : kind === COGNITIVE_MODES.RISK
          ? OPERATOR_INTENTS.RISK
          : kind === COGNITIVE_MODES.PROGRESS
            ? OPERATOR_INTENTS.PROGRESS
            : null);
  result.intent = intent || null;
  result.analysisMode = extras.analysisMode || intent || null;
  return result;
}

/**
 * Classify the operator's primary cognitive mode.
 * Recommendation and operating retrieval are not mutually exclusive:
 * a turn may have primaryIntent=recommendation with a retrieval requirement.
 * @param {string} question
 * @param {object} [input]
 * @returns {{ kind: string, via: string, explicitInvestigation: boolean, requiresOperatingRetrieval: boolean }}
 */
function classifyCognitiveMode(question, input = {}) {
  const q = String(question || '').trim();
  if (!q) {
    return modeResult(COGNITIVE_MODES.RETRIEVAL, 'empty');
  }

  if (looksLikeExecution(q)) {
    return modeResult(COGNITIVE_MODES.EXECUTION, 'execution_verb');
  }

  if (looksLikeClaimChallenge(q)) {
    return modeResult(COGNITIVE_MODES.EXPLANATION, 'claim_challenge', {
      requiresOperatingRetrieval: true,
      intent: OPERATOR_INTENTS.CHALLENGE,
      analysisMode: OPERATOR_INTENTS.CHALLENGE,
    });
  }

  const analysis = classifyNewAnalysisMode(q);
  if (analysis) {
    const kind =
      analysis.intent === OPERATOR_INTENTS.DIAGNOSIS
        ? COGNITIVE_MODES.DIAGNOSIS
        : analysis.intent === OPERATOR_INTENTS.UNKNOWN_ANALYSIS
          ? COGNITIVE_MODES.UNKNOWN_ANALYSIS
          : analysis.intent === OPERATOR_INTENTS.RISK
            ? COGNITIVE_MODES.RISK
            : COGNITIVE_MODES.PROGRESS;
    return modeResult(kind, analysis.via, {
      requiresOperatingRetrieval: true,
      intent: analysis.intent,
      analysisMode: analysis.analysisMode,
    });
  }

  const operatingRetrieval = looksLikeExistingEvidenceRetrieval(q);
  const recommendation = looksLikeRecommendation(q);

  if (operatingRetrieval && recommendation) {
    return modeResult(COGNITIVE_MODES.RECOMMENDATION, 'recommendation', {
      requiresOperatingRetrieval: true,
    });
  }

  if (operatingRetrieval) {
    return modeResult(COGNITIVE_MODES.RETRIEVAL, 'operating_evidence', {
      requiresOperatingRetrieval: true,
    });
  }

  if (looksLikeInvestigation(q)) {
    return modeResult(COGNITIVE_MODES.INVESTIGATION, 'investigation_verb', {
      explicitInvestigation: true,
      intent: OPERATOR_INTENTS.INVESTIGATION,
      analysisMode: OPERATOR_INTENTS.INVESTIGATION,
    });
  }

  if (looksLikePlanning(q)) {
    return modeResult(COGNITIVE_MODES.PLANNING, 'planning');
  }

  if (recommendation) {
    return modeResult(COGNITIVE_MODES.RECOMMENDATION, 'recommendation', {
      intent: OPERATOR_INTENTS.RECOMMENDATION,
      analysisMode: OPERATOR_INTENTS.RECOMMENDATION,
    });
  }

  if (looksLikeReflection(q)) {
    return modeResult(COGNITIVE_MODES.REFLECTION, 'reflection');
  }

  if (looksLikeExplanation(q)) {
    return modeResult(COGNITIVE_MODES.EXPLANATION, 'explanation');
  }

  if (looksLikeRetrieval(q)) {
    return modeResult(COGNITIVE_MODES.RETRIEVAL, 'retrieval', {
      requiresOperatingRetrieval: looksLikeCompletedRetrieval(q) || looksLikeExistingEvidenceRetrieval(q),
    });
  }

  if (looksLikeSummary(q)) {
    return modeResult(COGNITIVE_MODES.RETRIEVAL, 'summary', {
      requiresOperatingRetrieval: true,
      intent: OPERATOR_INTENTS.SUMMARY,
      analysisMode: OPERATOR_INTENTS.SUMMARY,
    });
  }

  if (hasRecentSpecialistWork(input)) {
    const tokenCount = q.split(/\s+/).filter(Boolean).length;
    if (WH_RETRIEVAL_PREFIX_RE.test(q) || tokenCount <= 4) {
      return modeResult(COGNITIVE_MODES.RETRIEVAL, 'conversation_continuity');
    }
  }

  return modeResult(COGNITIVE_MODES.UNCLASSIFIED, 'unclassified');
}

function forbidsSpecialistDelegation(mode) {
  const kind = typeof mode === 'string' ? mode : mode && mode.kind;
  return NEVER_DELEGATE_MODES.includes(kind);
}

module.exports = {
  COGNITIVE_MODES,
  NEVER_DELEGATE_MODES,
  EXECUTION_RE,
  INVESTIGATION_RE,
  PLANNING_RE,
  RECOMMENDATION_RE,
  REFLECTION_RE,
  EXPLANATION_RE,
  RETRIEVAL_RE,
  SUMMARY_RE,
  COMPLETED_RETRIEVAL_RE,
  classifyCognitiveMode,
  looksLikeInvestigation,
  looksLikeExecution,
  looksLikePlanning,
  looksLikeRecommendation,
  looksLikeReflection,
  looksLikeExplanation,
  looksLikeRetrieval,
  looksLikeSummary,
  looksLikeCompletedRetrieval,
  looksLikeExistingEvidenceRetrieval,
  looksLikeClaimChallenge,
  looksLikeDiagnosis,
  looksLikeUnknownAnalysis,
  looksLikeRisk,
  looksLikeProgress,
  hasRecentSpecialistWork,
  forbidsSpecialistDelegation,
};
