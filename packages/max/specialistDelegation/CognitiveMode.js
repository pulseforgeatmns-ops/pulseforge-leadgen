'use strict';

/**
 * SPEC-102 — operator cognitive-mode classification.
 *
 * Every question belongs primarily to one mode. Classification happens
 * before any specialist routing. Investigation verbs are not inferred
 * from topic words like "cleaning" or a prior Scout turn.
 */

const COGNITIVE_MODES = Object.freeze({
  RETRIEVAL: 'retrieval',
  EXPLANATION: 'explanation',
  REFLECTION: 'reflection',
  INVESTIGATION: 'investigation',
  RECOMMENDATION: 'recommendation',
  PLANNING: 'planning',
  EXECUTION: 'execution',
  UNCLASSIFIED: 'unclassified',
});

const NEVER_DELEGATE_MODES = Object.freeze([
  COGNITIVE_MODES.RETRIEVAL,
  COGNITIVE_MODES.EXPLANATION,
  COGNITIVE_MODES.REFLECTION,
]);

const SPECIALIST_NAMES =
  'scout|paige|penny|emmett|cal|link|faye|ivy|riley|sam';

const EXECUTION_RE =
  /\b((?:send|approve|launch|publish|execute|mail)\s+(?:this|that|the|it|them|those)\b|(?:send|approve|launch|publish|execute)\s+(?:the\s+)?(?:email|recommendation|campaign|post|outreach)|launch campaign|approve this recommendation)\b/i;

const INVESTIGATION_RE = new RegExp(
  [
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
  /\b(what should we(?: do| focus| target| pursue)?|should we (?:target|pursue|go after|look at)|what(?:'s| is) next|what do you recommend)\b/i;

const REFLECTION_RE =
  /\b(do you trust|what worries you|where are you uncertain|what are you (?:uncertain|unsure|worried)|what don'?t you (?:currently )?know|where are you unsure)\b/i;

const EXPLANATION_RE =
  /\b(why (?:did|didn'?t|is|are|was|weren'?t|couldn'?t)|explain why|explain that|why that)\b/i;

const RETRIEVAL_RE = new RegExp(
  [
    String.raw`\bwhat do (?:you|we) (?:currently )?(?:understand|know|remember)`,
    String.raw`\bwhat (?:is|are|was|were) (?:our|the|my|your)\b`,
    String.raw`\bwho is\b`,
    String.raw`\bwhat industries\b`,
    String.raw`\bwhat happened\b`,
    String.raw`\bwhat did (?:you|${SPECIALIST_NAMES})\b`,
    String.raw`\bsummarize\b`,
    String.raw`\bcompare\b`,
    String.raw`\bwhen (?:did|was|were)\b`,
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

function looksLikeInvestigation(question) {
  return INVESTIGATION_RE.test(String(question || ''));
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
  return RETRIEVAL_RE.test(String(question || ''));
}

/**
 * Classify the operator's primary cognitive mode.
 * @param {string} question
 * @param {object} [input]
 * @returns {{ kind: string, via: string, explicitInvestigation: boolean }}
 */
function classifyCognitiveMode(question, input = {}) {
  const q = String(question || '').trim();
  if (!q) {
    return {
      kind: COGNITIVE_MODES.RETRIEVAL,
      via: 'empty',
      explicitInvestigation: false,
    };
  }

  if (looksLikeExecution(q)) {
    return {
      kind: COGNITIVE_MODES.EXECUTION,
      via: 'execution_verb',
      explicitInvestigation: false,
    };
  }

  if (looksLikeInvestigation(q)) {
    return {
      kind: COGNITIVE_MODES.INVESTIGATION,
      via: 'investigation_verb',
      explicitInvestigation: true,
    };
  }

  if (looksLikePlanning(q)) {
    return {
      kind: COGNITIVE_MODES.PLANNING,
      via: 'planning',
      explicitInvestigation: false,
    };
  }

  if (looksLikeRecommendation(q)) {
    return {
      kind: COGNITIVE_MODES.RECOMMENDATION,
      via: 'recommendation',
      explicitInvestigation: false,
    };
  }

  if (looksLikeReflection(q)) {
    return {
      kind: COGNITIVE_MODES.REFLECTION,
      via: 'reflection',
      explicitInvestigation: false,
    };
  }

  if (looksLikeExplanation(q)) {
    return {
      kind: COGNITIVE_MODES.EXPLANATION,
      via: 'explanation',
      explicitInvestigation: false,
    };
  }

  if (looksLikeRetrieval(q)) {
    return {
      kind: COGNITIVE_MODES.RETRIEVAL,
      via: 'retrieval',
      explicitInvestigation: false,
    };
  }

  if (hasRecentSpecialistWork(input)) {
    const tokenCount = q.split(/\s+/).filter(Boolean).length;
    if (WH_RETRIEVAL_PREFIX_RE.test(q) || tokenCount <= 4) {
      return {
        kind: COGNITIVE_MODES.RETRIEVAL,
        via: 'conversation_continuity',
        explicitInvestigation: false,
      };
    }
  }

  return {
    kind: COGNITIVE_MODES.UNCLASSIFIED,
    via: 'unclassified',
    explicitInvestigation: false,
  };
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
  classifyCognitiveMode,
  looksLikeInvestigation,
  looksLikeExecution,
  looksLikePlanning,
  looksLikeRecommendation,
  looksLikeReflection,
  looksLikeExplanation,
  looksLikeRetrieval,
  hasRecentSpecialistWork,
  forbidsSpecialistDelegation,
};
