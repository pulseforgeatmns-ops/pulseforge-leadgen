'use strict';

/**
 * SPEC-111 — Operator Intent Taxonomy.
 *
 * Intent classification is explicit. Each operator question maps to one
 * analysis mode before retrieval, grounding, business intelligence, or
 * response composition.
 *
 * Intent → Analysis Mode → Response Contract → Retrieve → Ground →
 * Business Intelligence → Compose
 *
 * Distinct from SPEC-101 InterrogationIntent (`classifyOperatorIntent`),
 * which classifies specialist-trace follow-ups, not analytical modes.
 */

const OPERATOR_INTENTS = Object.freeze({
  RETRIEVAL: 'retrieval',
  SUMMARY: 'summary',
  RECOMMENDATION: 'recommendation',
  DIAGNOSIS: 'diagnosis',
  UNKNOWN_ANALYSIS: 'unknown_analysis',
  RISK: 'risk',
  PROGRESS: 'progress',
  CHALLENGE: 'challenge',
  INVESTIGATION: 'investigation',
});

const ANALYSIS_MODES = OPERATOR_INTENTS;

const INTENT_LABELS = Object.freeze({
  [OPERATOR_INTENTS.RETRIEVAL]: 'Retrieval',
  [OPERATOR_INTENTS.SUMMARY]: 'Summary',
  [OPERATOR_INTENTS.RECOMMENDATION]: 'Recommendation',
  [OPERATOR_INTENTS.DIAGNOSIS]: 'Diagnosis',
  [OPERATOR_INTENTS.UNKNOWN_ANALYSIS]: 'Unknown Analysis',
  [OPERATOR_INTENTS.RISK]: 'Risk Assessment',
  [OPERATOR_INTENTS.PROGRESS]: 'Progress Review',
  [OPERATOR_INTENTS.CHALLENGE]: 'Challenge',
  [OPERATOR_INTENTS.INVESTIGATION]: 'Investigation',
});

const DIAGNOSIS_RE = new RegExp(
  [
    String.raw`\bwhat(?:'s| is) preventing\b`,
    String.raw`\bwhat is preventing (?:us|growth)\b`,
    String.raw`\bwhat(?:'s| is) (?:the )?(?:current )?bottleneck\b`,
    String.raw`\bwhere(?:'s| is) the bottleneck\b`,
    String.raw`\bwhere are we stuck\b`,
    String.raw`\bwhy aren'?t we growing\b`,
    String.raw`\bwhat(?:'s| is) (?:holding|blocking|stopping) us\b`,
    String.raw`\bwhat(?:'s| is) blocking (?:growth|us)\b`,
    String.raw`\bwhat(?:'s| is) (?:the )?(?:limiting )?constraint\b`,
    String.raw`\bwhat(?:'s| is) (?:the )?limiting factor\b`,
  ].join('|'),
  'i'
);

const UNKNOWN_ANALYSIS_RE = new RegExp(
  [
    String.raw`\bwhat don'?t (?:we|you|i) (?:currently )?(?:yet )?know\b`,
    String.raw`\bwhat (?:do we|do you) not (?:yet )?know\b`,
    String.raw`\bwhat(?:'s| is) missing\b`,
    String.raw`\bwhat assumptions remain\b`,
    String.raw`\bwhat (?:unknowns?|uncertaint(?:y|ies)) (?:still )?(?:remain|matter)\b`,
    String.raw`\bwhat are we (?:still )?(?:missing|uncertain about)\b`,
    String.raw`\bwhere are (?:we|you) uncertain\b`,
    String.raw`\bwhat are you (?:uncertain|unsure) about\b`,
    String.raw`\bwhat don'?t you (?:currently )?know\b`,
    String.raw`\bwhat remains unknown\b`,
    String.raw`\bunknowns that matter\b`,
    String.raw`\bwhat evidence (?:is|are) (?:still )?missing\b`,
  ].join('|'),
  'i'
);

const RISK_RE = new RegExp(
  [
    String.raw`\bwhat(?:'s| is) (?:our |the )?(?:biggest |main |primary )?(?:operational )?risks?\b`,
    String.raw`\bwhat(?:'s| is) risky\b`,
    String.raw`\bwhere could this fail\b`,
    String.raw`\bwhere (?:could|might) (?:this|we|it) fail\b`,
    String.raw`\bwhat worries you\b`,
    String.raw`\bwhat (?:are|is) (?:the |our )?(?:biggest )?risks?\b`,
    String.raw`\boperational risk\b`,
  ].join('|'),
  'i'
);

const PROGRESS_RE = new RegExp(
  [
    String.raw`\bhow much progress\b`,
    String.raw`\bhow are we progressing\b`,
    String.raw`\bwhat(?:'s| has) improved\b`,
    String.raw`\bwhat(?:'s| is) remaining\b`,
    String.raw`\bremaining work\b`,
    String.raw`\bwhat progress have we made\b`,
    String.raw`\bhow much (?:have we|we've) (?:progressed|moved|advanced)\b`,
    String.raw`\bwhat(?:'s| is) completed\b`,
  ].join('|'),
  'i'
);

function looksLikeDiagnosis(question) {
  return DIAGNOSIS_RE.test(String(question || ''));
}

function looksLikeUnknownAnalysis(question) {
  return UNKNOWN_ANALYSIS_RE.test(String(question || ''));
}

function looksLikeRisk(question) {
  return RISK_RE.test(String(question || ''));
}

const COMPLETED_RETRIEVAL_OVERRIDE_RE =
  /\bwhat have we (?:completed|done|finished|accomplished)(?: recently)?\b|\brecently completed\b|\bwhat outreach has (?:already )?(?:been )?sent\b/i;

function looksLikeProgress(question) {
  const q = String(question || '');
  if (!PROGRESS_RE.test(q)) return false;
  if (COMPLETED_RETRIEVAL_OVERRIDE_RE.test(q)) return false;
  return true;
}

function looksLikeAnalyticalMode(question) {
  const q = String(question || '');
  return (
    looksLikeDiagnosis(q) ||
    looksLikeUnknownAnalysis(q) ||
    looksLikeRisk(q) ||
    looksLikeProgress(q)
  );
}

function intentResult(intent, via, extras = {}) {
  return {
    intent,
    analysisMode: intent,
    label: INTENT_LABELS[intent] || intent,
    via,
    requiresOperatingRetrieval: extras.requiresOperatingRetrieval !== false,
  };
}

/**
 * Classify the operator's analytical intent.
 * Returns null when the question is not one of the SPEC-111 registry modes
 * that this module owns exclusively (diagnosis / unknown / risk / progress).
 * CognitiveMode maps the full registry, including retrieval and recommendation.
 *
 * @param {string} question
 * @returns {{ intent: string, analysisMode: string, label: string, via: string, requiresOperatingRetrieval: boolean }|null}
 */
function classifyNewAnalysisMode(question) {
  const q = String(question || '').trim();
  if (!q) return null;
  if (looksLikeDiagnosis(q)) {
    return intentResult(OPERATOR_INTENTS.DIAGNOSIS, 'diagnosis');
  }
  if (looksLikeUnknownAnalysis(q)) {
    return intentResult(OPERATOR_INTENTS.UNKNOWN_ANALYSIS, 'unknown_analysis');
  }
  if (looksLikeRisk(q)) {
    return intentResult(OPERATOR_INTENTS.RISK, 'risk');
  }
  if (looksLikeProgress(q)) {
    return intentResult(OPERATOR_INTENTS.PROGRESS, 'progress');
  }
  return null;
}

function intentFromCognitiveMode(mode) {
  if (!mode) return null;
  const via = mode.via;
  const kind = mode.kind;
  if (via === 'claim_challenge' || kind === 'challenge') return OPERATOR_INTENTS.CHALLENGE;
  if (kind === OPERATOR_INTENTS.DIAGNOSIS || via === 'diagnosis') return OPERATOR_INTENTS.DIAGNOSIS;
  if (kind === OPERATOR_INTENTS.UNKNOWN_ANALYSIS || via === 'unknown_analysis') {
    return OPERATOR_INTENTS.UNKNOWN_ANALYSIS;
  }
  if (kind === OPERATOR_INTENTS.RISK || via === 'risk') return OPERATOR_INTENTS.RISK;
  if (kind === OPERATOR_INTENTS.PROGRESS || via === 'progress') return OPERATOR_INTENTS.PROGRESS;
  if (kind === 'investigation' || via === 'investigation_verb') return OPERATOR_INTENTS.INVESTIGATION;
  if (kind === 'recommendation' || via === 'recommendation') return OPERATOR_INTENTS.RECOMMENDATION;
  if (via === 'summary') return OPERATOR_INTENTS.SUMMARY;
  if (kind === 'retrieval' || via === 'operating_evidence' || via === 'retrieval') {
    return OPERATOR_INTENTS.RETRIEVAL;
  }
  if (kind === 'explanation' || kind === 'reflection') return OPERATOR_INTENTS.RETRIEVAL;
  return null;
}

function listOperatorIntents() {
  return Object.values(OPERATOR_INTENTS);
}

module.exports = {
  OPERATOR_INTENTS,
  ANALYSIS_MODES,
  INTENT_LABELS,
  DIAGNOSIS_RE,
  UNKNOWN_ANALYSIS_RE,
  RISK_RE,
  PROGRESS_RE,
  looksLikeDiagnosis,
  looksLikeUnknownAnalysis,
  looksLikeRisk,
  looksLikeProgress,
  looksLikeAnalyticalMode,
  classifyNewAnalysisMode,
  intentFromCognitiveMode,
  listOperatorIntents,
};
