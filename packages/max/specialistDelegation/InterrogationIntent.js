'use strict';

/**
 * SPEC-101 — follow-up intent recognition and recent-referent resolution.
 * Generic across specialists. Does not invent traces.
 */

const { asText } = require('./Types');
const { tracesShareObjective } = require('./CognitiveTrace');

const INTENT = Object.freeze({
  INTERROGATE: 'interrogate',
  NEW_INVESTIGATION: 'new_investigation',
  CONTEXT_INSPECTION: 'context_inspection',
  UNRELATED: 'unrelated',
});

const SPECIALIST_ALIASES = Object.freeze({
  scout: ['scout', 'he', 'him'],
  paige: ['paige', 'she', 'her'],
  penny: ['penny'],
  emmett: ['emmett'],
  cal: ['cal'],
  link: ['link'],
  faye: ['faye'],
  ivy: ['ivy'],
  sam: ['sam'],
  test_intelligence: ['test intelligence', 'test_intelligence'],
});

const INTERROGATION_RE = new RegExp(
  [
    String.raw`why couldn'?t`,
    String.raw`why didn'?t`,
    String.raw`why did you (?:elevate|decide|trust|accept|reject|give|send|ask|delegate|change)`,
    String.raw`why did (?:scout|paige|penny|emmett|cal|link|he|she)`,
    String.raw`what (?:geographic information|information|context|evidence) did you (?:give|send|provide|include)`,
    String.raw`what did (?:you|scout|paige|penny|emmett|cal|link|he|she) (?:actually )?(?:investigate|observe|find|do|give|recommend)`,
    String.raw`what did (?:you|he|she) (?:give|tell|send) (?:him|her|scout|paige)`,
    String.raw`what failed`,
    String.raw`what evidence did`,
    String.raw`why do you trust`,
    String.raw`why (?:didn'?t|did) you (?:elevate|change|trust|accept|reject)`,
    String.raw`which part of .+ did you accept`,
    String.raw`what would have changed your (?:decision|mind|judgment)`,
    String.raw`why weren'?t (?:those|these|they) evaluated`,
    String.raw`why (?:weren'?t|wasn'?t) (?:those|these|that|the) (?:compan(?:y|ies)|prospects)`,
    String.raw`what geographic`,
    String.raw`how thorough`,
    String.raw`what eliminated`,
    String.raw`where was (?:scout'?s? )?coverage`,
    String.raw`do you trust`,
    String.raw`what would you investigate next`,
    String.raw`what happened in that (?:conversation|investigation|search)`,
    String.raw`what advertising evidence`,
    String.raw`why did (?:paige|penny|emmett|cal|link) recommend`,
    String.raw`what did (?:link|faye|ivy) actually observe`,
  ].join('|'),
  'i'
);

const NEW_INVESTIGATION_RE =
  /\b(find (?:more )?(?:commercial|current)?\s*(?:cleaning )?opportunit(?:y|ies)?|look(?:ing)? for (?:commercial|more)|investigate (?:bedford|nashua|a new|another)|search (?:again|bedford)|run (?:another|a new) (?:search|investigation)|where should we (?:be )?look)\b/i;

const SERVICE_AREA_RE =
  /\bwhat do you (?:currently )?understand about (?:our )?(?:service area|geography|market area|territory)\b/i;

const PROSPECT_LIST_RE =
  /\b(?:you detected|prospect list).{0,80}(?:why weren'?t|not evaluated|weren'?t those evaluated)\b/i;

const REFERENT_RE =
  /\b(scout|paige|penny|emmett|cal|link|faye|ivy|he|she|him|her|that investigation|that result|those companies|your recommendation|the search|the failure|the evidence|that conclusion|this investigation|the investigation)\b/i;

function looksLikeInterrogation(question) {
  const q = String(question || '').trim();
  if (!q) return false;
  if (SERVICE_AREA_RE.test(q) || PROSPECT_LIST_RE.test(q)) return true;
  return INTERROGATION_RE.test(q);
}

function looksLikeNewInvestigation(question) {
  return NEW_INVESTIGATION_RE.test(String(question || ''));
}

function mentionedSpecialist(question) {
  const q = String(question || '').toLowerCase();
  for (const [specialist, aliases] of Object.entries(SPECIALIST_ALIASES)) {
    if (aliases.some((alias) => new RegExp(`\\b${alias}\\b`, 'i').test(q))) {
      if (aliasIsPronoun(aliases[0]) && aliases.every(aliasIsPronoun)) continue;
      const proper = aliases.find((alias) => !aliasIsPronoun(alias));
      if (proper && new RegExp(`\\b${proper}\\b`, 'i').test(q)) return specialist;
    }
  }
  if (/\b(scout|he|him)\b/i.test(q) && /\b(scout|investigation|search|geography|compan)/i.test(q)) {
    return 'scout';
  }
  if (/\bpaige\b/i.test(q)) return 'paige';
  return null;
}

function aliasIsPronoun(alias) {
  return ['he', 'him', 'she', 'her'].includes(String(alias || '').toLowerCase());
}

function classifyOperatorIntent(question, traces = []) {
  const q = String(question || '').trim();
  if (!q) return { kind: INTENT.UNRELATED, specialist: null };

  const hasTraces = Array.isArray(traces) && traces.length > 0;
  const interrogation = looksLikeInterrogation(q);
  const newWork = looksLikeNewInvestigation(q);
  const specialist = mentionedSpecialist(q);

  if (SERVICE_AREA_RE.test(q)) {
    return {
      kind: hasTraces ? INTENT.CONTEXT_INSPECTION : INTENT.UNRELATED,
      specialist: specialist || (hasTraces ? traces[0].specialist : null),
      topic: 'service_area',
    };
  }

  if (interrogation && newWork) {
    return {
      kind: INTENT.INTERROGATE,
      specialist,
      topic: 'mixed',
      recommendNewWork: true,
    };
  }

  if (interrogation) {
    return {
      kind: INTENT.INTERROGATE,
      specialist,
      topic: PROSPECT_LIST_RE.test(q)
        ? 'prospect_list'
        : /elevate|priority|immaterial|material/i.test(q)
          ? 'max_judgment'
          : /geograph|service area|location|manchester|bedford/i.test(q)
            ? 'geography'
            : /trust|accept|reject|conclusion/i.test(q)
              ? 'trust'
              : /evidence/i.test(q)
                ? 'evidence'
                : 'general',
    };
  }

  if (newWork) {
    return { kind: INTENT.NEW_INVESTIGATION, specialist, topic: 'acquisition' };
  }

  if (hasTraces && REFERENT_RE.test(q) && /why|what|how|which/.test(q.toLowerCase())) {
    return { kind: INTENT.INTERROGATE, specialist, topic: 'referent' };
  }

  return { kind: INTENT.UNRELATED, specialist: null };
}

function scoreTrace(trace, input = {}) {
  const question = String(input.question || '').toLowerCase();
  const specialist = input.specialist || mentionedSpecialist(question);
  let score = 0;
  if (specialist && trace.specialist === specialist) score += 5;
  if (!specialist && input.recentSpecialist && trace.specialist === input.recentSpecialist) {
    score += 3;
  }
  if (input.domain && /acquisit/i.test(String(input.domain))) {
    if (/acquisit|scout/i.test(String(trace.capability || ''))) score += 2;
  }
  if (input.objective && tracesShareObjective(trace, { operatorObjective: input.objective })) {
    score += 3;
  }
  const created =
    (trace.delegation && new Date(trace.delegation.createdAt || 0).getTime()) ||
    (trace.maxEvaluation && new Date(trace.maxEvaluation.evaluatedAt || 0).getTime()) ||
    0;
  if (created) score += Math.max(0, 2 - (Date.now() - created) / (6 * 60 * 60 * 1000));
  if (input.conversationMentions && input.conversationMentions.includes(trace.traceId)) {
    score += 4;
  }
  return score;
}

/**
 * Resolve a natural-language referent against recent traces.
 * Fail closed on genuine ambiguity.
 *
 * @param {object} input
 * @returns {{ status: 'resolved'|'ambiguous'|'none', trace?: object, candidates?: object[] }}
 */
function resolveRecentReferent(input = {}) {
  const traces = Array.isArray(input.traces) ? input.traces.filter(Boolean) : [];
  if (!traces.length) return { status: 'none', trace: null, candidates: [] };

  const specialist = input.specialist || mentionedSpecialist(input.question);
  const scored = traces
    .map((trace) => ({
      trace,
      score: scoreTrace(trace, { ...input, specialist }),
    }))
    .sort((a, b) => b.score - a.score);

  const top = scored[0];
  const second = scored[1];
  if (!top) return { status: 'none', trace: null, candidates: [] };

  if (
    second &&
    top.score > 0 &&
    top.score - second.score < 1.5 &&
    top.trace.specialist === second.trace.specialist &&
    !tracesShareObjective(top.trace, second.trace)
  ) {
    return {
      status: 'ambiguous',
      trace: null,
      candidates: [top.trace, second.trace],
    };
  }

  if (specialist) {
    const matching = scored.filter((row) => row.trace.specialist === specialist);
    if (!matching.length) return { status: 'none', trace: null, candidates: [] };
    if (
      matching.length > 1 &&
      matching[0].score - matching[1].score < 1.5 &&
      !tracesShareObjective(matching[0].trace, matching[1].trace)
    ) {
      return {
        status: 'ambiguous',
        trace: null,
        candidates: matching.slice(0, 2).map((row) => row.trace),
      };
    }
    return { status: 'resolved', trace: matching[0].trace, candidates: [] };
  }

  return { status: 'resolved', trace: top.trace, candidates: [] };
}

function formatDisambiguation(candidates) {
  const labels = (candidates || []).map((trace, i) => {
    const specialist = asText(trace.specialist) || 'specialist';
    const objective = asText(trace.operatorObjective) || 'recent work';
    return `${i === 0 ? 'the' : 'the other'} ${specialist} ${objective}`;
  });
  if (labels.length < 2) {
    return 'I have more than one recent investigation. Which one do you mean?';
  }
  return `Do you mean ${labels[0]} or ${labels[1]}?`;
}

module.exports = {
  INTENT,
  SPECIALIST_ALIASES,
  INTERROGATION_RE,
  NEW_INVESTIGATION_RE,
  looksLikeInterrogation,
  looksLikeNewInvestigation,
  mentionedSpecialist,
  classifyOperatorIntent,
  resolveRecentReferent,
  formatDisambiguation,
  scoreTrace,
};
