'use strict';

/**
 * SPEC-202 / SPEC-209 — Bounded typo normalization for short operator control replies.
 * Shared Levenshtein primitive for pending decisions and conversational mission controls.
 */

const TYPO_AFFIRM_VOCAB = Object.freeze([
  'approved',
  'approve',
  'yes',
  'yep',
  'yeah',
  'yup',
  'ok',
  'okay',
  'sure',
  'proceed',
  'continue',
]);

const TYPO_REJECT_VOCAB = Object.freeze(['cancel', 'no', 'reject', 'stop', 'abort']);

const CONTINUATION_CONTROL_VOCAB = Object.freeze(['continue', 'proceed', 'resume']);

const TYPO_MAX_EDIT_DISTANCE = 1;
const TYPO_MAX_EDIT_DISTANCE_LONG = 2;
const TYPO_LONG_TOKEN_MIN = 6;
const TYPO_MAX_PHRASE_CHARS = 24;
const TYPO_MAX_WORDS = 3;

const CONTINUATION_MORPHOLOGICAL_SUFFIXES = Object.freeze([
  'ed',
  'ing',
  'ous',
  'ation',
  'ally',
  'er',
]);

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function stripTrailingPunctuation(value) {
  return String(value || '')
    .trim()
    .replace(/[.!?,;:]+$/g, '');
}

function maxEditDistanceForToken(token) {
  return token.length >= TYPO_LONG_TOKEN_MIN
    ? TYPO_MAX_EDIT_DISTANCE_LONG
    : TYPO_MAX_EDIT_DISTANCE;
}

function levenshtein(a, b) {
  const left = String(a || '');
  const right = String(b || '');
  if (left === right) return 0;
  if (!left.length) return right.length;
  if (!right.length) return left.length;

  const row = new Array(right.length + 1);
  for (let j = 0; j <= right.length; j += 1) row[j] = j;

  for (let i = 1; i <= left.length; i += 1) {
    let prev = row[0];
    row[0] = i;
    for (let j = 1; j <= right.length; j += 1) {
      const temp = row[j];
      const cost = left[i - 1] === right[j - 1] ? 0 : 1;
      row[j] = Math.min(row[j] + 1, row[j - 1] + 1, prev + cost);
      prev = temp;
    }
  }
  return row[right.length];
}

function findTypoCandidates(token, vocabulary) {
  const maxDistance = maxEditDistanceForToken(token);
  const matches = [];
  for (const candidate of vocabulary) {
    const distance = levenshtein(token, candidate);
    if (distance <= maxDistance) {
      matches.push({ candidate, distance });
    }
  }
  matches.sort((a, b) => a.distance - b.distance || a.candidate.localeCompare(b.candidate));
  return matches;
}

function isMorphologicalContinuationVariant(token, candidate) {
  if (token.length <= candidate.length) return false;
  return CONTINUATION_MORPHOLOGICAL_SUFFIXES.some((suffix) => token.endsWith(suffix));
}

function resolveTypoTokenForVocabulary(token, vocabulary, options = {}) {
  const matches = findTypoCandidates(token, vocabulary);
  if (!matches.length) return null;
  const best = matches[0];
  if (
    options.rejectMorphologicalVariants &&
    best.candidate === 'continue' &&
    isMorphologicalContinuationVariant(token, best.candidate)
  ) {
    return null;
  }
  return best.candidate;
}

function resolveTypoToken(token) {
  const affirmMatches = findTypoCandidates(token, TYPO_AFFIRM_VOCAB);
  const rejectMatches = findTypoCandidates(token, TYPO_REJECT_VOCAB);
  const bestAffirm = affirmMatches[0] || null;
  const bestReject = rejectMatches[0] || null;

  if (bestAffirm && bestReject && bestAffirm.distance === bestReject.distance) {
    return null;
  }
  if (bestAffirm && (!bestReject || bestAffirm.distance < bestReject.distance)) {
    return bestAffirm.candidate;
  }
  if (bestReject && (!bestAffirm || bestReject.distance < bestAffirm.distance)) {
    return bestReject.candidate;
  }
  return null;
}

function resolveContinuationControlTypo(token) {
  return resolveTypoTokenForVocabulary(token, CONTINUATION_CONTROL_VOCAB, {
    rejectMorphologicalVariants: true,
  });
}

/**
 * Constrained typo normalization for short pending-decision replies only.
 * @param {string} question
 * @returns {string}
 */
function normalizePendingDecisionTypos(question) {
  const normalized = normalizeText(question);
  if (!normalized) return normalized;

  const stripped = stripTrailingPunctuation(normalized.toLowerCase());
  if (!stripped || stripped.length > TYPO_MAX_PHRASE_CHARS) return normalized;

  const words = stripped.split(/\s+/);
  if (words.length > TYPO_MAX_WORDS) return normalized;

  if (words.length === 1) {
    const normalizedToken = resolveTypoToken(words[0]);
    return normalizedToken || normalized;
  }

  return normalized;
}

/**
 * Bounded control-language normalization for conversational mission continuation routing.
 * Does not rewrite long natural-language turns.
 * @param {string} question
 * @returns {string}
 */
function normalizeConversationalControlLanguage(question) {
  const normalized = normalizeText(question);
  if (!normalized) return normalized;

  const stripped = stripTrailingPunctuation(normalized.toLowerCase());
  if (!stripped || stripped.length > TYPO_MAX_PHRASE_CHARS) return normalized;

  const words = stripped.split(/\s+/);
  if (words.length > TYPO_MAX_WORDS) return normalized;

  if (words.length === 1) {
    const fixed = resolveContinuationControlTypo(words[0]);
    return fixed || normalized;
  }

  let changed = false;
  const normalizedWords = words.map((word) => {
    const fixed = resolveContinuationControlTypo(word);
    if (fixed && fixed !== word) {
      changed = true;
      return fixed;
    }
    return word;
  });

  return changed ? normalizedWords.join(' ') : normalized;
}

module.exports = {
  TYPO_AFFIRM_VOCAB,
  TYPO_REJECT_VOCAB,
  CONTINUATION_CONTROL_VOCAB,
  TYPO_MAX_PHRASE_CHARS,
  TYPO_MAX_WORDS,
  normalizeText,
  stripTrailingPunctuation,
  levenshtein,
  maxEditDistanceForToken,
  findTypoCandidates,
  resolveTypoToken,
  resolveContinuationControlTypo,
  normalizePendingDecisionTypos,
  normalizeConversationalControlLanguage,
};
