'use strict';

const { classifyFailure } = require('../failures/taxonomy');
const { getMaxResponseForOperatorTurn } = require('../conversations/TranscriptCapture');

/**
 * Deterministic behavior checks against captured transcripts.
 * Scoring is separate — this layer classifies pass/fail on observable behaviors.
 */

function compilePatterns(pattern) {
  if (!pattern) return [];
  const list = Array.isArray(pattern) ? pattern : [pattern];
  return list.map(p => (p instanceof RegExp ? p : new RegExp(p, 'i')));
}

function matchesAny(text, patterns) {
  if (!patterns.length) return true;
  return patterns.some(re => re.test(text));
}

function matchesNone(text, patterns) {
  if (!patterns.length) return true;
  return !patterns.some(re => re.test(text));
}

/**
 * @param {import('../types').ExpectedBehavior} behavior
 * @param {import('../types').ConversationTurn[]} transcript
 * @returns {{ behaviorId: string, passed: boolean, evidence?: string, requiresHumanReview?: boolean }}
 */
function checkBehavior(behavior, transcript) {
  const turnIndex = behavior.turnIndex ?? null;
  let text = '';

  if (turnIndex !== null && turnIndex !== undefined) {
    const maxTurn = getMaxResponseForOperatorTurn(transcript, turnIndex);
    text = maxTurn?.content || '';
  } else {
    text = transcript
      .filter(t => t.role === 'max')
      .map(t => t.content)
      .join('\n');
  }

  const requiresHumanReview = Boolean(behavior.requiresHumanReview);
  let passed = false;
  let evidence = '';

  switch (behavior.checkType) {
    case 'pattern':
    case 'continuity':
    case 'identity':
    case 'confidence':
    case 'counterfactual':
    case 'revision':
    case 'abstraction':
    case 'proposition':
    case 'graph':
      passed = matchesAny(text, compilePatterns(behavior.pattern));
      evidence = passed ? 'Pattern matched' : `Pattern not found in: ${truncate(text, 200)}`;
      break;
    case 'absence':
      passed = matchesNone(text, compilePatterns(behavior.absencePattern || behavior.pattern));
      evidence = passed ? 'Forbidden pattern absent' : `Forbidden pattern found in: ${truncate(text, 200)}`;
      break;
    default:
      passed = false;
      evidence = `Unknown checkType: ${behavior.checkType}`;
  }

  if (requiresHumanReview && passed) {
    evidence = `${evidence} (automated pass — human review recommended)`;
  }

  return {
    behaviorId: behavior.id,
    passed,
    evidence,
    requiresHumanReview,
  };
}

/**
 * @param {import('../types').ExpectedBehavior[]} behaviors
 * @param {import('../types').ConversationTurn[]} transcript
 */
function checkAllBehaviors(behaviors, transcript) {
  return behaviors.map(b => checkBehavior(b, transcript));
}

/**
 * Map failed behaviors to failure taxonomy codes.
 * @param {import('../types').ExpectedBehavior[]} behaviors
 * @param {Array<{behaviorId: string, passed: boolean, evidence?: string, requiresHumanReview?: boolean}>} results
 * @param {import('../types').ConversationTurn[]} transcript
 * @returns {import('../types').FailureClassification[]}
 */
function classifyFailuresFromBehaviors(behaviors, results, transcript) {
  const behaviorMap = new Map(behaviors.map(b => [b.id, b]));
  const failures = [];

  for (const result of results) {
    if (result.passed) continue;
    const behavior = behaviorMap.get(result.behaviorId);
    const code = behavior?.failureCode || 'R-000';
    failures.push(classifyFailure(code, {
      behaviorId: result.behaviorId,
      evidence: result.evidence,
      turnIndex: behavior?.turnIndex,
      requiresHumanReview: result.requiresHumanReview ?? behavior?.requiresHumanReview ?? false,
      description: behavior?.description,
    }));
  }

  failures.push(...detectCrossTurnFailures(transcript));

  return dedupeFailures(failures);
}

/**
 * Heuristic cross-turn failure detection (conversation reset, proposition drift).
 */
function detectCrossTurnFailures(transcript) {
  const failures = [];
  const maxTurns = transcript.filter(t => t.role === 'max');

  if (maxTurns.length >= 2) {
    const first = maxTurns[0].content.toLowerCase();
    const last = maxTurns[maxTurns.length - 1].content.toLowerCase();

    const resetPhrases = [
      /how can i help/i,
      /what would you like/i,
      /i don't have context/i,
      /starting fresh/i,
    ];
    if (maxTurns.length >= 3 && resetPhrases.some(re => re.test(last)) && !resetPhrases.some(re => re.test(first))) {
      failures.push(classifyFailure('R-005', {
        evidence: 'Late-turn generic reset phrasing detected',
        turnIndex: maxTurns.length - 1,
        requiresHumanReview: true,
      }));
    }
  }

  return failures;
}

function dedupeFailures(failures) {
  const seen = new Set();
  return failures.filter(f => {
    const key = `${f.code}:${f.behaviorId || ''}:${f.turnIndex ?? ''}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function truncate(text, max) {
  const s = String(text || '');
  return s.length <= max ? s : `${s.slice(0, max)}…`;
}

module.exports = {
  checkBehavior,
  checkAllBehaviors,
  classifyFailuresFromBehaviors,
  compilePatterns,
  matchesAny,
  matchesNone,
};
