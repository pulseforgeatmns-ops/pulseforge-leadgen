'use strict';

const { OUTCOMES } = require('./OperatorTypes');

/**
 * Internal trust / usefulness signal from operator outcomes (SPEC-012).
 * Never replaces confidence. Never invents business intelligence.
 *
 * @param {object} learning - RecommendationLearning
 * @returns {{ score: number, label: string, basis: string[] }|null}
 */
function scoreTrust(learning) {
  if (!learning) return null;

  const basis = [];
  let score = 0.5;

  const viewed = Number(learning.viewed) || 0;
  const approved = Number(learning.approved) || 0;
  const dismissed = Number(learning.dismissed) || 0;
  const ignored = Number(learning.ignored) || 0;
  const openedInMax = Number(learning.openedInMax) || 0;
  const depth = Number(learning.investigatedDepth) || 0;
  const outcome = learning.outcome || OUTCOMES.RECOMMENDED;

  if (viewed === 0 && approved === 0 && dismissed === 0 && ignored === 0) {
    return {
      score: 0.5,
      label: 'unobserved',
      basis: ['No operator engagement yet'],
    };
  }

  if (approved > 0 || outcome === OUTCOMES.APPROVED) {
    score += 0.25;
    basis.push('Approved by operator');
  }
  if (outcome === OUTCOMES.EXECUTED) {
    score += 0.1;
    basis.push('Executed');
  }
  if (outcome === OUTCOMES.SUCCESSFUL) {
    score += 0.15;
    basis.push('Marked successful');
  }
  if (dismissed > 0 || outcome === OUTCOMES.DISMISSED) {
    score -= 0.2;
    basis.push('Dismissed');
  }
  if (ignored > 0) {
    score -= 0.1 * Math.min(ignored, 3);
    basis.push(`Ignored ${ignored}×`);
  }
  if (outcome === OUTCOMES.EXPIRED) {
    score -= 0.15;
    basis.push('Aged out / expired');
  }
  if (outcome === OUTCOMES.CONTRADICTED) {
    score -= 0.25;
    basis.push('Contradicting evidence arrived');
  }
  if (openedInMax > 0) {
    score += 0.05;
    basis.push('Opened in Max');
  }
  if (depth >= 2) {
    score += 0.05;
    basis.push(`Investigation depth ${depth}`);
  }
  if (viewed > 0 && approved === 0 && dismissed === 0 && ignored === 0) {
    basis.push(`Viewed ${viewed}× without decision`);
  }

  score = Math.max(0, Math.min(1, score));
  const label =
    score >= 0.75
      ? 'high_usefulness'
      : score >= 0.45
        ? 'moderate_usefulness'
        : 'low_usefulness';

  return { score: round3(score), label, basis };
}

function round3(n) {
  return Math.round(n * 1000) / 1000;
}

module.exports = { scoreTrust };
