'use strict';

/**
 * Human Test + Operator Confidence Score (SPEC-048).
 * Advisory only — never replaces operator approval.
 */

const { buildOperatorConfidence, CONFIDENCE_LABEL } = require('./types');

/**
 * Evaluate a draft (and optional profile) against Human Test dimensions.
 * @param {object} args
 * @param {object} args.profile
 * @param {string} [args.letterBody]
 * @returns {object} OperatorConfidence
 */
function evaluateHumanTest(args = {}) {
  const profile = args.profile || {};
  const body = String(args.letterBody || '');
  const notes = [];

  let industryAccuracy = 6;
  if (profile.industry) industryAccuracy = 8;
  if (profile.confidence === CONFIDENCE_LABEL.HIGH) industryAccuracy = 10;
  if (!profile.industry) {
    industryAccuracy = 2;
    notes.push('Missing industry');
  }
  if (
    body &&
    profile.industry &&
    !body.toLowerCase().includes(String(profile.industry).toLowerCase()) &&
    !(
      profile.company &&
      body.toLowerCase().includes(String(profile.company).toLowerCase())
    )
  ) {
    industryAccuracy = Math.min(industryAccuracy, 5);
    notes.push('Letter does not reflect industry/company');
  }

  let buyerRelevance = 6;
  if (profile.decision_maker) buyerRelevance = 8;
  if (profile.decision_maker_confidence === CONFIDENCE_LABEL.HIGH) {
    buyerRelevance = 9;
  }
  if (profile.decision_maker_confidence === CONFIDENCE_LABEL.LOW) {
    buyerRelevance = 5;
    notes.push('Low buyer confidence');
  }

  let evidenceUse = 5;
  const verified = (profile.personalization_claims || []).filter((c) => c.verified);
  evidenceUse = Math.min(10, 4 + verified.length * 2);
  if ((profile.buying_signals || []).length) {
    evidenceUse = Math.min(10, evidenceUse + 1);
  }

  let specificity = evidenceUse;
  if (verified.length === 0) {
    specificity = 3;
    notes.push('No verified personalization');
  }

  let naturalness = 8;
  if (
    /\bdelve\b|\bleverage\bsynerg|\bcutting[- ]edge\b|\bgame[- ]changer\b/i.test(
      body
    )
  ) {
    naturalness = 3;
    notes.push('AI cliché detected');
  }
  if (/^we\s+(provide|offer|specialize)\b/im.test(body)) {
    naturalness = Math.min(naturalness, 5);
    notes.push('Service-first opening');
  }

  let salesJudgment = 7;
  if (profile.recommended_angle && profile.call_to_action) salesJudgment = 9;
  if (profile.sendable === false) salesJudgment = Math.min(salesJudgment, 4);

  const firstPara = body
    .split(/\n\s*\n/)
    .map((p) => p.trim())
    .find((p) => p && !/^dear\b/i.test(p));
  if (firstPara && /^we\s+(provide|offer|specialize|at\s+)/i.test(firstPara)) {
    salesJudgment = Math.min(salesJudgment, 4);
    naturalness = Math.min(naturalness, 4);
    notes.push('Fails Prospect First');
  }

  const dims = {
    industryAccuracy,
    buyerRelevance,
    evidenceUse,
    specificity,
    naturalness,
    salesJudgment,
  };
  const avg =
    Object.values(dims).reduce((a, b) => a + b, 0) / Object.values(dims).length;
  const overall = Math.round(avg * 10);
  const editInstinct = overall < 75 || notes.includes('Fails Prospect First');

  return buildOperatorConfidence({
    ...dims,
    overall,
    passed: overall >= 70 && !editInstinct,
    editInstinct,
    notes,
  });
}

/**
 * Attach Human Test score to a profile (advisory).
 * @param {object} profile
 * @param {string} [letterBody]
 * @returns {object}
 */
function attachOperatorConfidence(profile, letterBody) {
  const operatorConfidence = evaluateHumanTest({
    profile,
    letterBody: letterBody || '',
  });
  return {
    ...profile,
    operatorConfidence,
  };
}

module.exports = {
  evaluateHumanTest,
  attachOperatorConfidence,
};
