'use strict';

/**
 * Scoring layer — LAST in the COG stack.
 *
 * Rubrics are defined per domain. Automated scoring is optional and evolving.
 * When scoring is not applied, results remain at score: null with review flags.
 */

const { SCORE_RANGE, REVIEW_STATUS } = require('../types');

/**
 * Apply rubric-based scoring to a domain result.
 * Returns null score when human review is required or automated scoring is disabled.
 *
 * @param {import('../types').DomainResult} domainResult
 * @param {import('../types').CognitiveDomain} domain
 * @param {object} [options]
 * @param {boolean} [options.automated=false] - Enable automated scoring (experimental)
 * @returns {{ score: number|null, method: string, reviewStatus: string, notes?: string }}
 */
function scoreDomainResult(domainResult, domain, options = {}) {
  if (!options.automated) {
    return {
      score: null,
      method: 'deferred',
      reviewStatus: domainResult.reviewStatus || REVIEW_STATUS.PENDING,
      notes: 'Automated scoring disabled — apply rubric via human review or enable --score',
    };
  }

  const totalBehaviors = domainResult.behaviorResults.length;
  if (totalBehaviors === 0) {
    return {
      score: null,
      method: 'insufficient_data',
      reviewStatus: REVIEW_STATUS.PENDING,
      notes: 'No behavior results to score',
    };
  }

  const passed = domainResult.behaviorResults.filter(r => r.passed).length;
  const automatedOnly = domainResult.behaviorResults.filter(r => !r.requiresHumanReview);
  const automatedPassed = automatedOnly.filter(r => r.passed).length;

  if (automatedOnly.length === 0) {
    return {
      score: null,
      method: 'human_required',
      reviewStatus: REVIEW_STATUS.PENDING,
      notes: 'All behaviors require human review',
    };
  }

  const ratio = automatedPassed / automatedOnly.length;
  const failurePenalty = Math.min(domainResult.failures.length * 0.5, 3);
  let raw = ratio * SCORE_RANGE.max - failurePenalty;
  raw = Math.max(SCORE_RANGE.min, Math.min(SCORE_RANGE.max, raw));
  const score = Math.round(raw * 10) / 10;

  const needsReview = domainResult.behaviorResults.some(r => r.requiresHumanReview && r.passed);

  return {
    score,
    method: 'automated_v0',
    reviewStatus: needsReview ? REVIEW_STATUS.PENDING : REVIEW_STATUS.NOT_REQUIRED,
    notes: `${automatedPassed}/${automatedOnly.length} automated behaviors passed; ${domainResult.failures.length} failures classified`,
  };
}

/**
 * Compute overall score from domain scores (ignores nulls).
 * @param {import('../types').DomainResult[]} domainResults
 * @returns {number|null}
 */
function computeOverallScore(domainResults) {
  const scored = domainResults.filter(d => d.score !== null && d.score !== undefined);
  if (scored.length === 0) return null;
  const sum = scored.reduce((acc, d) => acc + d.score, 0);
  return Math.round((sum / scored.length) * 10) / 10;
}

/**
 * Apply human review score override.
 * @param {import('../types').DomainResult} domainResult
 * @param {number} score
 * @param {string} [reviewerNotes]
 */
function applyHumanScore(domainResult, score, reviewerNotes) {
  if (score < SCORE_RANGE.min || score > SCORE_RANGE.max) {
    throw new Error(`Score must be between ${SCORE_RANGE.min} and ${SCORE_RANGE.max}`);
  }
  return {
    ...domainResult,
    score,
    reviewStatus: REVIEW_STATUS.APPROVED,
    metadata: {
      ...(domainResult.metadata || {}),
      humanReview: {
        score,
        notes: reviewerNotes || null,
        reviewedAt: new Date().toISOString(),
      },
    },
  };
}

function getRubric(domain) {
  return domain.rubric ? { ...domain.rubric } : null;
}

module.exports = {
  scoreDomainResult,
  computeOverallScore,
  applyHumanScore,
  getRubric,
};
