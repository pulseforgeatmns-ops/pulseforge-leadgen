'use strict';

/**
 * Outcome Intelligence validation (SPEC-036 / ADR-023).
 */

const {
  OUTCOME_TYPE_POLARITY,
  RECOMMENDATION_STATUS,
  LEARNING_STATUS,
  normalizeOutcomeType,
} = require('./types');

/**
 * Validate a single outcome capture input.
 * @param {object} input
 * @returns {{ ok: boolean, errors: string[] }}
 */
function validateOutcomeInput(input) {
  const errors = [];
  if (!input || typeof input !== 'object') {
    return { ok: false, errors: ['outcome_input_required'] };
  }
  if (!input.prospectId && !input.company && !input.companyId) {
    errors.push('prospect_or_company_required');
  }
  const type = normalizeOutcomeType(input.outcomeType || input.type || input.responseStatus);
  if (!OUTCOME_TYPE_POLARITY[type]) {
    errors.push('invalid_outcome_type');
  }
  return { ok: errors.length === 0, errors, outcomeType: type };
}

/**
 * Recommendations may only mutate strategy after approval (ADR-023).
 * @param {object} recommendation
 * @param {string} action - approve | reject | apply
 * @returns {{ ok: boolean, errors: string[] }}
 */
function validateRecommendationAction(recommendation, action) {
  const errors = [];
  if (!recommendation) {
    return { ok: false, errors: ['recommendation_not_found'] };
  }
  const status = recommendation.status;
  if (action === 'approve') {
    if (status !== RECOMMENDATION_STATUS.PENDING) {
      errors.push('recommendation_not_pending');
    }
    if (!recommendation.evidenceBacked) {
      errors.push('recommendation_not_evidence_backed');
    }
  } else if (action === 'reject') {
    if (
      status !== RECOMMENDATION_STATUS.PENDING &&
      status !== RECOMMENDATION_STATUS.APPROVED
    ) {
      errors.push('recommendation_not_rejectable');
    }
  } else if (action === 'apply') {
    if (status !== RECOMMENDATION_STATUS.APPROVED) {
      errors.push('recommendation_not_approved');
    }
  } else {
    errors.push('unknown_recommendation_action');
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Learnings may only be promoted when evidence_backed.
 * @param {object} learning
 * @returns {{ ok: boolean, errors: string[] }}
 */
function validateLearningPromotion(learning) {
  const errors = [];
  if (!learning) return { ok: false, errors: ['learning_not_found'] };
  if (learning.status !== LEARNING_STATUS.EVIDENCE_BACKED) {
    errors.push('learning_not_evidence_backed');
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Fail closed: playbook / ranking / discovery must not update without approval.
 * @param {object} recommendation
 * @returns {{ ok: boolean, errors: string[] }}
 */
function validateStrategyMutation(recommendation) {
  const errors = [];
  if (!recommendation) {
    return { ok: false, errors: ['recommendation_not_found'] };
  }
  if (
    recommendation.status !== RECOMMENDATION_STATUS.APPROVED &&
    recommendation.status !== RECOMMENDATION_STATUS.APPLIED
  ) {
    errors.push('strategy_mutation_requires_approval');
  }
  return { ok: errors.length === 0, errors };
}

module.exports = {
  validateOutcomeInput,
  validateRecommendationAction,
  validateLearningPromotion,
  validateStrategyMutation,
};
