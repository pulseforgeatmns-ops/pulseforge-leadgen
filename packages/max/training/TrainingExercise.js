'use strict';

/**
 * SPEC-102F — training exercise schema.
 */

const { isValidStage } = require('./CompetencyLifecycle');

const REQUIRED_FIELDS = Object.freeze([
  'assignment',
  'observedBehavior',
  'expectedBehavior',
  'failureMode',
  'generalLesson',
  'retest',
  'transferTest',
  'graduationDecision',
]);

function validateExercise(exercise, { competencyId } = {}) {
  const errors = [];
  if (!exercise || typeof exercise !== 'object') {
    return { valid: false, errors: ['exercise must be an object'] };
  }
  if (!exercise.id) errors.push('exercise.id is required');
  for (const field of REQUIRED_FIELDS) {
    if (!exercise[field] || typeof exercise[field] !== 'string') {
      errors.push(`exercise.${field} is required`);
    }
  }
  if (exercise.graduationDecision && !['graduated', 'training', 'failed', 'practicing'].includes(exercise.graduationDecision)) {
    errors.push(`exercise.graduationDecision invalid for ${competencyId || exercise.id}`);
  }
  return { valid: errors.length === 0, errors };
}

function validateCompetency(competency) {
  const errors = [];
  if (!competency?.id) errors.push('competency.id is required');
  if (!competency?.label) errors.push('competency.label is required');
  if (!isValidStage(competency?.stage)) errors.push(`competency.stage invalid: ${competency?.stage}`);
  for (const exercise of competency?.exercises || []) {
    const result = validateExercise(exercise, { competencyId: competency.id });
    errors.push(...result.errors);
  }
  return { valid: errors.length === 0, errors };
}

module.exports = {
  REQUIRED_FIELDS,
  validateExercise,
  validateCompetency,
};
