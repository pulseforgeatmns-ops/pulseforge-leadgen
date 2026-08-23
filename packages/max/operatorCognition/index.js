'use strict';

const {
  THINKING_MODES,
  THINKING_MODE_CATEGORY,
  MUTATING_MODES,
  thinkingModeCategory,
  modeMutatesMission,
} = require('./ThinkingModes');
const {
  classifyOperatorCognition,
  attachSpecialists,
} = require('./OperatorCognition');
const {
  mayMutateMission,
  isReadOnlyCognition,
  assertReadOnlyCognition,
} = require('./ExecutionGuard');
const {
  MODE_SPECIALISTS,
  selectSpecialists,
  primarySpecialist,
} = require('./SpecialistParticipation');

module.exports = {
  THINKING_MODES,
  THINKING_MODE_CATEGORY,
  MUTATING_MODES,
  thinkingModeCategory,
  modeMutatesMission,
  classifyOperatorCognition,
  attachSpecialists,
  mayMutateMission,
  isReadOnlyCognition,
  assertReadOnlyCognition,
  MODE_SPECIALISTS,
  selectSpecialists,
  primarySpecialist,
};
