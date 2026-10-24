'use strict';

/**
 * SPEC-102F — competency lifecycle stages.
 */

const STAGES = Object.freeze({
  NOT_STARTED: 'not_started',
  TRAINING: 'training',
  PRACTICING: 'practicing',
  GRADUATED: 'graduated',
  REGRESSION: 'regression',
});

const STAGE_ORDER = Object.freeze([
  STAGES.NOT_STARTED,
  STAGES.TRAINING,
  STAGES.PRACTICING,
  STAGES.GRADUATED,
]);

const STAGE_SYMBOLS = Object.freeze({
  [STAGES.NOT_STARTED]: '○',
  [STAGES.TRAINING]: '△',
  [STAGES.PRACTICING]: '◐',
  [STAGES.GRADUATED]: '✓',
  [STAGES.REGRESSION]: '↺',
});

function isValidStage(stage) {
  return Object.values(STAGES).includes(stage);
}

function canGraduate(stage) {
  return stage === STAGES.PRACTICING || stage === STAGES.TRAINING;
}

function stageRank(stage) {
  const idx = STAGE_ORDER.indexOf(stage);
  return idx === -1 ? -1 : idx;
}

module.exports = {
  STAGES,
  STAGE_ORDER,
  STAGE_SYMBOLS,
  isValidStage,
  canGraduate,
  stageRank,
};
