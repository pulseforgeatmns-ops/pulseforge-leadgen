'use strict';

/**
 * Canonical OBSERVE → LEARN progression when meaningful business outcomes exist.
 * Max owns lifecycle advancement; operators may also call progress() manually.
 */

const { STAGES, SPECIALISTS } = require('./types');
const { canEnter, specialistContext } = require('./Lifecycle');
const { hasMeaningfulBusinessOutcome } = require('./ObservationInterpretation');

function shouldProgressToLearn(mission, store) {
  if (!mission || mission.stage !== STAGES.OBSERVE) return false;
  const outcomes = store.listOutcomes(mission.id);
  if (!hasMeaningfulBusinessOutcome(outcomes)) return false;
  const extra = {
    hasMeaningfulBusinessOutcome: true,
    hasOutcomes: outcomes.length > 0,
  };
  const contributions = store.listContributions(mission.id);
  const ctx = specialistContext(contributions, extra);
  const gate = canEnter(STAGES.LEARN, { ...ctx, ...extra });
  return gate.ok === true;
}

/**
 * Advance OBSERVE → LEARN through canonical Engine.progress() (Max orchestrator).
 * @param {object} engine
 * @param {string} missionId
 * @param {object} [opts]
 * @returns {{ progressed: boolean, mission?: object, reason?: string }}
 */
function tryProgressToLearn(engine, missionId, opts = {}) {
  const mission = engine.get(missionId, opts.tenantId);
  if (!shouldProgressToLearn(mission, engine.store)) {
    return { progressed: false };
  }
  const updated = engine.progress(missionId, { role: SPECIALISTS.MAX }, {
    stage: STAGES.LEARN,
    tenantId: opts.tenantId,
  });
  return { progressed: true, mission: updated };
}

module.exports = {
  shouldProgressToLearn,
  tryProgressToLearn,
};
