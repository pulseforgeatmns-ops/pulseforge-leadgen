'use strict';

/**
 * Canonical LEARN → IMPROVE progression when meaningful mission-bound learning exists.
 * Max owns lifecycle advancement; operators may also call progress() manually.
 */

const { STAGES, SPECIALISTS } = require('./types');
const { canEnter, specialistContext } = require('./Lifecycle');
const { hasMeaningfulLearning } = require('./MeaningfulLearning');

function shouldProgressToImprove(mission, store) {
  if (!mission || mission.stage !== STAGES.LEARN) return false;
  if (!hasMeaningfulLearning(store, mission)) return false;

  const extra = {
    hasMeaningfulLearning: true,
    hasLearning: true,
  };
  const contributions = store.listContributions(mission.id);
  const ctx = specialistContext(contributions, extra);
  const gate = canEnter(STAGES.IMPROVE, { ...ctx, ...extra });
  return gate.ok === true;
}

/**
 * Advance LEARN → IMPROVE through canonical Engine.progress() (Max orchestrator).
 * @param {object} engine
 * @param {string} missionId
 * @param {object} [opts]
 * @returns {{ progressed: boolean, mission?: object, reason?: string }}
 */
function tryProgressToImprove(engine, missionId, opts = {}) {
  const mission = engine.get(missionId, opts.tenantId);
  if (!shouldProgressToImprove(mission, engine.store)) {
    return { progressed: false };
  }
  const updated = engine.progress(missionId, { role: SPECIALISTS.MAX }, {
    stage: STAGES.IMPROVE,
    tenantId: opts.tenantId,
  });
  return { progressed: true, mission: updated };
}

module.exports = {
  shouldProgressToImprove,
  tryProgressToImprove,
};
